use crate::aggregator::Aggregator;
use crate::config::{Committee, EpochNumber};
use crate::consensus::{ConsensusMessage, Round, CoreStartMode};
use crate::error::{ConsensusError, ConsensusResult};
use crate::leader::LeaderElector;
use crate::mempool::MempoolDriver;
use crate::messages::{Block, Timeout, Vote, QC, TC};
use crate::proposer::ProposerMessage;
use crate::synchronizer::Synchronizer;
use crate::timer::Timer;
use crate::helper::HelperRequest;
use async_recursion::async_recursion;
use bytes::Bytes;
use crypto::Hash as _;
use crypto::{PublicKey, SignatureService};
use log::{debug, error, info, warn};
use network::SimpleSender;
use std::cmp::max;
use std::collections::VecDeque;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(feature = "fast-sync")]
use tokio::time::{sleep, Duration};

#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

const ROUNDS_PER_EPOCH: Round = 5000; // 5k rounds/epoch gives us around 7 epochs per 20s bench run (assuming ~950 tx/s e2e, at 1k tx/s input)

pub struct Core {
    name: PublicKey,
    committee: Committee,
    store: Store,
    signature_service: SignatureService,
    leader_elector: LeaderElector,
    mempool_driver: MempoolDriver,
    synchronizer: Synchronizer,
    rx_message: Receiver<ConsensusMessage>,
    rx_loopback: Receiver<Block>,
    tx_proposer: Sender<ProposerMessage>,
    tx_commit: Sender<Block>,
    round: Round,
    last_voted_round: Round,
    last_committed_round: Round,
    current_epoch: EpochNumber,
    high_qc: QC,
    timer: Timer,
    aggregator: Aggregator,
    network: SimpleSender,
    tx_helper: Sender<HelperRequest>,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        store: Store,
        leader_elector: LeaderElector,
        mempool_driver: MempoolDriver,
        synchronizer: Synchronizer,
        timeout_delay: u64,
        rx_message: Receiver<ConsensusMessage>,
        rx_loopback: Receiver<Block>,
        tx_proposer: Sender<ProposerMessage>,
        tx_commit: Sender<Block>,
        start_mode: CoreStartMode,
        tx_helper: Sender<HelperRequest>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee: committee.clone(),
                signature_service,
                store,
                leader_elector,
                mempool_driver,
                synchronizer,
                rx_message,
                rx_loopback,
                tx_proposer,
                tx_commit,
                tx_helper,
                round: 1,
                last_voted_round: 0,
                last_committed_round: 0,
                current_epoch: 1,
                high_qc: QC::genesis(),
                timer: Timer::new(timeout_delay),
                aggregator: Aggregator::new(committee),
                network: SimpleSender::new(),
            }
            .run(start_mode)
            .await
        });
    }

    async fn store_block(&mut self, block: &Block) {
        let key = block.digest().to_vec();
        let value = bincode::serialize(block).expect("Failed to serialize block");
        self.store.write(key, value).await;
    }

    fn increase_last_voted_round(&mut self, target: Round) {
        self.last_voted_round = max(self.last_voted_round, target);
    }

    async fn make_vote(&mut self, block: &Block) -> Option<Vote> {
        // Check if we can vote for this block.
        let safety_rule_1 = block.round > self.last_voted_round;
        let mut safety_rule_2 = block.qc.round + 1 == block.round;
        if let Some(ref tc) = block.tc {
            let mut can_extend = tc.round + 1 == block.round;
            can_extend &= block.qc.round >= *tc.high_qc_rounds().iter().max().expect("Empty TC");
            safety_rule_2 |= can_extend;
        }
        if !(safety_rule_1 && safety_rule_2) {
            return None;
        }

        // Ensure we won't vote for contradicting blocks.
        self.increase_last_voted_round(block.round);
        // TODO [issue #15]: Write to storage preferred_round and last_voted_round.
        Some(Vote::new(block, self.name, self.signature_service.clone()).await)
    }

    async fn commit(&mut self, block: Block) -> ConsensusResult<()> {
        if self.last_committed_round >= block.round {
            return Ok(());
        }

        // Ensure we commit the entire chain. This is needed after view-change.
        let mut to_commit = VecDeque::new();
        let mut parent = block.clone();
        while self.last_committed_round + 1 < parent.round {
            let ancestor = self
                .synchronizer
                .get_parent_block(&parent)
                .await?
                .expect("We should have all the ancestors by now");
            to_commit.push_front(ancestor.clone());
            parent = ancestor;
        }
        to_commit.push_front(block.clone());

        // Save the last committed block.
        self.last_committed_round = block.round;

        // Send all the newly committed blocks to the node's application layer.
        while let Some(block) = to_commit.pop_back() {
            if !block.payload.is_empty() {
                info!("Committed {}", block);

                #[cfg(feature = "benchmark")]
                for x in &block.payload {
                    // NOTE: This log entry is used to compute performance.
                    info!("Committed {} -> {:?}", block, x);
                }
            }
            debug!("Committed {:?}", block);
            if let Err(e) = self.tx_commit.send(block).await {
                warn!("Failed to send block through the commit channel: {}", e);
            }
        }
        Ok(())
    }

    fn update_high_qc(&mut self, qc: &QC) {
        if qc.round > self.high_qc.round {
            self.high_qc = qc.clone();
        }
    }

    async fn local_timeout_round(&mut self) -> ConsensusResult<()> {
        warn!("Timeout reached for round {}", self.round);

        // Increase the last voted round.
        self.increase_last_voted_round(self.round);

        // Make a timeout message.
        let timeout = Timeout::new(
            self.high_qc.clone(),
            self.round,
            self.name,
            self.signature_service.clone(),
        )
        .await;
        debug!("Created {:?}", timeout);

        // Reset the timer.
        self.timer.reset();

        // Broadcast the timeout message.
        debug!("Broadcasting {:?}", timeout);
        let addresses = self
            .committee
            .broadcast_addresses(&self.name)
            .into_iter()
            .map(|(_, x)| x)
            .collect();
        let message = bincode::serialize(&ConsensusMessage::Timeout(timeout.clone()))
            .expect("Failed to serialize timeout message");
        self.network
            .broadcast(addresses, Bytes::from(message))
            .await;

        // Process our message.
        self.handle_timeout(&timeout).await
    }

    #[async_recursion]
    async fn handle_vote(&mut self, vote: &Vote) -> ConsensusResult<()> {
        debug!("Processing {:?}", vote);
        if vote.round < self.round {
            return Ok(());
        }

        // Ensure the vote is well formed.
        vote.verify(&self.committee)?;

        // Add the new vote to our aggregator and see if we have a quorum.
        if let Some(qc) = self.aggregator.add_vote(vote.clone())? {
            debug!("Assembled {:?}", qc);

            // Process the QC.
            self.process_qc(&qc).await;

            // Make a new block if we are the next leader.
            if self.name == self.leader_elector.get_leader(self.round) {
                self.generate_proposal(None).await;
            }
        }
        Ok(())
    }

    async fn handle_timeout(&mut self, timeout: &Timeout) -> ConsensusResult<()> {
        debug!("Processing {:?}", timeout);
        if timeout.round < self.round {
            return Ok(());
        }

        // Ensure the timeout is well formed.
        timeout.verify(&self.committee)?;

        // Process the QC embedded in the timeout.
        self.process_qc(&timeout.high_qc).await;

        // Add the new vote to our aggregator and see if we have a quorum.
        if let Some(tc) = self.aggregator.add_timeout(timeout.clone())? {
            debug!("Assembled {:?}", tc);

            // Try to advance the round.
            self.advance_round(tc.round).await;

            // Broadcast the TC.
            debug!("Broadcasting {:?}", tc);
            let addresses = self
                .committee
                .broadcast_addresses(&self.name)
                .into_iter()
                .map(|(_, x)| x)
                .collect();
            let message = bincode::serialize(&ConsensusMessage::TC(tc.clone()))
                .expect("Failed to serialize timeout certificate");
            self.network
                .broadcast(addresses, Bytes::from(message))
                .await;

            // Make a new block if we are the next leader.
            if self.name == self.leader_elector.get_leader(self.round) {
                self.generate_proposal(Some(tc)).await;
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn advance_round(&mut self, round: Round) {
        if round < self.round {
            return;
        }

        // Check if the round we are *completing* is the last of an epoch.
        // Example: If ROUNDS_PER_EPOCH is 10:
        // - Completing round 9 means next is round 10 (epoch 2). Change needed.
        // - Completing round 19 means next is round 20 (epoch 3). Change needed.
        // This assumes epochs E=1, 2, ... contain rounds (E-1)*N to E*N - 1.
        // And round numbers start at 1 (genesis block is round 0, QC is round 0, first proposal is round 1).
        // If round is the number of the block/QC triggering the advance:
        // We advance *to* round + 1. Check if round + 1 starts a new epoch.
        let next_round = round + 1;
        if next_round > 0 && next_round % ROUNDS_PER_EPOCH == 0 { // Check if the *next* round is a multiple
            // --- Epoch Change Triggered ---

            // 1. Get the *new* epoch number
            let new_epoch_number = (next_round / ROUNDS_PER_EPOCH) + 1; // Calculate the epoch we are entering

            // 2. Update Core's current epoch tracker
            self.current_epoch = new_epoch_number as EpochNumber;

            // 3. Log the change
            info!(
                "EPOCH CHANGE: Preparing for round {}, entering Epoch {}",
                next_round,
                self.current_epoch
            );
        }

        // Reset the timer and advance round.
        self.timer.reset();
        self.round = round + 1;
        debug!("Moved to round {}", self.round);

        // Cleanup the vote aggregator.
        self.aggregator.cleanup(&self.round);
    }

    #[async_recursion]
    async fn generate_proposal(&mut self, tc: Option<TC>) {
        self.tx_proposer
            .send(ProposerMessage::Make(self.round, self.current_epoch, self.high_qc.clone(), tc))
            .await
            .expect("Failed to send message to proposer");
    }

    async fn cleanup_proposer(&mut self, b0: &Block, b1: &Block, block: &Block) {
        let digests = b0
            .payload
            .iter()
            .cloned()
            .chain(b1.payload.iter().cloned())
            .chain(block.payload.iter().cloned())
            .collect();
        self.tx_proposer
            .send(ProposerMessage::Cleanup(digests))
            .await
            .expect("Failed to send message to proposer");
    }

    async fn process_qc(&mut self, qc: &QC) {
        // Check if processing this QC completes an epoch
        let next_round = qc.round + 1; // The round number we would enter *after* this block

        // Ensure we don't checkpoint for Genesis (qc.round 0)
        // Check if qc.round is the last round of an epoch
        if qc.round > 0 && next_round % ROUNDS_PER_EPOCH == 0 {
            let digest = qc.hash.clone(); // The digest of the block certified by this QC (last block of epoch)

            // This block is the last one of the epoch ending at qc.round.
            // Send an update to the Helper task to mark this block's digest as the new checkpoint.
            info!(
                "Core: Epoch {} ending. Marking block {} (Round {}) as the new checkpoint.",
                self.current_epoch, // Log epoch number *before* advance_round might update it
                digest,
                qc.round
            );
            if let Err(e) = self.tx_helper.send(HelperRequest::UpdateCheckpoint(digest)).await {
                // Log error but don't necessarily stop consensus progress
                error!("Core: Failed to send checkpoint update to Helper: {}", e);
            }
        }
        self.advance_round(qc.round).await;
        self.update_high_qc(qc);
    }

    #[async_recursion]
    async fn process_block(&mut self, block: &Block) -> ConsensusResult<()> {
        debug!("Processing {:?}", block);

        // Let's see if we have the last three ancestors of the block, that is:
        //      b0 <- |qc0; b1| <- |qc1; block|
        // If we don't, the synchronizer asks for them to other nodes. It will
        // then ensure we process both ancestors in the correct order, and
        // finally make us resume processing this block.
        let (b0, b1) = match self.synchronizer.get_ancestors(block).await? {
            Some(ancestors) => ancestors,
            None => {
                debug!("Processing of {} suspended: missing parent", block.digest());
                return Ok(());
            }
        };

        // Store the block only if we have already processed all its ancestors.
        self.store_block(block).await;

        self.cleanup_proposer(&b0, &b1, block).await;

        // Check if we can commit the head of the 2-chain.
        // Note that we commit blocks only if we have all its ancestors.
        if b0.round + 1 == b1.round {
            self.mempool_driver.cleanup(b0.round).await;
            self.commit(b0).await?;
        }

        // Ensure the block's round is as expected.
        // This check is important: it prevents bad leaders from producing blocks
        // far in the future that may cause overflow on the round number.
        if block.round != self.round {
            return Ok(());
        }

        // See if we can vote for this block.
        if let Some(vote) = self.make_vote(block).await {
            debug!("Created {:?}", vote);
            let next_leader = self.leader_elector.get_leader(self.round + 1);
            if next_leader == self.name {
                self.handle_vote(&vote).await?;
            } else {
                debug!("Sending {:?} to {}", vote, next_leader);
                let address = self
                    .committee
                    .address(&next_leader)
                    .expect("The next leader is not in the committee");
                let message = bincode::serialize(&ConsensusMessage::Vote(vote))
                    .expect("Failed to serialize vote");
                self.network.send(address, Bytes::from(message)).await;
            }
        }
        Ok(())
    }

    async fn handle_proposal(&mut self, block: &Block) -> ConsensusResult<()> {
        let digest = block.digest();

        // Ensure the block proposer is the right leader for the round.
        ensure!(
            block.author == self.leader_elector.get_leader(block.round),
            ConsensusError::WrongLeader {
                digest,
                leader: block.author,
                round: block.round
            }
        );

        // Check the block is correctly formed.
        block.verify(&self.committee)?;

        // Process the QC. This may allow us to advance round.
        self.process_qc(&block.qc).await;

        // Process the TC (if any). This may also allow us to advance round.
        if let Some(ref tc) = block.tc {
            self.advance_round(tc.round).await;
        }

        // Let's see if we have the block's data. If we don't, the mempool
        // will get it and then make us resume processing this block.
        if !self.mempool_driver.verify(block.clone()).await? {
            debug!("Processing of {} suspended: missing payload", digest);
            return Ok(());
        }

        // All check pass, we can process this block.
        self.process_block(block).await
    }

    async fn handle_tc(&mut self, tc: TC) -> ConsensusResult<()> {
        tc.verify(&self.committee)?;
        if tc.round < self.round {
            return Ok(());
        }
        self.advance_round(tc.round).await;
        if self.name == self.leader_elector.get_leader(self.round) {
            self.generate_proposal(Some(tc)).await;
        }
        Ok(())
    }

    #[cfg_attr(not(feature = "fast-sync"), allow(unused_variables))]
    pub async fn run(&mut self, start_mode: CoreStartMode) {
        // Log the received mode IMMEDIATELY for debugging
        info!("Core::run entered with start_mode: {:?}", start_mode);
        // --- Fast Sync Initialization Phase ---
        // This block only runs if the 'fast-sync' feature is enabled AND
        // the start_mode requests it.
        #[cfg(feature = "fast-sync")]
        if matches!(start_mode, CoreStartMode::FastSync) {
            info!("Fast Sync: Core entering sync mode...");
            const FAST_SYNC_TIMEOUT_MS: u64 = 10_000; // 10 second timeout

            // 1. Request Checkpoint from a peer
            // Simplification: Ask the first peer we know (excluding ourselves)
            let peers = self.committee.broadcast_addresses(&self.name);
            let mut checkpoint_fetched = false;
            if let Some((_peer_name, peer_addr)) = peers.first() {
                info!("Fast Sync: Requesting checkpoint from {}", peer_addr);
                let request = ConsensusMessage::SyncCheckpointRequest(self.name);
                match bincode::serialize(&request) {
                    Ok(serialized_request) => {
                        self.network.send(*peer_addr, Bytes::from(serialized_request)).await;
                    }
                    Err(e) => {
                        error!("Fast Sync: Failed to serialize checkpoint request: {}", e);
                        // Proceed to normal startup without checkpoint
                    }
                }

                // 2. Wait for Response or Timeout
                let deadline = sleep(Duration::from_millis(FAST_SYNC_TIMEOUT_MS));
                tokio::pin!(deadline);

                loop {
                    tokio::select! {
                        biased;
                        // Check for incoming messages
                        maybe_message = self.rx_message.recv() => {
                            match maybe_message {
                                Some(ConsensusMessage::SyncCheckpointResponse(Some(checkpoint_block))) => {
                                    info!("Fast Sync: Received checkpoint block {}", checkpoint_block);

                                    // --- PoC Verification ---
                                    // WARNING: Very happy-path approach.
                                    match checkpoint_block.verify(&self.committee) {
                                        Ok(()) => {
                                            info!("Fast Sync: Checkpoint verified (basic). Initializing state.");
                                            // TODO: Ideally, fetch parent block too to ensure commit rules work
                                            // For PoC, we might skip this, but log a warning. We also might have
                                            // some specific finality logic but deffo out of scope.
                                            warn!("Fast Sync PoC: Parent block of checkpoint not fetched/verified.");

                                            // Initialize Core state based on the checkpoint
                                            self.current_epoch = checkpoint_block.epoch;
                                            self.round = checkpoint_block.round + 1; // Start round *after* checkpoint
                                            self.high_qc = checkpoint_block.qc.clone();
                                            self.last_voted_round = checkpoint_block.round;
                                            // Simplification: Set commit round based on QC. Not fully correct finality.
                                            self.last_committed_round = self.high_qc.round;

                                            // Store the checkpoint block
                                            self.store_block(&checkpoint_block).await;
                                            checkpoint_fetched = true; // Mark success
                                            break; // Exit the response waiting loop
                                        }
                                        Err(e) => {
                                            warn!("Fast Sync: Received invalid checkpoint block: {}", e);
                                            // Continue waiting for potentially other responses or timeout
                                        }
                                    }
                                }
                                Some(ConsensusMessage::SyncCheckpointResponse(None)) => {
                                    info!("Fast Sync: Peer responded with no checkpoint block.");
                                    // Continue waiting
                                }
                                Some(other_message) => {
                                    // It's possible other messages arrive while waiting. Just log.
                                    warn!("Fast Sync: Ignoring non-checkpoint message {:?} during sync phase", other_message);
                                }
                                None => { // Channel closed
                                     warn!("Fast Sync: Message channel closed unexpectedly. Starting from Genesis.");
                                     break; // Exit the response waiting loop
                                }
                            }
                        },

                        // Handle timeout
                        () = &mut deadline => {
                            warn!("Fast Sync: Timeout waiting for checkpoint response. Starting from Genesis.");
                            break; // Exit the response waiting loop
                        },
                    }
                } // End of response waiting loop

            } else {
                warn!("Fast Sync: No peers found to request checkpoint from. Starting from Genesis.");
            }

            // If sync failed, core state remains at Genesis defaults. If succeeded, it's updated.
             if checkpoint_fetched {
                 info!("Fast Sync: Successfully initialized state from checkpoint.");
             } else {
                 info!("Fast Sync: Failed to obtain valid checkpoint, proceeding with Genesis state.");
             }

        } // End of #[cfg(feature = "fast-sync")] block


        // --- Normal Startup / Post-Sync ---
        // Initialize timer and potentially generate first block based on the current state
        // (which might be Genesis or from fast sync)
        self.timer.reset();
        info!("Core starting normal operation at Epoch {}, Round {}", self.current_epoch, self.round);
        if self.name == self.leader_elector.get_leader(self.round) {
            // generate_proposal uses self.round, self.current_epoch, self.high_qc which are now set correctly.
            self.generate_proposal(None).await;
        }

        // --- Main Loop ---
        loop {
            let result = tokio::select! {
                Some(message) = self.rx_message.recv() => {
                    // Add guards to ignore sync messages during normal operation
                    match message {
                        ConsensusMessage::SyncCheckpointRequest(_) | ConsensusMessage::SyncCheckpointResponse(_) => {
                            warn!("Ignoring sync checkpoint message during normal operation.");
                            Ok(()) // Return Ok to avoid outer error handling
                        }
                        ConsensusMessage::Propose(block) => self.handle_proposal(&block).await,
                        ConsensusMessage::Vote(vote) => self.handle_vote(&vote).await,
                        ConsensusMessage::Timeout(timeout) => self.handle_timeout(&timeout).await,
                        ConsensusMessage::TC(tc) => self.handle_tc(tc).await,
                        ConsensusMessage::SyncRequest(_,_) => {
                           warn!("Ignoring SyncRequest received in Core loop.");
                           Ok(())
                        }
                    }
                },
                Some(block) = self.rx_loopback.recv() => self.process_block(&block).await,
                () = &mut self.timer => self.local_timeout_round().await,
            };
            match result {
                Ok(()) => (),
                Err(ConsensusError::StoreError(e)) => error!("{}", e),
                Err(ConsensusError::SerializationError(e)) => error!("Store corrupted. {}", e),
                Err(e) => warn!("{}", e),
            }
        } // End of main loop
    } // End of run function
}
