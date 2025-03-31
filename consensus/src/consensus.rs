use crate::config::{Committee, Parameters};
use crate::core::Core;
use crate::error::ConsensusError;
use crate::helper::{Helper, HelperRequest};
use crate::leader::LeaderElector;
use crate::mempool::MempoolDriver;
use crate::messages::{Block, Timeout, Vote, TC};
use crate::proposer::Proposer;
use crate::synchronizer::Synchronizer;
use async_trait::async_trait;
use bytes::Bytes;
use crypto::{Digest, PublicKey, SignatureService};
use futures::SinkExt as _;
use log::info;
use mempool::ConsensusMempoolMessage;
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use serde::{Deserialize, Serialize};
use std::error::Error;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

/// The default channel capacity for each channel of the consensus.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The consensus round number.
pub type Round = u64;

#[derive(Serialize, Deserialize, Debug)]
pub enum ConsensusMessage {
    Propose(Block),
    Vote(Vote),
    Timeout(Timeout),
    TC(TC),
    SyncRequest(Digest, PublicKey),
    SyncCheckpointRequest(PublicKey), // Request latest checkpoint info from sender
    SyncCheckpointResponse(Option<Block>), // Respond with the first block of the latest finalized epoch known
}

#[derive(Debug, Clone, Copy)]
pub enum CoreStartMode {
    Genesis,
    FastSync,
}

pub struct Consensus;

impl Consensus {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        signature_service: SignatureService,
        store: Store,
        rx_mempool: Receiver<Digest>,
        tx_mempool: Sender<ConsensusMempoolMessage>,
        tx_commit: Sender<Block>,
        start_mode: CoreStartMode,
    ) {
        // NOTE: This log entry is used to compute performance.
        parameters.log();

        let (tx_consensus, rx_consensus) = channel(CHANNEL_CAPACITY);
        let (tx_loopback, rx_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_proposer, rx_proposer) = channel(CHANNEL_CAPACITY);
        let (tx_helper, rx_helper): (Sender<HelperRequest>, Receiver<HelperRequest>) = channel(CHANNEL_CAPACITY);

        // Spawn the network receiver.
        let mut address = committee
            .address(&name)
            .expect("Our public key is not in the committee");
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */
            ConsensusReceiverHandler {
                tx_consensus,
                tx_helper: tx_helper.clone(),
            },
        );
        info!(
            "Node {} listening to consensus messages on {}",
            name, address
        );

        // Make the leader election module.
        let leader_elector = LeaderElector::new(committee.clone());

        // Make the mempool driver.
        let mempool_driver = MempoolDriver::new(store.clone(), tx_mempool, tx_loopback.clone());

        // Make the synchronizer.
        let synchronizer = Synchronizer::new(
            name,
            committee.clone(),
            store.clone(),
            tx_loopback.clone(),
            parameters.sync_retry_delay,
        );

        // Spawn the consensus core.
        Core::spawn(
            name,
            committee.clone(),
            signature_service.clone(),
            store.clone(),
            leader_elector,
            mempool_driver,
            synchronizer,
            parameters.timeout_delay,
            /* rx_message */ rx_consensus,
            rx_loopback,
            tx_proposer,
            tx_commit,
            start_mode,
            tx_helper,
        );

        // Spawn the block proposer.
        Proposer::spawn(
            name,
            committee.clone(),
            signature_service,
            rx_mempool,
            /* rx_message */ rx_proposer,
            tx_loopback,
        );

        // Spawn the helper module.
        Helper::spawn(committee, store, /* rx_requests */ rx_helper);
    }
}

/// Defines how the network receiver handles incoming primary messages.
#[derive(Clone)]
struct ConsensusReceiverHandler {
    tx_consensus: Sender<ConsensusMessage>,
    tx_helper: Sender<HelperRequest>,
}

#[async_trait]
impl MessageHandler for ConsensusReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Deserialize the message
        let message = bincode::deserialize(&serialized);

        // Handle potential deserialization errors
        let consensus_message: ConsensusMessage = match message {
            Ok(msg) => msg,
            Err(e) => {
                // Log the error and return without crashing
                log::warn!("Failed to deserialize consensus message: {}", e);
                // Return the serialization error, wrapped
                return Err(Box::new(ConsensusError::SerializationError(e)));
            }
        };

        match consensus_message {
            // --- Handle Requests intended for Helper ---
            ConsensusMessage::SyncRequest(missing, origin) => {
                // Route to Helper task with the specific request type
                self.tx_helper
                    .send(HelperRequest::GetBlock(missing, origin))
                    .await
                    // Consider returning Err instead of panic for robustness
                    .map_err(|e| Box::new(e) as Box<dyn Error>)?;
            }
            ConsensusMessage::SyncCheckpointRequest(origin) => {
                // Route to Helper task with the specific request type
                self.tx_helper
                    .send(HelperRequest::GetCheckpoint(origin))
                    .await
                    // Consider returning Err instead of panic
                    .map_err(|e| Box::new(e) as Box<dyn Error>)?;
            }

            // --- Handle Responses or Messages for Core ---
            message @ ConsensusMessage::Propose(..) => {
                // ReliableSender expects an ACK for proposals
                let _ = writer.send(Bytes::from("Ack")).await;

                // Pass the message to the consensus core.
                self.tx_consensus
                    .send(message)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn Error>)?;
            }
            message @ ConsensusMessage::SyncCheckpointResponse(..) => {
                // This is a response to *our* request during fast-sync startup.
                // Route it to the Core, which needs logic (under cfg flag) to handle it.
                // SimpleSender used for the response doesn't need an ACK from us here.
                self.tx_consensus
                    .send(message)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn Error>)?;
            }

            // --- Default Handler for other Core messages (Vote, Timeout, TC) ---
            message => {
                // These messages (Vote, Timeout, TC) typically don't require ACKs in this design
                 // Pass the message to the consensus core.
                self.tx_consensus
                    .send(message)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn Error>)?;
            }
        }
        Ok(())
    }
}
