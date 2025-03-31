use log::{info, debug, error};
use crate::Block;
use crate::config::Committee;
use crate::consensus::ConsensusMessage;
use bytes::Bytes;
use crypto::{Digest, PublicKey};
use log::warn;
use network::SimpleSender;
use store::Store;
use tokio::sync::mpsc::Receiver;

#[cfg(test)]
#[path = "tests/helper_tests.rs"]
pub mod helper_tests;

#[derive(Debug)]
pub enum HelperRequest {
    GetBlock(Digest, PublicKey), // Request for a specific block by digest
    GetCheckpoint(PublicKey),    // Request for the latest checkpoint block
    UpdateCheckpoint(Digest),    // Request to update the checkpoint block
}

/// A task dedicated to help other authorities by replying to their sync requests.
pub struct Helper {
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Input channel to receive sync requests. Changed type to HelperRequest.
    rx_requests: Receiver<HelperRequest>,
    /// A network sender to reply to the sync requests.
    network: SimpleSender,
    /// Current checkpoint block digest
    current_checkpoint_digest: Option<Digest>,
}

impl Helper {
    // Spawn function already correctly updated to accept Receiver<HelperRequest>
    pub fn spawn(committee: Committee, store: Store, rx_requests: Receiver<HelperRequest>) {
        tokio::spawn(async move {
            Self {
                committee,
                store,
                rx_requests,
                network: SimpleSender::new(),
                current_checkpoint_digest: None,
            }
            .run()
            .await;
        });
    }

    // Updated run loop to handle HelperRequest enum
    async fn run(&mut self) {
        while let Some(request) = self.rx_requests.recv().await { // <-- Receive the enum
            match request {
                HelperRequest::GetBlock(digest, origin) => { // <-- Handle GetBlock variant
                    // Get the requester's address.
                    let address = match self.committee.address(&origin) {
                        Some(x) => x,
                        None => {
                            warn!("Received GetBlock request from unknown authority: {}", origin);
                            continue; // Skip if origin is unknown
                        }
                    };

                    // Reply with the requested block data (if we have it).
                    match self.store.read(digest.to_vec()).await {
                        Ok(Some(bytes)) => {
                            // Attempt to deserialize to ensure it's a valid block before sending
                            match bincode::deserialize::<Block>(&bytes) {
                                Ok(block) => {
                                    let message = bincode::serialize(&ConsensusMessage::Propose(block))
                                        .expect("Failed to serialize block"); // Should generally not fail
                                    self.network.send(address, Bytes::from(message)).await;
                                    debug!("Served block {} to {}", digest, origin);
                                }
                                Err(e) => {
                                    error!("Failed to deserialize block {} from store: {}", digest, e);
                                }
                            }
                        }
                        Ok(None) => {
                            // Block not found, do nothing. The requester will likely retry.
                            debug!("Block {} not found for GetBlock request from {}", digest, origin);
                        }
                        Err(e) => {
                            error!("Failed to read store for GetBlock request {}: {}", digest, e);
                        }
                    }
                }

                HelperRequest::UpdateCheckpoint(new_digest) => { // <-- Handle UpdateCheckpoint message from Core
                    info!("Helper: Received checkpoint update: {}", new_digest);
                    self.current_checkpoint_digest = Some(new_digest);
                }

                HelperRequest::GetCheckpoint(origin) => { // <-- Handle GetCheckpoint variant
                    // Get the requester's address.
                     let address = match self.committee.address(&origin) {
                        Some(x) => x,
                        None => {
                            warn!("Received GetCheckpoint request from unknown authority: {}", origin);
                            continue; // Skip if origin is unknown
                        }
                    };

                    // Start the checkpoint block logic
                    let mut checkpoint_block: Option<Block> = None;
                    // Check if core has given us a checkpoint block
                    if let Some(digest_to_serve) = self.current_checkpoint_digest.clone() {
                        info!("Helper: Serving checkpoint {} for request from {}", digest_to_serve, origin);
                        match self.store.read(digest_to_serve.to_vec()).await {
                            Ok(Some(block_bytes)) => {
                                match bincode::deserialize::<Block>(&block_bytes) {
                                    Ok(block) => {
                                        checkpoint_block = Some(block);
                                    }
                                    Err(e) => {
                                        error!("Helper: Failed to deserialize stored checkpoint block {}: {}", digest_to_serve, e);
                                        // Keep checkpoint_block = None
                                    }
                                }
                            }
                            Ok(None) => {
                                warn!("Helper: Stored checkpoint digest {} points to block not found in store!", digest_to_serve);
                                // Keep checkpoint_block = None
                            }
                             Err(e) => {
                                error!("Helper: Failed to read stored checkpoint block {} from store: {}", digest_to_serve, e);
                                // Keep checkpoint_block = None
                            }
                        }
                    } else {
                        warn!("Helper: No checkpoint digest set by Core yet. Cannot serve checkpoint for {}", origin);
                        // Keep checkpoint_block = None
                    }

                    // Serialize and send the response (which might contain None).
                    let response = ConsensusMessage::SyncCheckpointResponse(checkpoint_block.clone());
                    match bincode::serialize(&response) {
                        Ok(serialized) => {
                            debug!("Sending SyncCheckpointResponse (contains_block: {}) to {}", checkpoint_block.is_some(), origin);
                            self.network.send(address, Bytes::from(serialized)).await;
                        }
                        Err(e) => {
                             error!("Failed to serialize SyncCheckpointResponse: {}", e);
                        }
                    }
                }
            }
        }
    }
}