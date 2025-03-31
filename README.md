# HotStuff Epochs and Fast Sync Preparation - PoC

## 1. Introduction

This document describes modifications made to the original Jolteon HotStuff Proof-of-Concept (PoC) implementation.

The primary goals of this work were:
1.  To introduce **epoch support** into the consensus protocol, making epoch numbers visible in blocks and triggering epoch changes based on a configurable number of rounds (`ROUNDS_PER_EPOCH`).
2.  To **prepare for fast synchronization** by designing and implementing a mechanism where nodes can serve checkpoint blocks, representing the state at epoch boundaries, to potentially allow peers to bootstrap more quickly.

This implementation focuses on demonstrating the core logic for these features within the existing framework. It adheres to the PoC nature of the original codebase, prioritizing happy-path functionality over production-grade optimizations or comprehensive error handling. The chosen method for fast sync preparation involves a checkpoint request/response system managed by the `Helper` task, prioritizing a lean block structure.

---

## 2. Epoch Implementation

This implementation introduces the concept of epochs into the HotStuff consensus protocol. Epochs are sequential periods defined by a fixed number of consensus rounds.

* **How Epochs are Tracked:**
    * Each `Block` now includes an `epoch: EpochNumber` field to identify which epoch it belongs to. This is defined in the `Block` struct in `consensus/src/messages.rs`. (`EpochNumber` is a type alias for `u128`, defined in `consensus/src/config.rs`).
    * The `Core` component maintains the node's current view of the epoch number via the `current_epoch: EpochNumber` field within its state (`consensus/src/core.rs`). This state variable is initialized to `1` when the `Core` starts.
    * The `epoch` field is included in the block's digest calculation (as seen in `impl Hash for Block` in `consensus/src/messages.rs`), ensuring blocks from different epochs have unique hashes even if otherwise identical.

* **Epoch Advancement Logic:**
    * Epoch length is determined by the constant `ROUNDS_PER_EPOCH`, defined within `consensus/src/core.rs`.
    * This value was set to `5000` rounds. This choice was made to ensure multiple epoch changes (approximately 6-7) would typically occur within the standard 20-second benchmark duration, making the epoch transition mechanism observable during testing.
    * The logic for transitioning epochs resides in the `Core::advance_round` function.
    * Specifically, *before* advancing to `next_round` (where `next_round = current_round + 1`), the code checks if `next_round > 0 && next_round % ROUNDS_PER_EPOCH == 0`.
    * If this condition is true (meaning the *next* round is the first round of a new epoch), the `Core` calculates the `new_epoch_number` and updates its `self.current_epoch` state accordingly. This happens *before* the block for `next_round` is proposed, ensuring that the first block of the new epoch correctly carries the new epoch number.

* **Visibility & Logging:**
    * The epoch number is part of every block's structure and hash.
    * When an epoch transition occurs, the `Core` logs the following message: `info!("EPOCH CHANGE: Preparing for round {}, entering Epoch {}", next_round, self.current_epoch);`. This message clearly indicates epoch changes in the node's logs.

---

## 3. Fast Sync Preparation: Checkpoint Mechanism

Beyond simply tracking epochs, this implementation prepares for efficient node synchronization ("fast sync") by enabling nodes to fetch epoch checkpoints. A checkpoint represents the state (specifically, the last block) of the chain at the end of a finalized epoch. This allows a new or recovering node to potentially bootstrap its state from a recent checkpoint, skipping the need to process the entire blockchain history from genesis.

The preparation involves the `Core` component identifying checkpoints and the `Helper` component storing and serving them upon request.

* **Checkpoint Identification:**
    * The `Core` component identifies a checkpoint block during the processing of Quorum Certificates (QCs) in the `process_qc` function (`consensus/src/core.rs`).
    * When `Core` processes a `qc` where `qc.round` is the last round of an epoch (i.e., `(qc.round + 1) % ROUNDS_PER_EPOCH == 0`), the block referenced by that QC's hash (`qc.hash`) is designated as the checkpoint block for the just-completed epoch.

* **Core -> Helper Communication:**
    * Upon identifying a new checkpoint digest (`qc.hash`), the `Core` sends an update message to the `Helper` task.
    * This is done using an internal `HelperRequest` enum, specifically the `HelperRequest::UpdateCheckpoint(digest)` variant, sent via the `tx_helper` channel (`consensus/src/helper.rs`, `consensus/src/core.rs`).
    * A log message `Core: Epoch {} ending. Marking block {} (Round {}) as the new checkpoint.` indicates this event.

* **Helper Logic:**
    * The `Helper` struct (`consensus/src/helper.rs`) maintains the latest known checkpoint digest in its state (`current_checkpoint_digest: Option<Digest>`).
    * It listens for requests on the `rx_requests` channel, which receives `HelperRequest` messages.
    * When it receives a `HelperRequest::GetCheckpoint(origin)` message (forwarded by the `ConsensusReceiverHandler` from a network request), the `Helper` performs the following:
        1. Retrieves its stored `current_checkpoint_digest`.
        2. If a digest exists, it attempts to read the corresponding block data from the `Store`.
        3. If the block is successfully read and deserialized, it prepares to send the `Block` back in the response (`SyncCheckpointResponse(Some(block))`).
        4. If no digest is stored, or if the block cannot be read/deserialized from the store, it prepares to send back an empty response (`SyncCheckpointResponse(None)`).
    * ***Important Limitation:*** *In this PoC, the `Helper` only learns about checkpoints when its local `Core` processes the relevant QC. If the node starts late, restarts, or otherwise doesn't locally process the specific epoch-ending QC, its `Helper` may not have the latest checkpoint digest available to serve.*
    * A log message `Helper: Serving checkpoint {} for request from {}` indicates a successful checkpoint lookup, while warnings are logged if the checkpoint is missing or fails to load.

* **Network Messages & Handling:**
    * Two new variants were added to `ConsensusMessage` (`consensus/src/consensus.rs`):
        * `SyncCheckpointRequest(PublicKey)`: Sent by a node to request the latest checkpoint from a peer.
        * `SyncCheckpointResponse(Option<Block>)`: Sent by the `Helper` task in response, containing either the checkpoint `Block` (`Some(block)`) or `None`.
    * The `ConsensusReceiverHandler` (`consensus/src/consensus.rs`) was updated:
        * Incoming `SyncCheckpointRequest` messages are dispatched to the `Helper` task via the `tx_helper` channel (as a `HelperRequest::GetCheckpoint`).
        * Incoming `SyncCheckpointResponse` messages (intended for a node performing fast sync) are routed to the `Core` via `tx_consensus`. *(Note: The Core's handling of this response during startup is part of the actual fast sync logic, which is not implemented in this PoC beyond the test scenario triggered by `fab local-fast-sync` using a feature flag).*
    * The `Helper` uses its `SimpleSender` (`self.network`) to send the `SyncCheckpointResponse` directly to the requesting node's network address.

* **Facilitating Fast Sync:** This entire mechanism allows a node to ask peers, "What's your latest epoch checkpoint block?". By receiving and verifying this block, the node could (in a future implementation) initialize its consensus state (`round`, `epoch`, `high_qc`, etc.) to match the checkpoint, significantly speeding up its synchronization process.

---

## 4. Alternative Design Considered: Embedded Hash

During development, an alternative approach for preparing for fast synchronization was considered. This involved embedding epoch linkage directly into the block structure itself, rather than using a separate messaging system via the `Helper`.

* **Brief Description:**
    * This alternative involved adding an optional field to the `Block` struct, such as `prev_epoch_final_qc_hash: Option<Digest>`.
    * When the `Core` detected an epoch change (e.g., in `advance_round` upon processing the final QC of the previous epoch), it would capture the hash digest of that final QC.
    * This captured hash would then be passed to the `Proposer` when generating the *first* block of the *new* epoch.
    * The `Proposer` would include this hash in the `prev_epoch_final_qc_hash` field (as `Some(hash)`) for that specific block. For all other blocks within the epoch, this field would be `None`.
    * Critically, this new optional field would need to be included in the block's digest calculation (`impl Hash for Block`) to ensure chain integrity.
    * Fast sync would be facilitated because a client could potentially sync by fetching only blocks where `prev_epoch_final_qc_hash.is_some()`, verifying the chain of epochs through these linked hashes.

* **Trade-offs:**

    | Feature                 | Implemented (Checkpoint Msgs)                      | Alternative (Embedded Hash)                          |
    | :---------------------- | :------------------------------------------------- | :--------------------------------------------------- |
    | **Block Structure** | Unchanged ("Lighter" blocks)                       | Modified (Adds `Option<Digest>`, "Heavier" blocks*) |
    | **System Complexity** | Higher (New messages, Helper state, channels)      | Lower (Uses existing components, simpler flow)       |
    | **Mechanism** | Explicit request/response for checkpoints          | Implicit link within block data                    |
    | **State Dependency** | Relies on Helper state (see Limitations)           | Self-contained within block data                   |
    | **Separation of Concerns**| Clearer separation (Helper handles serving)        | Less distinct separation                         |

    *\*Note: The "Heavier" blocks occur once per epoch. Based on the code, the `Digest` type is 32 bytes. The `Option<Digest>` field likely adds ~33 bytes when present. This adds 32 bytes of link-specific data once per 5000 rounds, resulting in an average size increase of **~0.0064 bytes per block** specifically for the epoch link data. For context, a single digest is 32 bytes, and base block headers (without payload) are typically over 400 bytes.*

* **Justification for Chosen Approach:**
    * The Checkpoint Mechanism approach, despite requiring additional system components (Helper state, specific messages), was implemented for this PoC primarily based on a design philosophy favoring lean core data structures.
    * The guiding principle was to keep the fundamental `Block` primitive as minimal and lightweight as possible. Adding fields directly to the `Block`, even optional ones like `prev_epoch_final_qc_hash`, was viewed as a potential "slippery slope" that could lead to bloated primitives over the long term as more features might be added.
    * While the average size increase from the embedded hash approach is quantitatively extremely small (~0.0064 bytes per block on average – significantly smaller than a single digest or typical header overhead), the decision was made based on the principle of avoiding *any* persistent increase in the core block structure's data footprint. The concern centered on the cumulative storage/bandwidth impact of even small additions when extrapolated across potentially billions of blocks in a production system. Therefore, prioritizing an absolutely minimal block structure led to the choice of the externalized checkpoint mechanism, even acknowledging the increased system complexity within this PoC.

---

## 5. Assumptions and Limitations

* **PoC Scope:** This implementation is a Proof-of-Concept focused on the happy path. Error handling, complex state recovery, and performance optimizations are minimal.
* **Checkpoint Availability (Key Limitation):** The `Helper` task only learns the latest checkpoint digest when its associated `Core` component locally processes the specific Quorum Certificate (QC) that finalizes an epoch (within the `process_qc` function). This passive learning mechanism means:
    * If a node starts significantly later than the network or recovers state without reprocessing the relevant history, its `Helper` will likely not possess the digest for the most recent network-wide checkpoint.
    * Consequently, a node running this code **cannot be guaranteed** to serve the actual latest checkpoint block when requested by a peer via `SyncCheckpointRequest`. A production-ready fast sync mechanism would need a more robust method for nodes to discover, validate, and store recent checkpoints (e.g., active fetching, validating against state commitments).
* **Static Committee:** The implementation assumes the validator committee remains the same across all epochs. Epoch changes do not trigger committee rotation or updates.
* **Fixed Epoch Length:** The number of rounds per epoch (`ROUNDS_PER_EPOCH`) is a compile-time constant (`5000`) and cannot be changed dynamically during runtime.
* **No Fast Sync Client Logic:** While the `fab local-fast-sync` task *simulates* a fast sync using a feature flag to enable client logic in `Core::run`, the primary goal of the assignment was preparation. The robust implementation of the client-side fast sync logic (requesting, verification, state initialization) itself is considered beyond the core scope of this preparation task, although basic functionality exists under the feature flag for testing the checkpoint serving mechanism.

---

## 6. How to Run and Verify

This section details the steps required to set up the environment, run the benchmarks (which demonstrate the implemented features), and verify the functionality using log outputs.

**1. Prerequisites & Setup:**

* **Clone Repository:** First, clone this repository fork to your local machine:
    ```bash
    git clone -b epoch https://github.com/giaki3003/hotstuff
    cd hotstuff/benchmark # Navigate into the benchmark directory
    ```
* **System Dependencies:** Ensure you have the following installed on your system:
	* **`rust`**: Version 1.85 was used in tests, older versions might probably work fine too.
    * **`clang`**: Required by the `rocksdb` Rust crate.
    * **`tmux`**: Used by Fabric to manage running nodes in the background.
    * (Installation methods vary by OS - e.g., `apt-get install clang tmux rustup` on Debian/Ubuntu, `brew install llvm tmux rustup` on macOS).
* **Python Environment:**
    * The Fabric scripts use Python syntax that requires **Python version < 3.12**. It is recommended to use a version like **Python 3.9**.
    * Using a tool like `pyenv` is highly recommended to manage Python versions easily:
        ```bash
        # Example using pyenv (install pyenv first if needed)
        pyenv install 3.9.18 # Or another 3.9.x version
        pyenv local 3.9.18 # Set python version for this directory
        ```
* **Python Dependencies:** Create a virtual environment and use it to install the required Python packages using pip:
    ```bash
    python -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```
* **Build:** No manual Rust build step (like `cargo build`) is needed; Fabric (`fab`) will handle compiling the Rust code when you run the benchmark tasks.

**2. Running & Verifying Epoch Changes (`fab local`):**

* **Run Command:** From the `benchmark` directory, execute:
    ```bash
    fab local
    ```
* **What it Does:** This sets up and runs 4 nodes locally for the default benchmark duration (~20 seconds). Fabric compiles the code and runs the nodes within `tmux` sessions.
* **Log Location:** Log files for each node (`node-0.log` to `node-3.log`) are generated in the `benchmark/logs/` directory within the `benchmark` folder.
* **Verification:**
    * Inspect the log files (e.g., `tail -f logs/node-0.log` while it's running, or check them after completion).
    * Search for log entries indicating an epoch change. Key messages:
        * `INFO consensus::core] Core: Epoch <E> ending. Marking block <Digest> (Round <R>) as the new checkpoint.`
        * `INFO consensus::core] EPOCH CHANGE: Preparing for round <R+1>, entering Epoch <E+1>`
    * With `ROUNDS_PER_EPOCH = 5000`, several epoch changes should occur during the run.
    * Also, observe general block logs (like `Committed...` or `Created...`) which now format as `B<Round>, <Epoch>`.

**3. Running & Verifying Fast Sync Preparation (`fab local-fast-sync`):**

* **Run Command:** From the `benchmark` directory, execute:
    ```bash
    fab local-fast-sync
    ```
* **What it Does:** This script runs 4 nodes, stops node 0 after ~30 seconds (allowing epochs to pass), and restarts node 0 with the `fast-sync` feature automatically enabled via build flags. This tests the checkpoint serving and client initialization logic.
* **Log Location:** Logs are in `benchmark/logs/`. The restarted node's log is named `node-0_restarted.log`. Logs for other nodes are `node-1.log`, etc.

* **Verification (Restarted Node - `node-0_restarted.log`):**
    * Check `node-0_restarted.log`. Look for the sequence indicating fast sync initialization:
        * `[INFO node::node] 'fast-sync' feature enabled...`
        * `[INFO consensus::core] Core::run entered with start_mode: FastSync`
        * `[INFO consensus::core] Fast Sync: Requesting checkpoint from <Peer Address>`
        * `[INFO consensus::core] Fast Sync: Received checkpoint block B<Round>, <Epoch>`
        * `[INFO consensus::core] Fast Sync: Checkpoint verified (basic). Initializing state.`
        * `[INFO consensus::core] Fast Sync: Successfully initialized state from checkpoint.`
        * `[INFO consensus::core] Core starting normal operation at Epoch <E>, Round <R>`

* **Verification (Serving Nodes - `node-1.log`, `node-2.log`, `node-3.log`):**
    * Around the time node 0 restarts, check the logs of the other nodes (1-3).
    * Look for logs showing the `Helper` serving the checkpoint:
        * `[INFO consensus::helper] Helper: Serving checkpoint <Digest> for request from <Node0 PubKey>`
        * `[DEBUG consensus::helper] Sending SyncCheckpointResponse (contains_block: true) to <Node0 PubKey>`

* **Summary:** This test verifies that the `Helper` correctly stores and serves checkpoints identified by the `Core` at epoch boundaries, and that a node starting with the `fast-sync` flag can request, receive, and (in this PoC) initialize its state from such a checkpoint.