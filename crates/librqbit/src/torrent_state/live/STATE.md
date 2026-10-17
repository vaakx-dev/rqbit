# Live Torrent State Architecture

This document describes the shared state used during live torrent downloading and the invariants that are maintained.

## Key Data Structures

### 1. `PieceTracker` (in `piece_tracker.rs`)

Coordinates piece download state by wrapping `ChunkTracker` and `inflight_pieces`. Lives in `TorrentStateLocked`.

```rust
pub struct PieceTracker {
    chunks: ChunkTracker,
    inflight: HashMap<ValidPieceIndex, InflightPiece>,
}

pub struct InflightPiece {
    pub owner: PieceOwner,    // Who requests the missing chunks
    pub started: Instant,     // When download started (for steal threshold)
}

pub enum PieceOwner {
    Peer(PeerHandle),                  // That peer's chunk requester asks it for every missing chunk
    TimeCritical(TimeCriticalPiece),   // The time-critical scheduler spreads them over the fastest peers
}
```

Key methods that maintain invariants:
- `acquire_piece()` - Reserve from queue or steal from slow peer
- `take_inflight()` - Remove from inflight (before hash check)
- `mark_piece_hash_ok()` - Mark as completed after hash verification
- `mark_piece_hash_failed()` - Requeue after hash failure
- `release_pieces_owned_by()` - Release all pieces owned by a dead peer
- `make_time_critical()` / `release_time_critical()` - Hand a piece to the time-critical scheduler and back

### 2. `ChunkTracker` (in `chunk_tracker.rs`)

Tracks piece/chunk download progress. Wrapped by `PieceTracker`.

| Field | Type | Description |
|-------|------|-------------|
| `have` | `BitVec` | Pieces fully downloaded and verified |
| `queue_pieces` | `BitVec` | Pieces needed but not currently being downloaded |
| `chunk_status` | `BitVec` | Per-chunk completion status |

### 3. `inflight_requests` (in `LivePeerState`)

```rust
HashSet<ChunkInfo>  // aka InflightRequest

struct ChunkInfo {
    piece_index: ValidPieceIndex,
    chunk_index: u32,
    offset: u32,
    size: u32,
}
```

Per-peer tracking of which chunks have been requested from this peer. Used for:
- Knowing which chunks to expect from peer
- Detecting unexpected data ("peer sent us a piece we did not ask")
- Cleanup when peer dies

## Piece State Invariant

A piece is in exactly ONE of these states:

```
have[piece] = true                    → COMPLETED (verified)
inflight.contains(piece)              → IN_FLIGHT (being downloaded)
queue_pieces[piece] = true            → QUEUED (needed, waiting)
none of the above                     → NOT_NEEDED (deprioritized)
```

These are **disjoint** - a piece is never in multiple states simultaneously. This invariant is maintained by `PieceTracker` methods.

### Chunk-Piece Consistency

Chunks of piece P may be requested from several peers: the owner, a peer it was stolen from
(until the cancel is processed), or any peers the time-critical scheduler picked.
A received chunk is written if P is in-flight and the chunk isn't written yet, whoever sent it.
The writer holds the per-piece read lock; the peer that writes the last chunk takes the piece
out of `inflight` and then takes the per-piece write lock for the hash check, so that no write
to the piece can still be running.

## State Transitions

### Normal Download Flow

```
QUEUED → IN_FLIGHT → COMPLETED
```

1. `PieceTracker::acquire_piece()`:
   - Finds piece in `queue_pieces` (or steals from slow peer)
   - Calls `chunks.reserve_needed_piece(p)` → clears `queue_pieces[p] = false`
   - Inserts into `inflight[p] = (peer, now)`
   - Returns `AcquireResult::Reserved(p)` or `AcquireResult::Stolen { piece, from_peer }`

2. Chunk requesting (one chunk at a time, by the peer's chunk requester):
   - Time-critical chunks the scheduler gave to this peer first
   - Then the next chunk of its own piece that isn't written yet, as long as it still owns the piece
   - Insert into `inflight_requests`, send Request message to peer

3. Data arrival (`on_incoming_piece`):
   - Remove chunk from `inflight_requests`
   - Mark chunk complete in `chunk_status`
   - If all chunks done → verify hash

4. Piece completion:
   - `PieceTracker::take_inflight(piece)` → removes from `inflight`
   - Hash check passes: `PieceTracker::mark_piece_hash_ok(piece)` → sets `have[p] = true`
   - Hash check fails: `PieceTracker::mark_piece_hash_failed(piece)` → sets `queue_pieces[p] = true`

### Piece Stealing Flow

```
IN_FLIGHT (peer A) → IN_FLIGHT (peer B)
```

`PieceTracker::acquire_piece()` with steal logic:
1. Finds piece in `inflight` owned by slow peer (elapsed > threshold × avg_time)
2. Updates `inflight[p].peer = self`
3. Updates `inflight[p].started = now`
4. Returns `AcquireResult::Stolen { piece, from_peer }`

Caller then:
5. Calls `peers.on_steal(from_peer, to_peer, piece)`:
   - Sends Cancel messages to victim peer
   - Removes chunks from victim's `inflight_requests`
6. Stealer requests the chunks that aren't written yet:
   - Inserts into own `inflight_requests`
   - Sends Request messages
7. The victim's chunk requester stops requesting chunks of the piece once it sees it no longer owns it

**Note:** Stealing does NOT call `reserve_needed_piece()` because the piece is already in `inflight`, not in `queue_pieces`.

### Peer Death Flow

```
IN_FLIGHT → QUEUED (for pieces owned by dead peer)
```

1. `on_peer_died()`:
   - Takes `LivePeerState` (consumes it)
   - Calls `PieceTracker::release_pieces_owned_by(peer_addr)`

2. `release_pieces_owned_by()`:
   - Removes all entries from `inflight` owned by the dead peer
   - For each removed piece, calls `chunks.requeue_piece_if_needed(piece)`
   - This sets `queue_pieces[p] = true` and keeps `chunk_status`, so only missing chunks are downloaded again
   - Returns count of released pieces

This maintains the invariant: pieces transition cleanly from IN_FLIGHT → QUEUED.

### Checksum Failure Flow

```
IN_FLIGHT → QUEUED
```

1. All chunks received, hash verification fails
2. `PieceTracker::take_inflight(piece)` → removes from `inflight`
3. `PieceTracker::mark_piece_hash_failed(piece)` → calls `mark_piece_broken_if_not_have(piece)` → sets `queue_pieces[p] = true`

### Pause Flow

```
IN_FLIGHT → QUEUED (for all in-flight pieces)
```

1. `pause()`:
   - Calls `PieceTracker::into_chunks()` which:
     - For each piece in `inflight`, calls `mark_piece_broken_if_not_have(piece)`
     - Returns the inner `ChunkTracker`
   - Stores the `ChunkTracker` for resume

## Architectural Notes

### State Encapsulation

Piece state is coordinated by `PieceTracker`:
- `ChunkTracker`: `have`, `queue_pieces`, `chunk_status` (wrapped)
- `PieceTracker`: `inflight` (owned)
- `LivePeerState`: `inflight_requests` (per-peer, separate)

`PieceTracker` methods ensure atomic state transitions that maintain invariants. Direct access to `ChunkTracker` is read-only via `chunks()`.

### Lock Ordering

To avoid deadlocks, locks must be acquired in a consistent order:

1. `peers` lock (via `with_live_mut` / `with_peer_mut`) - DashMap per-peer locks
2. `TorrentStateLive` lock (via `lock_write` / `lock_read`) - global torrent state

**Critical Rule: Never access other peers while holding a peer lock.**

The `peers` field is a `DashMap<PeerHandle, Peer>` which uses sharded locking. When you hold
a lock on one peer's shard via `with_live_mut` or `with_peer_mut`, you must NOT:
- Call `with_peer`, `with_live_mut`, `with_peer_mut` on a different peer
- Iterate over the peers DashMap
- Call any method that internally accesses other peers (e.g., `on_steal`)

This is because:
1. Thread A holds write lock on shard S1 (peer X)
2. Thread A tries to access peer Y which is in shard S2
3. Thread B holds/waits for shard S2 and wants shard S1
4. Deadlock!

**Example of what NOT to do:**
```rust
// BAD - accessing other peers inside with_live_mut
self.peers.with_live_mut(self.addr, "example", |live| {
    // ... do something ...
    self.peers.on_steal(other_peer, self.addr, piece);  // DEADLOCK RISK!
});
```

**Correct pattern:**
```rust
// GOOD - collect info inside closure, process outside
let steal_info = self.peers.with_live_mut(self.addr, "example", |live| {
    // ... return data needed for on_steal ...
    Some((other_peer, piece))
});
if let Some((from_peer, piece)) = steal_info {
    self.peers.on_steal(from_peer, self.addr, piece);  // Safe - no peer lock held
}
```

Care must be taken when modifying state transition logic to maintain this ordering.

### Acquire Strategy

`PieceTracker::acquire_piece()` uses a three-phase strategy:

1. **Try steal (10x threshold)** - Very slow peers get pieces stolen first
2. **Try reserve** - Check priority pieces, then queue_pieces
3. **Try steal (3x threshold)** - Moderately slow peers as fallback

This balances fairness with efficiency - we prefer reserving new pieces but will steal from slow peers to avoid bottlenecks.

Time-critical pieces are never stolen.

### Time-Critical Flow

```
QUEUED or IN_FLIGHT (peer A) → IN_FLIGHT (time critical) → COMPLETED
```

See `time_critical/mod.rs`, which follows libtorrent's streaming design:
1. The pieces under each stream's read position are handed to the scheduler with `make_time_critical()`.
   Peer A keeps its outstanding requests but stops requesting more.
2. Each pass, the missing chunks nobody was asked for go to the peers with the lowest download queue time,
   earliest deadline first, through the peers' `time_critical_requests` queues.
3. A piece that takes longer than the average time-critical piece gets its chunks requested again from other
   peers. When one copy arrives, the duplicate requests are cancelled.
4. When the streams move away, a piece without outstanding requests goes back to the queue with `release_time_critical()`.
