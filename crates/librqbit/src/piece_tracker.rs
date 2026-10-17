//! Coordinates piece download state by wrapping ChunkTracker with inflight piece tracking.
//!
//! This module provides the [`PieceTracker`] type which encapsulates the relationship between
//! queued pieces (in ChunkTracker) and in-flight pieces (being downloaded by one peer, or
//! spread over many peers when the piece is time critical).
//!
//! The key invariant maintained is: a piece is in exactly one state at any time:
//! - HAVE (completed)
//! - QUEUED (available to download)
//! - IN_FLIGHT (currently being downloaded)
//! - NOT_NEEDED (not selected for download)

use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use buffers::ByteBuf;
use librqbit_core::lengths::ValidPieceIndex;
use peer_binary_protocol::Piece;

use crate::{
    chunk_tracker::{ChunkMarkingResult, ChunkTracker},
    file_info::FileInfo,
    type_aliases::{FileInfos, FilePriorities, PeerHandle},
};

/// Who requests the chunks of an in-flight piece.
#[derive(Debug, Clone)]
pub enum PieceOwner {
    /// The peer's chunk requester asks this peer for every missing chunk.
    Peer(PeerHandle),
    /// The time-critical scheduler spreads the missing chunks over the fastest peers.
    TimeCritical(TimeCriticalPiece),
}

#[derive(Debug, Clone)]
pub struct TimeCriticalPiece {
    pub first_requested: Option<Instant>,
    /// When a chunk nobody had asked for yet was last requested.
    pub last_requested: Option<Instant>,
    /// Some chunk was requested from more than one peer.
    pub busy: bool,
}

/// Tracks a piece currently being downloaded.
#[derive(Debug, Clone)]
pub struct InflightPiece {
    pub owner: PieceOwner,
    pub started: Instant,
}

impl InflightPiece {
    pub fn is_owned_by(&self, peer: PeerHandle) -> bool {
        matches!(self.owner, PieceOwner::Peer(p) if p == peer)
    }
}

/// Running average of how long time-critical pieces take, and of the deviation from it.
#[derive(Debug, Default, Clone, Copy)]
pub struct PieceTimes {
    pub average: Duration,
    pub deviation: Duration,
}

impl PieceTimes {
    fn add(&mut self, sample: Duration) {
        if self.average.is_zero() {
            self.average = sample;
            return;
        }
        let diff = self.average.abs_diff(sample);
        self.deviation = if self.deviation.is_zero() {
            diff
        } else {
            (self.deviation * 9 + diff) / 10
        };
        self.average = (self.average * 9 + sample) / 10;
    }
}

/// Result of attempting to acquire a piece.
#[derive(Debug)]
pub enum AcquireResult {
    /// A new piece was reserved from the queue.
    Reserved(ValidPieceIndex),
    /// A piece was stolen from a slower peer.
    Stolen {
        piece: ValidPieceIndex,
        from_peer: PeerHandle,
    },
    /// No pieces are available for this peer.
    NoneAvailable,
}

/// Parameters for acquiring a piece.
pub struct AcquireRequest<'a, I, P, S>
where
    I: Iterator<Item = ValidPieceIndex>,
    P: Fn(ValidPieceIndex) -> bool,
    S: Fn(ValidPieceIndex) -> bool,
{
    /// The peer requesting a piece.
    pub peer: PeerHandle,
    /// The peer's average piece download time (for steal calculations).
    pub peer_avg_time: Option<Duration>,
    /// Priority pieces to check first (e.g., for streaming).
    pub priority_pieces: I,
    /// File download priority ordering.
    pub file_priorities: &'a FilePriorities,
    /// File metadata for iterating pieces.
    pub file_infos: &'a FileInfos,
    /// Returns true if the peer has the given piece.
    pub peer_has_piece: P,
    /// Returns true if the piece can be stolen (e.g., not locked for writing).
    pub can_steal: S,
}

/// Coordinates piece download state.
///
/// Wraps a [`ChunkTracker`] with tracking of which pieces are currently being downloaded
/// (in-flight) and by which peer. This ensures that:
///
/// - A piece is only assigned to one peer at a time (unless stolen)
/// - Pieces are properly requeued when a peer dies
/// - State transitions maintain invariants
pub struct PieceTracker {
    chunks: ChunkTracker,
    inflight: HashMap<ValidPieceIndex, InflightPiece>,
    time_critical_times: PieceTimes,
}

impl PieceTracker {
    // === CONSTRUCTION ===

    /// Create a new PieceTracker wrapping the given ChunkTracker.
    pub fn new(chunks: ChunkTracker) -> Self {
        Self {
            chunks,
            inflight: HashMap::new(),
            time_critical_times: PieceTimes::default(),
        }
    }

    /// Read-only access to the underlying ChunkTracker.
    pub fn chunks(&self) -> &ChunkTracker {
        &self.chunks
    }

    /// Consume the PieceTracker, requeuing any in-flight pieces.
    ///
    /// This is used when pausing a torrent - any pieces that were being downloaded
    /// need to be put back in the queue so they can be re-downloaded on resume.
    pub fn into_chunks(mut self) -> ChunkTracker {
        // Requeue all in-flight pieces so they'll be re-downloaded on resume
        for piece in self.inflight.into_keys() {
            self.chunks.mark_piece_broken_if_not_have(piece);
        }
        self.chunks
    }

    // === PIECE ACQUISITION ===

    /// Attempt to acquire a piece for the requesting peer.
    ///
    /// The acquisition strategy is:
    /// 1. Try to steal a piece from a peer that's 10x slower
    /// 2. Try to reserve a piece from the queue (priority pieces first)
    /// 3. Try to steal a piece from a peer that's 3x slower
    ///
    /// If `Stolen` is returned, the caller MUST call `peers.on_steal()` to notify
    /// the old peer and update counters.
    pub fn acquire_piece<I, P, S>(&mut self, mut req: AcquireRequest<I, P, S>) -> AcquireResult
    where
        I: Iterator<Item = ValidPieceIndex>,
        P: Fn(ValidPieceIndex) -> bool,
        S: Fn(ValidPieceIndex) -> bool,
    {
        // 1. Try steal with 10x threshold (very slow peer)
        if let Some(result) = self.try_steal(&req, 10.0) {
            return result;
        }

        // 2. Try reserve from priority_pieces then queued pieces
        // First check priority pieces that aren't already downloaded or in-flight
        for piece in &mut req.priority_pieces {
            if !self.chunks.is_piece_have(piece)
                && !self.chunks.are_all_chunks_downloaded(piece)
                && !self.inflight.contains_key(&piece)
                && (req.peer_has_piece)(piece)
            {
                return self.reserve_piece(piece, PieceOwner::Peer(req.peer));
            }
        }

        // Then check naturally ordered queued pieces
        // Note: iter_queued_pieces only returns pieces in queue_pieces (not in-flight)
        let queued: Vec<_> = self
            .chunks
            .iter_queued_pieces(req.file_priorities, req.file_infos)
            .collect();

        for piece in queued {
            if (req.peer_has_piece)(piece) {
                return self.reserve_piece(piece, PieceOwner::Peer(req.peer));
            }
        }

        // 3. Try steal with 3x threshold (moderately slow peer)
        if let Some(result) = self.try_steal(&req, 3.0) {
            return result;
        }

        AcquireResult::NoneAvailable
    }

    /// Reserve a piece: remove from queue, add to inflight.
    fn reserve_piece(&mut self, piece: ValidPieceIndex, owner: PieceOwner) -> AcquireResult {
        self.chunks.reserve_needed_piece(piece);
        self.inflight.insert(
            piece,
            InflightPiece {
                owner,
                started: Instant::now(),
            },
        );
        AcquireResult::Reserved(piece)
    }

    /// Try to steal a piece from a slower peer.
    fn try_steal<I, P, S>(
        &mut self,
        req: &AcquireRequest<I, P, S>,
        threshold: f64,
    ) -> Option<AcquireResult>
    where
        I: Iterator<Item = ValidPieceIndex>,
        P: Fn(ValidPieceIndex) -> bool,
        S: Fn(ValidPieceIndex) -> bool,
    {
        let my_avg = req.peer_avg_time?;
        let min_elapsed = Duration::from_secs_f64(my_avg.as_secs_f64() * threshold);

        // Find the slowest piece from another peer that exceeds threshold
        // and that the stealing peer actually has (can download)
        let (piece, old_peer, _) = self
            .inflight
            .iter()
            .filter_map(|(p, info)| match info.owner {
                PieceOwner::Peer(owner) if owner != req.peer => Some((*p, owner, info.started)),
                _ => None,
            })
            .filter(|(p, _, _)| (req.peer_has_piece)(*p))
            .map(|(p, owner, started)| (p, owner, started.elapsed()))
            .filter(|(_, _, elapsed)| *elapsed >= min_elapsed)
            .max_by_key(|(_, _, elapsed)| *elapsed)?;

        // Check can_steal (e.g., per_piece_lock)
        if !(req.can_steal)(piece) {
            return None;
        }

        // Update ownership (piece stays in inflight, just changes owner)
        let info = self.inflight.get_mut(&piece)?;
        info.owner = PieceOwner::Peer(req.peer);
        info.started = Instant::now();

        Some(AcquireResult::Stolen {
            piece,
            from_peer: old_peer,
        })
    }

    // === PIECE COMPLETION ===

    /// Remove piece from inflight tracking (e.g., after all chunks received).
    ///
    /// Returns download duration if piece was in-flight.
    /// Note: Does NOT mark the piece as downloaded - caller should do hash check
    /// and then call `mark_piece_hash_ok` or `mark_piece_hash_failed`.
    pub fn take_inflight(&mut self, piece: ValidPieceIndex) -> Option<Duration> {
        let inflight = self.inflight.remove(&piece)?;
        if let PieceOwner::TimeCritical(TimeCriticalPiece {
            first_requested: Some(first_requested),
            ..
        }) = &inflight.owner
        {
            self.time_critical_times.add(first_requested.elapsed());
        }
        Some(inflight.started.elapsed())
    }

    /// Mark piece as downloaded after successful hash verification.
    pub fn mark_piece_hash_ok(&mut self, piece: ValidPieceIndex) {
        self.chunks.mark_piece_downloaded(piece);
    }

    /// Mark piece as failed after hash verification failure - requeues the piece.
    pub fn mark_piece_hash_failed(&mut self, piece: ValidPieceIndex) {
        self.chunks.mark_piece_broken_if_not_have(piece);
    }

    /// Release all pieces owned by a peer (on peer death).
    ///
    /// Moves all pieces owned by the peer from IN_FLIGHT back to QUEUED, keeping
    /// the chunks already written so that only the missing ones get downloaded again.
    /// Returns the number of pieces released.
    pub fn release_pieces_owned_by(&mut self, peer: PeerHandle) -> usize {
        // Collect pieces to release (can't modify while iterating)
        let pieces_to_release: Vec<_> = self
            .inflight
            .iter()
            .filter(|(_, info)| info.is_owned_by(peer))
            .map(|(p, _)| *p)
            .collect();

        let count = pieces_to_release.len();
        for piece in pieces_to_release {
            self.inflight.remove(&piece);
            self.chunks.requeue_piece_if_needed(piece);
        }
        count
    }

    // === TIME-CRITICAL PIECES ===

    /// Hand the piece to the time-critical scheduler, reserving it if it's queued.
    ///
    /// A previous owner's outstanding requests stay valid, it just stops requesting more.
    /// Returns false if the piece is downloaded or being hash checked.
    pub fn make_time_critical(&mut self, piece: ValidPieceIndex) -> bool {
        let time_critical = || {
            PieceOwner::TimeCritical(TimeCriticalPiece {
                first_requested: None,
                last_requested: None,
                busy: false,
            })
        };
        if let Some(inflight) = self.inflight.get_mut(&piece) {
            if let PieceOwner::Peer(_) = inflight.owner {
                inflight.owner = time_critical();
            }
            return true;
        }
        if self.chunks.is_piece_have(piece) || self.chunks.are_all_chunks_downloaded(piece) {
            return false;
        }
        self.reserve_piece(piece, time_critical());
        true
    }

    pub fn get_time_critical_mut(
        &mut self,
        piece: ValidPieceIndex,
    ) -> Option<&mut TimeCriticalPiece> {
        match &mut self.inflight.get_mut(&piece)?.owner {
            PieceOwner::TimeCritical(tc) => Some(tc),
            PieceOwner::Peer(_) => None,
        }
    }

    pub fn iter_time_critical(&self) -> impl Iterator<Item = ValidPieceIndex> + '_ {
        self.inflight
            .iter()
            .filter(|(_, info)| matches!(info.owner, PieceOwner::TimeCritical(_)))
            .map(|(p, _)| *p)
    }

    /// Give a piece that is no longer time critical back to the queue, keeping its chunks.
    pub fn release_time_critical(&mut self, piece: ValidPieceIndex) {
        if self.get_time_critical_mut(piece).is_some() {
            self.inflight.remove(&piece);
            self.chunks.requeue_piece_if_needed(piece);
        }
    }

    pub fn time_critical_times(&self) -> PieceTimes {
        self.time_critical_times
    }

    // === QUERIES ===

    /// Get the inflight info for a piece, if it's currently being downloaded.
    pub fn get_inflight(&self, piece: ValidPieceIndex) -> Option<&InflightPiece> {
        self.inflight.get(&piece)
    }

    /// Check if a piece is currently in-flight.
    #[allow(dead_code)]
    pub fn is_inflight(&self, piece: ValidPieceIndex) -> bool {
        self.inflight.contains_key(&piece)
    }

    /// Get the number of pieces currently in-flight.
    #[allow(dead_code)]
    pub fn inflight_count(&self) -> usize {
        self.inflight.len()
    }

    // === PASS-THROUGH METHODS ===

    /// Mark a chunk as downloaded. Returns the result indicating if the piece is complete.
    pub fn mark_chunk_downloaded(
        &mut self,
        piece: &Piece<ByteBuf<'_>>,
    ) -> Option<ChunkMarkingResult> {
        self.chunks.mark_chunk_downloaded(piece)
    }

    /// Update which files are selected for download.
    pub fn update_only_files(
        &mut self,
        file_infos: &FileInfos,
        new_only_files: &HashSet<usize>,
    ) -> anyhow::Result<crate::chunk_tracker::HaveNeededSelected> {
        self.chunks.update_only_files(file_infos, new_only_files)
    }

    /// Update per-file have bytes when a piece completes. Returns remaining bytes for the file.
    pub fn update_file_have_on_piece_completed(
        &mut self,
        piece_id: ValidPieceIndex,
        file_id: usize,
        file_info: &FileInfo,
    ) -> u64 {
        self.chunks
            .update_file_have_on_piece_completed(piece_id, file_id, file_info)
    }

    /// Flush the have pieces bitfield to disk.
    pub fn flush_have_pieces(&mut self, flush_async: bool) -> anyhow::Result<()> {
        self.chunks.get_have_pieces_mut().flush(flush_async)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{bitv::BitV as BitVTrait, type_aliases::BF};
    use librqbit_core::lengths::Lengths;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn peer(id: u8) -> PeerHandle {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, id)), 6881)
    }

    /// Create a simple ChunkTracker for testing.
    /// Creates a torrent with the specified number of pieces, all selected.
    fn make_test_chunk_tracker(num_pieces: u32) -> ChunkTracker {
        // Create a simple single-file torrent
        let piece_length = 16384u32; // 16KB pieces
        let total_length = piece_length as u64 * num_pieces as u64;

        let lengths = Lengths::new(total_length, piece_length).unwrap();

        let bf_len = lengths.piece_bitfield_bytes();

        // No pieces downloaded yet (empty have)
        let have = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());

        // All pieces selected
        let mut selected = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        for i in 0..num_pieces as usize {
            selected.set(i, true);
        }

        // Single file spanning all pieces
        let file_infos = vec![crate::file_info::FileInfo {
            relative_filename: "test.dat".into(),
            offset_in_torrent: 0,
            len: total_length,
            piece_range: 0..num_pieces,
            attrs: Default::default(),
        }];

        ChunkTracker::new(have.into_dyn(), selected, lengths, &file_infos).unwrap()
    }

    fn make_test_file_infos(num_pieces: u32) -> FileInfos {
        let piece_length = 16384u64;
        vec![crate::file_info::FileInfo {
            relative_filename: "test.dat".into(),
            offset_in_torrent: 0,
            len: piece_length * num_pieces as u64,
            piece_range: 0..num_pieces,
            attrs: Default::default(),
        }]
    }

    fn make_default_file_priorities(file_infos: &FileInfos) -> FilePriorities {
        (0..file_infos.len()).collect()
    }

    #[test]
    fn test_new_piece_tracker() {
        let chunks = make_test_chunk_tracker(10);
        let tracker = PieceTracker::new(chunks);
        assert_eq!(tracker.inflight_count(), 0);
    }

    #[test]
    fn test_reserve_piece_from_queue() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true, // Peer has all pieces
            can_steal: |_| true,
        });

        // Should reserve piece 0 (first in queue)
        match result {
            AcquireResult::Reserved(piece) => {
                assert_eq!(piece.get(), 0);
                assert!(tracker.is_inflight(piece));
                assert_eq!(tracker.inflight_count(), 1);
            }
            _ => panic!("Expected Reserved, got {:?}", result),
        }
    }

    #[test]
    fn test_reserve_filters_by_peer_has_piece() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Peer only has piece 2 and later
        // Note: iter_queued_pieces iterates in order: first, last, middle
        // So for 0..5: 0, 4, 1, 2, 3
        // With filter >= 2, we get 4 first (skips 0, takes 4)
        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |p| p.get() >= 2,
            can_steal: |_| true,
        });

        match result {
            AcquireResult::Reserved(piece) => {
                // Got piece 4 (first one peer has in iteration order)
                assert!(piece.get() >= 2, "Should have gotten a piece >= 2");
            }
            _ => panic!("Expected Reserved, got {:?}", result),
        }
    }

    #[test]
    fn test_complete_piece() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Reserve a piece first
        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        });

        let piece = match result {
            AcquireResult::Reserved(p) => p,
            _ => panic!("Expected Reserved"),
        };

        // Complete the piece (take_inflight + hash check + mark_piece_hash_ok)
        let duration = tracker.take_inflight(piece);
        assert!(duration.is_some());
        assert!(!tracker.is_inflight(piece));
        // Simulate successful hash check
        tracker.mark_piece_hash_ok(piece);
        assert!(tracker.chunks().is_piece_have(piece));
    }

    #[test]
    fn test_fail_piece_requeues() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Reserve piece 0
        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        });

        let piece = match result {
            AcquireResult::Reserved(p) => p,
            _ => panic!("Expected Reserved"),
        };

        assert!(tracker.is_inflight(piece));

        // Fail the piece (take_inflight + hash check fails + mark_piece_hash_failed)
        let duration = tracker.take_inflight(piece);
        assert!(duration.is_some());
        // Simulate failed hash check
        tracker.mark_piece_hash_failed(piece);

        // Should no longer be in-flight
        assert!(!tracker.is_inflight(piece));
        // Should not be in have
        assert!(!tracker.chunks().is_piece_have(piece));
        // Should be back in queue - verify by trying to reserve it again
        let result2 = tracker.acquire_piece(AcquireRequest {
            peer: peer(2),
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |p| p == piece, // Only has the failed piece
            can_steal: |_| true,
        });

        match result2 {
            AcquireResult::Reserved(p) => assert_eq!(p, piece),
            _ => panic!("Expected piece to be re-reservable after fail"),
        }
    }

    #[test]
    fn test_release_pieces_owned_by_peer() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        let peer_a = peer(1);
        let peer_b = peer(2);

        // Peer A reserves first two pieces (order: 0, 4, 1, 2, 3)
        // So peer A gets pieces 0 and 4
        let piece_a1 = match tracker.acquire_piece(AcquireRequest {
            peer: peer_a,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        }) {
            AcquireResult::Reserved(p) => p,
            _ => panic!("Expected Reserved"),
        };
        let piece_a2 = match tracker.acquire_piece(AcquireRequest {
            peer: peer_a,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        }) {
            AcquireResult::Reserved(p) => p,
            _ => panic!("Expected Reserved"),
        };

        // Peer B reserves next piece
        let piece_b = match tracker.acquire_piece(AcquireRequest {
            peer: peer_b,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        }) {
            AcquireResult::Reserved(p) => p,
            _ => panic!("Expected Reserved"),
        };

        assert_eq!(tracker.inflight_count(), 3);
        assert!(tracker.is_inflight(piece_a1));
        assert!(tracker.is_inflight(piece_a2));
        assert!(tracker.is_inflight(piece_b));

        // Peer A dies
        let released = tracker.release_pieces_owned_by(peer_a);
        assert_eq!(released, 2);
        assert_eq!(tracker.inflight_count(), 1); // Only peer B's piece remains

        // Verify peer B's piece is still in-flight
        assert!(tracker.is_inflight(piece_b));
        // Verify peer A's pieces are no longer in-flight
        assert!(!tracker.is_inflight(piece_a1));
        assert!(!tracker.is_inflight(piece_a2));
    }

    #[test]
    fn test_into_chunks_requeues_inflight() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Reserve pieces 0 and 1
        tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        });
        tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        });

        assert_eq!(tracker.inflight_count(), 2);

        // Convert back to chunks (simulates pause)
        let chunks = tracker.into_chunks();

        // Create a new tracker and verify pieces are back in queue
        let mut new_tracker = PieceTracker::new(chunks);
        let result = new_tracker.acquire_piece(AcquireRequest {
            peer: peer(2),
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        });

        // Should get piece 0 again (was requeued)
        match result {
            AcquireResult::Reserved(p) => assert_eq!(p.get(), 0),
            _ => panic!("Expected to reserve piece 0 after into_chunks"),
        }
    }

    #[test]
    fn test_priority_pieces_checked_first() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Priority pieces: piece 3, then piece 2
        let piece3 = tracker
            .chunks()
            .get_lengths()
            .validate_piece_index(3)
            .unwrap();
        let piece2 = tracker
            .chunks()
            .get_lengths()
            .validate_piece_index(2)
            .unwrap();
        let priority = vec![piece3, piece2];

        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            peer_avg_time: None,
            priority_pieces: priority.into_iter(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        });

        // Should get piece 3 (first priority piece)
        match result {
            AcquireResult::Reserved(p) => assert_eq!(p.get(), 3),
            _ => panic!("Expected Reserved(3), got {:?}", result),
        }
    }

    #[test]
    fn test_none_available_when_no_pieces() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Peer has no pieces
        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| false, // Peer has nothing
            can_steal: |_| true,
        });

        match result {
            AcquireResult::NoneAvailable => {}
            _ => panic!("Expected NoneAvailable, got {:?}", result),
        }
    }

    #[test]
    fn test_take_inflight_nonexistent_piece_returns_none() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);
        let piece = tracker
            .chunks()
            .get_lengths()
            .validate_piece_index(0)
            .unwrap();

        // Try to take a piece that's not in-flight
        let result = tracker.take_inflight(piece);
        assert!(result.is_none());
    }

    #[test]
    fn test_steal_only_pieces_peer_has() {
        // This test verifies the fix for a bug where try_steal didn't check
        // if the stealing peer actually has the piece in their bitfield.
        // Without this check, a peer could "steal" a piece they can't download,
        // leaving it stuck in inflight forever.

        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        let peer_a = peer(1);
        let peer_b = peer(2);

        // Peer A reserves pieces 0 and 4 (first two in iteration order)
        let piece_0 = match tracker.acquire_piece(AcquireRequest {
            peer: peer_a,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        }) {
            AcquireResult::Reserved(p) => {
                assert_eq!(p.get(), 0);
                p
            }
            _ => panic!("Expected Reserved"),
        };

        let piece_4 = match tracker.acquire_piece(AcquireRequest {
            peer: peer_a,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        }) {
            AcquireResult::Reserved(p) => {
                assert_eq!(p.get(), 4);
                p
            }
            _ => panic!("Expected Reserved"),
        };

        // Sleep briefly so pieces become stealable
        std::thread::sleep(Duration::from_millis(5));

        // Peer B tries to acquire with:
        // - Very short avg_time (1ms) so 3x threshold = 3ms < 5ms elapsed
        // - peer_has_piece returns true ONLY for piece 4, NOT piece 0
        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer_b,
            peer_avg_time: Some(Duration::from_millis(1)),
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |p| p.get() == 4, // Peer B only has piece 4
            can_steal: |_| true,
        });

        // Should steal piece 4 (which peer B has), NOT piece 0 (which peer B doesn't have)
        match result {
            AcquireResult::Stolen { piece, from_peer } => {
                assert_eq!(piece, piece_4, "Should steal piece 4 (the one peer B has)");
                assert_eq!(from_peer, peer_a);
                // Verify piece 0 is still owned by peer A (wasn't stolen)
                assert!(tracker.get_inflight(piece_0).unwrap().is_owned_by(peer_a));
            }
            _ => panic!("Expected Stolen, got {:?}", result),
        }
    }
}
