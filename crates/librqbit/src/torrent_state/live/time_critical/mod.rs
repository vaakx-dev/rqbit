// Requesting the pieces right under the read position of the streams, following libtorrent's
// streaming design (https://www.libtorrent.org/streaming.html):
// - The time-critical pieces are visited earliest deadline first. Their missing chunks are spread
//   over the peers with the lowest download queue time, i.e. the peers that will most likely deliver
//   them soonest. The slowest 10% of the peers are never used.
// - A peer's chunk requester sends the chunks it was given before anything else. While some
//   time-critical chunk has nobody to ask, peers keep a single normal request in flight, so that
//   their queues drain and they can take it (like libtorrent 1.2's time-critical mode).
// - When every chunk of a piece is requested and the piece takes longer than the average time-critical
//   piece, its chunks are requested once more from other peers for every time it timed out.
//   Whichever copy arrives first is written, and the requests to the other peers are cancelled.

mod candidates;

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};

use librqbit_core::lengths::{ChunkInfo, Lengths, ValidPieceIndex};
use tracing::{debug, trace};

use crate::{
    piece_tracker::{PieceTimes, PieceTracker, TimeCriticalPiece},
    type_aliases::PeerHandle,
};

use self::candidates::{Candidates, PeersSnapshot};

use super::TorrentStateLive;

// How often to look for late pieces even if nothing else happens.
const RESCHEDULE_INTERVAL: Duration = Duration::from_millis(250);

// Don't queue more on a peer than it would deliver in this time.
const MAX_DOWNLOAD_QUEUE_TIME: Duration = Duration::from_secs(2);

// Normal requests a peer keeps in flight while a time-critical chunk has nobody to ask.
pub(super) const NORMAL_REQUESTS_WHILE_STARVED: usize = 1;

impl TorrentStateLive {
    pub(super) async fn task_time_critical_scheduler(self: Arc<Self>) -> crate::Result<()> {
        loop {
            let deadlines_changed = self.streams.deadlines_changed();
            let woken = self.time_critical_wake.notified();
            if self.request_time_critical_pieces()? {
                tokio::select! {
                    _ = deadlines_changed => {}
                    _ = woken => {}
                    _ = tokio::time::sleep(RESCHEDULE_INTERVAL) => {}
                }
            } else {
                tokio::select! {
                    _ = deadlines_changed => {}
                    _ = woken => {}
                }
            }
        }
    }

    pub(super) fn is_time_critical_starved(&self) -> bool {
        self.time_critical_starved.load(Ordering::Relaxed)
    }

    // Returns false if there's nothing time critical to look after.
    fn request_time_critical_pieces(&self) -> crate::Result<bool> {
        let now = Instant::now();
        let mut critical: Vec<ValidPieceIndex> = Vec::new();
        for piece in self.streams.iter_time_critical_pieces(&self.lengths) {
            if !critical.contains(&piece) {
                critical.push(piece);
            }
        }
        let previously_critical: Vec<ValidPieceIndex> = self
            .lock_read("time_critical_pieces")
            .get_pieces()?
            .iter_time_critical()
            .collect();
        if critical.is_empty() && previously_critical.is_empty() {
            self.set_time_critical_starved(false);
            return Ok(false);
        }

        let watched: HashSet<ValidPieceIndex> = critical
            .iter()
            .chain(previously_critical.iter())
            .copied()
            .collect();
        let PeersSnapshot {
            candidates,
            requested_from,
        } = self.snapshot_peers(&critical, &watched, now);

        let (plan, released_any) = {
            let mut g = self.lock_write("request_time_critical_pieces");
            let pieces = g.get_pieces_mut()?;
            let released_any =
                release_abandoned(pieces, &critical, &previously_critical, &requested_from);
            let mut plan = Plan {
                now,
                timeout: self.time_critical_timeout(pieces.time_critical_times()),
                candidates,
                requested_from,
                assignments: Vec::new(),
                starved: false,
            };
            plan.request(pieces, &critical, &self.lengths);
            (plan, released_any)
        };

        debug!(
            critical = critical.len(),
            requests = plan.assignments.len(),
            starved = plan.starved,
            best_queue_time = ?plan.candidates.best().map(|c| c.download_queue_time()),
            "requested time-critical chunks"
        );
        self.set_time_critical_starved(plan.starved);
        if released_any {
            self.new_pieces_notify.notify_waiters();
        }
        for (peer, chunk) in plan.assignments {
            trace!(?peer, ?chunk, "time-critical request");
            self.peers
                .with_live_mut(peer, "push_time_critical_request", |live| {
                    live.push_time_critical_request(chunk)
                });
        }
        Ok(true)
    }

    // When a piece counts as late. Until a time-critical piece completed, the average of all pieces
    // is the best guess.
    fn time_critical_timeout(&self, times: PieceTimes) -> Duration {
        if !times.average.is_zero() {
            return times.average + times.deviation / 2;
        }
        let pieces = self
            .stats
            .downloaded_and_checked_pieces
            .load(Ordering::Relaxed);
        let total_ms = self.stats.total_piece_download_ms.load(Ordering::Relaxed);
        if pieces == 0 || total_ms == 0 {
            return MAX_DOWNLOAD_QUEUE_TIME;
        }
        Duration::from_millis(total_ms / pieces)
    }

    fn set_time_critical_starved(&self, starved: bool) {
        let was_starved = self.time_critical_starved.swap(starved, Ordering::Relaxed);
        if was_starved && !starved {
            self.new_pieces_notify.notify_waiters();
        }
    }
}

// Pieces the streams moved away from go back to the queue once nothing is in flight for them.
fn release_abandoned(
    pieces: &mut PieceTracker,
    critical: &[ValidPieceIndex],
    previously_critical: &[ValidPieceIndex],
    requested_from: &HashMap<ChunkInfo, Vec<PeerHandle>>,
) -> bool {
    let mut released_any = false;
    for piece in previously_critical.iter().filter(|p| !critical.contains(p)) {
        if !requested_from.keys().any(|c| c.piece_index == *piece) {
            pieces.release_time_critical(*piece);
            released_any = true;
        }
    }
    released_any
}

enum Pick {
    Chunk { chunk: ChunkInfo, busy: bool },
    PeerAskedForAll,
    Nothing,
}

struct Plan {
    now: Instant,
    timeout: Duration,
    candidates: Candidates,
    requested_from: HashMap<ChunkInfo, Vec<PeerHandle>>,
    assignments: Vec<(PeerHandle, ChunkInfo)>,
    starved: bool,
}

impl Plan {
    fn request(
        &mut self,
        pieces: &mut PieceTracker,
        critical: &[ValidPieceIndex],
        lengths: &Lengths,
    ) {
        // Taking all of them right away keeps peers from reserving them whole.
        let mut requestable: Vec<(usize, ValidPieceIndex, Vec<ChunkInfo>)> = Vec::new();
        for (critical_idx, piece) in critical.iter().copied().enumerate() {
            if pieces.make_time_critical(piece) {
                let missing = lengths
                    .iter_chunk_infos(piece)
                    .filter(|c| !pieces.chunks().is_chunk_downloaded(c))
                    .collect();
                requestable.push((critical_idx, piece, missing));
            }
        }

        for (urgency, (critical_idx, piece, missing)) in requestable.iter().enumerate() {
            // The most urgent piece is always requested, the next ones only while some peer
            // can deliver them in time.
            let uncapped = urgency == 0;
            if !self.has_peer_in_time(uncapped) {
                break;
            }
            if let Some(tc) = pieces.get_time_critical_mut(*piece) {
                self.request_piece(tc, *critical_idx, missing, uncapped);
            }
        }

        self.starved = requestable
            .iter()
            .any(|(_, _, missing)| missing.iter().any(|c| !self.requested_from.contains_key(c)));
    }

    fn has_peer_in_time(&self, uncapped: bool) -> bool {
        self.candidates
            .best()
            .is_some_and(|c| uncapped || c.download_queue_time() <= MAX_DOWNLOAD_QUEUE_TIME)
    }

    fn request_piece(
        &mut self,
        tc: &mut TimeCriticalPiece,
        critical_idx: usize,
        missing: &[ChunkInfo],
        uncapped: bool,
    ) {
        let any_free = missing.iter().any(|c| !self.requested_from.contains_key(c));
        let timed_out = if any_free {
            0
        } else {
            let last = *tc.last_requested.get_or_insert(self.now);
            let waited = self.now.saturating_duration_since(last).as_millis();
            usize::try_from(waited / self.timeout.as_millis().max(1)).unwrap_or(usize::MAX)
        };
        if !any_free && timed_out == 0 {
            return;
        }

        let mut ignored: Vec<PeerHandle> = Vec::new();
        while self.has_peer_in_time(uncapped) {
            let Some(pos) = self.candidates.best_with_piece(critical_idx, &ignored) else {
                return;
            };
            let peer = self.candidates.get(pos).addr;
            let (chunk, busy) = match self.pick_chunk(missing, peer, timed_out) {
                Pick::Chunk { chunk, busy } => (chunk, busy),
                Pick::PeerAskedForAll => {
                    ignored.push(peer);
                    continue;
                }
                Pick::Nothing => return,
            };

            self.requested_from.entry(chunk).or_default().push(peer);
            self.assignments.push((peer, chunk));
            self.candidates.on_requested(pos);
            tc.first_requested.get_or_insert(self.now);
            if busy {
                tc.busy = true;
            } else {
                tc.last_requested = Some(self.now);
            }
        }
    }

    fn pick_chunk(&self, missing: &[ChunkInfo], peer: PeerHandle, timed_out: usize) -> Pick {
        if let Some(chunk) = missing
            .iter()
            .find(|c| !self.requested_from.contains_key(c))
        {
            return Pick::Chunk {
                chunk: *chunk,
                busy: false,
            };
        }

        // Ask once more for the chunks asked from the fewest peers, never twice from the same peer.
        let mut stalled = missing
            .iter()
            .filter_map(|c| Some((c, self.requested_from.get(c)?)))
            .filter(|(_, peers)| peers.len() <= timed_out)
            .peekable();
        if stalled.peek().is_none() {
            return Pick::Nothing;
        }
        match stalled
            .filter(|(_, peers)| !peers.contains(&peer))
            .min_by_key(|(_, peers)| peers.len())
        {
            Some((chunk, _)) => Pick::Chunk {
                chunk: *chunk,
                busy: true,
            },
            None => Pick::PeerAskedForAll,
        }
    }
}
