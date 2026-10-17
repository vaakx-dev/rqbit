use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use librqbit_core::{
    constants::CHUNK_SIZE,
    lengths::{ChunkInfo, ValidPieceIndex},
};

use crate::type_aliases::PeerHandle;

use super::super::TorrentStateLive;

// Rate assumed for a peer we know nothing about, if no other peer sends anything either.
const MIN_RATE_BYTES_PER_SECOND: f64 = 50.;

pub(super) struct Candidate {
    pub addr: PeerHandle,
    bytes_per_second: f64,
    outstanding: usize,
    max_outstanding: usize,
    has_piece: Vec<bool>,
}

impl Candidate {
    // The estimated time until this peer starts sending what we ask it for now.
    pub fn download_queue_time(&self) -> Duration {
        Duration::from_secs_f64(
            (self.outstanding as f64 * CHUNK_SIZE as f64) / self.bytes_per_second,
        )
    }
}

/// The peers time-critical chunks can be requested from, the lowest download queue time first.
pub(super) struct Candidates(Vec<Candidate>);

impl Candidates {
    pub fn best(&self) -> Option<&Candidate> {
        self.0.first()
    }

    pub fn best_with_piece(&self, critical_idx: usize, ignored: &[PeerHandle]) -> Option<usize> {
        self.0
            .iter()
            .position(|c| c.has_piece[critical_idx] && !ignored.contains(&c.addr))
    }

    pub fn get(&self, pos: usize) -> &Candidate {
        &self.0[pos]
    }

    pub fn on_requested(&mut self, mut pos: usize) {
        let candidate = &mut self.0[pos];
        candidate.outstanding += 1;
        if candidate.outstanding >= candidate.max_outstanding {
            self.0.remove(pos);
            return;
        }
        while pos + 1 < self.0.len()
            && self.0[pos].download_queue_time() > self.0[pos + 1].download_queue_time()
        {
            self.0.swap(pos, pos + 1);
            pos += 1;
        }
    }
}

pub(super) struct PeersSnapshot {
    pub candidates: Candidates,
    // Who was asked for the chunks of the watched pieces, including requests not sent yet.
    pub requested_from: HashMap<ChunkInfo, Vec<PeerHandle>>,
}

impl TorrentStateLive {
    pub(super) fn snapshot_peers(
        &self,
        critical: &[ValidPieceIndex],
        watched: &HashSet<ValidPieceIndex>,
        now: Instant,
    ) -> PeersSnapshot {
        let mut requested_from: HashMap<ChunkInfo, Vec<PeerHandle>> = HashMap::new();
        let mut candidates = Vec::new();
        let mut unknown_rate = Vec::new();

        for peer in self.peers.states.iter() {
            let Some(live) = peer.value().get_live() else {
                continue;
            };
            for chunk in live
                .iter_outstanding_requests()
                .filter(|c| watched.contains(&c.piece_index))
            {
                requested_from.entry(*chunk).or_default().push(*peer.key());
            }

            // Like libtorrent, skip peers that choked us or are already far behind.
            let max_outstanding = live.request_window * 2;
            if live.i_am_choked
                || live.bitfield.is_empty()
                || live.outstanding_requests() >= max_outstanding
            {
                continue;
            }
            let rate = live.download_rate.estimate(now);
            if rate.is_none() {
                unknown_rate.push(candidates.len());
            }
            candidates.push(Candidate {
                addr: *peer.key(),
                bytes_per_second: rate.unwrap_or_default(),
                outstanding: live.outstanding_requests(),
                max_outstanding,
                has_piece: critical
                    .iter()
                    .map(|p| live.bitfield.get(p.get_usize()).is_some_and(|b| *b))
                    .collect(),
            });
        }

        // A peer that hasn't sent anything yet is assumed to be as fast as the average one.
        let known = candidates.len() - unknown_rate.len();
        let average = if known > 0 {
            candidates.iter().map(|c| c.bytes_per_second).sum::<f64>() / known as f64
        } else {
            self.down_speed_estimator.bps() as f64 / candidates.len().max(1) as f64
        };
        for idx in unknown_rate {
            candidates[idx].bytes_per_second = average;
        }
        for candidate in candidates.iter_mut() {
            candidate.bytes_per_second = candidate.bytes_per_second.max(MIN_RATE_BYTES_PER_SECOND);
        }

        // The slowest 10% are left out, they would only stall the stream.
        candidates.sort_by_key(|c| c.download_queue_time());
        candidates.truncate((candidates.len() * 9).div_ceil(10));

        PeersSnapshot {
            candidates: Candidates(candidates),
            requested_from,
        }
    }
}
