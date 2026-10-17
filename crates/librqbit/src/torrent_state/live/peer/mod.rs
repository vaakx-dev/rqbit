pub mod download_rate;
pub mod stats;

use std::collections::{HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, atomic::Ordering};
use std::time::Instant;

use librqbit_core::hash_id::Id20;
use librqbit_core::lengths::{ChunkInfo, ValidPieceIndex};
use peer_binary_protocol::{Message, Request};

use tokio::sync::{
    Notify,
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
};
use tracing::debug;

use crate::peer_connection::WriterRequest;
use crate::stream_connect::ConnectionKind;
use crate::type_aliases::BF;

use self::download_rate::DownloadRate;
use super::PeerStates;

pub(crate) const DEFAULT_PEER_REQUEST_WINDOW: usize = 128;

pub(crate) type InflightRequest = ChunkInfo;
pub(crate) type PeerRx = UnboundedReceiver<WriterRequest>;
pub(crate) type PeerTx = UnboundedSender<WriterRequest>;

#[derive(Debug)]
pub(crate) struct Peer {
    pub addr: SocketAddr,
    state: PeerState,
    pub stats: stats::atomic::PeerStats,
    pub outgoing_address: Option<SocketAddr>,
}

impl Peer {
    pub fn new_live_for_incoming_connection(
        addr: SocketAddr,
        peer_id: Id20,
        tx: PeerTx,
        counters: &PeerStates,
        connection_kind: ConnectionKind,
    ) -> Self {
        let state = PeerState::Live(LivePeerState::new(peer_id, tx, true, connection_kind));
        for counter in [&counters.session_stats, &counters.stats] {
            counter.inc(&state);
        }
        Self {
            addr,
            state,
            stats: Default::default(),
            outgoing_address: None,
        }
    }

    pub fn new_with_outgoing_address(addr: SocketAddr) -> Self {
        Self {
            addr,
            outgoing_address: Some(addr),
            stats: Default::default(),
            state: Default::default(),
        }
    }

    pub(crate) fn reconnect_not_needed_peer(
        &mut self,
        counters: &PeerStates,
    ) -> Option<SocketAddr> {
        if let PeerState::NotNeeded = self.get_state() {
            match self.outgoing_address {
                None => None,
                Some(socket_addr) if self.addr == socket_addr => {
                    self.set_state(PeerState::Queued, counters);
                    Some(socket_addr)
                }
                Some(socket_addr) => {
                    debug!(
                        peer = %self.addr,
                        outgoing_addr = %socket_addr,
                        "peer will by retried on different address",
                    );
                    Some(socket_addr)
                }
            }
        } else {
            None
        }
    }
}

#[derive(Debug, Default)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum PeerState {
    #[default]
    // Will be tried to be connected as soon as possible.
    Queued,
    Connecting(PeerTx),
    Live(LivePeerState),
    // There was an error, and it's waiting for exponential backoff.
    Dead,
    // We don't need to do anything with the peer any longer.
    // The peer has the full torrent, and we have the full torrent, so no need
    // to keep talking to it.
    NotNeeded,
}

impl std::fmt::Display for PeerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl PeerState {
    pub fn name(&self) -> &'static str {
        match self {
            PeerState::Queued => "queued",
            PeerState::Connecting(_) => "connecting",
            PeerState::Live(_) => "live",
            PeerState::Dead => "dead",
            PeerState::NotNeeded => "not needed",
        }
    }

    pub fn take_live_no_counters(self) -> Option<LivePeerState> {
        match self {
            PeerState::Live(l) => Some(l),
            _ => None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IncomingConnectionResult {
    #[error("peer already active")]
    AlreadyActive,
}

impl Peer {
    pub fn get_state(&self) -> &PeerState {
        &self.state
    }

    pub fn take_state(&mut self, counters: &PeerStates) -> PeerState {
        self.set_state(Default::default(), counters)
    }

    pub fn destroy(self, counters: &PeerStates) {
        for counter in [&counters.session_stats, &counters.stats] {
            counter.dec(&self.state);
        }
        if let (Some(addr), PeerState::Live(..)) = (self.outgoing_address, &self.state) {
            counters.live_outgoing_peers.write().remove(&addr);
        }
    }

    pub fn set_state(&mut self, new: PeerState, counters: &PeerStates) -> PeerState {
        for counter in [&counters.session_stats, &counters.stats] {
            counter.incdec(&self.state, &new);
        }
        if let Some(addr) = self.outgoing_address {
            if matches!(&self.state, PeerState::Live(..)) {
                counters.live_outgoing_peers.write().remove(&addr);
            }
            if matches!(&new, PeerState::Live(..))
                && self
                    .stats
                    .counters
                    .outgoing_connections
                    .load(Ordering::Relaxed)
                    > 0
            {
                counters.live_outgoing_peers.write().insert(addr);
            }
        }

        std::mem::replace(&mut self.state, new)
    }

    pub fn get_live(&self) -> Option<&LivePeerState> {
        match &self.state {
            PeerState::Live(l) => Some(l),
            _ => None,
        }
    }

    pub fn get_live_mut(&mut self) -> Option<&mut LivePeerState> {
        match &mut self.state {
            PeerState::Live(l) => Some(l),
            _ => None,
        }
    }

    pub fn idle_to_connecting(&mut self, counters: &PeerStates) -> Option<(PeerRx, PeerTx)> {
        match &self.state {
            PeerState::Queued | PeerState::NotNeeded => {
                let (tx, rx) = unbounded_channel();
                let tx_2 = tx.clone();
                self.set_state(PeerState::Connecting(tx), counters);
                Some((rx, tx_2))
            }
            _ => None,
        }
    }

    pub fn incoming_connection(
        &mut self,
        peer_id: Id20,
        tx: PeerTx,
        counters: &PeerStates,
        connection_kind: ConnectionKind,
    ) -> Result<(), IncomingConnectionResult> {
        if matches!(&self.state, PeerState::Connecting(..) | PeerState::Live(..)) {
            return Err(IncomingConnectionResult::AlreadyActive);
        }
        match self.take_state(counters) {
            PeerState::Queued | PeerState::Dead | PeerState::NotNeeded => {
                self.set_state(
                    PeerState::Live(LivePeerState::new(peer_id, tx, true, connection_kind)),
                    counters,
                );
            }
            PeerState::Connecting(..) | PeerState::Live(..) => unreachable!(),
        }
        Ok(())
    }

    pub fn connecting_to_live(
        &mut self,
        peer_id: Id20,
        counters: &PeerStates,
        conn_kind: ConnectionKind,
    ) -> Option<&mut LivePeerState> {
        if let PeerState::Connecting(_) = &self.state {
            let tx = match self.take_state(counters) {
                PeerState::Connecting(tx) => tx,
                _ => unreachable!(),
            };
            self.set_state(
                PeerState::Live(LivePeerState::new(peer_id, tx, false, conn_kind)),
                counters,
            );
            self.get_live_mut()
        } else {
            None
        }
    }

    pub fn set_not_needed(&mut self, counters: &PeerStates) -> PeerState {
        self.set_state(PeerState::NotNeeded, counters)
    }
}

#[derive(Debug)]
pub(crate) struct LivePeerState {
    #[allow(dead_code)]
    peer_id: Id20,

    pub client_name: Option<String>,

    pub peer_interested: bool,

    // This is used to track the pieces the peer has.
    pub bitfield: BF,

    // When the peer sends us data this is used to track if we asked for it.
    inflight_requests: HashSet<InflightRequest>,

    // Chunks of time-critical pieces the scheduler gave to this peer, sent before anything else.
    time_critical_requests: VecDeque<ChunkInfo>,
    time_critical_requests_added: Arc<Notify>,

    pub i_am_choked: bool,
    pub request_window: usize,
    pub download_rate: DownloadRate,

    // Bounded tolerance for chunks that arrive after we cancel requests.
    // This is intentionally approximate: we track a count instead of storing every
    // canceled chunk, so a peer is not disconnected for racing our cancel messages.
    late_cancelled_request_tolerance: u32,

    request_slots_changed: Arc<Notify>,

    // The main channel to send requests to peer.
    pub tx: PeerTx,

    pub connection_kind: ConnectionKind,
}

impl LivePeerState {
    pub fn new(
        peer_id: Id20,
        tx: PeerTx,
        initial_interested: bool,
        connection_kind: ConnectionKind,
    ) -> Self {
        LivePeerState {
            peer_id,
            client_name: None,
            peer_interested: initial_interested,
            bitfield: BF::default(),
            inflight_requests: Default::default(),
            time_critical_requests: Default::default(),
            time_critical_requests_added: Default::default(),
            i_am_choked: true,
            request_window: DEFAULT_PEER_REQUEST_WINDOW,
            download_rate: Default::default(),
            late_cancelled_request_tolerance: 0,
            request_slots_changed: Default::default(),
            tx,
            connection_kind,
        }
    }

    pub fn has_full_torrent(&self, total_pieces: usize) -> bool {
        self.bitfield.get(0..total_pieces).is_some_and(|s| s.all())
    }

    pub fn request_slots_changed(&self) -> Arc<Notify> {
        self.request_slots_changed.clone()
    }

    pub fn can_send_request(&self) -> bool {
        !self.i_am_choked && self.inflight_requests.len() < self.request_window
    }

    // Requests sent and not answered yet plus the time-critical ones waiting to be sent.
    pub fn outstanding_requests(&self) -> usize {
        self.inflight_requests.len() + self.time_critical_requests.len()
    }

    pub fn has_requested(&self, chunk: &ChunkInfo) -> bool {
        self.inflight_requests.contains(chunk) || self.time_critical_requests.contains(chunk)
    }

    pub fn add_inflight_request(&mut self, chunk: ChunkInfo) -> bool {
        self.inflight_requests.insert(chunk)
    }

    pub fn push_time_critical_request(&mut self, chunk: ChunkInfo) {
        self.time_critical_requests.push_back(chunk);
        self.time_critical_requests_added.notify_waiters();
    }

    pub fn time_critical_requests_added(&self) -> Arc<Notify> {
        self.time_critical_requests_added.clone()
    }

    pub fn pop_time_critical_request(&mut self) -> Option<ChunkInfo> {
        self.time_critical_requests.pop_front()
    }

    // Returns true if there were any, so that the scheduler gives them to other peers.
    pub fn clear_time_critical_requests(&mut self) -> bool {
        let had_any = !self.time_critical_requests.is_empty();
        self.time_critical_requests.clear();
        had_any
    }

    pub fn iter_outstanding_requests(&self) -> impl Iterator<Item = &ChunkInfo> {
        self.inflight_requests
            .iter()
            .chain(self.time_critical_requests.iter())
    }

    pub fn remove_inflight_request(&mut self, chunk: &ChunkInfo) -> RemoveInflightRequestResult {
        if self.inflight_requests.remove(chunk) {
            self.download_rate.on_payload(chunk.size, Instant::now());
            self.request_slots_changed.notify_waiters();
            return RemoveInflightRequestResult::Expected;
        }

        if self.late_cancelled_request_tolerance > 0 {
            // This may be any unexpected chunk, not necessarily the exact canceled
            // one, but the tolerance is bounded by cancels we sent.
            self.late_cancelled_request_tolerance -= 1;
            RemoveInflightRequestResult::LateCanceled
        } else {
            RemoveInflightRequestResult::Unexpected
        }
    }

    pub fn cancel_inflight_requests_for_piece(&mut self, piece: ValidPieceIndex) {
        let tx = &self.tx;
        let late_cancelled_request_tolerance = &mut self.late_cancelled_request_tolerance;
        let before = self.inflight_requests.len();
        self.inflight_requests.retain(|req| {
            if req.piece_index == piece {
                let _ = tx.send(WriterRequest::Message(Message::Cancel(Request {
                    index: piece.get(),
                    begin: req.offset,
                    length: req.size,
                })));
                *late_cancelled_request_tolerance += 1;
                false
            } else {
                true
            }
        });

        if self.inflight_requests.len() != before {
            self.request_slots_changed.notify_waiters();
        }
    }

    // Drop a duplicate request for a chunk that another peer delivered.
    pub fn cancel_request(&mut self, chunk: &ChunkInfo) {
        if let Some(pos) = self.time_critical_requests.iter().position(|c| c == chunk) {
            self.time_critical_requests.remove(pos);
            return;
        }
        if self.inflight_requests.remove(chunk) {
            let _ = self
                .tx
                .send(WriterRequest::Message(Message::Cancel(Request {
                    index: chunk.piece_index.get(),
                    begin: chunk.offset,
                    length: chunk.size,
                })));
            self.late_cancelled_request_tolerance += 1;
            self.request_slots_changed.notify_waiters();
        }
    }

    pub fn inflight_requests(&self) -> impl Iterator<Item = &InflightRequest> {
        self.inflight_requests.iter()
    }

    pub fn inflight_requests_debug(&self) -> &HashSet<InflightRequest> {
        &self.inflight_requests
    }
}

pub(crate) enum RemoveInflightRequestResult {
    Expected,
    LateCanceled,
    Unexpected,
}
