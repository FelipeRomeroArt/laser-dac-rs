//! IDN DAC streaming backend implementation.

use crate::backend::{DacBackend, FifoBackend, WriteOutcome};
use crate::buffer_estimate::{BufferEstimator, SoftwareDecayEstimator};
use crate::device::{DacCapabilities, DacType};
use crate::error::{Error, Result};
use crate::point::LaserPoint;
use crate::protocols::idn::dac::stream::PointFormat;
use crate::protocols::idn::dac::{stream, ServerInfo, ServiceInfo};
use crate::protocols::idn::protocol::{PointExtended, PointXyrgbHighRes, PointXyrgbi};
use crossbeam_queue::ArrayQueue;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const IDN_WORKER_QUEUE_CAPACITY: usize = 8;

/// How often the worker upgrades a data send to an ACK request (or, while
/// idle, sends a ping) to prove the device is still alive.
const ACK_CHECK_INTERVAL: Duration = Duration::from_millis(500);

/// How long to wait for an ACK/ping response before counting it as a miss.
const ACK_TIMEOUT: Duration = Duration::from_millis(100);

/// Consecutive ACK/ping misses after which the device is considered dead and
/// the backend is marked disconnected so the driver's reconnect path engages.
const MAX_CONSECUTIVE_ACK_MISSES: u32 = 4;

/// Reachability check timeout used at `connect()`.
const CONNECT_PING_TIMEOUT: Duration = Duration::from_millis(300);

/// Number of extra ping retries at `connect()` before giving up.
const CONNECT_PING_RETRIES: u32 = 2;

struct WorkerRuntime {
    tx: mpsc::SyncSender<WorkerCommand>,
    connected: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// A chunk of converted points in the backend's selected wire format.
enum ChunkPoints {
    Xyrgbi(Vec<PointXyrgbi>),
    HighRes(Vec<PointXyrgbHighRes>),
    Extended(Vec<PointExtended>),
}

impl ChunkPoints {
    fn len(&self) -> usize {
        match self {
            ChunkPoints::Xyrgbi(v) => v.len(),
            ChunkPoints::HighRes(v) => v.len(),
            ChunkPoints::Extended(v) => v.len(),
        }
    }
}

struct QueuedChunk {
    pps: u32,
    points: ChunkPoints,
}

/// Bounded free-list of conversion buffers shared between the scheduler
/// thread (producer) and the worker thread (consumer). The producer takes a
/// buffer, fills it, and hands it to the worker inside a [`QueuedChunk`]; the
/// worker returns the drained buffer after sending so the next chunk reuses
/// its capacity instead of growing a fresh allocation on the hot path.
struct ChunkBufferPool {
    xyrgbi: ArrayQueue<Vec<PointXyrgbi>>,
    high_res: ArrayQueue<Vec<PointXyrgbHighRes>>,
    extended: ArrayQueue<Vec<PointExtended>>,
}

impl ChunkBufferPool {
    /// Sized to cover the full worker queue plus in-flight chunks, so in the
    /// steady state buffers are always recycled rather than dropped.
    const CAPACITY: usize = 16;

    fn new() -> Self {
        Self {
            xyrgbi: ArrayQueue::new(Self::CAPACITY),
            high_res: ArrayQueue::new(Self::CAPACITY),
            extended: ArrayQueue::new(Self::CAPACITY),
        }
    }

    fn take_xyrgbi(&self) -> Vec<PointXyrgbi> {
        self.xyrgbi.pop().unwrap_or_default()
    }

    fn take_high_res(&self) -> Vec<PointXyrgbHighRes> {
        self.high_res.pop().unwrap_or_default()
    }

    fn take_extended(&self) -> Vec<PointExtended> {
        self.extended.pop().unwrap_or_default()
    }

    /// Return a sent chunk's backing buffer to the pool. Dropping it when the
    /// pool is full is fine: that allocation is simply recreated on a later
    /// take.
    fn recycle(&self, points: &mut ChunkPoints) {
        match points {
            ChunkPoints::Xyrgbi(v) => {
                v.clear();
                let _ = self.xyrgbi.push(std::mem::take(v));
            }
            ChunkPoints::HighRes(v) => {
                v.clear();
                let _ = self.high_res.push(std::mem::take(v));
            }
            ChunkPoints::Extended(v) => {
                v.clear();
                let _ = self.extended.push(std::mem::take(v));
            }
        }
    }
}

enum WorkerCommand {
    Chunk(QueuedChunk),
    Stop,
    Shutdown,
}

trait WorkerStream {
    fn scan_speed(&self) -> u32;
    fn set_scan_speed(&mut self, pps: u32);
    fn needs_keepalive(&self) -> bool;
    fn send_keepalive(&mut self) -> bool;
    fn write_frame(&mut self, points: &ChunkPoints) -> bool;
    /// Send the frame requesting an acknowledgment; returns whether the ACK
    /// was received within `timeout` (used for liveness detection).
    fn write_frame_with_ack(&mut self, points: &ChunkPoints, timeout: Duration) -> bool;
    /// Send a ping and report whether a response arrived within `timeout`.
    fn ping(&mut self, timeout: Duration) -> bool;
    fn close(&mut self);
}

impl WorkerStream for stream::Stream {
    fn scan_speed(&self) -> u32 {
        stream::Stream::scan_speed(self)
    }

    fn set_scan_speed(&mut self, pps: u32) {
        stream::Stream::set_scan_speed(self, pps);
    }

    fn needs_keepalive(&self) -> bool {
        stream::Stream::needs_keepalive(self)
    }

    fn send_keepalive(&mut self) -> bool {
        stream::Stream::send_keepalive(self).is_ok()
    }

    fn write_frame(&mut self, points: &ChunkPoints) -> bool {
        match points {
            ChunkPoints::Xyrgbi(v) => stream::Stream::write_frame(self, v).is_ok(),
            ChunkPoints::HighRes(v) => stream::Stream::write_frame(self, v).is_ok(),
            ChunkPoints::Extended(v) => stream::Stream::write_frame(self, v).is_ok(),
        }
    }

    fn write_frame_with_ack(&mut self, points: &ChunkPoints, timeout: Duration) -> bool {
        match points {
            ChunkPoints::Xyrgbi(v) => {
                stream::Stream::write_frame_with_ack(self, v, timeout).is_ok()
            }
            ChunkPoints::HighRes(v) => {
                stream::Stream::write_frame_with_ack(self, v, timeout).is_ok()
            }
            ChunkPoints::Extended(v) => {
                stream::Stream::write_frame_with_ack(self, v, timeout).is_ok()
            }
        }
    }

    fn ping(&mut self, timeout: Duration) -> bool {
        stream::Stream::ping(self, timeout).is_ok()
    }

    fn close(&mut self) {
        let _ = stream::Stream::close(self);
    }
}

/// IDN DAC backend (ILDA Digital Network).
pub struct IdnBackend {
    server: ServerInfo,
    service: ServiceInfo,
    runtime: Option<WorkerRuntime>,
    caps: DacCapabilities,
    /// Free-list of conversion buffers recycled with the worker thread; see
    /// [`ChunkBufferPool`]. Keeps the per-chunk write path allocation-free in
    /// the steady state for every point format.
    pool: Arc<ChunkBufferPool>,
    /// Software-only buffer estimator. Driven by `record_send` from inside
    /// `try_write_points`; not yet consulted by the adapter (Phase 1).
    estimator: SoftwareDecayEstimator,
    /// Wire point format. Defaults to [`PointFormat::Xyrgbi`] (8-bit colour);
    /// callers can opt into a 16-bit format via [`IdnBackend::set_point_format`]
    /// before connecting.
    point_format: PointFormat,
}

impl IdnBackend {
    pub fn new(server: ServerInfo, service: ServiceInfo) -> Self {
        Self {
            server,
            service,
            runtime: None,
            caps: super::default_capabilities(),
            pool: Arc::new(ChunkBufferPool::new()),
            estimator: SoftwareDecayEstimator::new(),
            point_format: PointFormat::Xyrgbi,
        }
    }

    /// Select the wire point format for streaming.
    ///
    /// Defaults to [`PointFormat::Xyrgbi`] (8-bit RGBI). Selecting
    /// [`PointFormat::XyrgbHighRes`] or [`PointFormat::Extended`] streams the
    /// crate's full 16-bit colour depth end-to-end, at the cost of larger
    /// samples (10 or 20 bytes vs 8) and therefore more packets per frame.
    ///
    /// Only receivers that advertise support for the chosen descriptors will
    /// render hi-res formats correctly, so the default is left at the
    /// universally understood XYRGBI format. Must be called **before**
    /// [`connect`](crate::backend::DacBackend::connect); changing it on a live
    /// connection has no effect until the next connect.
    pub fn set_point_format(&mut self, format: PointFormat) {
        self.point_format = format;
        self.caps.max_points_per_chunk = max_points_per_chunk_for(format);
    }

    /// The currently selected wire point format.
    pub fn point_format(&self) -> PointFormat {
        self.point_format
    }

    /// Convert `points` into the selected wire format, reusing a pooled buffer
    /// when one is available, and defensively clamp `pps` to the device's
    /// supported rate range. [`SessionControl::set_pps`](crate::SessionControl::set_pps)
    /// validates runtime changes; this backend boundary remains a final guard
    /// for direct/internal callers (mirroring the other backends' clamping).
    fn build_chunk(&self, pps: u32, points: &[LaserPoint]) -> QueuedChunk {
        // Mirror `clamp_point_rate` in the lasercube-network backend: floor at
        // the device minimum (at least 1) as well as capping at the maximum.
        let pps = pps.clamp(self.caps.pps_min.max(1), self.caps.pps_max);
        let chunk_points = match self.point_format {
            PointFormat::Xyrgbi => {
                let mut buf = self.pool.take_xyrgbi();
                buf.clear();
                buf.extend(points.iter().map(PointXyrgbi::from));
                ChunkPoints::Xyrgbi(buf)
            }
            PointFormat::XyrgbHighRes => {
                let mut buf = self.pool.take_high_res();
                buf.clear();
                buf.extend(points.iter().map(PointXyrgbHighRes::from));
                ChunkPoints::HighRes(buf)
            }
            PointFormat::Extended => {
                let mut buf = self.pool.take_extended();
                buf.clear();
                buf.extend(points.iter().map(PointExtended::from));
                ChunkPoints::Extended(buf)
            }
        };
        QueuedChunk {
            pps,
            points: chunk_points,
        }
    }
}

/// Points that fit in one MTU-sized datagram (without a config header) for a
/// given format, used to keep the scheduler's chunk sizing aligned with the
/// sample size.
fn max_points_per_chunk_for(format: PointFormat) -> usize {
    use crate::protocols::idn::protocol::{
        ChannelMessageHeader, PacketHeader, SampleChunkHeader, SizeBytes, MAX_UDP_PAYLOAD,
    };
    let header =
        PacketHeader::SIZE_BYTES + ChannelMessageHeader::SIZE_BYTES + SampleChunkHeader::SIZE_BYTES;
    (MAX_UDP_PAYLOAD - header) / format.size_bytes()
}

impl DacBackend for IdnBackend {
    fn dac_type(&self) -> DacType {
        DacType::Idn
    }

    fn caps(&self) -> &DacCapabilities {
        &self.caps
    }

    fn connect(&mut self) -> Result<()> {
        if self.runtime.is_some() {
            return Ok(());
        }

        let mut stream =
            stream::connect(&self.server, self.service.service_id).map_err(Error::backend)?;
        stream.set_frame_mode(stream::FrameMode::Wave);
        stream.set_point_format(self.point_format);

        // Validate reachability before committing: a plain UDP connect always
        // "succeeds", so without this a dead address would appear connected and
        // stream into the void. A ping (short timeout, a couple of retries)
        // confirms the server is actually answering.
        let mut reachable = false;
        for attempt in 0..=CONNECT_PING_RETRIES {
            match stream.ping(CONNECT_PING_TIMEOUT) {
                Ok(_) => {
                    reachable = true;
                    break;
                }
                Err(e) => log::debug!("idn connect: ping attempt {} failed: {}", attempt, e),
            }
        }
        if !reachable {
            return Err(Error::disconnected(
                "IDN server did not respond to ping at connect",
            ));
        }

        // Fresh session: reset the estimator so stale decay state from a prior
        // connection does not bias admission.
        self.estimator.reset(Instant::now());

        let (tx, rx) = mpsc::sync_channel(IDN_WORKER_QUEUE_CAPACITY);
        let connected = Arc::new(AtomicBool::new(true));
        let worker_connected = Arc::clone(&connected);
        let point_format = self.point_format;
        let pool = Arc::clone(&self.pool);
        let handle =
            thread::spawn(move || worker_loop(stream, rx, worker_connected, point_format, pool));

        self.runtime = Some(WorkerRuntime {
            tx,
            connected,
            handle: Some(handle),
        });
        Ok(())
    }

    fn disconnect(&mut self) -> Result<()> {
        if let Some(mut runtime) = self.runtime.take() {
            let _ = runtime.tx.send(WorkerCommand::Shutdown);
            if let Some(handle) = runtime.handle.take() {
                let _ = handle.join();
            }
        }
        self.estimator.reset(Instant::now());
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.runtime
            .as_ref()
            .map(|runtime| runtime.connected.load(Ordering::Acquire))
            .unwrap_or(false)
    }

    fn stop(&mut self) -> Result<()> {
        if let Some(runtime) = &self.runtime {
            let _ = runtime.tx.send(WorkerCommand::Stop);
        }
        self.estimator.reset(Instant::now());
        Ok(())
    }

    fn set_shutter(&mut self, _open: bool) -> Result<()> {
        Ok(())
    }
}

impl FifoBackend for IdnBackend {
    fn try_write_points(&mut self, pps: u32, points: &[LaserPoint]) -> Result<WriteOutcome> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| Error::disconnected("Not connected"))?;

        if !runtime.connected.load(Ordering::Acquire) {
            return Err(Error::disconnected("IDN sender thread disconnected"));
        }

        let n = points.len();
        let chunk = self.build_chunk(pps, points);

        match runtime.tx.try_send(WorkerCommand::Chunk(chunk)) {
            Ok(()) => {
                self.estimator.record_send(Instant::now(), n as u64, pps);
                Ok(WriteOutcome::Written)
            }
            Err(mpsc::TrySendError::Full(_)) => {
                log::debug!("idn worker queue full, back-pressuring scheduler");
                Ok(WriteOutcome::WouldBlock)
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                Err(Error::disconnected("IDN sender thread disconnected"))
            }
        }
    }

    fn estimator(&self) -> &dyn BufferEstimator {
        &self.estimator
    }
}

fn blank_chunk(pps: u32, format: PointFormat) -> QueuedChunk {
    let points = match format {
        PointFormat::Xyrgbi => ChunkPoints::Xyrgbi(vec![PointXyrgbi::default(); 10]),
        PointFormat::XyrgbHighRes => ChunkPoints::HighRes(vec![PointXyrgbHighRes::default(); 10]),
        PointFormat::Extended => ChunkPoints::Extended(vec![PointExtended::default(); 10]),
    };
    QueuedChunk { pps, points }
}

fn worker_loop<S: WorkerStream>(
    mut stream: S,
    rx: mpsc::Receiver<WorkerCommand>,
    connected: Arc<AtomicBool>,
    point_format: PointFormat,
    pool: Arc<ChunkBufferPool>,
) {
    let mut queue = VecDeque::new();
    let mut last_pps = stream.scan_speed();

    // Liveness tracking. Data sends are periodically upgraded to ACK requests
    // (and idle intervals send a ping); after `MAX_CONSECUTIVE_ACK_MISSES`
    // consecutive misses the device is treated as dead so the worker exits and
    // the backend is marked disconnected (triggering the driver's reconnect).
    let mut last_ack_check: Option<Instant> = None;
    let mut misses: u32 = 0;

    'worker: loop {
        loop {
            match rx.try_recv() {
                Ok(cmd) => {
                    if !handle_worker_command(cmd, &mut queue, last_pps, point_format) {
                        break 'worker;
                    }
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => break 'worker,
            }
        }

        if let Some(mut chunk) = queue.pop_front() {
            let remaining = queue.len();
            log::trace!(
                "idn worker: sending {} pts, {} queued behind",
                chunk.points.len(),
                remaining,
            );
            last_pps = chunk.pps;
            stream.set_scan_speed(chunk.pps);

            let now = Instant::now();
            if liveness_check_due(last_ack_check, misses, now) {
                last_ack_check = Some(now);
                if stream.write_frame_with_ack(&chunk.points, ACK_TIMEOUT) {
                    misses = 0;
                } else {
                    misses += 1;
                    log::debug!(
                        "idn worker: liveness ACK miss {}/{}",
                        misses,
                        MAX_CONSECUTIVE_ACK_MISSES
                    );
                    if misses >= MAX_CONSECUTIVE_ACK_MISSES {
                        log::warn!(
                            "idn worker: {} consecutive ACK misses; marking disconnected",
                            misses
                        );
                        break;
                    }
                }
            } else if !stream.write_frame(&chunk.points) {
                break;
            }
            // Return the drained conversion buffer so the producer can reuse
            // its capacity on the next chunk.
            pool.recycle(&mut chunk.points);
            continue;
        }

        match rx.recv_timeout(stream::KEEPALIVE_INTERVAL) {
            Ok(cmd) => {
                if !handle_worker_command(cmd, &mut queue, last_pps, point_format) {
                    break;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if stream.needs_keepalive() && !stream.send_keepalive() {
                    break;
                }
                // Detect a device that dies while idle: ping periodically.
                let now = Instant::now();
                if liveness_check_due(last_ack_check, misses, now) {
                    last_ack_check = Some(now);
                    if stream.ping(ACK_TIMEOUT) {
                        misses = 0;
                    } else {
                        misses += 1;
                        if misses >= MAX_CONSECUTIVE_ACK_MISSES {
                            log::warn!(
                                "idn worker: {} consecutive ping misses while idle; \
                                 marking disconnected",
                                misses
                            );
                            break;
                        }
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    connected.store(false, Ordering::Release);
    stream.close();
}

/// Whether a liveness (ACK/ping) check is due: once we start missing we probe
/// on every opportunity so a dead device is detected quickly; otherwise we
/// probe at most once per `ACK_CHECK_INTERVAL`.
fn liveness_check_due(last_ack_check: Option<Instant>, misses: u32, now: Instant) -> bool {
    misses > 0
        || match last_ack_check {
            None => true,
            Some(t) => now.duration_since(t) >= ACK_CHECK_INTERVAL,
        }
}

fn handle_worker_command(
    cmd: WorkerCommand,
    queue: &mut VecDeque<QueuedChunk>,
    last_pps: u32,
    point_format: PointFormat,
) -> bool {
    match cmd {
        WorkerCommand::Chunk(chunk) => {
            queue.push_back(chunk);
            true
        }
        WorkerCommand::Stop => {
            queue.clear();
            queue.push_back(blank_chunk(last_pps, point_format));
            true
        }
        WorkerCommand::Shutdown => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        handle_worker_command, worker_loop, ChunkBufferPool, ChunkPoints, QueuedChunk,
        WorkerCommand, WorkerStream, MAX_CONSECUTIVE_ACK_MISSES,
    };
    use crate::point::LaserPoint;
    use crate::protocols::idn::dac::stream::PointFormat;
    use crate::protocols::idn::dac::{ServerInfo, ServiceInfo, ServiceType};
    use crate::protocols::idn::protocol::PointXyrgbi;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{mpsc, Arc};
    use std::thread;
    use std::time::{Duration, Instant};

    struct FakeStream {
        scan_speed: u32,
        writes: Arc<AtomicUsize>,
        /// Whether ACK/ping probes succeed (false simulates a dead device).
        ack_ok: Arc<AtomicBool>,
    }

    impl FakeStream {
        fn new(writes: Arc<AtomicUsize>) -> Self {
            Self {
                scan_speed: 30_000,
                writes,
                ack_ok: Arc::new(AtomicBool::new(true)),
            }
        }

        fn with_ack(writes: Arc<AtomicUsize>, ack_ok: Arc<AtomicBool>) -> Self {
            Self {
                scan_speed: 30_000,
                writes,
                ack_ok,
            }
        }
    }

    impl WorkerStream for FakeStream {
        fn scan_speed(&self) -> u32 {
            self.scan_speed
        }

        fn set_scan_speed(&mut self, pps: u32) {
            self.scan_speed = pps;
        }

        fn needs_keepalive(&self) -> bool {
            false
        }

        fn send_keepalive(&mut self) -> bool {
            true
        }

        fn write_frame(&mut self, _points: &ChunkPoints) -> bool {
            self.writes.fetch_add(1, Ordering::Acquire);
            true
        }

        fn write_frame_with_ack(&mut self, _points: &ChunkPoints, _timeout: Duration) -> bool {
            self.writes.fetch_add(1, Ordering::Acquire);
            self.ack_ok.load(Ordering::Acquire)
        }

        fn ping(&mut self, _timeout: Duration) -> bool {
            self.ack_ok.load(Ordering::Acquire)
        }

        fn close(&mut self) {}
    }

    fn test_chunk(pps: u32, point_count: usize) -> QueuedChunk {
        QueuedChunk {
            pps,
            points: ChunkPoints::Xyrgbi(vec![PointXyrgbi::new(0, 0, 0, 0, 0, 0); point_count]),
        }
    }

    fn wait_for_writes(writes: &AtomicUsize, target: usize, timeout: Duration) -> usize {
        let start = Instant::now();
        loop {
            let count = writes.load(Ordering::Acquire);
            if count >= target || start.elapsed() >= timeout {
                return count;
            }
            thread::yield_now();
        }
    }

    #[test]
    fn handle_worker_command_stop_replaces_backlog_with_blank() {
        let mut queue = VecDeque::new();

        assert!(handle_worker_command(
            WorkerCommand::Chunk(test_chunk(30_000, 179)),
            &mut queue,
            30_000,
            PointFormat::Xyrgbi,
        ));
        assert!(handle_worker_command(
            WorkerCommand::Chunk(test_chunk(30_000, 179)),
            &mut queue,
            30_000,
            PointFormat::Xyrgbi,
        ));

        assert!(handle_worker_command(
            WorkerCommand::Stop,
            &mut queue,
            30_000,
            PointFormat::Xyrgbi,
        ));
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.front().unwrap().points.len(), 10);
    }

    #[test]
    fn worker_loop_bursts_backlog_without_realtime_delay() {
        let writes = Arc::new(AtomicUsize::new(0));
        let fake_stream = FakeStream::new(Arc::clone(&writes));
        let (tx, rx) = mpsc::sync_channel(4);
        let connected = Arc::new(AtomicBool::new(true));
        let worker_connected = Arc::clone(&connected);

        let handle = thread::spawn(move || {
            worker_loop(
                fake_stream,
                rx,
                worker_connected,
                PointFormat::Xyrgbi,
                Arc::new(ChunkBufferPool::new()),
            )
        });

        tx.send(WorkerCommand::Chunk(test_chunk(1_000, 179)))
            .unwrap();
        tx.send(WorkerCommand::Chunk(test_chunk(1_000, 179)))
            .unwrap();

        let count = wait_for_writes(&writes, 2, Duration::from_millis(20));

        tx.send(WorkerCommand::Shutdown).unwrap();
        handle.join().unwrap();

        assert_eq!(
            count, 2,
            "worker should drain backlog immediately to build timestamp lead"
        );
        assert!(!connected.load(Ordering::Acquire));
    }

    #[test]
    fn worker_disconnects_after_consecutive_ack_misses() {
        let writes = Arc::new(AtomicUsize::new(0));
        // A dead device: ACK/ping probes never succeed.
        let ack_ok = Arc::new(AtomicBool::new(false));
        let fake_stream = FakeStream::with_ack(Arc::clone(&writes), Arc::clone(&ack_ok));
        let (tx, rx) = mpsc::sync_channel(8);
        let connected = Arc::new(AtomicBool::new(true));
        let worker_connected = Arc::clone(&connected);

        let handle = thread::spawn(move || {
            worker_loop(
                fake_stream,
                rx,
                worker_connected,
                PointFormat::Xyrgbi,
                Arc::new(ChunkBufferPool::new()),
            )
        });

        // Feed more chunks than the miss threshold; the first send is always an
        // ACK request (last_ack_check == None), and once misses start every
        // subsequent send is probed, so the worker gives up quickly.
        for _ in 0..10 {
            let _ = tx.send(WorkerCommand::Chunk(test_chunk(30_000, 20)));
        }

        handle.join().unwrap();

        assert!(
            !connected.load(Ordering::Acquire),
            "worker should mark disconnected after consecutive ACK misses"
        );
        assert!(
            writes.load(Ordering::Acquire) >= MAX_CONSECUTIVE_ACK_MISSES as usize,
            "worker should have attempted the frames before giving up"
        );
    }

    fn test_backend() -> super::IdnBackend {
        let server = ServerInfo::new([0u8; 16], "test".to_string(), (1, 0), 0);
        let service = ServiceInfo {
            service_id: 0,
            service_type: ServiceType::LaserProjector,
            name: "test".to_string(),
            flags: 0,
            relay_number: 0,
        };
        super::IdnBackend::new(server, service)
    }

    /// Chunk construction remains a defensive final checkpoint: an
    /// out-of-range rate supplied directly to the backend must never reach the
    /// worker (and from there the device).
    #[test]
    fn build_chunk_clamps_pps_to_device_maximum() {
        let backend = test_backend();
        let pps_max = super::super::default_capabilities().pps_max;
        assert_eq!(pps_max, 100_000);

        for requested in [u32::MAX, pps_max + 1, 1_000_000] {
            let chunk = backend.build_chunk(requested, &[LaserPoint::blanked(0.0, 0.0)]);
            assert_eq!(
                chunk.pps, pps_max,
                "requested {requested} must clamp to pps_max"
            );
        }

        let in_range = backend.build_chunk(30_000, &[LaserPoint::blanked(0.0, 0.0)]);
        assert_eq!(in_range.pps, 30_000);
    }

    /// The lower end of the range is clamped too: a zero (or sub-minimum) rate
    /// supplied directly to the backend must never reach the worker as an
    /// invalid device rate (mirroring the lasercube-network clamp).
    #[test]
    fn build_chunk_clamps_below_device_minimum() {
        let backend = test_backend();
        let caps = super::super::default_capabilities();
        assert_eq!(caps.pps_min, 1);

        for requested in [0, u32::MAX] {
            let chunk = backend.build_chunk(requested, &[LaserPoint::blanked(0.0, 0.0)]);
            // Zero clamps up to pps_min; u32::MAX still clamps down to pps_max.
            let expected = requested.clamp(caps.pps_min.max(1), caps.pps_max);
            assert_eq!(
                chunk.pps,
                expected,
                "requested {requested} must clamp into [{}, {}]",
                caps.pps_min.max(1),
                caps.pps_max
            );
        }
    }

    #[test]
    fn chunk_buffer_pool_recycles_capacity() {
        let pool = ChunkBufferPool::new();

        let mut buf = pool.take_xyrgbi();
        assert_eq!(buf.capacity(), 0, "empty pool hands out fresh buffers");
        buf.extend((0..179).map(|_| PointXyrgbi::new(0, 0, 0, 0, 0, 0)));

        // Simulate the worker returning a sent chunk's buffer.
        let mut chunk_points = ChunkPoints::Xyrgbi(buf);
        pool.recycle(&mut chunk_points);
        assert_eq!(chunk_points.len(), 0);

        let recycled = pool.take_xyrgbi();
        assert_eq!(recycled.len(), 0);
        assert_eq!(recycled.capacity(), 179, "capacity must survive recycling");
    }
}
