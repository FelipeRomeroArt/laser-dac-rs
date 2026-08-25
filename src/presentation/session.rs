//! FrameSession and FrameSessionConfig — public frame-mode API.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::backend::BackendKind;
use crate::config::StreamConfig;
use crate::device::{DacInfo, OutputModel};
use crate::error::{Error, Result};
use crate::reconnect::ReconnectPolicy;
use crate::session::{ControlMsg, SessionControl, SessionExit};

use super::driver::{self, DriverInputs, SourceOwned};
use super::engine::PresentationEngine;
use super::slice_pipeline::SlicePipeline;
use super::{default_transition, Frame, OutputResetReason, TransitionFn};

// =============================================================================
// FrameSessionConfig
// =============================================================================

/// Configuration for a frame-mode streaming session.
pub struct FrameSessionConfig {
    pps: u32,
    transition_fn: TransitionFn,
    startup_blank: Duration,
    color_delay: Duration,
    reconnect: Option<crate::config::ReconnectConfig>,
    idle_policy: crate::config::IdlePolicy,
    output_filter: Option<Box<dyn super::OutputFilter>>,
}

impl FrameSessionConfig {
    /// Create a new config with the given PPS and default transition.
    pub fn new(pps: u32) -> Self {
        Self {
            pps,
            transition_fn: default_transition(),
            startup_blank: Duration::from_millis(1),
            color_delay: Duration::ZERO,
            idle_policy: crate::config::IdlePolicy::default(),
            reconnect: None,
            output_filter: None,
        }
    }

    /// Points per second output rate.
    pub fn pps(&self) -> u32 {
        self.pps
    }
    /// Transition callback used for newly composed seams.
    pub fn transition_fn(&self) -> &TransitionFn {
        &self.transition_fn
    }
    /// Duration of startup blanking after arming.
    pub fn startup_blank(&self) -> Duration {
        self.startup_blank
    }
    /// Color delay duration.
    pub fn color_delay(&self) -> Duration {
        self.color_delay
    }
    /// Reconnection configuration, when enabled.
    pub fn reconnect(&self) -> Option<&crate::config::ReconnectConfig> {
        self.reconnect.as_ref()
    }

    pub(crate) fn take_reconnect(&mut self) -> Option<crate::config::ReconnectConfig> {
        self.reconnect.take()
    }
    /// Policy used while disarmed.
    pub fn idle_policy(&self) -> &crate::config::IdlePolicy {
        &self.idle_policy
    }
    /// Installed final-output filter, when present.
    pub fn output_filter(&self) -> Option<&dyn super::OutputFilter> {
        self.output_filter.as_deref()
    }

    /// Set the transition function (builder pattern).
    pub fn with_transition_fn(mut self, f: TransitionFn) -> Self {
        self.transition_fn = f;
        self
    }

    /// Set the startup blank duration (builder pattern).
    pub fn with_startup_blank(mut self, duration: std::time::Duration) -> Self {
        self.startup_blank = duration;
        self
    }

    /// Set the color delay duration (builder pattern).
    pub fn with_color_delay(mut self, delay: Duration) -> Self {
        self.color_delay = delay;
        self
    }

    /// Enable automatic reconnection (builder pattern).
    ///
    /// Requires the device to have been opened via [`open_device`](crate::open_device).
    pub fn with_reconnect(mut self, config: crate::config::ReconnectConfig) -> Self {
        self.reconnect = Some(config);
        self
    }

    /// Set the idle policy (builder pattern).
    ///
    /// Controls scanner behavior when disarmed. See [`crate::config::IdlePolicy`].
    pub fn with_idle_policy(mut self, policy: crate::config::IdlePolicy) -> Self {
        self.idle_policy = policy;
        self
    }

    /// Install a final-output filter (builder pattern).
    pub fn with_output_filter(mut self, filter: Box<dyn super::OutputFilter>) -> Self {
        self.output_filter = Some(filter);
        self
    }
}

// =============================================================================
// FrameSession
// =============================================================================

/// Read-only liveness and connectivity metrics for a [`FrameSession`].
#[derive(Clone)]
pub struct FrameSessionMetrics {
    inner: Arc<FrameSessionMetricsInner>,
}

struct FrameSessionMetricsInner {
    connected: AtomicBool,
    origin: Instant,
    last_loop_activity_nanos: AtomicU64,
    last_write_success_nanos: AtomicU64,
}

impl FrameSessionMetrics {
    pub(crate) fn new(connected: bool) -> Self {
        let metrics = Self {
            inner: Arc::new(FrameSessionMetricsInner {
                connected: AtomicBool::new(connected),
                origin: Instant::now(),
                last_loop_activity_nanos: AtomicU64::new(0),
                last_write_success_nanos: AtomicU64::new(0),
            }),
        };
        metrics.mark_loop_activity();
        metrics
    }

    /// Returns whether the session currently has a connected backend.
    pub fn connected(&self) -> bool {
        self.inner.connected.load(Ordering::SeqCst)
    }

    /// Returns the last time the scheduler thread showed progress.
    pub fn last_loop_activity(&self) -> Option<Instant> {
        self.instant_from_nanos(self.inner.last_loop_activity_nanos.load(Ordering::SeqCst))
    }

    /// Returns the last time a backend write completed successfully.
    pub fn last_write_success(&self) -> Option<Instant> {
        self.instant_from_nanos(self.inner.last_write_success_nanos.load(Ordering::SeqCst))
    }

    fn instant_from_nanos(&self, nanos: u64) -> Option<Instant> {
        if nanos == 0 {
            None
        } else {
            self.inner.origin.checked_add(Duration::from_nanos(nanos))
        }
    }

    fn now_nanos(&self) -> u64 {
        (self.inner.origin.elapsed().as_nanos().min(u64::MAX as u128) as u64).max(1)
    }

    pub(super) fn mark_loop_activity(&self) {
        self.inner
            .last_loop_activity_nanos
            .store(self.now_nanos(), Ordering::SeqCst);
    }

    pub(super) fn mark_write_success(&self) {
        let now = self.now_nanos();
        self.inner
            .last_loop_activity_nanos
            .store(now, Ordering::SeqCst);
        self.inner
            .last_write_success_nanos
            .store(now, Ordering::SeqCst);
    }

    pub(super) fn set_connected(&self, connected: bool) {
        self.inner.connected.store(connected, Ordering::SeqCst);
        self.mark_loop_activity();
    }
}

struct MetricsDisconnectGuard(FrameSessionMetrics);

impl Drop for MetricsDisconnectGuard {
    fn drop(&mut self) {
        self.0.set_connected(false);
    }
}

/// Keeps explicit safe teardown attached to backend ownership until the
/// scheduler thread has successfully started and taken that ownership.
struct StartBackendGuard(Option<BackendKind>);

impl StartBackendGuard {
    fn take(&mut self) -> BackendKind {
        self.0.take().expect("start backend already taken")
    }
}

impl Drop for StartBackendGuard {
    fn drop(&mut self) {
        if let Some(backend) = self.0.as_mut() {
            backend.close_and_disconnect();
        }
    }
}

/// A frame-mode streaming session.
///
/// Owns a scheduler thread that reads frames from a channel and writes them
/// to the DAC backend using the appropriate strategy (FIFO or frame-swap).
///
/// # Example
///
/// ```ignore
/// use laser_dac::{open_device, FrameSessionConfig, Frame, LaserPoint};
///
/// let device = open_device("my-device")?;
/// let config = FrameSessionConfig::new(30_000);
/// let (session, _info) = device.start_frame_session(config)?;
///
/// session.control().arm()?;
/// session.send_frame(Frame::new(vec![
///     LaserPoint::new(0.0, 0.0, 65535, 0, 0, 65535),
/// ]));
/// ```
pub struct FrameSession {
    control: SessionControl,
    thread: Option<JoinHandle<Result<SessionExit>>>,
    frame_slot: Arc<Mutex<Option<Frame>>>,
    metrics: FrameSessionMetrics,
}

impl FrameSession {
    /// Start a frame session on the given backend.
    pub(crate) fn start(
        mut backend: BackendKind,
        config: FrameSessionConfig,
        reconnect_policy: Option<ReconnectPolicy>,
    ) -> Result<Self> {
        if let Err(err) = backend.validate_output_model() {
            backend.close_and_disconnect();
            return Err(err);
        }
        if !backend.is_connected() {
            if let Err(err) = backend.connect() {
                backend.close_and_disconnect();
                return Err(err);
            }
            if let Err(err) = backend.set_shutter(false) {
                backend.close_and_disconnect();
                return Err(err);
            }
        }

        let (control_tx, control_rx) = mpsc::channel();
        let control = SessionControl::new_with_pps_bounds(
            control_tx,
            config.color_delay,
            config.pps,
            backend.caps().pps_min,
            backend.caps().pps_max,
        );
        let frame_slot: Arc<Mutex<Option<Frame>>> = Arc::new(Mutex::new(None));
        let metrics = FrameSessionMetrics::new(backend.is_connected());

        let control_clone = control.clone();
        let slot_clone = frame_slot.clone();
        let metrics_clone = metrics.clone();
        let mut backend_guard = StartBackendGuard(Some(backend));

        // Named for diagnosability in profilers / thread dumps. If spawning
        // fails, dropping the captured guard explicitly closes and disconnects
        // the backend instead of relying on its implementation's `Drop`.
        let thread = std::thread::Builder::new()
            .name("laser-frame-scheduler".to_string())
            .spawn(move || {
                let backend = backend_guard.take();
                // Elevate this thread's scheduling priority so pacing sleeps are
                // less likely to be preempted under system load. Best-effort:
                // many systems disallow raising priority without privileges
                // (e.g. Linux without CAP_SYS_NICE), so we log and continue at
                // default priority rather than fail the session.
                if let Err(e) = thread_priority::set_current_thread_priority(
                    thread_priority::ThreadPriority::Max,
                ) {
                    log::warn!("laser-frame-scheduler: could not raise thread priority: {e:?}");
                }
                let _disconnect_guard = MetricsDisconnectGuard(metrics_clone.clone());
                Self::run_loop(
                    backend,
                    config,
                    control_clone,
                    control_rx,
                    slot_clone,
                    metrics_clone,
                    reconnect_policy,
                )
            })
            .map_err(Error::backend)?;

        Ok(Self {
            control,
            thread: Some(thread),
            frame_slot,
            metrics,
        })
    }

    /// Returns a control handle for arm/disarm/stop.
    pub fn control(&self) -> SessionControl {
        self.control.clone()
    }

    /// Submit a frame for display. Latest-wins: overwrites any unconsumed
    /// pending frame immediately, with no buffering or memory growth.
    pub fn send_frame(&self, frame: Frame) {
        // Poisoning policy: the frame slot is latest-wins state, so recover
        // from a poisoned mutex instead of panicking on the caller's thread.
        *self.frame_slot.lock().unwrap_or_else(|p| p.into_inner()) = Some(frame);
    }

    /// Returns true if the scheduler thread has finished.
    pub fn is_finished(&self) -> bool {
        self.thread.as_ref().is_some_and(|h| h.is_finished())
    }

    /// Returns a metrics handle for observing scheduler liveness.
    pub fn metrics(&self) -> FrameSessionMetrics {
        self.metrics.clone()
    }

    /// Wait for the session thread to finish and return the exit reason.
    pub fn join(mut self) -> Result<SessionExit> {
        if let Some(handle) = self.thread.take() {
            handle
                .join()
                .unwrap_or(Err(Error::disconnected("thread panicked")))
        } else {
            Ok(SessionExit::Stopped)
        }
    }

    // =========================================================================
    // Driver loop
    // =========================================================================

    fn run_loop(
        mut backend: BackendKind,
        config: FrameSessionConfig,
        control: SessionControl,
        control_rx: mpsc::Receiver<ControlMsg>,
        frame_slot: Arc<Mutex<Option<Frame>>>,
        metrics: FrameSessionMetrics,
        reconnect_policy: Option<ReconnectPolicy>,
    ) -> Result<SessionExit> {
        let FrameSessionConfig {
            pps: _,
            transition_fn,
            startup_blank,
            color_delay,
            idle_policy,
            output_filter,
            reconnect: _,
        } = config;

        let mut engine = PresentationEngine::new(transition_fn);
        if backend.is_frame_swap() {
            engine.set_frame_capacity(backend.frame_capacity());
        }

        // Per-OutputModel initial buffer sizing: FIFO bounds by max_points_per_chunk;
        // frame-swap bounds by frame_capacity (max_points_per_chunk is meaningless there).
        let initial_buf_capacity = match backend.caps().output_model {
            OutputModel::UsbFrameSwap => backend.frame_capacity().unwrap_or(0),
            OutputModel::NetworkFifo | OutputModel::UdpTimed | OutputModel::BlockingFifo => {
                backend.caps().max_points_per_chunk
            }
        };
        let mut pipeline = SlicePipeline::with_startup_blank(
            engine,
            color_delay,
            output_filter,
            idle_policy,
            initial_buf_capacity,
            startup_blank,
        );
        pipeline.reset_output_filter(OutputResetReason::SessionStart);

        let expected_frame_swap = backend.is_frame_swap();
        let source: SourceOwned = if expected_frame_swap {
            SourceOwned::Frame(Box::new(pipeline))
        } else {
            SourceOwned::Fifo(Box::new(pipeline))
        };

        let validator = Self::reconnect_validator(reconnect_policy.as_ref());
        if !backend.is_connected() {
            backend.connect()?;
        }

        let target_buffer = target_buffer_for_backend(&backend);

        driver::run(DriverInputs {
            backend,
            source,
            control,
            control_rx,
            metrics,
            reconnect_policy,
            validator,
            // Frame-mode has no user-facing on_error channel; log non-fatal
            // errors so they are not silently swallowed. Disconnection still
            // surfaces via `metrics.connected()`.
            error_sink: Box::new(|e: Error| {
                log::warn!("frame-session output error: {e}");
            }),
            target_buffer,
            drain_timeout: Duration::ZERO,
            pending_frame: Some(frame_slot),
            clock: DriverInputs::system_clock(),
        })
    }

    fn reconnect_validator(policy: Option<&ReconnectPolicy>) -> driver::ReconnectValidator {
        let target_id = policy
            .map(|p| p.target.device_id.clone())
            .unwrap_or_default();
        Box::new(move |info: &DacInfo, _backend: &BackendKind, pps: u32| {
            if pps < info.caps.pps_min || pps > info.caps.pps_max {
                log::error!(
                    "'{}' PPS {} outside new device range [{}, {}]",
                    target_id,
                    pps,
                    info.caps.pps_min,
                    info.caps.pps_max
                );
                return Err(SessionExit::IncompatibleDevice);
            }
            Ok(())
        })
    }
}

impl Drop for FrameSession {
    fn drop(&mut self) {
        let _ = self.control.stop();
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

pub(super) fn target_buffer_for_backend(backend: &BackendKind) -> Duration {
    // Delegate to the shared policy so the frame path and the stream path's
    // `apply_backend_buffer_defaults` can never disagree on the cushion.
    StreamConfig::default_target_buffer_for(&backend.dac_type(), &backend.caps().output_model)
}
