//! Per-`OutputModel` scheduler adapters.
//!
//! The shared driver in [`super::session`] runs the pre-iter bookkeeping
//! (frame intake, reconnect, control messages, shutter transitions). Each
//! adapter owns the variable parts: pacing wait, slice production, write,
//! and post-write bookkeeping.

use std::sync::mpsc;
use std::time::{Duration, Instant};

use crate::backend::BackendKind;
use crate::buffer_estimate::BufferEstimator;
use crate::device::DacInfo;
use crate::error::Error;
use crate::session::{ControlMsg, SessionControl};

use super::content_source::ContentSourceKind;
use super::session::FrameSessionMetrics;

mod blocking_fifo;
mod network_fifo;
mod udp_timed;
mod usb_frame_swap;

/// One step's outcome.
pub(crate) enum StepOutcome {
    Continue,
    StateChanged,
    Stopped,
    Disconnected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ControlAction {
    None,
    StateChanged,
    Stop,
}

/// Injectable time source for the driver loop's pacing sleeps.
///
/// Production uses [`SystemClock`], a zero-overhead pass-through to `std`, so
/// behavior is unchanged. Tests can inject a fake clock whose `sleep` advances a
/// virtual `now` instantly, making the [`LoopCtx`] pacing waits deterministic
/// and free of real wall-clock delay.
///
/// This is the seam the audit's "injectable clock at the `driver::run` seam"
/// recommendation asks for. It currently covers the `LoopCtx` sleep helpers and
/// the estimator drain-poll; virtualizing the adapters' internal deadline state
/// and converting the remaining wall-clock test sleeps is a deliberate,
/// separately-scoped follow-up (the "large" option in the audit).
pub(crate) trait Clock: Send {
    fn now(&self) -> Instant;
    fn sleep(&self, dur: Duration);
}

/// Real wall clock: `Instant::now()` + `thread::sleep`. The production clock.
pub(crate) struct SystemClock;

impl Clock for SystemClock {
    #[inline]
    fn now(&self) -> Instant {
        Instant::now()
    }
    #[inline]
    fn sleep(&self, dur: Duration) {
        std::thread::sleep(dur);
    }
}

/// Test clock: `sleep` advances a virtual `now` instantly instead of blocking,
/// so pacing loops run to completion with zero wall-clock delay while their
/// time-based logic still sees time pass. Records total virtual sleep for
/// assertions.
#[cfg(test)]
pub(crate) struct FakeClock {
    now: std::sync::Mutex<Instant>,
    slept: std::sync::Mutex<Duration>,
}

#[cfg(test)]
impl FakeClock {
    pub(crate) fn new() -> Self {
        Self {
            now: std::sync::Mutex::new(Instant::now()),
            slept: std::sync::Mutex::new(Duration::ZERO),
        }
    }

    /// Total virtual time advanced via `sleep`.
    pub(crate) fn total_slept(&self) -> Duration {
        *self.slept.lock().unwrap()
    }
}

#[cfg(test)]
impl Clock for FakeClock {
    fn now(&self) -> Instant {
        *self.now.lock().unwrap()
    }
    fn sleep(&self, dur: Duration) {
        *self.now.lock().unwrap() += dur;
        *self.slept.lock().unwrap() += dur;
    }
}

/// Driver knowledge of the physical shutter after the last command.
///
/// An error cannot prove which state hardware reached, so failures always
/// produce `Unknown` and safety-closing paths retry until closure is confirmed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ShutterState {
    Open,
    Closed,
    Unknown,
}

/// Shared context borrowed by the adapter for one `step`.
pub(crate) struct LoopCtx<'a> {
    pub backend: &'a mut BackendKind,
    pub source: ContentSourceKind<'a>,
    pub control: &'a SessionControl,
    pub control_rx: &'a mpsc::Receiver<ControlMsg>,
    pub metrics: &'a FrameSessionMetrics,
    pub shutter_open: &'a mut ShutterState,
    pub error_sink: &'a mut dyn FnMut(Error),
    pub target_buffer: Duration,
    pub pps: u32,
    pub is_armed: bool,
    /// Time source for all pacing waits in this step. `SystemClock` in
    /// production; a virtual clock in tests.
    pub clock: &'a dyn Clock,
}

impl<'a> LoopCtx<'a> {
    /// Sleep with control-message processing and shutter handling.
    /// Returns `StepOutcome::Stopped` if a stop is requested mid-sleep
    /// (either via the atomic flag or a `ControlMsg::Stop` on the channel).
    pub fn sleep_with_control_check(&mut self, duration: Duration) -> Result<(), StepOutcome> {
        const SLICE: Duration = Duration::from_millis(2);
        // Absolute deadline: decrementing `remaining` by the *nominal* slice
        // over-sleeps badly on coarse timers (a 2ms sleep can take 15.6ms on
        // Windows' default resolution), so the total wait can run many times
        // long. Loop on the real remaining time to `deadline` instead.
        let deadline = self.clock.now() + duration;
        loop {
            let remaining = deadline.saturating_duration_since(self.clock.now());
            if remaining.is_zero() {
                return Ok(());
            }
            self.clock.sleep(remaining.min(SLICE));
            self.metrics.mark_loop_activity();
            if self.control.is_stop_requested() {
                return Err(StepOutcome::Stopped);
            }
            close_if_disarmed(self.control, self.shutter_open, self.backend);
            match process_control_messages(self.control_rx) {
                ControlAction::Stop => return Err(StepOutcome::Stopped),
                ControlAction::StateChanged => return Err(StepOutcome::StateChanged),
                ControlAction::None => {}
            }
        }
    }

    /// High-precision sleep until `deadline` (UDP-timed pacing).
    ///
    /// WARNING — do NOT drive this method with a virtual [`Clock`]. Its
    /// `deadline` is sourced from real wall-clock time (`Instant::now()` in
    /// `UdpTimedAdapter::next_send`), so it is only coherent when
    /// `self.clock.now()` is also real wall-clock time (i.e. [`SystemClock`]).
    /// Under a [`FakeClock`] whose `now` only advances on `sleep`, the final
    /// sub-threshold approach falls into the busy-wait branch below, which spins
    /// `thread::yield_now()` *without advancing the clock* — so `clock.now()`
    /// never reaches the real `deadline` and the loop hangs forever. Tests that
    /// inject a virtual clock into `driver::run` must therefore exercise a
    /// pacing path that never calls this (e.g. the `BlockingFifo` model, which
    /// paces only through the clock-driven `sleep_*` helpers). Making UDP-timed
    /// pacing virtual-clock-safe would require sourcing `deadline` from
    /// `clock.now()` too — a deliberately separate, larger change.
    pub fn sleep_until_precise(&mut self, deadline: Instant) -> Result<(), StepOutcome> {
        const BUSY_WAIT_THRESHOLD: Duration = Duration::from_micros(500);
        const SLEEP_SLICE: Duration = Duration::from_millis(1);

        loop {
            let now = self.clock.now();
            if now >= deadline {
                return Ok(());
            }

            let remaining = deadline.duration_since(now);
            if remaining > BUSY_WAIT_THRESHOLD {
                let slice = remaining
                    .saturating_sub(BUSY_WAIT_THRESHOLD)
                    .min(SLEEP_SLICE);
                self.clock.sleep(slice);
            } else {
                // Busy-wait for sub-threshold precision. NOTE: this does not
                // touch `self.clock`, so it only makes progress against a real
                // wall clock. See the method doc: never drive this with a
                // virtual clock — `now` would never reach `deadline`.
                std::thread::yield_now();
            }
            self.metrics.mark_loop_activity();

            if self.control.is_stop_requested() {
                return Err(StepOutcome::Stopped);
            }
            close_if_disarmed(self.control, self.shutter_open, self.backend);
            match process_control_messages(self.control_rx) {
                ControlAction::Stop => return Err(StepOutcome::Stopped),
                ControlAction::StateChanged => return Err(StepOutcome::StateChanged),
                ControlAction::None => {}
            }
        }
    }

    pub fn sleep_and_mark_activity(&self, duration: Duration) {
        self.clock.sleep(duration);
        self.metrics.mark_loop_activity();
    }
}

/// Drain scheduler notifications. Desired atomic state remains authoritative;
/// arm/disarm messages never command hardware directly. Stop wins the drain.
pub(crate) fn process_control_messages(control_rx: &mpsc::Receiver<ControlMsg>) -> ControlAction {
    use std::sync::mpsc::TryRecvError;
    let mut action = ControlAction::None;
    loop {
        match control_rx.try_recv() {
            Ok(ControlMsg::Arm | ControlMsg::Disarm) => action = ControlAction::StateChanged,
            Ok(ControlMsg::Stop) => return ControlAction::Stop,
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => return action,
        }
    }
}

/// During waits and retained-write retries, only the safety-closing direction
/// may touch the shutter. Opening is exclusively owned by the shared driver.
pub(crate) fn close_if_disarmed(
    control: &SessionControl,
    shutter_state: &mut ShutterState,
    backend: &mut BackendKind,
) {
    if !control.is_armed() && *shutter_state != ShutterState::Closed {
        *shutter_state = match backend.set_shutter(false) {
            Ok(()) => ShutterState::Closed,
            Err(_) => ShutterState::Unknown,
        };
    }
}

/// One per [`OutputModel`](crate::device::OutputModel) variant.
pub(crate) trait OutputModelAdapter: Send {
    /// Run one step of the variable scheduler work.
    fn step(&mut self, ctx: &mut LoopCtx<'_>) -> StepOutcome;

    /// Called by the driver after a successful reconnect. Adapters update
    /// their own pacing state and may reach into the backend; source-side
    /// reset (re-pending the last frame, resetting buffers, frame capacity)
    /// is performed by the driver against the `ContentSource` separately.
    fn on_reconnect(&mut self, info: &DacInfo, backend: &mut BackendKind);

    /// Clear adapter-owned retain state on an observed arm/disarm edge.
    fn on_state_change(&mut self, _armed: bool) {}

    /// Wait for queued points to drain, then output a small blank chunk and
    /// close the shutter. Called by the driver on graceful end-of-stream.
    /// Default is a best-effort blank-and-close — FIFO models override to
    /// poll the estimator until empty or `timeout` elapses; UsbFrameSwap
    /// uses the default (no queue to drain).
    fn drain_and_blank(&mut self, ctx: &mut LoopCtx<'_>, _timeout: Duration) {
        blank_and_close_shutter(ctx);
    }
}

/// Output a small blank chunk and close the shutter. Best-effort safety
/// shutdown shared between drain paths.
pub(crate) fn blank_and_close_shutter(ctx: &mut LoopCtx<'_>) {
    use crate::point::LaserPoint;
    *ctx.shutter_open = match ctx.backend.set_shutter(false) {
        Ok(()) => ShutterState::Closed,
        Err(_) => ShutterState::Unknown,
    };
    let blank = [LaserPoint::blanked(0.0, 0.0); 16];
    let _ = ctx.backend.try_write(ctx.pps, &blank);
}

/// Query an optional [`BufferEstimator`] for queued points, skipping the
/// `Instant::now()` clock query when the estimator ignores it
/// ([`RuntimeAuthorityEstimator`] for AVB/Oscilloscope). The sentinel is a
/// thread-local cached `Instant` — safe because estimators that ignore `now`
/// never read it.
pub(crate) fn estimator_fullness(estimator: Option<&dyn BufferEstimator>, pps: u32) -> u64 {
    estimator.map_or(0, |e| {
        let now = if e.needs_clock() {
            Instant::now()
        } else {
            sentinel_instant()
        };
        e.estimated_fullness(now, pps)
    })
}

fn sentinel_instant() -> Instant {
    thread_local! {
        static SENTINEL: Instant = Instant::now();
    }
    SENTINEL.with(|t| *t)
}

/// Poll the backend's [`BufferEstimator`] until it reports an empty queue or
/// the timeout elapses. Used by FIFO/UdpTimed `drain_and_blank`.
pub(crate) fn drain_via_estimator(ctx: &mut LoopCtx<'_>, timeout: Duration) {
    if timeout.is_zero() {
        return;
    }
    // Drain at the rate the device actually plays back, not the caller's
    // configured `ctx.pps`. The backend clamps the programmed rate to
    // `caps.pps_max` (updated to the device's real `max_dac_rate` at connect)
    // and anchors its estimator via `record_send` at that *effective* rate.
    // `validate_pps` runs against the pre-connect default caps, so a configured
    // `pps` above the connected device's true max survives validation; reading
    // the drain at that unclamped rate would decay the software model faster
    // than the hardware drains and blank the tail of the final frame early —
    // the exact symptom the record-side clamp is meant to prevent.
    let drain_pps = ctx.pps.min(ctx.backend.caps().pps_max);
    let start = ctx.clock.now();
    let deadline = start + timeout;
    const POLL: Duration = Duration::from_millis(5);
    let mut now = start;
    while now < deadline {
        let buffered = estimator_fullness(ctx.backend.estimator(), drain_pps);
        if buffered == 0 {
            break;
        }
        if ctx.control.is_stop_requested() {
            break;
        }
        close_if_disarmed(ctx.control, ctx.shutter_open, ctx.backend);
        if !matches!(
            process_control_messages(ctx.control_rx),
            ControlAction::None
        ) {
            break;
        }
        ctx.clock.sleep(POLL);
        ctx.metrics.mark_loop_activity();
        now = ctx.clock.now();
    }
}

/// Defense-in-depth for the adapter/source-model pairing invariant that
/// [`for_backend`] validates at construction. Reports the violation through the
/// error sink and stops the loop cleanly instead of panicking.
pub(crate) fn source_mismatch(ctx: &mut LoopCtx<'_>, adapter: &str) -> StepOutcome {
    let msg = format!(
        "{adapter} requires a content source matching its output model \
         (pairing violated; this is a driver wiring bug)"
    );
    log::error!("{msg}");
    (ctx.error_sink)(Error::invalid_config(msg));
    StepOutcome::Stopped
}

/// Construct the adapter for a backend's output model.
///
/// `source_is_frame` must describe the content source that will drive the
/// adapter: frame-swap models require a [`FrameContentSource`](super::content_source::FrameContentSource)
/// and every FIFO model requires a [`FifoContentSource`](super::content_source::FifoContentSource).
/// A mismatch is a configuration error and returns `Err` rather than building
/// an adapter that could only fail (or panic) at runtime.
pub(crate) fn for_backend(
    backend: &BackendKind,
    source_is_frame: bool,
) -> std::result::Result<Box<dyn OutputModelAdapter>, Error> {
    use crate::device::OutputModel;

    backend.validate_output_model()?;
    let model = &backend.caps().output_model;
    let model_is_frame = matches!(model, OutputModel::UsbFrameSwap);
    if model_is_frame != source_is_frame {
        return Err(Error::invalid_config(format!(
            "output model {:?} requires a {} content source, got {}",
            model,
            if model_is_frame { "frame" } else { "FIFO" },
            if source_is_frame { "frame" } else { "FIFO" },
        )));
    }
    Ok(match model {
        OutputModel::UsbFrameSwap => Box::new(usb_frame_swap::UsbFrameSwapAdapter::new(backend)),
        OutputModel::NetworkFifo => Box::new(network_fifo::NetworkFifoAdapter::new(backend)),
        OutputModel::UdpTimed => Box::new(udp_timed::UdpTimedAdapter::new(backend)),
        OutputModel::BlockingFifo => Box::new(blocking_fifo::BlockingFifoAdapter::new(backend)),
    })
}
