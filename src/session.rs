//! Shared session lifecycle control and exit types.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::error::{Error, Result};

/// How a shared presentation session ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionExit {
    /// The session received a stop request.
    Stopped,
    /// The content source ended normally.
    ///
    /// For callback streams, this means the producer returned
    /// [`crate::ChunkResult::End`].
    ProducerEnded,
    /// The device disconnected and reconnection was unavailable or exhausted.
    Disconnected,
}

// =============================================================================
// Stream Control
// =============================================================================

/// Scheduler notifications sent by [`SessionControl`].
///
/// Desired atomic state is authoritative; arm/disarm messages only interrupt
/// pacing/backpressure so the shared driver can reconcile that state.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ControlMsg {
    /// Desired arm state may have changed.
    Arm,
    /// Desired disarm state may have changed.
    Disarm,
    /// Request the stream to stop.
    Stop,
}

/// One atomically coherent desired arm state and transition generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DesiredState(u64);

impl DesiredState {
    const ARMED: u64 = 1;

    pub(crate) const fn initial() -> Self {
        Self(0)
    }

    pub(crate) fn is_armed(self) -> bool {
        self.0 & Self::ARMED != 0
    }
}

#[derive(Clone, Copy)]
struct RateState {
    pps: u32,
    min: u32,
    max: u32,
}

/// Thread-safe control handle for safety-critical actions.
///
/// This allows out-of-band control of the stream (arm/disarm/stop) from
/// a different thread, e.g., for E-stop functionality.
///
/// Control actions take effect as soon as possible - the stream processes
/// control messages at every opportunity (during waits, between retries, etc.).
#[derive(Clone)]
pub struct SessionControl {
    inner: Arc<SessionControlInner>,
}

struct SessionControlInner {
    /// Desired arm bit plus a generation incremented for every actual edge.
    desired_state: AtomicU64,
    /// Whether a stop has been requested.
    stop_requested: AtomicBool,
    /// Channel for waking the scheduler after control state changes.
    control_tx: Mutex<Sender<ControlMsg>>,
    /// Full-precision color delay, read when fresh output is composed.
    color_delay: Mutex<Duration>,
    /// Coherent active PPS and capability bounds.
    rate: Mutex<RateState>,
}

impl SessionControl {
    #[cfg(test)]
    pub(crate) fn new(control_tx: Sender<ControlMsg>, color_delay: Duration, pps: u32) -> Self {
        Self::new_with_pps_bounds(control_tx, color_delay, pps, 0, u32::MAX)
    }

    pub(crate) fn new_with_pps_bounds(
        control_tx: Sender<ControlMsg>,
        color_delay: Duration,
        pps: u32,
        pps_min: u32,
        pps_max: u32,
    ) -> Self {
        Self {
            inner: Arc::new(SessionControlInner {
                desired_state: AtomicU64::new(0),
                stop_requested: AtomicBool::new(false),
                control_tx: Mutex::new(control_tx),
                color_delay: Mutex::new(color_delay),
                rate: Mutex::new(RateState {
                    pps,
                    min: pps_min,
                    max: pps_max,
                }),
            }),
        }
    }

    fn set_desired_armed(&self, armed: bool, msg: ControlMsg) -> Result<()> {
        let armed_bit = u64::from(armed);
        let mut current = self.inner.desired_state.load(Ordering::SeqCst);
        loop {
            if current & DesiredState::ARMED == armed_bit {
                break;
            }
            // The low bit is state; adding two advances the generation, then
            // replacing the low bit records this edge in the same atomic word.
            let next = (current.wrapping_add(2) & !DesiredState::ARMED) | armed_bit;
            match self.inner.desired_state.compare_exchange(
                current,
                next,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }

        // Same-state calls do not advance the generation, but still attempt a
        // wake so a prior notification failure can never be silently masked.
        let tx = self
            .inner
            .control_tx
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        tx.send(msg).map_err(|_| {
            Error::disconnected("desired session state recorded, but scheduler notification failed")
        })
    }

    /// Record an armed desired state and notify the scheduler.
    ///
    /// Success means the desired state was recorded and the notification was
    /// accepted; it does not mean hardware completion. The shared driver
    /// discards retained output, primes startup blanking, resets filters/device
    /// buffering, and only then opens the shutter if that exact desired-state
    /// generation is still current.
    pub fn arm(&self) -> Result<()> {
        self.set_desired_armed(true, ControlMsg::Arm)
    }

    /// Disarm the output (force laser off). Designed for E-stop use.
    ///
    /// Success means the desired state was recorded and the notification was
    /// accepted; it does not confirm hardware completion. The driver closes the
    /// shutter first, discards retained output, and resets source/filter state.
    /// Already queued points can still play out.
    ///
    /// **Hardware shutter**: Best-effort. LaserCube and Helios have actual hardware
    /// control; Ether Dream and IDN are no-ops (safety relies on software blanking).
    pub fn disarm(&self) -> Result<()> {
        self.set_desired_armed(false, ControlMsg::Disarm)
    }

    pub(crate) fn desired_state(&self) -> DesiredState {
        DesiredState(self.inner.desired_state.load(Ordering::SeqCst))
    }

    /// Check if the output is armed.
    pub fn is_armed(&self) -> bool {
        self.desired_state().is_armed()
    }

    /// Set the color delay for scanner sync compensation.
    ///
    /// Takes effect within one chunk period. The delay is quantized to
    /// whole points: `ceil(delay * pps)`.
    pub fn set_color_delay(&self, delay: Duration) {
        *self
            .inner
            .color_delay
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = delay;
    }

    /// Get the current color delay.
    pub fn color_delay(&self) -> Duration {
        *self
            .inner
            .color_delay
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Set the points per second rate.
    ///
    /// Takes effect within one chunk period. No session restart is required.
    /// The update is rejected when it falls outside the connected backend's
    /// current capability range.
    pub fn set_pps(&self, pps: u32) -> Result<()> {
        let mut rate = self
            .inner
            .rate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pps < rate.min || pps > rate.max {
            return Err(Error::invalid_config(format!(
                "PPS {pps} outside active backend range [{}, {}]",
                rate.min, rate.max
            )));
        }
        rate.pps = pps;
        Ok(())
    }

    pub(crate) fn update_pps_bounds(&self, min: u32, max: u32) -> Result<()> {
        let mut rate = self
            .inner
            .rate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if rate.pps < min || rate.pps > max {
            return Err(Error::invalid_config(format!(
                "current PPS {} outside replacement backend range [{min}, {max}]",
                rate.pps
            )));
        }
        rate.min = min;
        rate.max = max;
        Ok(())
    }

    /// Get the current points per second rate.
    pub fn pps(&self) -> u32 {
        self.inner
            .rate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pps
    }

    /// Request the session to stop.
    ///
    /// Success means the stop flag was recorded and the notification was
    /// accepted; `run()` later returns [`SessionExit::Stopped`]. Hardware
    /// closure is asynchronous and best-effort.
    pub fn stop(&self) -> Result<()> {
        self.inner.stop_requested.store(true, Ordering::SeqCst);
        let tx = self
            .inner
            .control_tx
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        tx.send(ControlMsg::Stop).map_err(|_| {
            Error::disconnected("session stop recorded, but scheduler notification failed")
        })
    }

    /// Check if a stop has been requested.
    pub fn is_stop_requested(&self) -> bool {
        self.inner.stop_requested.load(Ordering::SeqCst)
    }
}
