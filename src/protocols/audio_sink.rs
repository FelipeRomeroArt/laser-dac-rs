//! Shared scaffolding for the audio-clocked backends (AVB, oscilloscope).
//!
//! Both backends drive their output through a cpal audio stream fed from a
//! producer thread via a lock-free ring. The pieces they have in common live
//! here so the real-time-critical invariants are stated and enforced in one
//! place:
//!
//! - [`AudioSinkState`] is the shared producer/consumer core: a
//!   `crossbeam_queue::ArrayQueue` plus atomics only. Everything inside the
//!   cpal audio callback must be wait-free — no `Mutex`, no allocation, no
//!   blocking syscalls.
//! - [`RunningAudioStream`] / [`CpalStreamHandle`] are the tiny lifecycle seam
//!   that keeps the non-`Send` cpal stream on its owning thread.
//! - [`push_chunk_resampled`] implements the take-and-restore scratch
//!   discipline so the hot write path stays allocation-free in steady state.
//!
//! What is deliberately *not* consolidated: device enumeration/config
//! resolution, callback payload formatting (AVB premultiplies RGBI across 4–6
//! channels; the oscilloscope maps XY to stereo with a mute ramp), and thread
//! lifecycle (AVB uses an init-handshake worker; the oscilloscope a simple
//! stop-flag loop). These differ enough per backend that forcing them through
//! one generic would obscure rather than unify.

use std::sync::atomic::{AtomicU32, Ordering};

use crossbeam_queue::ArrayQueue;

use crate::buffer_estimate::QueueDepthSource;
use crate::resample::{CatmullInterp, StreamingResampler};

/// Shared producer/consumer core for audio-clocked backends.
///
/// A lock-free ring of samples/points plus the "last emitted output" pair of
/// atomics used to hold position on underrun. Safe to share between the
/// producer thread (`Arc<AudioSinkState>`) and the realtime cpal callback.
pub(crate) struct AudioSinkState<P> {
    /// Lock-free queue bridging the producer thread and the audio callback.
    /// Direct access is permitted (tests push/pop directly); realtime users
    /// should prefer [`AudioSinkState::push_point`] / [`AudioSinkState::pop`].
    pub(crate) queue: ArrayQueue<P>,
    /// Device output sample rate in Hz (used to convert queue depth to
    /// pps-points and to size time-based behavior such as mute ramps).
    sample_rate: u32,
    /// Last emitted output pair, as f32 bits. Held on underrun so the beam
    /// stays put instead of snapping somewhere arbitrary. Written only by
    /// the audio callback; read by the producer for diagnostics.
    last_a_bits: AtomicU32,
    last_b_bits: AtomicU32,
}

impl<P> AudioSinkState<P> {
    pub(crate) fn new(capacity: usize, sample_rate: u32) -> Self {
        Self {
            queue: ArrayQueue::new(capacity),
            sample_rate,
            last_a_bits: AtomicU32::new(0.0f32.to_bits()),
            last_b_bits: AtomicU32::new(0.0f32.to_bits()),
        }
    }

    pub(crate) fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    pub(crate) fn remaining_capacity(&self) -> usize {
        self.queue.capacity().saturating_sub(self.queue.len())
    }

    pub(crate) fn has_capacity_for(&self, count: usize) -> bool {
        count == 0 || self.remaining_capacity() >= count
    }

    pub(crate) fn queued_points(&self) -> u64 {
        self.queue.len() as u64
    }

    pub(crate) fn clear_queue(&self) {
        while self.queue.pop().is_some() {}
    }

    pub(crate) fn pop(&self) -> Option<P> {
        self.queue.pop()
    }

    /// Push one item, skipping it in release builds if the queue is full.
    ///
    /// Callers must reserve capacity first via
    /// [`has_capacity_for`](Self::has_capacity_for) using the resampler's
    /// `pending_output_count`, so a full queue indicates a contract violation
    /// elsewhere rather than expected backpressure. In debug builds that
    /// violation is caught by the assertion below; in release builds we drop
    /// the point (laser-safe silence) instead of panicking on the audio path.
    ///
    ///
    /// Note concurrent consumption by the audio callback only ever *shrinks*
    /// the queue, so a successful capacity check cannot become stale.
    pub(crate) fn push_point(&self, point: P) {
        let pushed = self.queue.push(point);
        debug_assert!(pushed.is_ok(), "queue capacity validated before push");
    }

    pub(crate) fn set_last_output(&self, a: f32, b: f32) {
        self.last_a_bits.store(a.to_bits(), Ordering::Release);
        self.last_b_bits.store(b.to_bits(), Ordering::Release);
    }

    pub(crate) fn last_output(&self) -> (f32, f32) {
        (
            f32::from_bits(self.last_a_bits.load(Ordering::Acquire)),
            f32::from_bits(self.last_b_bits.load(Ordering::Acquire)),
        )
    }
}

impl<P: Send> QueueDepthSource for AudioSinkState<P> {
    fn queued_points(&self) -> u64 {
        AudioSinkState::queued_points(self)
    }
    fn sample_rate(&self) -> u32 {
        AudioSinkState::sample_rate(self)
    }
}

/// A running audio output stream. Dropping the concrete value stops output.
///
/// The concrete type (a cpal stream in production) stays on the thread that
/// built it and is never sent across threads, so no `Send` bound is required.
pub(crate) trait RunningAudioStream {}

/// Production [`RunningAudioStream`] handle wrapping a cpal stream.
pub(crate) struct CpalStreamHandle {
    _stream: cpal::Stream,
}

impl RunningAudioStream for CpalStreamHandle {}

impl CpalStreamHandle {
    /// Box a freshly-built, already-playing cpal stream as a generic handle.
    pub(crate) fn boxed(stream: cpal::Stream) -> Box<dyn RunningAudioStream> {
        Box::new(CpalStreamHandle { _stream: stream })
    }
}

/// Convert a chunk into `scratch`, resample it into `sink`'s queue, and
/// restore the scratch buffer — the take-and-restore discipline that keeps
/// the steady-state write path heap-allocation-free.
///
/// `fill` converts the caller's input chunk into `scratch` (which is cleared
/// first). The scratch buffer is then handed to the resampler by move and
/// written back afterwards, so capacity is retained across calls. Capacity
/// must already have been reserved on `sink` for exactly
/// `resampler.pending_output_count(input_len)` items (see
/// [`AudioSinkState::push_point`]).
pub(crate) fn push_chunk_resampled<P: CatmullInterp>(
    sink: &AudioSinkState<P>,
    scratch: &mut Vec<P>,
    fill: impl FnOnce(&mut Vec<P>),
    resampler: &mut StreamingResampler<P>,
) {
    scratch.clear();
    fill(scratch);
    let taken = std::mem::take(scratch);
    resampler.process(&taken, |p| sink.push_point(p));
    *scratch = taken;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_accounting_and_clear() {
        let state = AudioSinkState::<(f32, f32)>::new(4, 48_000);
        assert_eq!(state.sample_rate(), 48_000);
        assert!(state.has_capacity_for(0));
        assert!(state.has_capacity_for(4));
        assert!(!state.has_capacity_for(5));

        state.push_point((1.0, -1.0));
        assert_eq!(state.queued_points(), 1);
        assert!(state.has_capacity_for(3));

        // Filling exactly to capacity succeeds (reservation discipline).
        state.push_point((0.0, 0.0));
        state.push_point((0.0, 0.0));
        state.push_point((0.0, 0.0));
        assert_eq!(state.queued_points(), 4);
        // Overfilling past capacity would trip the debug_assert in
        // `push_point`; callers must reserve via `has_capacity_for` first.
        assert!(!state.has_capacity_for(1));

        state.clear_queue();
        assert_eq!(state.queued_points(), 0);
        assert!(state.has_capacity_for(4));
    }

    #[test]
    fn held_output_round_trips_through_bits() {
        let state = AudioSinkState::<u8>::new(2, 48_000);
        assert_eq!(state.last_output(), (0.0, 0.0));
        state.set_last_output(0.25, -0.75);
        assert_eq!(state.last_output(), (0.25, -0.75));
    }
}
