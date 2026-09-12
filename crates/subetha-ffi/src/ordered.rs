//! Exact global order on a stamped adaptive ring, from the consumer's
//! side. The receiver picks the cheapest strategy that is exact for the
//! ring's stamp source: time-based stamps are delivered directly, since
//! the freshness-guarded merge already orders them; shared-counter stamps
//! are merged by stamp and re-ordered in a window sized to the producer
//! count, which is provably exact; and past 256 producers the ring is
//! flipped to the strict merge and delivered directly. The window holds
//! items while the stream runs; `flush` drains it at the end.
//!
//! One receiver serves one consumer id of one ring. A wait parks on the
//! ring's consumer waker, as the ring's own pop does.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use subetha_cxc::adaptive_ring::AdaptiveRing;
use subetha_cxc::cross_process_waker::CrossProcessWaker;
use subetha_cxc::ordering::{OrderingMode, StampKind, STAMPED_PAYLOAD_BYTES};
use subetha_cxc::reorder::{ReorderBuffer, DEFAULT_CAP, DEFAULT_FLOOR, REORDER_PRODUCER_CAP};
use subetha_cxc::shared_ring::RingError;

use crate::error::{fail, ring_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_EMPTY, SUBETHA_E_WRONG_KIND, SUBETHA_OK};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_ORDERED_RECEIVER};
use crate::ring::{deadline_from, out_buffer, SUBETHA_RING_SLOT_BYTES};
use crate::runtime::{issue, resolve_mode, with_kind, with_ring};
use crate::wait::{wait_until, Waiting};

/// Strategy: the merge by stamp with a consumer-side re-ordering window.
pub const SUBETHA_ORDERED_REORDER: u32 = 0;
/// Strategy: the strict merge; the pop is exact, nothing is buffered.
pub const SUBETHA_ORDERED_STRICT: u32 = 1;
/// Strategy: direct delivery; the stamps need no correction.
pub const SUBETHA_ORDERED_DIRECT: u32 = 2;

/// A snapshot of an ordered receiver.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_ordered_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// One of the `SUBETHA_ORDERED_` constants.
    pub strategy: u32,
    /// The re-ordering window, in items; zero outside the reorder strategy.
    pub window: u32,
    /// Items held in the window right now.
    pub buffered: u32,
    /// Times the window had to grow past the producer count.
    pub corrections: u64,
    /// Ordering-mode flips the ring refused while this receiver ran.
    pub mode_refusals: u64,
}

enum Strategy {
    Reorder(ReorderBuffer),
    Strict,
    Direct,
}

pub(crate) struct OrderedObject {
    ring: Arc<AdaptiveRing>,
    waker: Arc<CrossProcessWaker>,
    consumer_id: usize,
    strategy: Mutex<Strategy>,
    mode_refusals: AtomicU64,
    mode: u32,
    waiting: Waiting,
}

impl OrderedObject {
    /// Pick the strategy for the ring's stamp source and set the ring's
    /// merge mode to match; a flip the ring refuses is counted.
    fn new(ring: Arc<AdaptiveRing>, waker: Arc<CrossProcessWaker>, consumer_id: usize, mode: u32) -> Self {
        let mode_refusals = AtomicU64::new(0);
        let producers = ring.published_producers().max(ring.max_producers());
        let strategy = match ring.stamp_kind() {
            Some(StampKind::SharedCounter) => {
                if producers <= REORDER_PRODUCER_CAP {
                    if ring.set_ordering_mode(OrderingMode::MergeByStamp).is_err() {
                        mode_refusals.fetch_add(1, Ordering::Relaxed);
                    }
                    let window = producers.max(DEFAULT_FLOOR);
                    Strategy::Reorder(ReorderBuffer::with_window(window, window.max(DEFAULT_CAP)))
                } else {
                    if ring.set_ordering_mode(OrderingMode::MergeStrict).is_err() {
                        mode_refusals.fetch_add(1, Ordering::Relaxed);
                    }
                    Strategy::Strict
                }
            }
            _ => Strategy::Direct,
        };
        Self {
            ring,
            waker,
            consumer_id,
            strategy: Mutex::new(strategy),
            mode_refusals,
            mode,
            waiting: Waiting::new(),
        }
    }

    /// Wake this receiver's parked wait so a destroy can proceed.
    pub(crate) fn interrupt(&self) {
        self.waiting.close();
        self.waker.wake_all();
    }

    /// The next in-order item. Under the reorder strategy the ring is
    /// pulled into the window until the window releases an item or the
    /// ring is empty, so `SUBETHA_E_RING_EMPTY` means the ring has nothing
    /// more right now; what the window still holds comes out of `flush`.
    fn try_next(&self, out: &mut [u8]) -> Result<(usize, u64), i32> {
        let mut strategy = self.strategy.lock();
        match &mut *strategy {
            Strategy::Reorder(buffer) => {
                let producers = self.ring.published_producers();
                if producers > buffer.window() {
                    if producers > REORDER_PRODUCER_CAP
                        && self.ring.set_ordering_mode(OrderingMode::MergeStrict).is_err()
                    {
                        self.mode_refusals.fetch_add(1, Ordering::Relaxed);
                    }
                    buffer.widen_to(producers.min(REORDER_PRODUCER_CAP));
                }
                let mut scratch = [0u8; STAMPED_PAYLOAD_BYTES];
                loop {
                    match self.ring.try_recv_with_stamp(self.consumer_id, &mut scratch) {
                        Ok((n, stamp)) => {
                            buffer.push(stamp, &scratch[..n]);
                            match buffer.try_take(out) {
                                Some((stamp, len)) => return Ok((len, stamp)),
                                None => continue,
                            }
                        }
                        Err(RingError::Empty) => {
                            return match buffer.try_take(out) {
                                Some((stamp, len)) => Ok((len, stamp)),
                                None => Err(SUBETHA_E_RING_EMPTY),
                            };
                        }
                        Err(e) => return Err(ring_code(e)),
                    }
                }
            }
            Strategy::Strict | Strategy::Direct => {
                self.ring.try_recv_with_stamp(self.consumer_id, out).map_err(ring_code)
            }
        }
    }

    fn next_wait(&self, out: &mut [u8], deadline: Option<Instant>) -> Result<(usize, u64), i32> {
        wait_until(&self.waiting, &self.waker, deadline, SUBETHA_E_RING_EMPTY, || self.try_next(out))
    }

    /// One buffered item in order, or `SUBETHA_E_RING_EMPTY` once the
    /// window is drained; nothing is ever buffered outside the reorder
    /// strategy.
    fn flush(&self, out: &mut [u8]) -> Result<(usize, u64), i32> {
        let mut strategy = self.strategy.lock();
        match &mut *strategy {
            Strategy::Reorder(buffer) => match buffer.flush_one(out) {
                Some((stamp, len)) => Ok((len, stamp)),
                None => Err(SUBETHA_E_RING_EMPTY),
            },
            Strategy::Strict | Strategy::Direct => Err(SUBETHA_E_RING_EMPTY),
        }
    }

    fn stats(&self) -> subetha_ordered_stats {
        let strategy = self.strategy.lock();
        let (kind, window, buffered, corrections) = match &*strategy {
            Strategy::Reorder(buffer) => (SUBETHA_ORDERED_REORDER, buffer.window() as u32, buffer.len() as u32, buffer.corrections()),
            Strategy::Strict => (SUBETHA_ORDERED_STRICT, 0, 0, 0),
            Strategy::Direct => (SUBETHA_ORDERED_DIRECT, 0, 0, 0),
        };
        subetha_ordered_stats {
            mode: self.mode,
            strategy: kind,
            window,
            buffered,
            corrections,
            mode_refusals: self.mode_refusals.load(Ordering::Relaxed),
        }
    }
}

fn with_ordered(handle: subetha_handle, f: impl FnOnce(&OrderedObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_ORDERED_RECEIVER, |object| match object {
        Object::Ordered(o) => f(o),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name an ordered receiver"),
    })
}

/// Make an exact-delivery receiver for `consumer_id` on the stamped ring
/// `ring` names; the strategy follows the ring's stamp source and the
/// ring's merge mode is set to match. `SUBETHA_E_RING_NOT_STAMPED` on a
/// ring built without stamps.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ring_ordered_receiver(ring: subetha_handle, consumer_id: u32, mode: u32, out: *mut subetha_handle) -> i32 {
    with_ring(ring, |r| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let mode = match resolve_mode(mode) {
            Ok(m) => m,
            Err(code) => return code,
        };
        if r.ring.stamp_kind().is_none() {
            return crate::error::SUBETHA_E_RING_NOT_STAMPED;
        }
        let object = OrderedObject::new(Arc::clone(&r.ring), Arc::clone(&r.consumer_waker), consumer_id as usize, mode);
        unsafe { issue(Object::Ordered(object), out) }
    })
}

/// The next item in exact order into `out`, at least
/// `SUBETHA_RING_SLOT_BYTES`, its length into `out_len` and its stamp into
/// `out_stamp`. `SUBETHA_E_RING_EMPTY` while nothing is ready, which under
/// the reorder strategy includes the window still filling.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` and `out_stamp` are
/// valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ordered_try_next(handle: subetha_handle, out: *mut u8, cap: usize, out_len: *mut usize, out_stamp: *mut u64) -> i32 {
    with_ordered(handle, |o| {
        if out_stamp.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_stamp is null");
        }
        let buf = match unsafe { out_buffer(out, cap, out_len, SUBETHA_RING_SLOT_BYTES) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match o.try_next(buf) {
            Ok((n, stamp)) => {
                // SAFETY: checked non-null; the caller guarantees they are writable.
                unsafe {
                    *out_len = n;
                    *out_stamp = stamp;
                }
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// `subetha_ordered_try_next`, parking on the ring's consumer waker until
/// a push arrives or `timeout_ms` elapses; the waiting contract is
/// `subetha_ring_pop_wait`'s.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` and `out_stamp` are
/// valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ordered_next_wait(
    handle: subetha_handle,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
    out_stamp: *mut u64,
    timeout_ms: i64,
) -> i32 {
    with_ordered(handle, |o| {
        if out_stamp.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_stamp is null");
        }
        let buf = match unsafe { out_buffer(out, cap, out_len, SUBETHA_RING_SLOT_BYTES) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        let deadline = match deadline_from(timeout_ms) {
            Ok(d) => d,
            Err(code) => return code,
        };
        match o.next_wait(buf, deadline) {
            Ok((n, stamp)) => {
                // SAFETY: checked non-null; the caller guarantees they are writable.
                unsafe {
                    *out_len = n;
                    *out_stamp = stamp;
                }
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// One item the window still holds, in order; call in a loop at the end
/// of the stream until `SUBETHA_E_RING_EMPTY`.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` and `out_stamp` are
/// valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ordered_flush(handle: subetha_handle, out: *mut u8, cap: usize, out_len: *mut usize, out_stamp: *mut u64) -> i32 {
    with_ordered(handle, |o| {
        if out_stamp.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_stamp is null");
        }
        let buf = match unsafe { out_buffer(out, cap, out_len, SUBETHA_RING_SLOT_BYTES) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match o.flush(buf) {
            Ok((n, stamp)) => {
                // SAFETY: checked non-null; the caller guarantees they are writable.
                unsafe {
                    *out_len = n;
                    *out_stamp = stamp;
                }
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// A snapshot of the receiver into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_ordered_read_stats(handle: subetha_handle, out: *mut subetha_ordered_stats) -> i32 {
    with_ordered(handle, |o| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = o.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::SUBETHA_MODE_STRICT;

    /// Every item a receiver yields, streaming then flushing, with the
    /// stamps checked monotone along the way.
    fn drain(receiver: &OrderedObject) -> Vec<u8> {
        let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
        let mut got = Vec::new();
        let mut last_stamp = 0u64;
        loop {
            match receiver.try_next(&mut out) {
                Ok((_, stamp)) => {
                    assert!(stamp >= last_stamp, "stamps come out in order");
                    last_stamp = stamp;
                    got.push(out[0]);
                }
                Err(code) => {
                    assert_eq!(code, SUBETHA_E_RING_EMPTY);
                    break;
                }
            }
        }
        loop {
            match receiver.flush(&mut out) {
                Ok((_, stamp)) => {
                    assert!(stamp >= last_stamp, "flushed stamps come out in order");
                    last_stamp = stamp;
                    got.push(out[0]);
                }
                Err(code) => {
                    assert_eq!(code, SUBETHA_E_RING_EMPTY);
                    break;
                }
            }
        }
        got
    }

    #[test]
    fn counter_stamps_get_the_reorder_strategy_and_come_out_in_stamp_order() {
        let ring = Arc::new(
            AdaptiveRing::create_anon(2, 1, 64)
                .unwrap()
                .with_ordering_stamps_kind(StampKind::SharedCounter)
                .unwrap(),
        );
        let waker = Arc::new(CrossProcessWaker::create_anon(4).unwrap());
        let p0 = ring.register_producer().unwrap();
        let p1 = ring.register_producer().unwrap();
        let cid = ring.register_consumer().unwrap();
        let receiver = OrderedObject::new(Arc::clone(&ring), waker, cid, SUBETHA_MODE_STRICT);
        assert_eq!(receiver.stats().strategy, SUBETHA_ORDERED_REORDER);
        assert_eq!(ring.ordering_mode(), Some(OrderingMode::MergeByStamp));
        for i in 0..20u8 {
            let producer = if i % 2 == 0 { p0 } else { p1 };
            ring.try_send(producer, &[i]).unwrap();
        }
        let mut got = drain(&receiver);
        got.sort_unstable();
        assert_eq!(got, (0..20u8).collect::<Vec<_>>());
    }

    #[test]
    fn time_stamps_are_delivered_directly() {
        let ring = Arc::new(
            AdaptiveRing::create_anon(1, 1, 8)
                .unwrap()
                .with_ordering_stamps_kind(StampKind::Monotonic)
                .unwrap(),
        );
        let waker = Arc::new(CrossProcessWaker::create_anon(4).unwrap());
        let cid = ring.register_consumer().unwrap();
        let receiver = OrderedObject::new(Arc::clone(&ring), waker, cid, SUBETHA_MODE_STRICT);
        assert_eq!(receiver.stats().strategy, SUBETHA_ORDERED_DIRECT);
        assert!(drain(&receiver).is_empty());
    }
}
