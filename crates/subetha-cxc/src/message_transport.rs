//! `MessageTransport` - byte-slice transport trait for the
//! `BackgroundScheduler`, abstracting over `SharedRing` (MPMC) and
//! `SharedDeque<PassSlot>` (SPMC work-stealing).
//!
//! The scheduler's wire format is a fixed-size payload (56 bytes
//! per slot, the encoded `Pass` representation). Both MPMC ring and
//! SPMC deque transports can carry this byte-slice payload; the
//! caller picks based on the workload's topology.
//!
//! ## Picking a transport
//!
//! - `SharedRing` (MPMC): multiple producers + multiple consumers.
//!   The canonical scheduler-submit-ring shape: any process
//!   submits, the worker pops.
//! - `SharedDeque<PassSlot>` (SPMC): single producer, many thieves.
//!   The canonical scheduler-result-ring shape with a single
//!   worker producing and many collectors draining.

use std::sync::Arc;

use subetha_core::{Marshal, MarshalError};

use crate::shared_deque::{DequeError, SharedDeque};
use crate::shared_ring::{RingError, SharedRing, PAYLOAD_BYTES};

/// Errors a `MessageTransport` returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportError {
    /// Transport is at capacity; producer must back off.
    Full,
    /// Transport is empty; consumer has nothing to take.
    Empty,
    /// Caller-supplied payload exceeds the wire-format slot size.
    PayloadTooLarge,
    /// Caller-supplied output buffer is shorter than the slot size.
    OutBufferTooSmall,
    /// Transport-specific error not covered by the categories above.
    Other,
}

/// A byte-slice transport for fixed-size scheduler payloads. Both
/// `SharedRing` and `SharedDeque<PassSlot>` impl this trait so the
/// `BackgroundScheduler` can pick its underlying primitive at
/// construction without changing its hot-loop code.
pub trait MessageTransport: Send + Sync {
    /// Push a payload of length `<= PAYLOAD_BYTES`. Returns
    /// `Err(Full)` if the transport is at capacity.
    fn try_push(&self, payload: &[u8]) -> Result<(), TransportError>;

    /// Pop a payload into `out` (which must be `>= PAYLOAD_BYTES`
    /// long). Returns the byte count written on success, or
    /// `Err(Empty)` if there is nothing to take.
    fn try_pop(&self, out: &mut [u8]) -> Result<usize, TransportError>;
}

/// 56-byte fixed-size payload type for the `SharedDeque<PassSlot>`
/// transport path. Mirrors the byte layout that `SharedRing` uses
/// for `BackgroundScheduler` so the same encoded Pass slot can ride
/// either transport.
#[repr(C, align(8))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PassSlot(pub [u8; PAYLOAD_BYTES]);

impl Default for PassSlot {
    fn default() -> Self {
        Self([0u8; PAYLOAD_BYTES])
    }
}

// SAFETY: `PassSlot` is `#[repr(C, align(8))]` over a single
// `[u8; PAYLOAD_BYTES]` field. The bytes are position-independent
// across address spaces; round-trip is a memcpy.
unsafe impl Marshal for PassSlot {
    const PAYLOAD_BYTES: usize = PAYLOAD_BYTES;

    fn marshal(&self, dst: &mut [u8]) {
        dst[..PAYLOAD_BYTES].copy_from_slice(&self.0);
    }

    fn unmarshal(src: &[u8]) -> Result<Self, MarshalError> {
        if src.len() < PAYLOAD_BYTES {
            return Err(MarshalError::ShortBuffer {
                expected: PAYLOAD_BYTES,
                got: src.len(),
            });
        }
        let mut s = Self([0u8; PAYLOAD_BYTES]);
        s.0.copy_from_slice(&src[..PAYLOAD_BYTES]);
        Ok(s)
    }
}

impl MessageTransport for SharedRing {
    fn try_push(&self, payload: &[u8]) -> Result<(), TransportError> {
        SharedRing::try_push(self, payload).map_err(map_ring_err)
    }

    fn try_pop(&self, out: &mut [u8]) -> Result<usize, TransportError> {
        SharedRing::try_pop(self, out).map_err(map_ring_err)
    }
}

impl MessageTransport for SharedDeque<PassSlot> {
    fn try_push(&self, payload: &[u8]) -> Result<(), TransportError> {
        if payload.len() > PAYLOAD_BYTES {
            return Err(TransportError::PayloadTooLarge);
        }
        let mut slot = PassSlot([0u8; PAYLOAD_BYTES]);
        slot.0[..payload.len()].copy_from_slice(payload);
        self.push(&slot).map_err(map_deque_err)
    }

    fn try_pop(&self, out: &mut [u8]) -> Result<usize, TransportError> {
        if out.len() < PAYLOAD_BYTES {
            return Err(TransportError::OutBufferTooSmall);
        }
        // For SPMC Chase-Lev, the consumer-side primitive is `steal`,
        // not `pop` (pop is the owner-side LIFO end). The
        // BackgroundScheduler's worker is a thief in this topology.
        match self.steal() {
            Some(slot) => {
                out[..PAYLOAD_BYTES].copy_from_slice(&slot.0);
                Ok(PAYLOAD_BYTES)
            }
            None => Err(TransportError::Empty),
        }
    }
}

/// Blanket impl that lets `Arc<dyn MessageTransport>` delegate
/// trait calls through the `Arc`.
impl<T: MessageTransport + ?Sized> MessageTransport for Arc<T> {
    fn try_push(&self, payload: &[u8]) -> Result<(), TransportError> {
        (**self).try_push(payload)
    }

    fn try_pop(&self, out: &mut [u8]) -> Result<usize, TransportError> {
        (**self).try_pop(out)
    }
}

fn map_ring_err(e: RingError) -> TransportError {
    match e {
        RingError::Full => TransportError::Full,
        RingError::Empty => TransportError::Empty,
        RingError::PayloadTooLarge => TransportError::PayloadTooLarge,
        _ => TransportError::Other,
    }
}

fn map_deque_err(e: DequeError) -> TransportError {
    match e {
        DequeError::Full => TransportError::Full,
        _ => TransportError::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha_transport_{name}_{pid}.bin"));
        p
    }

    #[test]
    fn shared_ring_satisfies_message_transport() {
        let path = tmp("ring");
        let ring = SharedRing::create(&path, 4).expect("create");
        let mut payload = [0u8; PAYLOAD_BYTES];
        payload[0] = 0xAB;
        payload[1] = 0xCD;
        ring.try_push(&payload).expect("push");

        let mut out = [0u8; PAYLOAD_BYTES];
        let n = (&ring as &dyn MessageTransport)
            .try_pop(&mut out)
            .expect("pop");
        assert_eq!(n, PAYLOAD_BYTES);
        assert_eq!(out, payload);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn shared_deque_passslot_satisfies_message_transport() {
        let path = tmp("deque");
        let owner = SharedDeque::<PassSlot>::create(&path, 8).expect("create");
        let thief = SharedDeque::<PassSlot>::open_as_thief(&path).expect("thief");

        let mut payload = [0u8; PAYLOAD_BYTES];
        payload[0] = 0x12;
        payload[10] = 0x34;
        (&owner as &dyn MessageTransport)
            .try_push(&payload)
            .expect("push");

        // Steal-side path requires the thief handle.
        let mut out = [0u8; PAYLOAD_BYTES];
        let n = (&thief as &dyn MessageTransport)
            .try_pop(&mut out)
            .expect("pop");
        assert_eq!(n, PAYLOAD_BYTES);
        assert_eq!(out, payload);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn arc_dyn_transport_delegates() {
        let path = tmp("arc_dyn");
        let ring: Arc<dyn MessageTransport> =
            Arc::new(SharedRing::create(&path, 4).expect("create"));
        let mut payload = [0u8; PAYLOAD_BYTES];
        payload[5] = 0xEF;
        ring.try_push(&payload).expect("push");

        let mut out = [0u8; PAYLOAD_BYTES];
        let n = ring.try_pop(&mut out).expect("pop");
        assert_eq!(n, PAYLOAD_BYTES);
        assert_eq!(out, payload);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn payload_too_large_rejected() {
        let path = tmp("oversize");
        let ring = SharedRing::create(&path, 4).expect("create");
        let oversized = vec![0u8; PAYLOAD_BYTES + 1];
        let err = (&ring as &dyn MessageTransport)
            .try_push(&oversized)
            .expect_err("oversize");
        assert_eq!(err, TransportError::PayloadTooLarge);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn empty_returns_err_empty() {
        let path = tmp("empty");
        let ring = SharedRing::create(&path, 4).expect("create");
        let mut out = [0u8; PAYLOAD_BYTES];
        let err = (&ring as &dyn MessageTransport)
            .try_pop(&mut out)
            .expect_err("empty");
        assert_eq!(err, TransportError::Empty);
        std::fs::remove_file(&path).ok();
    }
}
