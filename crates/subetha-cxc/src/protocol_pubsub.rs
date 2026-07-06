//! `PubSubRing`: one-producer many-subscriber broadcast primitive
//! with per-subscriber positions.
//!
//! Where a regular ring (SpscRingCore) has one consumer position
//! (the tail), PubSubRing exposes the producer's monotonic head
//! as the absolute position and lets each subscriber walk
//! positions independently. Subscriber positions are tracked
//! externally via [`SubscriberPosition`], so they can survive a
//! subscriber restart.
//!
//! # Slot layout
//!
//! Each slot carries a `sequence: AtomicU64` + 56-byte payload.
//! On a successful `publish(payload)`, the producer:
//! 1. Writes the payload into slot[head % capacity].
//! 2. Releases the new sequence = head + 1.
//! 3. Releases head + 1 into the header.
//!
//! On `read_at(position)`, a subscriber:
//! 1. Reads the slot's sequence with Acquire.
//! 2. Validates `sequence == position + 1` (matches expected slot).
//!    If `sequence > position + 1`, the slot has been overwritten
//!    (wraparound); subscriber returns `PubSubReadError::Lost`.
//!    If `sequence < position + 1`, the slot is not yet published;
//!    subscriber returns `PubSubReadError::Pending`.
//! 3. On match: copies the payload to the out buffer.
//!
//! # KeepAll vs KeepLastN policy
//!
//! The primitive itself is KeepLastN-shaped: producer never blocks
//! on subscribers; wraparound happens at capacity. Callers that
//! want KeepAll semantics check the minimum subscriber position
//! before publishing and back off when the ring is about to wrap
//! past it. Helpers for that pattern can layer on top.

use std::cell::UnsafeCell;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use memmap2::{MmapMut, MmapOptions};

use crate::replay_positions::SubscriberPosition;

/// Payload bytes per slot. Matches the Vyukov-side payload size to
/// keep the substrate's per-slot byte layout consistent across
/// primitives.
pub const PUBSUB_PAYLOAD_BYTES: usize = 56;

const PUBSUB_SLOT_SIZE: usize = 64; // 8-byte seq + 56-byte payload
const PUBSUB_MAGIC: u64 = 0xE7_E7_E7_E7_50_55_42_53; // ASCII "...PUBS"

#[repr(C, align(64))]
struct PubSubHeader {
    magic: u64,
    capacity: u64,
    slot_size: u64,
    _pad_meta: [u8; 64 - 24],
    head: AtomicU64,
    _pad_head: [u8; 64 - 8],
}

#[repr(C, align(64))]
struct PubSubSlot {
    sequence: AtomicU64,
    payload: UnsafeCell<[u8; PUBSUB_PAYLOAD_BYTES]>,
}

/// One-producer many-subscriber broadcast ring with per-subscriber
/// positions.
pub struct PubSubRing {
    _backing: PubSubBacking,
    raw_ptr: *mut u8,
    capacity: usize,
}

unsafe impl Send for PubSubRing {}
unsafe impl Sync for PubSubRing {}

/// Storage backing for a `PubSubRing`. Owns the underlying
/// resource for the lifetime of the ring; the hot-path
/// pointer (`raw_ptr` on `PubSubRing`) is cached at construction.
/// The held values are intentionally only kept for ownership;
/// the `File` and `ShmFile` payloads are not read at runtime.
#[allow(dead_code)]
enum PubSubBacking {
    /// In-process anonymous mmap.
    Anon(MmapMut),
    /// File-backed mmap (cross-process via OS page cache).
    File(File, MmapMut),
    /// Named shared-memory mmap (ShmFs locale; cross-process,
    /// RAM-resident).
    Shm(crate::shm_file::ShmFile),
}

/// Errors a subscriber read can return.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PubSubReadError {
    /// The requested position has not been published yet.
    Pending,
    /// The requested position has been overwritten by the producer.
    /// Subscriber lagged more than `capacity` positions behind.
    Lost,
}

pub const fn pubsub_ring_file_size(capacity: usize) -> usize {
    std::mem::size_of::<PubSubHeader>() + capacity * PUBSUB_SLOT_SIZE
}

impl PubSubRing {
    /// Construct an anonymous in-process pub/sub ring.
    pub fn create_anon(capacity: usize) -> std::io::Result<Self> {
        assert!(capacity.is_power_of_two() && capacity >= 2,
                "capacity must be pow2 >= 2");
        let total = pubsub_ring_file_size(capacity);
        let mut mmap = MmapOptions::new().len(total).map_anon()?;
        let raw_ptr = mmap.as_mut_ptr();
        init_pubsub_layout(raw_ptr, capacity);
        Ok(Self {
            _backing: PubSubBacking::Anon(mmap),
            raw_ptr, capacity,
        })
    }

    /// Construct a file-backed pub/sub ring. Cross-process via
    /// the OS page cache.
    pub fn create(path: impl AsRef<Path>, capacity: usize) -> std::io::Result<Self> {
        assert!(capacity.is_power_of_two() && capacity >= 2,
                "capacity must be pow2 >= 2");
        let total = pubsub_ring_file_size(capacity);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let raw_ptr = mmap.as_mut_ptr();
        init_pubsub_layout(raw_ptr, capacity);
        Ok(Self {
            _backing: PubSubBacking::File(file, mmap),
            raw_ptr, capacity,
        })
    }

    /// Open an existing file-backed pub/sub ring. Validates magic
    /// + capacity.
    pub fn open(path: impl AsRef<Path>, expected_capacity: usize) -> std::io::Result<Self> {
        let total = pubsub_ring_file_size(expected_capacity);
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if (file.metadata()?.len() as usize) < total {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "pubsub file too small for expected capacity",
            ));
        }
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let raw_ptr = mmap.as_mut_ptr();
        let header = unsafe { &*(raw_ptr as *const PubSubHeader) };
        if header.magic != PUBSUB_MAGIC
            || header.capacity != expected_capacity as u64
            || header.slot_size != PUBSUB_SLOT_SIZE as u64
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "pubsub file layout mismatch",
            ));
        }
        Ok(Self {
            _backing: PubSubBacking::File(file, mmap),
            raw_ptr, capacity: expected_capacity,
        })
    }

    /// Construct a fresh pub/sub ring on top of a named
    /// RAM-resident shared-memory backing.
    pub fn create_from_shm(
        mut shm: crate::shm_file::ShmFile,
        capacity: usize,
    ) -> std::io::Result<Self> {
        assert!(capacity.is_power_of_two() && capacity >= 2,
                "capacity must be pow2 >= 2");
        let total = pubsub_ring_file_size(capacity);
        if shm.len() < total {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "shm region too small for pubsub layout",
            ));
        }
        let raw_ptr = shm.as_mut_slice().as_mut_ptr();
        init_pubsub_layout(raw_ptr, capacity);
        Ok(Self {
            _backing: PubSubBacking::Shm(shm),
            raw_ptr, capacity,
        })
    }

    /// Open an existing named ShmFs-backed pub/sub ring.
    pub fn open_from_shm(
        mut shm: crate::shm_file::ShmFile,
        expected_capacity: usize,
    ) -> std::io::Result<Self> {
        let total = pubsub_ring_file_size(expected_capacity);
        if shm.len() < total {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "shm region too small for expected capacity",
            ));
        }
        let raw_ptr = shm.as_mut_slice().as_mut_ptr();
        let header = unsafe { &*(raw_ptr as *const PubSubHeader) };
        if header.magic != PUBSUB_MAGIC
            || header.capacity != expected_capacity as u64
            || header.slot_size != PUBSUB_SLOT_SIZE as u64
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "shm layout mismatch",
            ));
        }
        Ok(Self {
            _backing: PubSubBacking::Shm(shm),
            raw_ptr, capacity: expected_capacity,
        })
    }

    fn header(&self) -> &PubSubHeader {
        unsafe { &*(self.raw_ptr as *const PubSubHeader) }
    }

    fn slot(&self, idx: usize) -> &PubSubSlot {
        let slots_base = unsafe {
            self.raw_ptr.add(std::mem::size_of::<PubSubHeader>())
        };
        let masked = idx & (self.capacity - 1);
        unsafe { &*(slots_base.add(masked * PUBSUB_SLOT_SIZE) as *const PubSubSlot) }
    }

    /// Producer's published head. Equals the next position that
    /// will be assigned to a `publish` call.
    pub fn head(&self) -> u64 {
        self.header().head.load(Ordering::Acquire)
    }

    /// Capacity in slots (always a power of 2).
    pub fn capacity(&self) -> usize { self.capacity }

    /// Publish one payload. Returns the absolute position assigned
    /// to this item. Caller MUST be the single producer.
    pub fn publish(&self, payload: &[u8]) -> u64 {
        assert!(payload.len() <= PUBSUB_PAYLOAD_BYTES);
        let header = self.header();
        let head = header.head.load(Ordering::Relaxed);
        let slot = self.slot(head as usize);
        // Write payload first.
        unsafe {
            let dst = (*slot.payload.get()).as_mut_ptr();
            std::ptr::copy_nonoverlapping(payload.as_ptr(), dst, payload.len());
            if payload.len() < PUBSUB_PAYLOAD_BYTES {
                std::ptr::write_bytes(
                    dst.add(payload.len()), 0,
                    PUBSUB_PAYLOAD_BYTES - payload.len(),
                );
            }
        }
        // Release-store the slot sequence so subscribers see the
        // payload BEFORE the sequence advances.
        slot.sequence.store(head + 1, Ordering::Release);
        // Advance the header head; subscribers walking the head
        // pointer see the new item.
        header.head.store(head + 1, Ordering::Release);
        head
    }

    /// Read the payload at absolute `position`. The subscriber
    /// supplies a buffer of at least [`PUBSUB_PAYLOAD_BYTES`].
    ///
    /// Returns `Ok(())` on success (payload copied into `out`),
    /// `Err(Pending)` when the position has not been published yet,
    /// `Err(Lost)` when the slot has wrapped past `position`.
    pub fn read_at(
        &self,
        position: u64,
        out: &mut [u8],
    ) -> Result<(), PubSubReadError> {
        assert!(out.len() >= PUBSUB_PAYLOAD_BYTES);
        let slot = self.slot(position as usize);
        let observed_seq = slot.sequence.load(Ordering::Acquire);
        let expected_seq = position + 1;
        if observed_seq == expected_seq {
            unsafe {
                let src = (*slot.payload.get()).as_ptr();
                std::ptr::copy_nonoverlapping(
                    src, out.as_mut_ptr(), PUBSUB_PAYLOAD_BYTES,
                );
            }
            Ok(())
        } else if observed_seq > expected_seq {
            Err(PubSubReadError::Lost)
        } else {
            Err(PubSubReadError::Pending)
        }
    }
}

/// Subscriber-side helper that holds a [`SubscriberPosition`] and
/// pulls items from a `PubSubRing` in order.
pub struct PubSubSubscriber {
    ring: Arc<PubSubRing>,
    position: SubscriberPosition,
}

impl PubSubSubscriber {
    /// Wrap a ring + position into a subscriber.
    pub fn new(ring: Arc<PubSubRing>, position: SubscriberPosition) -> Self {
        Self { ring, position }
    }

    /// Current absolute position this subscriber has consumed up to.
    pub fn position(&self) -> u64 { self.position.get() }

    /// Ring this subscriber is attached to.
    pub fn ring(&self) -> &Arc<PubSubRing> { &self.ring }

    /// Advance the subscriber's position by `n` without reading.
    /// Used by callers that want to skip items deliberately
    /// (sampled subscriptions, late-join skip-ahead).
    pub fn skip(&self, n: u64) -> u64 {
        self.position.advance(n)
    }

    /// Try to read the next item. On success, advances the
    /// subscriber's position by 1. On `Pending`, leaves the
    /// position alone. On `Lost`, advances the position to the
    /// ring's current head (skipping past the gap).
    pub fn try_next(&self, out: &mut [u8]) -> Result<(), PubSubReadError> {
        let pos = self.position.get();
        match self.ring.read_at(pos, out) {
            Ok(()) => {
                self.position.advance(1);
                Ok(())
            }
            Err(PubSubReadError::Lost) => {
                // Skip past the gap by jumping to the current head.
                self.position.set(self.ring.head());
                Err(PubSubReadError::Lost)
            }
            Err(other) => Err(other),
        }
    }
}

fn init_pubsub_layout(ptr: *mut u8, capacity: usize) {
    let header_ptr = ptr as *mut PubSubHeader;
    unsafe {
        std::ptr::write(header_ptr, PubSubHeader {
            magic: PUBSUB_MAGIC,
            capacity: capacity as u64,
            slot_size: PUBSUB_SLOT_SIZE as u64,
            _pad_meta: [0; 64 - 24],
            head: AtomicU64::new(0),
            _pad_head: [0; 64 - 8],
        });
    }
    let slots_base = unsafe { ptr.add(std::mem::size_of::<PubSubHeader>()) };
    for i in 0..capacity {
        let slot_ptr = unsafe { slots_base.add(i * PUBSUB_SLOT_SIZE) as *mut PubSubSlot };
        unsafe {
            std::ptr::write(slot_ptr, PubSubSlot {
                sequence: AtomicU64::new(0),
                payload: UnsafeCell::new([0; PUBSUB_PAYLOAD_BYTES]),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_pos(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        p.push(format!("pubsub_pos_{pid}_{nonce}_{name}.bin"));
        p
    }

    #[test]
    fn publish_then_read_at() {
        let ring = PubSubRing::create_anon(8).expect("create");
        let payload = [0xABu8; PUBSUB_PAYLOAD_BYTES];
        let pos = ring.publish(&payload);
        assert_eq!(pos, 0);
        assert_eq!(ring.head(), 1);

        let mut out = [0u8; PUBSUB_PAYLOAD_BYTES];
        ring.read_at(0, &mut out).expect("read at 0");
        assert_eq!(out, payload);
    }

    #[test]
    fn read_pending_for_unpublished_position() {
        let ring = PubSubRing::create_anon(8).expect("create");
        let mut out = [0u8; PUBSUB_PAYLOAD_BYTES];
        assert_eq!(ring.read_at(0, &mut out), Err(PubSubReadError::Pending));
    }

    #[test]
    fn read_lost_for_overwritten_position() {
        let ring = PubSubRing::create_anon(4).expect("create");
        // Publish 8 items into a 4-slot ring; position 0 gets
        // overwritten by position 4.
        for i in 0u64..8 {
            let mut payload = [0u8; PUBSUB_PAYLOAD_BYTES];
            payload[..8].copy_from_slice(&i.to_le_bytes());
            ring.publish(&payload);
        }
        let mut out = [0u8; PUBSUB_PAYLOAD_BYTES];
        // Position 0's slot has been overwritten by position 4
        // (both map to slot index 0). Reading at 0 sees seq=5 > 1.
        assert_eq!(ring.read_at(0, &mut out), Err(PubSubReadError::Lost));
        // Position 7 (just published) still has the right seq.
        ring.read_at(7, &mut out).expect("read at 7");
        assert_eq!(&out[..8], &7u64.to_le_bytes());
    }

    #[test]
    fn two_subscribers_independent_positions() {
        let ring = Arc::new(PubSubRing::create_anon(16).expect("create"));
        for i in 0u64..5 {
            let mut payload = [0u8; PUBSUB_PAYLOAD_BYTES];
            payload[..8].copy_from_slice(&i.to_le_bytes());
            ring.publish(&payload);
        }

        let pos_a = SubscriberPosition::create(tmp_pos("sub_a"), 0).expect("pos a");
        let pos_b = SubscriberPosition::create(tmp_pos("sub_b"), 0).expect("pos b");
        let sub_a = PubSubSubscriber::new(ring.clone(), pos_a);
        let sub_b = PubSubSubscriber::new(ring.clone(), pos_b);

        // Both subs read independently; their positions stay separate.
        let mut buf = [0u8; PUBSUB_PAYLOAD_BYTES];
        sub_a.try_next(&mut buf).expect("a 0"); assert_eq!(&buf[..8], &0u64.to_le_bytes());
        sub_a.try_next(&mut buf).expect("a 1"); assert_eq!(&buf[..8], &1u64.to_le_bytes());
        // Sub B is still at 0.
        sub_b.try_next(&mut buf).expect("b 0"); assert_eq!(&buf[..8], &0u64.to_le_bytes());
        assert_eq!(sub_a.position(), 2);
        assert_eq!(sub_b.position(), 1);
    }

    #[test]
    fn subscriber_skips_past_lost_items() {
        let ring = Arc::new(PubSubRing::create_anon(4).expect("create"));
        let pos = SubscriberPosition::create(tmp_pos("lost"), 0).expect("pos");
        let sub = PubSubSubscriber::new(ring.clone(), pos);

        // Publish 8 items into 4 slots -> positions 0..4 overwritten.
        for i in 0u64..8 {
            let mut payload = [0u8; PUBSUB_PAYLOAD_BYTES];
            payload[..8].copy_from_slice(&i.to_le_bytes());
            ring.publish(&payload);
        }

        // Subscriber at position 0 reads -> sees Lost.
        let mut buf = [0u8; PUBSUB_PAYLOAD_BYTES];
        assert_eq!(sub.try_next(&mut buf), Err(PubSubReadError::Lost));
        // Subscriber's position is now at ring head = 8.
        assert_eq!(sub.position(), 8);
        // Next read is Pending (no item at position 8 yet).
        assert_eq!(sub.try_next(&mut buf), Err(PubSubReadError::Pending));
    }
}
