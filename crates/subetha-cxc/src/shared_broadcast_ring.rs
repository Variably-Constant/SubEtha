//! `SharedBroadcastRing` - single-producer, multi-consumer pub/sub
//! ring backed by an MMF.
//!
//! Distinct from [`SharedRing`](crate::SharedRing) (MPMC; each slot
//! consumed exactly once). In a broadcast ring, EVERY registered
//! consumer sees EVERY message independently with its own cursor.
//! This is the pub/sub / kafka-topic / log-tail shape.
//!
//! # Layout
//!
//! ```text
//! +---------------------------+
//! | BroadcastHeader (128B)    |
//! |   magic, capacity         |
//! |   producer_seq: AtomicU64 |
//! |   consumer_seqs[MAX]:     |
//! |     [AtomicU64; MAX_CONS] |
//! |   consumer_active[MAX]:   |
//! |     [AtomicU32; MAX_CONS] |
//! +---------------------------+
//! | Slot[0]    (64B)          |  version + payload
//! | ...                       |
//! | Slot[capacity - 1]        |
//! +---------------------------+
//! ```
//!
//! Each slot is a SeqLock cell; producer writes under odd version
//! and bumps even on completion. Consumers read with version-spin.
//!
//! # Protocol
//!
//! ## Producer
//!
//! `try_push(payload)`:
//! 1. Find oldest consumer cursor: `min_consumer = min(active
//!    consumer_seqs)`.
//! 2. If `producer_seq - min_consumer >= capacity`, the ring is
//!    full from at least one consumer's perspective: return
//!    `BroadcastFull`. (A slot in `[min_consumer, producer_seq)`
//!    can't be overwritten without making that consumer miss it.)
//! 3. Write payload into `slot[producer_seq % capacity]` under
//!    SeqLock.
//! 4. `producer_seq.fetch_add(1, Release)`.
//!
//! ## Consumer
//!
//! `register()` -> consumer_idx in 0..MAX_CONSUMERS:
//! - CAS the first inactive slot to active; initialise
//!   `consumer_seqs[i]` to current `producer_seq` (so the consumer
//!   starts from "now," not from the beginning of history).
//!
//! `try_recv(consumer_idx, buf)`:
//! 1. Load `my_seq = consumer_seqs[consumer_idx]`.
//! 2. If `my_seq >= producer_seq`, no new messages: return Empty.
//! 3. SeqLock-read `slot[my_seq % capacity]` into `buf`.
//! 4. `consumer_seqs[consumer_idx].fetch_add(1, Release)`.
//!
//! `unregister(consumer_idx)`: mark slot inactive; producer no
//! longer waits for this consumer's cursor.
//!
//! # Why single-producer?
//!
//! Multi-producer broadcast adds complexity (producers must
//! coordinate slot claim AND ordering must be preserved per topic).
//! The single-producer case covers most pub/sub use cases: one
//! source, many subscribers. For multi-producer fan-in, the
//! producers should fan into a SharedRing first, then a single
//! relay process re-emits into a SharedBroadcastRing.

use std::fs::{File, OpenOptions};
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const BROADCAST_MAGIC: u32 = 0x4150_4243;
pub const BROADCAST_PAYLOAD_BYTES: usize = 52;
pub const MAX_CONSUMERS: usize = 16;

/// Header is two cache lines so the producer_seq and the consumer
/// seq array can sit on separate lines, reducing false-sharing
/// between producer-side and consumer-side hot atomics.
#[repr(C, align(64))]
pub struct BroadcastHeader {
    pub magic: u32,
    pub capacity: u32,
    pub producer_seq: AtomicU64,
    _pad1: [u8; 48],

    pub consumer_seqs: [AtomicU64; MAX_CONSUMERS],
    pub consumer_active: [AtomicU32; MAX_CONSUMERS],
}

#[repr(C, align(64))]
pub struct BroadcastSlot {
    pub version: AtomicU32,
    _pad: [u8; 4],
    pub payload: [u8; BROADCAST_PAYLOAD_BYTES],
}

const _: () = {
    assert!(size_of::<BroadcastSlot>() == 64);
};

pub const fn broadcast_file_size(capacity: usize) -> usize {
    size_of::<BroadcastHeader>() + capacity * size_of::<BroadcastSlot>()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BroadcastError {
    Full,
    Empty,
    NoConsumerSlot,
    InvalidConsumer,
    PayloadTooLarge,
    LayoutMismatch,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for BroadcastError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

pub struct SharedBroadcastRing {
    _backing: BroadcastBacking,
    raw_ptr: *mut u8,
    capacity: usize,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl Send for SharedBroadcastRing {}
unsafe impl Sync for SharedBroadcastRing {}

/// Storage backing for a `SharedBroadcastRing`. The hot-path
/// pointer (`raw_ptr` on `SharedBroadcastRing`) is cached at
/// construction; this enum holds the owning resource alive for
/// the lifetime of the ring. The held values are intentionally
/// only kept for ownership / flush-target purposes; the
/// `File` and `ShmFile` payloads are not read at runtime.
#[allow(dead_code)]
enum BroadcastBacking {
    /// In-process anonymous mmap. No file, no shm-name.
    Anon(MmapMut),
    /// File-backed mmap. Cross-process via the OS page cache.
    File(File, MmapMut),
    /// Named shared-memory mmap (ShmFs locale). Cross-process,
    /// RAM-resident, never touches the page cache.
    Shm(crate::shm_file::ShmFile),
}

/// Backing-agnostic layout init. Writes the header magic + capacity
/// and zeroes the slot array at the given raw pointer. Caller
/// guarantees that `ptr` points to at least
/// `broadcast_file_size(capacity)` bytes of mutable, suitably-
/// aligned memory.
unsafe fn init_broadcast_layout_raw(ptr: *mut u8, capacity: usize) {
    let hdr_ptr = ptr as *mut BroadcastHeader;
    unsafe {
        std::ptr::write_bytes(hdr_ptr as *mut u8, 0, size_of::<BroadcastHeader>());
        (*hdr_ptr).magic = BROADCAST_MAGIC;
        (*hdr_ptr).capacity = capacity as u32;
    }
    for i in 0..capacity {
        let slot_ptr = unsafe {
            ptr.add(size_of::<BroadcastHeader>())
                .add(i * size_of::<BroadcastSlot>())
        } as *mut BroadcastSlot;
        unsafe {
            std::ptr::write(slot_ptr, BroadcastSlot {
                version: AtomicU32::new(0),
                _pad: [0; 4],
                payload: [0u8; BROADCAST_PAYLOAD_BYTES],
            });
        }
    }
}

impl subetha_sidecar::AdaptiveInstance for SharedBroadcastRing {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl SharedBroadcastRing {
    /// File-backed broadcast ring; cross-process visibility via
    /// the OS page cache.
    pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, BroadcastError> {
        assert!(capacity >= 2);
        assert!(capacity <= u32::MAX as usize);
        let total = broadcast_file_size(capacity);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let raw_ptr = mmap.as_mut_ptr();
        unsafe { init_broadcast_layout_raw(raw_ptr, capacity); }
        Ok(Self {
            _backing: BroadcastBacking::File(file, mmap),
            raw_ptr, capacity,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Anonymous in-process broadcast ring. Fastest construction;
    /// skips file create + ftruncate + first-page-fault. In-process
    /// only - subscribers in other processes cannot connect.
    pub fn create_anon(capacity: usize) -> Result<Self, BroadcastError> {
        assert!(capacity >= 2);
        assert!(capacity <= u32::MAX as usize);
        let total = broadcast_file_size(capacity);
        let mut mmap = MmapOptions::new().len(total).map_anon()?;
        let raw_ptr = mmap.as_mut_ptr();
        unsafe { init_broadcast_layout_raw(raw_ptr, capacity); }
        Ok(Self {
            _backing: BroadcastBacking::Anon(mmap),
            raw_ptr, capacity,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Open an existing file-backed broadcast ring. Validates
    /// magic + capacity.
    pub fn open(path: impl AsRef<Path>, expected_capacity: usize) -> Result<Self, BroadcastError> {
        let total = broadcast_file_size(expected_capacity);
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if file.metadata()?.len() < total as u64 {
            return Err(BroadcastError::LayoutMismatch);
        }
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let raw_ptr = mmap.as_mut_ptr();
        let hdr = unsafe { &*(raw_ptr as *const BroadcastHeader) };
        if hdr.magic != BROADCAST_MAGIC || hdr.capacity != expected_capacity as u32 {
            return Err(BroadcastError::LayoutMismatch);
        }
        Ok(Self {
            _backing: BroadcastBacking::File(file, mmap),
            raw_ptr, capacity: expected_capacity,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Build a fresh broadcast ring on top of a named RAM-resident
    /// shared-memory backing. Cross-process visible via the
    /// `logical_name` of the underlying
    /// [`ShmFile`](crate::shm_file::ShmFile); never touches
    /// the page cache. The `ShmFile` must be sized to at least
    /// `broadcast_file_size(capacity)` bytes.
    pub fn create_from_shm(
        mut shm: crate::shm_file::ShmFile,
        capacity: usize,
    ) -> Result<Self, BroadcastError> {
        assert!(capacity >= 2);
        assert!(capacity <= u32::MAX as usize);
        let total = broadcast_file_size(capacity);
        if shm.len() < total {
            return Err(BroadcastError::LayoutMismatch);
        }
        let raw_ptr = shm.as_mut_slice().as_mut_ptr();
        unsafe { init_broadcast_layout_raw(raw_ptr, capacity); }
        Ok(Self {
            _backing: BroadcastBacking::Shm(shm),
            raw_ptr, capacity,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Open an existing named ShmFs-backed broadcast ring.
    /// Validates magic + capacity. Does NOT re-initialise the
    /// layout - the layout must already be present from a prior
    /// `create_from_shm` on the same logical name.
    pub fn open_from_shm(
        mut shm: crate::shm_file::ShmFile,
        expected_capacity: usize,
    ) -> Result<Self, BroadcastError> {
        let total = broadcast_file_size(expected_capacity);
        if shm.len() < total {
            return Err(BroadcastError::LayoutMismatch);
        }
        let raw_ptr = shm.as_mut_slice().as_mut_ptr();
        let hdr = unsafe { &*(raw_ptr as *const BroadcastHeader) };
        if hdr.magic != BROADCAST_MAGIC || hdr.capacity != expected_capacity as u32 {
            return Err(BroadcastError::LayoutMismatch);
        }
        Ok(Self {
            _backing: BroadcastBacking::Shm(shm),
            raw_ptr, capacity: expected_capacity,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }


    #[inline]
    pub fn capacity(&self) -> usize { self.capacity }

    fn header(&self) -> &BroadcastHeader {
        unsafe { &*(self.raw_ptr as *const BroadcastHeader) }
    }

    /// Reduce a logical sequence number to a physical slot index. Uses a
    /// bit-mask when capacity is a power of two (the common ring sizing) -
    /// removing the `% capacity` hardware DIV - and the modulo otherwise.
    /// `capacity` is loop-invariant, so the pow2 test folds away in hot
    /// loops; no cached field needed.
    #[inline]
    fn wrap(&self, i: usize) -> usize {
        if self.capacity.is_power_of_two() {
            i & (self.capacity - 1)
        } else {
            i % self.capacity
        }
    }

    fn slot(&self, idx: usize) -> &BroadcastSlot {
        let physical = self.wrap(idx);
        let base = unsafe { self.raw_ptr.add(size_of::<BroadcastHeader>()) };
        unsafe { &*(base.add(physical * size_of::<BroadcastSlot>()) as *const BroadcastSlot) }
    }

    /// Register as a consumer. Returns a consumer index in
    /// `0..MAX_CONSUMERS`; that index is used for all subsequent
    /// recv calls. Initialises the consumer's cursor to the current
    /// producer_seq (consumer starts reading from "now," not history).
    pub fn register_consumer(&self) -> Result<usize, BroadcastError> {
        let hdr = self.header();
        for i in 0..MAX_CONSUMERS {
            if hdr.consumer_active[i].compare_exchange(
                0, 1, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                let cur_producer = hdr.producer_seq.load(Ordering::Acquire);
                hdr.consumer_seqs[i].store(cur_producer, Ordering::Release);
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::broadcast_ring::OP_REGISTER, 0);
                return Ok(i);
            }
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::broadcast_ring::OP_REGISTER, 1); // no consumer slot available
        Err(BroadcastError::NoConsumerSlot)
    }

    /// Unregister a consumer. After this, the producer no longer
    /// waits for this cursor when reclaiming slots.
    pub fn unregister_consumer(&self, consumer_idx: usize) {
        if consumer_idx >= MAX_CONSUMERS { return; }
        let hdr = self.header();
        hdr.consumer_active[consumer_idx].store(0, Ordering::Release);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::broadcast_ring::OP_UNREGISTER, 0);
    }

    /// Compute the slot-reclaim safety margin: the producer can push
    /// when `producer_seq - min_active_consumer_seq < capacity`.
    /// Returns the smallest cursor among active consumers (or
    /// producer_seq when there are no active consumers; broadcasts
    /// to nobody can always push).
    fn min_consumer_seq(&self) -> u64 {
        let hdr = self.header();
        let producer = hdr.producer_seq.load(Ordering::Acquire);
        let mut min = u64::MAX;
        let mut any = false;
        for i in 0..MAX_CONSUMERS {
            if hdr.consumer_active[i].load(Ordering::Acquire) != 0 {
                let s = hdr.consumer_seqs[i].load(Ordering::Acquire);
                if s < min { min = s; }
                any = true;
            }
        }
        if !any { producer } else { min }
    }

    /// Push a message. Returns `Err(Full)` when at least one active
    /// consumer hasn't yet read a previous slot we'd overwrite.
    pub fn try_push(&self, payload: &[u8]) -> Result<(), BroadcastError> {
        if payload.len() > BROADCAST_PAYLOAD_BYTES {
            return Err(BroadcastError::PayloadTooLarge);
        }
        let hdr = self.header();
        let producer = hdr.producer_seq.load(Ordering::Acquire);
        let min_consumer = self.min_consumer_seq();
        if producer.saturating_sub(min_consumer) >= self.capacity as u64 {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::broadcast_ring::OP_PUSH, 1); // full
            return Err(BroadcastError::Full);
        }
        // SeqLock write the slot at producer_seq % capacity.
        let slot = self.slot(producer as usize);
        slot.version.fetch_add(1, Ordering::AcqRel); // odd
        let dst = unsafe {
            self.raw_ptr
                .add(size_of::<BroadcastHeader>())
                .add(self.wrap(producer as usize) * size_of::<BroadcastSlot>())
                .add(std::mem::offset_of!(BroadcastSlot, payload))
        };
        unsafe {
            std::ptr::write_bytes(dst, 0, BROADCAST_PAYLOAD_BYTES);
            std::ptr::copy_nonoverlapping(payload.as_ptr(), dst, payload.len());
        }
        slot.version.fetch_add(1, Ordering::AcqRel); // even
        hdr.producer_seq.fetch_add(1, Ordering::Release);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::broadcast_ring::OP_PUSH, 0);
        Ok(())
    }

    /// Receive the next unread message for `consumer_idx`. Returns
    /// the number of bytes filled (always BROADCAST_PAYLOAD_BYTES;
    /// caller knows the inner-event size from its T contract).
    pub fn try_recv(&self, consumer_idx: usize, out: &mut [u8]) -> Result<usize, BroadcastError> {
        if consumer_idx >= MAX_CONSUMERS { return Err(BroadcastError::InvalidConsumer); }
        let hdr = self.header();
        if hdr.consumer_active[consumer_idx].load(Ordering::Acquire) == 0 {
            return Err(BroadcastError::InvalidConsumer);
        }
        let my_seq = hdr.consumer_seqs[consumer_idx].load(Ordering::Acquire);
        let producer = hdr.producer_seq.load(Ordering::Acquire);
        if my_seq >= producer {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::broadcast_ring::OP_RECV, 2); // empty
            return Err(BroadcastError::Empty);
        }
        // SeqLock read the slot at my_seq % capacity.
        let slot = self.slot(my_seq as usize);
        loop {
            let v1 = slot.version.load(Ordering::Acquire);
            if v1 & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            let src = unsafe {
                self.raw_ptr
                    .add(size_of::<BroadcastHeader>())
                    .add(self.wrap(my_seq as usize) * size_of::<BroadcastSlot>())
                    .add(std::mem::offset_of!(BroadcastSlot, payload))
            };
            let n = out.len().min(BROADCAST_PAYLOAD_BYTES);
            unsafe { std::ptr::copy_nonoverlapping(src, out.as_mut_ptr(), n); }
            let v2 = slot.version.load(Ordering::Acquire);
            if v1 == v2 {
                hdr.consumer_seqs[consumer_idx].fetch_add(1, Ordering::Release);
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::broadcast_ring::OP_RECV, 0);
                return Ok(n);
            }
        }
    }

    /// Number of messages this consumer has not yet read.
    pub fn lag(&self, consumer_idx: usize) -> u64 {
        let hdr = self.header();
        if consumer_idx >= MAX_CONSUMERS { return 0; }
        let prod = hdr.producer_seq.load(Ordering::Acquire);
        let my = hdr.consumer_seqs[consumer_idx].load(Ordering::Acquire);
        prod.saturating_sub(my)
    }

    /// Current producer cursor (total messages pushed since creation).
    pub fn producer_position(&self) -> u64 {
        self.header().producer_seq.load(Ordering::Acquire)
    }

    /// Number of currently active consumers.
    pub fn active_consumer_count(&self) -> usize {
        let hdr = self.header();
        (0..MAX_CONSUMERS)
            .filter(|&i| hdr.consumer_active[i].load(Ordering::Acquire) != 0)
            .count()
    }

    pub fn flush(&self) -> Result<(), BroadcastError> {
        match &self._backing {
            BroadcastBacking::File(_, mmap) => mmap.flush()?,
            BroadcastBacking::Anon(mmap) => mmap.flush()?,
            BroadcastBacking::Shm(_) => {} // RAM-resident; nothing to sync
        }
        Ok(())
    }

    /// Non-blocking flush: schedules a writeback via the OS.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), BroadcastError> {
        match &self._backing {
            BroadcastBacking::File(_, mmap) => mmap.flush_async()?,
            BroadcastBacking::Anon(mmap) => mmap.flush_async()?,
            BroadcastBacking::Shm(_) => {} // RAM-resident; nothing to sync
        }
        Ok(())
    }

    /// Whether every currently-active consumer has read every
    /// item the producer has published. Used by the capacity-morph
    /// wrapper to decide whether a stale broadcast backing can be
    /// dropped (all subscribers have caught up to the frozen
    /// producer position).
    pub fn is_fully_drained(&self) -> bool {
        let hdr = self.header();
        let prod = hdr.producer_seq.load(Ordering::Acquire);
        for i in 0..MAX_CONSUMERS {
            if hdr.consumer_active[i].load(Ordering::Acquire) != 0
                && hdr.consumer_seqs[i].load(Ordering::Acquire) < prod
            {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-broadcast-{name}-{pid}.bin"));
        p
    }

    fn payload_of(v: u32) -> [u8; BROADCAST_PAYLOAD_BYTES] {
        let mut b = [0u8; BROADCAST_PAYLOAD_BYTES];
        b[0..4].copy_from_slice(&v.to_le_bytes());
        b
    }
    fn unpack(b: &[u8]) -> u32 {
        u32::from_le_bytes(b[0..4].try_into().unwrap())
    }

    #[test]
    fn create_initial_state() {
        let p = tmp("init");
        let r = SharedBroadcastRing::create(&p, 8).unwrap();
        assert_eq!(r.capacity(), 8);
        assert_eq!(r.producer_position(), 0);
        assert_eq!(r.active_consumer_count(), 0);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn one_producer_one_consumer_round_trip() {
        let p = tmp("1p1c");
        let r = SharedBroadcastRing::create(&p, 8).unwrap();
        let c = r.register_consumer().unwrap();
        r.try_push(&payload_of(42)).unwrap();
        let mut buf = [0u8; BROADCAST_PAYLOAD_BYTES];
        let n = r.try_recv(c, &mut buf).unwrap();
        assert_eq!(n, BROADCAST_PAYLOAD_BYTES);
        assert_eq!(unpack(&buf), 42);
        assert_eq!(r.try_recv(c, &mut buf).err(), Some(BroadcastError::Empty));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn three_consumers_each_see_all_messages() {
        let p = tmp("1p3c");
        let r = SharedBroadcastRing::create(&p, 16).unwrap();
        let c0 = r.register_consumer().unwrap();
        let c1 = r.register_consumer().unwrap();
        let c2 = r.register_consumer().unwrap();
        for i in 0..5u32 { r.try_push(&payload_of(i * 10)).unwrap(); }
        let mut buf = [0u8; BROADCAST_PAYLOAD_BYTES];
        for c in [c0, c1, c2] {
            for i in 0..5u32 {
                r.try_recv(c, &mut buf).unwrap();
                assert_eq!(unpack(&buf), i * 10,
                    "consumer {c} should see message {i}");
            }
            assert_eq!(r.try_recv(c, &mut buf).err(), Some(BroadcastError::Empty));
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn lagging_consumer_blocks_producer() {
        let p = tmp("lag-blocks");
        let r = SharedBroadcastRing::create(&p, 4).unwrap();
        let c = r.register_consumer().unwrap();
        let _c = c;
        // Fill the ring.
        for i in 0..4u32 { r.try_push(&payload_of(i)).unwrap(); }
        // Consumer hasn't read anything; next push should fail Full.
        assert_eq!(r.try_push(&payload_of(99)).err(), Some(BroadcastError::Full));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn unregistering_lagging_consumer_unblocks_producer() {
        let p = tmp("unreg-unblocks");
        let r = SharedBroadcastRing::create(&p, 4).unwrap();
        let c = r.register_consumer().unwrap();
        for i in 0..4u32 { r.try_push(&payload_of(i)).unwrap(); }
        assert_eq!(r.try_push(&payload_of(99)).err(), Some(BroadcastError::Full));
        r.unregister_consumer(c);
        // With no active consumers, producer can push freely.
        r.try_push(&payload_of(99)).unwrap();
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn consumer_registered_late_starts_at_current_producer() {
        let p = tmp("late-consumer");
        let r = SharedBroadcastRing::create(&p, 8).unwrap();
        // Push 3 messages BEFORE registering any consumer.
        for i in 0..3u32 { r.try_push(&payload_of(i)).unwrap(); }
        // Now register; this consumer should see only NEW messages.
        let c = r.register_consumer().unwrap();
        let mut buf = [0u8; BROADCAST_PAYLOAD_BYTES];
        assert_eq!(r.try_recv(c, &mut buf).err(), Some(BroadcastError::Empty));
        r.try_push(&payload_of(100)).unwrap();
        r.try_recv(c, &mut buf).unwrap();
        assert_eq!(unpack(&buf), 100);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn lag_returns_pending_count() {
        let p = tmp("lag");
        let r = SharedBroadcastRing::create(&p, 8).unwrap();
        let c = r.register_consumer().unwrap();
        for i in 0..3u32 { r.try_push(&payload_of(i)).unwrap(); }
        assert_eq!(r.lag(c), 3);
        let mut buf = [0u8; BROADCAST_PAYLOAD_BYTES];
        r.try_recv(c, &mut buf).unwrap();
        assert_eq!(r.lag(c), 2);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn max_consumers_returns_no_slot_when_full() {
        let p = tmp("max-cons");
        let r = SharedBroadcastRing::create(&p, 4).unwrap();
        for _ in 0..MAX_CONSUMERS {
            r.register_consumer().unwrap();
        }
        assert_eq!(r.register_consumer().err(), Some(BroadcastError::NoConsumerSlot));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_pub_sub() {
        let p = tmp("cross-handle");
        let pub_handle = SharedBroadcastRing::create(&p, 8).unwrap();
        let sub_handle = SharedBroadcastRing::open(&p, 8).unwrap();
        let c = sub_handle.register_consumer().unwrap();
        pub_handle.try_push(&payload_of(7777)).unwrap();
        let mut buf = [0u8; BROADCAST_PAYLOAD_BYTES];
        sub_handle.try_recv(c, &mut buf).unwrap();
        assert_eq!(unpack(&buf), 7777);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn payload_too_large_rejected_at_push() {
        let p = tmp("oversized");
        let r = SharedBroadcastRing::create(&p, 4).unwrap();
        let _c = r.register_consumer().unwrap();
        let big = vec![0u8; BROADCAST_PAYLOAD_BYTES + 1];
        assert_eq!(r.try_push(&big).err(), Some(BroadcastError::PayloadTooLarge));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_consumers_all_drain_correctly() {
        let p = tmp("concurrent");
        let r = Arc::new(SharedBroadcastRing::create(&p, 256).unwrap());
        let n_consumers = 4;
        let n_msgs = 100u32;
        let consumer_ids: Vec<usize> = (0..n_consumers).map(|_| r.register_consumer().unwrap()).collect();

        let r_p = r.clone();
        let producer = thread::spawn(move || {
            for i in 0..n_msgs {
                while r_p.try_push(&payload_of(i)).is_err() {
                    thread::yield_now();
                }
            }
        });

        let mut handles = vec![];
        for &c in &consumer_ids {
            let r = r.clone();
            handles.push(thread::spawn(move || {
                let mut received = vec![];
                let mut buf = [0u8; BROADCAST_PAYLOAD_BYTES];
                while received.len() < n_msgs as usize {
                    match r.try_recv(c, &mut buf) {
                        Ok(_) => received.push(unpack(&buf)),
                        Err(BroadcastError::Empty) => thread::yield_now(),
                        Err(e) => panic!("unexpected error: {e:?}"),
                    }
                }
                received
            }));
        }
        producer.join().unwrap();
        for h in handles {
            let got = h.join().unwrap();
            assert_eq!(got.len(), n_msgs as usize);
            for (i, v) in got.iter().enumerate() {
                assert_eq!(*v, i as u32, "message order must be preserved");
            }
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn slow_consumer_doesnt_break_fast_consumer() {
        let p = tmp("slow-fast");
        let r = Arc::new(SharedBroadcastRing::create(&p, 8).unwrap());
        let _slow = r.register_consumer().unwrap();
        let fast = r.register_consumer().unwrap();

        for i in 0..4u32 { r.try_push(&payload_of(i)).unwrap(); }
        // Fast consumer drains; slow consumer hasn't moved.
        let mut buf = [0u8; BROADCAST_PAYLOAD_BYTES];
        for i in 0..4u32 {
            r.try_recv(fast, &mut buf).unwrap();
            assert_eq!(unpack(&buf), i);
        }
        // Producer is still bounded by slow consumer's cursor though;
        // try to push 5 more - should hit Full at some point.
        let mut pushed = 0;
        for i in 100..200u32 {
            if r.try_push(&payload_of(i)).is_err() { break; }
            pushed += 1;
        }
        assert!(pushed <= 4, "should be bounded by slow consumer; pushed {pushed}");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn observer_can_wait_for_publisher() {
        let p = tmp("wait");
        let r = Arc::new(SharedBroadcastRing::create(&p, 8).unwrap());
        let c = r.register_consumer().unwrap();

        let r_p = r.clone();
        let pusher = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            r_p.try_push(&payload_of(555)).unwrap();
        });
        let mut buf = [0u8; BROADCAST_PAYLOAD_BYTES];
        let r_c = r.clone();
        let consumer = thread::spawn(move || {
            loop {
                match r_c.try_recv(c, &mut buf) {
                    Ok(_) => break unpack(&buf),
                    Err(BroadcastError::Empty) => thread::yield_now(),
                    Err(e) => panic!("{e:?}"),
                }
            }
        });
        pusher.join().unwrap();
        assert_eq!(consumer.join().unwrap(), 555);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk");
        {
            let r = SharedBroadcastRing::create(&p, 8).unwrap();
            let _c = r.register_consumer().unwrap();
            r.try_push(&payload_of(1234)).unwrap();
            r.try_push(&payload_of(5678)).unwrap();
            r.flush().unwrap();
        }
        let r2 = SharedBroadcastRing::open(&p, 8).unwrap();
        // Producer seq should be 2; consumer 0 should still be at 0.
        assert_eq!(r2.producer_position(), 2);
        assert_eq!(r2.lag(0), 2);
        let mut buf = [0u8; BROADCAST_PAYLOAD_BYTES];
        r2.try_recv(0, &mut buf).unwrap();
        assert_eq!(unpack(&buf), 1234);
        r2.try_recv(0, &mut buf).unwrap();
        assert_eq!(unpack(&buf), 5678);
        std::fs::remove_file(&p).ok();
    }
}
