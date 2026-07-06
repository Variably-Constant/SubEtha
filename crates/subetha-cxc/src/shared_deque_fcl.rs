//! `SharedDequeFcl` - Fat Chase-Lev: counter-only Chase-Lev with
//! `K_inner = 3` items per slot.
//!
//! This primitive answers the design-cube question "is there middle
//! ground between Chase-Lev (`K_inner=1`, counter-only) and KHL
//! (`K_inner=3`, per-slot atomic)?". The answer is **yes**: Chase-
//! Lev's safety proof never required `K_inner = 1`; it only required
//! that the producer's bottom store be Release-fenced after the slot
//! bytes are written. With `K_inner = 3` the protocol becomes:
//!
//! 1. Producer loads `bottom` (Relaxed) + `top` (Acquire).
//! 2. Capacity check: `(bottom - top) + n_slots <= capacity`.
//! 3. For each of the `n_slots` slots: write 64 bytes (sequence-
//!    number-free) carrying 3 [`LineItem`] payloads + a count.
//! 4. ONE Release fence orders all slot writes.
//! 5. ONE Relaxed store on owner-private `bottom` advances bottom by
//!    `n_slots`, atomically publishing all slots from the thieves'
//!    perspective.
//!
//! No per-slot atomic. No per-slot Acquire-Release pair. Per K=64
//! items the producer pays one `top` load + 22 cache-line writes +
//! one Release fence + one `bottom` store - 24 atomic ops total, of
//! which 22 are just memory writes.
//!
//! ## Cost-model comparison (K=64 producer-fast)
//!
//! | Primitive | Producer atomics |
//! |---|---:|
//! | `SharedDeque<u64>` (Chase-Lev `K_inner=1`) | 64 Release fences + 64 Relaxed bottom stores + 64 top loads |
//! | `SharedDequeKhpd::publish_batch` | 22 slot Release-stores on state + 1 `fetch_add` (LOCK XADD) |
//! | `SharedDequeLoh::publish_batch` | 64 slot Release-stores on sequence + 1 LOCK XADD |
//! | `SharedDequeKhl::publish_batch` | 22 slot Acquire-loads on sequence + 22 slot Release-stores on sequence + 1 Release-store on owner-private tail |
//! | **`SharedDequeFcl::publish_batch`** | **1 top Acquire-load + 22 cache-line writes + 1 Release fence + 1 Relaxed bottom store** |
//!
//! Fcl's producer side has the **fewest atomic operations** of any
//! batched deque-family primitive on this substrate. The trade-off
//! is on the thief side: Chase-Lev's steal protocol does a
//! speculative slot read BEFORE the head CAS, so a thief that loses
//! the CAS has read a 64-byte slot for nothing. Under heavy
//! contention this wastes cache bandwidth; under producer-fast
//! single-thief (the workload-shape Fcl targets) the speculative
//! reads never get wasted because the CAS never loses.
//!
//! ## Why this is novel
//!
//! The Chase-Lev literature treats `K_inner = 1` as a fixed feature
//! of the protocol, but inspecting the safety proof shows it never
//! depended on the slot size. SubEtha's byte-oriented [`LineItem`]
//! decoupling makes the natural fat-slot extension trivial: three
//! [`LineItem`] payloads (16 B each = 48 B) plus an 8 B count word
//! plus 8 B of tail padding fit exactly in 64 B. The slot becomes
//! cache-line aligned by construction; sequential slot writes are
//! sequential cache-line writes. This is the counter-only end's
//! analogue of the `K_inner = 3` lever that KHPD pulled on the
//! per-slot end.
//!
//! ## When to use this
//!
//! - **Producer-fast single-thief batched workloads**: this is the
//!   win zone. Fcl's per-batch cost is dominated by 22 cache-line
//!   writes; everything else is essentially free.
//! - **NOT for multi-thief contention**: the speculative slot read
//!   before head CAS wastes cache when the CAS races. Use
//!   [`SharedDequeUrd`](crate::SharedDequeUrd) instead.
//! - **NOT for per-item dispatch with K = 1**: just use plain
//!   [`SharedDeque`]; Fcl's K_inner = 3 wastes slot bytes if the
//!   caller has nothing to fill them with.

#![allow(clippy::missing_errors_doc)]

use std::io;
use std::path::Path;

use crate::shared_deque::{DequeError, SharedDeque};
use crate::shared_deque_khpd::{FatLineItem, LineItem, PushError, LINE_ITEMS};

/// MMF-backed Fat Chase-Lev deque. Counter-only Chase-Lev protocol
/// with `K_inner = 3` items per slot, single owner, N thieves.
///
/// Wraps [`SharedDeque<FatLineItem>`](crate::SharedDeque) with a
/// caller-facing [`publish_batch`](Self::publish_batch) API that
/// packs [`LineItem`] payloads into 64-byte fat slots.
pub struct SharedDequeFcl {
    inner: SharedDeque<FatLineItem>,
}

impl SharedDequeFcl {
    /// Create a fresh Fcl file. `capacity_slots` rounds up to the
    /// next power of two. Total item capacity is
    /// `capacity_slots * LINE_ITEMS`.
    pub fn create<P: AsRef<Path>>(path: P, capacity_slots: usize) -> io::Result<Self> {
        let inner = SharedDeque::<FatLineItem>::create(path, capacity_slots)
            .map_err(|e| io::Error::other(format!("Fcl create: {e:?}")))?;
        Ok(Self { inner })
    }

    /// Open an existing Fcl file as a thief (read-side).
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let inner = SharedDeque::<FatLineItem>::open_as_thief(path)
            .map_err(|e| io::Error::other(format!("Fcl open: {e:?}")))?;
        Ok(Self { inner })
    }

    /// Capacity in slots (power of two). Total item capacity is
    /// `capacity_slots() * LINE_ITEMS`.
    pub fn capacity_slots(&self) -> usize {
        self.inner.capacity()
    }

    /// Snapshot the current ring fill in slots.
    pub fn approx_len_slots(&self) -> usize {
        self.inner.approx_len()
    }

    /// Owner-side batched publish. Packs `items` into
    /// `ceil(items.len() / LINE_ITEMS)` fat slots, then publishes
    /// them with ONE top load + ONE Release fence + ONE Relaxed
    /// bottom store via [`SharedDeque::push_batch`].
    ///
    /// Cost: 1 top load + `ceil(K/3)` cache-line writes + 1 Release
    /// fence + 1 bottom store. No per-slot atomic.
    ///
    /// Returns the number of items published. Returns
    /// `Err(DequeError::Full)` if the batch would overflow the ring.
    pub fn publish_batch(&self, items: &[LineItem]) -> Result<usize, DequeError> {
        if items.is_empty() {
            return Ok(0);
        }
        let n_slots = items.len().div_ceil(LINE_ITEMS);
        // SubEtha-style raw-pointer hot path: cast the slot's mapped
        // bytes to `*mut FatLineItem` and write each field directly
        // through the pointer. No intermediate `T` buffer, no
        // `Marshal::marshal` byte copy, no slice bounds checks on
        // the hot path. The slot is already 64-byte aligned (the
        // header is `repr(align(64))` and `slot_bytes` = 64 for
        // `FatLineItem`), so the cast is sound.
        self.inner.push_batch_with(n_slots, |slot_i, slot_bytes| {
            let start = slot_i * LINE_ITEMS;
            let end = (start + LINE_ITEMS).min(items.len());
            let chunk = &items[start..end];
            let n = chunk.len();
            // SAFETY: `slot_bytes` is a `slot_bytes_for::<FatLineItem>()`
            // = 64-byte mapped region aligned to 64; cast to
            // `*mut FatLineItem` is sound. Producer holds the
            // reservation for this slot via the outer push_batch_with
            // capacity check; no concurrent access until the Release
            // fence + bottom store.
            unsafe {
                let dst = slot_bytes.as_mut_ptr() as *mut FatLineItem;
                std::ptr::addr_of_mut!((*dst).n_items).write(n as u32);
                std::ptr::addr_of_mut!((*dst).reserved).write(0);
                // Each `(*dst).items[i] = *item` lowers to a single
                // 16-byte SIMD store on x86_64.
                let items_ptr = std::ptr::addr_of_mut!((*dst).items) as *mut LineItem;
                for i in 0..n {
                    items_ptr.add(i).write(*chunk.get_unchecked(i));
                }
                // Zero the unused tail of the items array so the
                // consumer's `live_items()` decode does not return
                // stale bytes from a prior round.
                for i in n..LINE_ITEMS {
                    items_ptr.add(i).write(LineItem::default());
                }
                std::ptr::addr_of_mut!((*dst)._pad).write([0u8; 8]);
            }
        })?;
        Ok(items.len())
    }

    /// Thief-side steal. Returns one fat slot (1..=LINE_ITEMS items)
    /// or `None` if the ring is empty / CAS lost.
    pub fn steal_slot(&self) -> Option<FatLineItem> {
        self.inner.steal()
    }
}

impl From<PushError> for DequeError {
    fn from(e: PushError) -> Self {
        DequeError::Io(format!("Fcl pack: {e:?}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as O};
    use std::thread;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        p.push(format!("subetha_fcl_{pid}_{nonce}_{name}.bin"));
        p
    }

    fn u32_item(id: u32) -> LineItem {
        LineItem::new(&id.to_le_bytes()).expect("item")
    }

    fn item_id(item: &LineItem) -> u32 {
        u32::from_le_bytes(item.payload[..4].try_into().unwrap())
    }

    #[test]
    fn publish_batch_packs_three_items_per_slot() {
        let path = tmp("packs");
        let d = SharedDequeFcl::create(&path, 64).expect("create");
        let items: Vec<LineItem> = (1..=7u32).map(u32_item).collect();
        let n = d.publish_batch(&items).expect("publish");
        assert_eq!(n, 7);
        // 7 items = ceil(7/3) = 3 slots.
        assert_eq!(d.approx_len_slots(), 3);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn publish_batch_empty_is_noop() {
        let path = tmp("empty");
        let d = SharedDequeFcl::create(&path, 4).expect("create");
        assert_eq!(d.publish_batch(&[]).expect("noop"), 0);
        assert_eq!(d.approx_len_slots(), 0);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn publish_batch_full_returns_full() {
        let path = tmp("full");
        let d = SharedDequeFcl::create(&path, 2).expect("create");
        // Capacity is 2 slots = 6 items.
        let first: Vec<LineItem> = (1..=6u32).map(u32_item).collect();
        d.publish_batch(&first).expect("first batch");
        let err = d
            .publish_batch(&[u32_item(99)])
            .expect_err("publish past capacity");
        assert_eq!(err, DequeError::Full);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn steal_drains_in_publication_order() {
        let path = tmp("order");
        let d = SharedDequeFcl::create(&path, 8).expect("create");
        let items: Vec<LineItem> = (1..=7u32).map(u32_item).collect();
        d.publish_batch(&items).expect("publish");
        let mut drained = Vec::new();
        while let Some(fat) = d.steal_slot() {
            for item in fat.live_items() {
                drained.push(item_id(item));
            }
        }
        assert_eq!(drained, vec![1, 2, 3, 4, 5, 6, 7]);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn concurrent_thieves_no_double_take() {
        let path = tmp("stress");
        let d = Arc::new(SharedDequeFcl::create(&path, 256).expect("create"));
        let n: usize = 5_000;
        let consumed = Arc::new(AtomicUsize::new(0));
        let sum = Arc::new(AtomicUsize::new(0));

        let mut thieves = Vec::new();
        for _ in 0..2 {
            let d = Arc::clone(&d);
            let consumed = Arc::clone(&consumed);
            let sum = Arc::clone(&sum);
            thieves.push(thread::spawn(move || {
                while consumed.load(O::Relaxed) < n {
                    match d.steal_slot() {
                        Some(fat) => {
                            for item in fat.live_items() {
                                consumed.fetch_add(1, O::Relaxed);
                                sum.fetch_add(item_id(item) as usize, O::Relaxed);
                            }
                        }
                        None => std::thread::yield_now(),
                    }
                }
            }));
        }

        let burst = 64usize;
        let mut pushed = 0usize;
        while pushed < n {
            let want = burst.min(n - pushed);
            let batch: Vec<LineItem> =
                (0..want).map(|j| u32_item((pushed + j) as u32)).collect();
            loop {
                match d.publish_batch(&batch) {
                    Ok(_) => break,
                    Err(DequeError::Full) => std::thread::yield_now(),
                    Err(other) => panic!("publish_batch: {other:?}"),
                }
            }
            pushed += want;
        }

        for t in thieves {
            t.join().expect("thief");
        }
        let expected: usize = (0..n).sum();
        assert_eq!(
            sum.load(O::Relaxed),
            expected,
            "every item consumed exactly once"
        );
        std::fs::remove_file(&path).ok();
    }
}
