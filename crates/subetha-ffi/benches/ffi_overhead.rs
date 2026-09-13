//! What the C ABI costs over the Rust API it wraps: each family driven
//! directly and through its C entry points on the same anonymous object,
//! which add the handle table lookup, the panic guard, the argument checks
//! and the waker wake. The baseline is the direct call, so the difference
//! is the ABI's whole overhead per operation.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use subetha_cxc::adaptive_ring::AdaptiveRing;
use subetha_cxc::blocking_mpmc_ring::BlockingMpmcRing;
use subetha_cxc::blocking_mpsc_ring::BlockingMpscRing;
use subetha_cxc::blocking_spsc_ring::BlockingSpscRing;
use subetha_cxc::capacity_adaptive_ring::CapacityAdaptiveRing;
use subetha_cxc::ordering::StampKind;
use subetha_cxc::protocol_pubsub::PubSubRing;
use std::sync::atomic::Ordering;

use subetha_cxc::frame_region::FrameRegion;
use subetha_cxc::raw_btree_map::RawBTreeMap;
use subetha_cxc::raw_cell::RawCell;
use subetha_cxc::raw_deque::RawDeque;
use subetha_cxc::shared_epochs::SharedEpochs;
use subetha_cxc::cross_process_waker::CrossProcessWaker;
use subetha_cxc::shared_fence_clock::SharedFenceClock;
use subetha_cxc::shared_once_cell::SharedOnceCellDyn;
use subetha_cxc::shared_rw_lock::SharedRWLock;
use subetha_cxc::shared_semaphore::SharedSemaphore;
use subetha_cxc::raw_hash_map::RawHashMap;
use subetha_cxc::raw_linked_list::RawLinkedList;
use subetha_cxc::raw_region::RawRegion;
use subetha_cxc::raw_slab::RawSlab;
use subetha_cxc::raw_treiber_stack::{ElementLayout, RawTreiberStack};
use subetha_cxc::raw_vec::RawVec;
use subetha_cxc::shared_vec::VecError;
use subetha_cxc::reorder::AdaptiveOrderedReceiver;
use subetha_cxc::shared_broadcast_ring::SharedBroadcastRing;
use subetha_cxc::shared_atomic::SharedAtomicU64;
use subetha_cxc::shared_ring::{SharedRing, SharedRingSpsc};
use subetha_cxc::shared_string_arena::{ArenaError, SharedStringArena};
use subetha_ffi::{
    subetha_arena_clear, subetha_arena_create, subetha_arena_get, subetha_arena_intern, subetha_arena_unlink,
    SUBETHA_E_ARENA_FULL, subetha_atomic_u64_create, subetha_atomic_u64_fetch_add, subetha_atomic_u64_load,
    subetha_atomic_unlink, subetha_list_create, subetha_list_pop_front, subetha_list_push_back,
    subetha_list_unlink, subetha_region_allocate, subetha_region_create, subetha_region_free,
    subetha_region_unlink, subetha_slab_create, subetha_slab_get, subetha_slab_set, subetha_slab_unlink,
    subetha_vec_clear, subetha_vec_create, subetha_vec_get, subetha_vec_push_back, subetha_vec_unlink,
    SUBETHA_E_RING_FULL,
    subetha_broadcast_create_anon, subetha_broadcast_register_consumer, subetha_broadcast_try_push,
    subetha_broadcast_try_recv, subetha_capacity_create_anon, subetha_capacity_register_consumer,
    subetha_deque_create, subetha_deque_try_pop, subetha_deque_try_push, subetha_deque_try_steal, subetha_deque_unlink,
    subetha_element_layout, subetha_hashmap_create, subetha_hashmap_get, subetha_hashmap_insert, subetha_hashmap_unlink,
    subetha_stack_create, subetha_stack_try_pop, subetha_stack_try_push, subetha_stack_unlink,
    subetha_unlink_report,
    subetha_capacity_register_producer, subetha_capacity_try_pop, subetha_capacity_try_push, subetha_handle,
    subetha_handle_destroy, subetha_init, subetha_ordered_try_next, subetha_ring_ordered_receiver,
    subetha_ring_try_pop_stamped, SUBETHA_STAMPS_DEFAULT, SUBETHA_STAMPS_SHARED_COUNTER,
    subetha_lamport_create_anon_pair, subetha_lamport_try_pop, subetha_lamport_try_push,
    subetha_mpmc_create_anon_grid, subetha_mpmc_try_pop, subetha_mpmc_try_push, subetha_mpsc_create_anon_pool,
    subetha_mpsc_try_pop, subetha_mpsc_try_push, subetha_pubsub_create_anon, subetha_pubsub_publish,
    subetha_pubsub_subscribe, subetha_ring_create_anon, subetha_ring_options, subetha_ring_recv_frame,
    subetha_ring_register_consumer, subetha_ring_register_producer, subetha_ring_send_frame,
    subetha_ring_try_pop, subetha_ring_try_pop_many, subetha_ring_try_push, subetha_ring_try_push_many,
    subetha_shutdown, subetha_spsc_create_anon,
    subetha_spsc_try_pop, subetha_spsc_try_push, subetha_subscriber_try_next, subetha_vyukov_create_anon,
    subetha_vyukov_try_pop, subetha_vyukov_try_push, SUBETHA_BROADCAST_PAYLOAD_BYTES, SUBETHA_LAYOUT_AUTO,
    SUBETHA_MODE_STRICT, SUBETHA_OK, SUBETHA_PUBSUB_PAYLOAD_BYTES, SUBETHA_RING_PAYLOAD_MAX,
    SUBETHA_RING_SLOT_BYTES,
    subetha_btree_create, subetha_btree_get, subetha_btree_insert, subetha_btree_unlink,
    subetha_cell_create, subetha_cell_get, subetha_cell_set, subetha_cell_unlink,
    subetha_abi_version, subetha_handle_kind,
    subetha_lazy_claim, subetha_lazy_create, subetha_lazy_publish, subetha_lazy_try_get,
    subetha_lazy_unlink,
    subetha_waker_create, subetha_waker_park, subetha_waker_release, subetha_waker_unlink,
    subetha_epoch_barrier_create, subetha_epoch_barrier_unlink, subetha_epoch_barrier_wait,
    subetha_epochs_create, subetha_epochs_pin, subetha_epochs_unlink, subetha_pin_release,
    subetha_fence_clock_create, subetha_fence_clock_merge, subetha_fence_clock_register,
    subetha_fence_clock_tick, subetha_fence_clock_unlink, subetha_hlc,
    subetha_heartbeat_create, subetha_heartbeat_register, subetha_heartbeat_unlink,
    subetha_frame_region_alloc, subetha_frame_region_create, subetha_frame_region_free,
    subetha_frame_region_read, subetha_frame_region_unlink, subetha_frame_region_write,
    subetha_borrow_registry_len, subetha_rwlock_create, subetha_rwlock_try_write, subetha_rwlock_unlink,
    subetha_rwlock_unlock,
    subetha_semaphore_create, subetha_semaphore_release, subetha_semaphore_try_acquire,
    subetha_semaphore_unlink,
    subetha_atomic_u64_load_explicit, SUBETHA_ORDER_RELAXED, SUBETHA_ORDER_SEQ_CST,
};
#[cfg(feature = "test-hooks")]
use subetha_ffi::handle::SUBETHA_KIND_ATOMIC;
#[cfg(feature = "test-hooks")]
use subetha_ffi::{subetha_test_atomic_borrow_only, subetha_test_borrow_only, subetha_test_entry_only};

const CAPACITY: usize = 1024;
const PAYLOAD: [u8; 16] = *b"sixteen bytes!!!";

fn ring_direct(c: &mut Criterion) {
    let ring = AdaptiveRing::create_anon(1, 1, CAPACITY).expect("an anonymous ring");
    let pid = ring.register_producer().expect("a producer id");
    let cid = ring.register_consumer().expect("a consumer id");
    let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
    c.bench_function("ring push+pop, direct Rust", |b| {
        b.iter(|| {
            ring.try_send(pid, black_box(&PAYLOAD)).expect("room in an otherwise empty ring");
            let n = ring.try_recv(cid, black_box(&mut out)).expect("the item just pushed");
            black_box(n);
        })
    });
    let mut frame = Vec::with_capacity(64);
    c.bench_function("ring send_frame+recv_frame inline, direct Rust", |b| {
        b.iter(|| {
            ring.send_frame(pid, black_box(&PAYLOAD)).expect("room in an otherwise empty ring");
            ring.recv_frame(cid, black_box(&mut frame)).expect("the frame just sent");
            black_box(frame.len());
        })
    });
}

fn ring_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, max_waiters: 0, scan_interval_us: 0, ..Default::default() };
    let mut handle = 0u64;
    let rc = unsafe { subetha_ring_create_anon(1, 1, CAPACITY as u32, &options, &mut handle) };
    assert_eq!(rc, SUBETHA_OK);
    let mut pid = 0u32;
    let mut cid = 0u32;
    assert_eq!(unsafe { subetha_ring_register_producer(handle, &mut pid) }, SUBETHA_OK);
    assert_eq!(unsafe { subetha_ring_register_consumer(handle, &mut cid) }, SUBETHA_OK);
    let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
    let mut len = 0usize;
    c.bench_function("ring push+pop, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_ring_try_push(handle, pid, black_box(PAYLOAD.as_ptr()), PAYLOAD.len()) };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_ring_try_pop(handle, cid, black_box(out.as_mut_ptr()), out.len(), &mut len) };
            assert_eq!(rc, SUBETHA_OK);
            black_box(len);
        })
    });
    c.bench_function("ring send_frame+recv_frame inline, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe {
                subetha_ring_send_frame(
                    handle,
                    pid,
                    black_box(PAYLOAD.as_ptr()),
                    PAYLOAD.len(),
                    SUBETHA_LAYOUT_AUTO,
                    std::ptr::null_mut(),
                )
            };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_ring_recv_frame(handle, cid, black_box(out.as_mut_ptr()), out.len(), &mut len) };
            assert_eq!(rc, SUBETHA_OK);
            black_box(len);
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

fn spsc_direct(c: &mut Criterion) {
    let ring = BlockingSpscRing::create_anon(CAPACITY).expect("an anonymous blocking ring");
    let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
    c.bench_function("spsc push+pop, direct Rust", |b| {
        b.iter(|| {
            ring.try_push(black_box(&PAYLOAD)).expect("room in an otherwise empty ring");
            let n = ring.try_pop(black_box(&mut out)).expect("the item just pushed");
            black_box(n);
        })
    });
}

fn spsc_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let mut handle: subetha_handle = 0;
    assert_eq!(unsafe { subetha_spsc_create_anon(CAPACITY as u32, SUBETHA_MODE_STRICT, &mut handle) }, SUBETHA_OK);
    let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
    let mut len = 0usize;
    c.bench_function("spsc push+pop, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_spsc_try_push(handle, black_box(PAYLOAD.as_ptr()), PAYLOAD.len()) };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_spsc_try_pop(handle, black_box(out.as_mut_ptr()), out.len(), &mut len) };
            assert_eq!(rc, SUBETHA_OK);
            black_box(len);
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

fn mpsc_direct(c: &mut Criterion) {
    let (producers, consumer) = BlockingMpscRing::create_anon_pool(1, CAPACITY).expect("an anonymous pool");
    let producer = &producers[0];
    let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
    c.bench_function("mpsc push+pop, direct Rust", |b| {
        b.iter(|| {
            producer.try_push(black_box(&PAYLOAD)).expect("room in an otherwise empty ring");
            let n = consumer.try_pop(black_box(&mut out)).expect("the item just pushed");
            black_box(n);
        })
    });
}

fn mpsc_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let mut producer: subetha_handle = 0;
    let mut consumer: subetha_handle = 0;
    let rc = unsafe { subetha_mpsc_create_anon_pool(1, CAPACITY as u32, SUBETHA_MODE_STRICT, &mut producer, &mut consumer) };
    assert_eq!(rc, SUBETHA_OK);
    let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
    let mut len = 0usize;
    c.bench_function("mpsc push+pop, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_mpsc_try_push(producer, black_box(PAYLOAD.as_ptr()), PAYLOAD.len()) };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_mpsc_try_pop(consumer, black_box(out.as_mut_ptr()), out.len(), &mut len) };
            assert_eq!(rc, SUBETHA_OK);
            black_box(len);
        })
    });
    assert_eq!(subetha_handle_destroy(producer), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(consumer), SUBETHA_OK);
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

fn mpmc_direct(c: &mut Criterion) {
    let (producers, consumers) = BlockingMpmcRing::create_anon_grid(1, 1, CAPACITY).expect("an anonymous grid");
    let producer = &producers[0];
    let consumer = &consumers[0];
    let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
    c.bench_function("mpmc push+pop, direct Rust", |b| {
        b.iter(|| {
            producer.try_push(black_box(&PAYLOAD)).expect("room in an otherwise empty ring");
            let n = consumer.try_pop(black_box(&mut out)).expect("the item just pushed");
            black_box(n);
        })
    });
}

fn mpmc_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let mut producer: subetha_handle = 0;
    let mut consumer: subetha_handle = 0;
    let rc = unsafe {
        subetha_mpmc_create_anon_grid(1, 1, CAPACITY as u32, SUBETHA_MODE_STRICT, &mut producer, &mut consumer)
    };
    assert_eq!(rc, SUBETHA_OK);
    let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
    let mut len = 0usize;
    c.bench_function("mpmc push+pop, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_mpmc_try_push(producer, black_box(PAYLOAD.as_ptr()), PAYLOAD.len()) };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_mpmc_try_pop(consumer, black_box(out.as_mut_ptr()), out.len(), &mut len) };
            assert_eq!(rc, SUBETHA_OK);
            black_box(len);
        })
    });
    assert_eq!(subetha_handle_destroy(producer), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(consumer), SUBETHA_OK);
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

fn vyukov_direct(c: &mut Criterion) {
    let ring = SharedRing::create_anon(CAPACITY).expect("an anonymous Vyukov ring");
    let mut out = [0u8; SUBETHA_RING_PAYLOAD_MAX];
    c.bench_function("vyukov push+pop, direct Rust", |b| {
        b.iter(|| {
            ring.try_push(black_box(&PAYLOAD)).expect("room in an otherwise empty ring");
            let n = ring.try_pop(black_box(&mut out)).expect("the item just pushed");
            black_box(n);
        })
    });
}

fn vyukov_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, max_waiters: 0, scan_interval_us: 0, ..Default::default() };
    let mut handle: subetha_handle = 0;
    assert_eq!(unsafe { subetha_vyukov_create_anon(CAPACITY as u32, &options, &mut handle) }, SUBETHA_OK);
    let mut out = [0u8; SUBETHA_RING_PAYLOAD_MAX];
    let mut len = 0usize;
    c.bench_function("vyukov push+pop, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_vyukov_try_push(handle, black_box(PAYLOAD.as_ptr()), PAYLOAD.len()) };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_vyukov_try_pop(handle, black_box(out.as_mut_ptr()), out.len(), &mut len) };
            assert_eq!(rc, SUBETHA_OK);
            black_box(len);
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

fn lamport_direct(c: &mut Criterion) {
    let (producer, consumer) = SharedRingSpsc::create_anon_pair(CAPACITY).expect("an anonymous pair");
    let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
    c.bench_function("lamport push+pop, direct Rust", |b| {
        b.iter(|| {
            producer.try_push(black_box(&PAYLOAD)).expect("room in an otherwise empty ring");
            let n = consumer.try_pop(black_box(&mut out)).expect("the item just pushed");
            black_box(n);
        })
    });
}

fn lamport_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let mut producer: subetha_handle = 0;
    let mut consumer: subetha_handle = 0;
    let rc = unsafe { subetha_lamport_create_anon_pair(CAPACITY as u32, SUBETHA_MODE_STRICT, &mut producer, &mut consumer) };
    assert_eq!(rc, SUBETHA_OK);
    let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
    let mut len = 0usize;
    c.bench_function("lamport push+pop, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_lamport_try_push(producer, black_box(PAYLOAD.as_ptr()), PAYLOAD.len()) };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_lamport_try_pop(consumer, black_box(out.as_mut_ptr()), out.len(), &mut len) };
            assert_eq!(rc, SUBETHA_OK);
            black_box(len);
        })
    });
    assert_eq!(subetha_handle_destroy(producer), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(consumer), SUBETHA_OK);
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

fn broadcast_direct(c: &mut Criterion) {
    let ring = SharedBroadcastRing::create_anon(CAPACITY).expect("an anonymous broadcast ring");
    let consumer = ring.register_consumer().expect("a consumer index");
    let mut out = [0u8; SUBETHA_BROADCAST_PAYLOAD_BYTES];
    c.bench_function("broadcast push+recv one consumer, direct Rust", |b| {
        b.iter(|| {
            ring.try_push(black_box(&PAYLOAD)).expect("room in an otherwise drained ring");
            let n = ring.try_recv(consumer, black_box(&mut out)).expect("the item just pushed");
            black_box(n);
        })
    });
}

fn broadcast_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, max_waiters: 0, scan_interval_us: 0, ..Default::default() };
    let mut handle: subetha_handle = 0;
    assert_eq!(unsafe { subetha_broadcast_create_anon(CAPACITY as u32, &options, &mut handle) }, SUBETHA_OK);
    let mut consumer = 0u32;
    assert_eq!(unsafe { subetha_broadcast_register_consumer(handle, &mut consumer) }, SUBETHA_OK);
    let mut out = [0u8; SUBETHA_BROADCAST_PAYLOAD_BYTES];
    let mut len = 0usize;
    c.bench_function("broadcast push+recv one consumer, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_broadcast_try_push(handle, black_box(PAYLOAD.as_ptr()), PAYLOAD.len()) };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_broadcast_try_recv(handle, consumer, black_box(out.as_mut_ptr()), out.len(), &mut len) };
            assert_eq!(rc, SUBETHA_OK);
            black_box(len);
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

fn pubsub_direct(c: &mut Criterion) {
    let ring = PubSubRing::create_anon(CAPACITY).expect("an anonymous pub/sub ring");
    let mut out = [0u8; SUBETHA_PUBSUB_PAYLOAD_BYTES];
    let mut position = 0u64;
    c.bench_function("pubsub publish+read_at, direct Rust", |b| {
        b.iter(|| {
            let at = ring.publish(black_box(&PAYLOAD));
            ring.read_at(at, black_box(&mut out)).expect("the item just published");
            position = at;
            black_box(position);
        })
    });
}

fn pubsub_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, max_waiters: 0, scan_interval_us: 0, ..Default::default() };
    let mut handle: subetha_handle = 0;
    assert_eq!(unsafe { subetha_pubsub_create_anon(CAPACITY as u32, &options, &mut handle) }, SUBETHA_OK);
    let mut subscriber: subetha_handle = 0;
    assert_eq!(unsafe { subetha_pubsub_subscribe(handle, 0, SUBETHA_MODE_STRICT, &mut subscriber) }, SUBETHA_OK);
    let mut out = [0u8; SUBETHA_PUBSUB_PAYLOAD_BYTES];
    let mut len = 0usize;
    c.bench_function("pubsub publish+subscriber next, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_pubsub_publish(handle, black_box(PAYLOAD.as_ptr()), PAYLOAD.len(), std::ptr::null_mut()) };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_subscriber_try_next(subscriber, black_box(out.as_mut_ptr()), out.len(), &mut len) };
            assert_eq!(rc, SUBETHA_OK);
            black_box(len);
        })
    });
    assert_eq!(subetha_handle_destroy(subscriber), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

fn capacity_direct(c: &mut Criterion) {
    let ring = CapacityAdaptiveRing::create_anon(1, 1, CAPACITY).expect("an anonymous capacity ring");
    let pid = ring.register_producer().expect("a producer id");
    let cid = ring.register_consumer().expect("a consumer id");
    let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
    c.bench_function("capacity ring push+pop, direct Rust", |b| {
        b.iter(|| {
            ring.try_send(pid, black_box(&PAYLOAD)).expect("room in an otherwise empty ring");
            let n = ring.try_recv(cid, black_box(&mut out)).expect("the item just pushed");
            black_box(n);
        })
    });
}

fn capacity_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, ..Default::default() };
    let mut handle: subetha_handle = 0;
    assert_eq!(unsafe { subetha_capacity_create_anon(1, 1, CAPACITY as u32, &options, &mut handle) }, SUBETHA_OK);
    let mut pid = 0u32;
    let mut cid = 0u32;
    assert_eq!(unsafe { subetha_capacity_register_producer(handle, &mut pid) }, SUBETHA_OK);
    assert_eq!(unsafe { subetha_capacity_register_consumer(handle, &mut cid) }, SUBETHA_OK);
    let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
    let mut len = 0usize;
    c.bench_function("capacity ring push+pop, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_capacity_try_push(handle, pid, black_box(PAYLOAD.as_ptr()), PAYLOAD.len()) };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_capacity_try_pop(handle, cid, black_box(out.as_mut_ptr()), out.len(), &mut len) };
            assert_eq!(rc, SUBETHA_OK);
            black_box(len);
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

fn stamped_ring_direct(c: &mut Criterion) {
    let ring = AdaptiveRing::create_anon(1, 1, CAPACITY)
        .expect("an anonymous ring")
        .with_ordering_stamps()
        .expect("stamps on it");
    let pid = ring.register_producer().expect("a producer id");
    let cid = ring.register_consumer().expect("a consumer id");
    let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
    c.bench_function("stamped ring push+pop with stamp, direct Rust", |b| {
        b.iter(|| {
            ring.try_send(pid, black_box(&PAYLOAD)).expect("room in an otherwise empty ring");
            let taken = ring.try_recv_with_stamp(cid, black_box(&mut out)).expect("the item just pushed");
            black_box(taken);
        })
    });
}

fn stamped_ring_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, stamps: SUBETHA_STAMPS_DEFAULT, ..Default::default() };
    let mut handle = 0u64;
    assert_eq!(unsafe { subetha_ring_create_anon(1, 1, CAPACITY as u32, &options, &mut handle) }, SUBETHA_OK);
    let mut pid = 0u32;
    let mut cid = 0u32;
    assert_eq!(unsafe { subetha_ring_register_producer(handle, &mut pid) }, SUBETHA_OK);
    assert_eq!(unsafe { subetha_ring_register_consumer(handle, &mut cid) }, SUBETHA_OK);
    let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
    let mut len = 0usize;
    let mut stamp = 0u64;
    c.bench_function("stamped ring push+pop with stamp, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_ring_try_push(handle, pid, black_box(PAYLOAD.as_ptr()), PAYLOAD.len()) };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe {
                subetha_ring_try_pop_stamped(handle, cid, black_box(out.as_mut_ptr()), out.len(), &mut len, &mut stamp)
            };
            assert_eq!(rc, SUBETHA_OK);
            black_box((len, stamp));
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

fn ordered_direct(c: &mut Criterion) {
    let ring = AdaptiveRing::create_anon(1, 1, CAPACITY)
        .expect("an anonymous ring")
        .with_ordering_stamps_kind(StampKind::SharedCounter)
        .expect("counter stamps on it");
    let pid = ring.register_producer().expect("a producer id");
    let cid = ring.register_consumer().expect("a consumer id");
    let mut receiver = AdaptiveOrderedReceiver::new(&ring, cid);
    let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
    c.bench_function("ordered receiver push+next, direct Rust", |b| {
        b.iter(|| {
            ring.try_send(pid, black_box(&PAYLOAD)).expect("room in an otherwise empty ring");
            // The window holds the first items; a steady stream releases one per push.
            let taken = receiver.try_recv(black_box(&mut out));
            black_box(taken);
        })
    });
}

fn ordered_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, stamps: SUBETHA_STAMPS_SHARED_COUNTER, ..Default::default() };
    let mut handle = 0u64;
    assert_eq!(unsafe { subetha_ring_create_anon(1, 1, CAPACITY as u32, &options, &mut handle) }, SUBETHA_OK);
    let mut pid = 0u32;
    let mut cid = 0u32;
    assert_eq!(unsafe { subetha_ring_register_producer(handle, &mut pid) }, SUBETHA_OK);
    assert_eq!(unsafe { subetha_ring_register_consumer(handle, &mut cid) }, SUBETHA_OK);
    let mut receiver: subetha_handle = 0;
    assert_eq!(unsafe { subetha_ring_ordered_receiver(handle, cid, SUBETHA_MODE_STRICT, &mut receiver) }, SUBETHA_OK);
    let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
    let mut len = 0usize;
    let mut stamp = 0u64;
    c.bench_function("ordered receiver push+next, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_ring_try_push(handle, pid, black_box(PAYLOAD.as_ptr()), PAYLOAD.len()) };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_ordered_try_next(receiver, black_box(out.as_mut_ptr()), out.len(), &mut len, &mut stamp) };
            black_box(rc);
        })
    });
    assert_eq!(subetha_handle_destroy(receiver), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// A file under the temp directory for one bench's stack or deque; the
/// file-backed forms are the only ones these two primitives have.
fn bench_file(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("subetha-ffi-bench-{name}-{}.bin", std::process::id()))
}

fn c_path(path: &std::path::Path) -> std::ffi::CString {
    std::ffi::CString::new(path.to_str().expect("the temp directory is UTF-8")).expect("the path has no NUL")
}

const ELEMENT: ElementLayout = ElementLayout { slot_size: 16, alignment: 1, tag: 1 };
const C_ELEMENT: subetha_element_layout = subetha_element_layout { element_size: 16, alignment: 1, tag: 1 };

fn stack_direct(c: &mut Criterion) {
    let path = bench_file("stack-direct");
    let stack = RawTreiberStack::create(&path, CAPACITY, ELEMENT).expect("a file-backed stack");
    let mut out = [0u8; 16];
    c.bench_function("stack push+pop, direct Rust", |b| {
        b.iter(|| {
            stack.push(black_box(&PAYLOAD)).expect("room in an otherwise empty stack");
            let n = stack.pop(black_box(&mut out)).expect("the element just pushed");
            black_box(n);
        })
    });
    drop(stack);
    std::fs::remove_file(&path).expect("the bench's stack file is removed");
}

fn stack_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = bench_file("stack-abi");
    let c_path = c_path(&path);
    let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, max_waiters: 0, scan_interval_us: 0, ..Default::default() };
    let mut handle: subetha_handle = 0;
    assert_eq!(unsafe { subetha_stack_create(c_path.as_ptr(), CAPACITY as u32, &C_ELEMENT, &options, &mut handle) }, SUBETHA_OK);
    let mut out = [0u8; 16];
    let mut len = 0usize;
    c.bench_function("stack push+pop, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_stack_try_push(handle, black_box(PAYLOAD.as_ptr()), PAYLOAD.len()) };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_stack_try_pop(handle, black_box(out.as_mut_ptr()), out.len(), &mut len) };
            assert_eq!(rc, SUBETHA_OK);
            black_box(len);
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_stack_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 3, "the stack file and its two wakers");
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

fn deque_direct(c: &mut Criterion) {
    let path = bench_file("deque-direct");
    let deque = RawDeque::create(&path, CAPACITY, ELEMENT).expect("a file-backed deque");
    let mut out = [0u8; 16];
    c.bench_function("deque push+pop, direct Rust", |b| {
        b.iter(|| {
            deque.push(black_box(&PAYLOAD)).expect("room in an otherwise empty deque");
            let n = deque.pop(black_box(&mut out)).expect("the element just pushed");
            black_box(n);
        })
    });
    c.bench_function("deque push+steal, direct Rust", |b| {
        b.iter(|| {
            deque.push(black_box(&PAYLOAD)).expect("room in an otherwise empty deque");
            let n = deque.steal(black_box(&mut out)).expect("the element just pushed");
            black_box(n);
        })
    });
    drop(deque);
    std::fs::remove_file(&path).expect("the bench's deque file is removed");
}

fn deque_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = bench_file("deque-abi");
    let c_path = c_path(&path);
    let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, max_waiters: 0, scan_interval_us: 0, ..Default::default() };
    let mut handle: subetha_handle = 0;
    assert_eq!(unsafe { subetha_deque_create(c_path.as_ptr(), CAPACITY as u32, &C_ELEMENT, &options, &mut handle) }, SUBETHA_OK);
    let mut out = [0u8; 16];
    let mut len = 0usize;
    c.bench_function("deque push+pop, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_deque_try_push(handle, black_box(PAYLOAD.as_ptr()), PAYLOAD.len()) };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_deque_try_pop(handle, black_box(out.as_mut_ptr()), out.len(), &mut len) };
            assert_eq!(rc, SUBETHA_OK);
            black_box(len);
        })
    });
    c.bench_function("deque push+steal, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_deque_try_push(handle, black_box(PAYLOAD.as_ptr()), PAYLOAD.len()) };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_deque_try_steal(handle, black_box(out.as_mut_ptr()), out.len(), &mut len) };
            assert_eq!(rc, SUBETHA_OK);
            black_box(len);
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_deque_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 3, "the deque file and its two wakers");
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

fn hashmap_direct(c: &mut Criterion) {
    let path = bench_file("hashmap-direct");
    let map = RawHashMap::create(&path, CAPACITY, 8, 16).expect("a file-backed map");
    let mut out = [0u8; 16];
    let mut key = 0u64;
    c.bench_function("hashmap insert+get, direct Rust", |b| {
        b.iter(|| {
            key = (key + 1) % 512;
            let k = key.to_ne_bytes();
            map.insert(black_box(&k), black_box(&PAYLOAD)).expect("room in a half-full map");
            let found = map.get(black_box(&k), black_box(&mut out)).expect("a key of the right size");
            black_box(found);
        })
    });
    drop(map);
    std::fs::remove_file(&path).expect("the bench's map file is removed");
}

fn hashmap_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = bench_file("hashmap-abi");
    let c_path = c_path(&path);
    let mut handle: subetha_handle = 0;
    assert_eq!(unsafe { subetha_hashmap_create(c_path.as_ptr(), CAPACITY as u32, 8, 16, SUBETHA_MODE_STRICT, &mut handle) }, SUBETHA_OK);
    let mut out = [0u8; 16];
    let mut len = 0usize;
    let mut key = 0u64;
    c.bench_function("hashmap insert+get, through the C ABI", |b| {
        b.iter(|| {
            key = (key + 1) % 512;
            let k = key.to_ne_bytes();
            let rc = unsafe { subetha_hashmap_insert(handle, black_box(k.as_ptr()), k.len(), PAYLOAD.as_ptr(), PAYLOAD.len(), std::ptr::null_mut()) };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_hashmap_get(handle, black_box(k.as_ptr()), k.len(), out.as_mut_ptr(), out.len(), &mut len) };
            assert_eq!(rc, SUBETHA_OK);
            black_box(len);
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_hashmap_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the map file");
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// The bench's arena holds 65536 payloads; a full arena is cleared and the
/// intern retried, so the clear's cost lands once in every 65536 rounds.
const ARENA_BYTES: u64 = 1 << 20;

fn arena_direct(c: &mut Criterion) {
    let path = bench_file("arena-direct");
    let arena = SharedStringArena::create(&path, ARENA_BYTES as usize).expect("a file-backed arena");
    let mut out = [0u8; 16];
    c.bench_function("arena intern+get, direct Rust", |b| {
        b.iter(|| {
            let reference = match arena.intern_bytes(black_box(&PAYLOAD)) {
                Ok(r) => r,
                Err(ArenaError::Full) => {
                    arena.clear();
                    arena.intern_bytes(&PAYLOAD).expect("room in a cleared arena")
                }
                Err(e) => panic!("the intern failed: {e:?}"),
            };
            let found = arena.get_bytes(black_box(reference)).expect("the reference just minted");
            out.copy_from_slice(found);
            black_box(&out);
        })
    });
    drop(arena);
    std::fs::remove_file(&path).expect("the bench's arena file is removed");
}

fn arena_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = bench_file("arena-abi");
    let c_path = c_path(&path);
    let mut handle: subetha_handle = 0;
    assert_eq!(unsafe { subetha_arena_create(c_path.as_ptr(), ARENA_BYTES, SUBETHA_MODE_STRICT, &mut handle) }, SUBETHA_OK);
    let mut out = [0u8; 16];
    let mut len = 0usize;
    let mut reference = 0u64;
    c.bench_function("arena intern+get, through the C ABI", |b| {
        b.iter(|| {
            let mut rc = unsafe { subetha_arena_intern(handle, black_box(PAYLOAD.as_ptr()), PAYLOAD.len(), &mut reference) };
            if rc == SUBETHA_E_ARENA_FULL {
                assert_eq!(subetha_arena_clear(handle), SUBETHA_OK);
                rc = unsafe { subetha_arena_intern(handle, PAYLOAD.as_ptr(), PAYLOAD.len(), &mut reference) };
            }
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_arena_get(handle, black_box(reference), out.as_mut_ptr(), out.len(), &mut len) };
            assert_eq!(rc, SUBETHA_OK);
            black_box(len);
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_arena_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the arena file");
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// The bench's vec holds `CAPACITY` elements; a full vec is cleared and
/// the push retried, so the clear's cost lands once in every `CAPACITY`
/// rounds.
fn vec_direct(c: &mut Criterion) {
    let path = bench_file("vec-direct");
    let vec = RawVec::create(&path, CAPACITY, ELEMENT).expect("a file-backed vec");
    let mut out = [0u8; 16];
    c.bench_function("vec push+get, direct Rust", |b| {
        b.iter(|| {
            let index = match vec.push_back(black_box(&PAYLOAD)) {
                Ok(i) => i,
                Err(VecError::Full) => {
                    vec.clear().expect("a writable vec clears");
                    vec.push_back(&PAYLOAD).expect("room in a cleared vec")
                }
                Err(e) => panic!("the push failed: {e:?}"),
            };
            let found = vec.get(black_box(index), &mut out).expect("a buffer of the element size");
            black_box(found);
        })
    });
    drop(vec);
    std::fs::remove_file(&path).expect("the bench's vec file is removed");
}

fn vec_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = bench_file("vec-abi");
    let c_path = c_path(&path);
    let mut handle: subetha_handle = 0;
    let rc = unsafe { subetha_vec_create(c_path.as_ptr(), CAPACITY as u32, &C_ELEMENT, SUBETHA_MODE_STRICT, &mut handle) };
    assert_eq!(rc, SUBETHA_OK);
    let mut out = [0u8; 16];
    let mut len = 0usize;
    let mut index = 0u64;
    c.bench_function("vec push+get, through the C ABI", |b| {
        b.iter(|| {
            let mut rc = unsafe { subetha_vec_push_back(handle, black_box(PAYLOAD.as_ptr()), PAYLOAD.len(), &mut index) };
            if rc == SUBETHA_E_RING_FULL {
                assert_eq!(subetha_vec_clear(handle), SUBETHA_OK);
                rc = unsafe { subetha_vec_push_back(handle, PAYLOAD.as_ptr(), PAYLOAD.len(), &mut index) };
            }
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_vec_get(handle, black_box(index), out.as_mut_ptr(), out.len(), &mut len) };
            assert_eq!(rc, SUBETHA_OK);
            black_box(len);
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_vec_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the vec file");
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

fn slab_direct(c: &mut Criterion) {
    let path = bench_file("slab-direct");
    let slab = RawSlab::create(&path, CAPACITY, ELEMENT).expect("a file-backed slab");
    let mut out = [0u8; 16];
    let mut index = 0usize;
    c.bench_function("slab set+get, direct Rust", |b| {
        b.iter(|| {
            index = (index + 1) % CAPACITY;
            slab.set(black_box(index), black_box(&PAYLOAD)).expect("a slot below the capacity");
            slab.get(black_box(index), &mut out).expect("a buffer of the record size");
            black_box(&out);
        })
    });
    drop(slab);
    std::fs::remove_file(&path).expect("the bench's slab file is removed");
}

fn slab_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = bench_file("slab-abi");
    let c_path = c_path(&path);
    let mut handle: subetha_handle = 0;
    let rc = unsafe { subetha_slab_create(c_path.as_ptr(), CAPACITY as u32, &C_ELEMENT, SUBETHA_MODE_STRICT, &mut handle) };
    assert_eq!(rc, SUBETHA_OK);
    let mut out = [0u8; 16];
    let mut len = 0usize;
    let mut index = 0u64;
    c.bench_function("slab set+get, through the C ABI", |b| {
        b.iter(|| {
            index = (index + 1) % CAPACITY as u64;
            let rc = unsafe { subetha_slab_set(handle, black_box(index), PAYLOAD.as_ptr(), PAYLOAD.len()) };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_slab_get(handle, black_box(index), out.as_mut_ptr(), out.len(), &mut len) };
            assert_eq!(rc, SUBETHA_OK);
            black_box(len);
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_slab_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the slab file");
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// Items per batch in the batched-against-single rows: enough that the
/// per-call cost the batch amortizes is the whole difference between them.
const BATCH: usize = 64;

/// The same work twice over: sixty-four pushes and sixty-four pops through
/// the single-call entry points, then through the batched ones.
fn ring_batch_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, max_waiters: 0, scan_interval_us: 0, ..Default::default() };
    let mut handle: subetha_handle = 0;
    assert_eq!(unsafe { subetha_ring_create_anon(1, 1, CAPACITY as u32, &options, &mut handle) }, SUBETHA_OK);
    let mut producer = 0u32;
    let mut consumer = 0u32;
    assert_eq!(unsafe { subetha_ring_register_producer(handle, &mut producer) }, SUBETHA_OK);
    assert_eq!(unsafe { subetha_ring_register_consumer(handle, &mut consumer) }, SUBETHA_OK);
    let items = vec![PAYLOAD; BATCH];
    let mut out = vec![0u8; BATCH * SUBETHA_RING_SLOT_BYTES];
    let mut len = 0usize;

    c.bench_function("ring 64 push+pop one call at a time, through the C ABI", |b| {
        b.iter(|| {
            for item in &items {
                let rc = unsafe { subetha_ring_try_push(handle, producer, black_box(item.as_ptr()), item.len()) };
                assert_eq!(rc, SUBETHA_OK);
            }
            for slot in out.chunks_mut(SUBETHA_RING_SLOT_BYTES) {
                let rc = unsafe { subetha_ring_try_pop(handle, consumer, slot.as_mut_ptr(), slot.len(), &mut len) };
                assert_eq!(rc, SUBETHA_OK);
            }
        })
    });

    let mut done = 0usize;
    c.bench_function("ring 64 push+pop batched, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe {
                subetha_ring_try_push_many(
                    handle,
                    producer,
                    black_box(items.as_ptr().cast::<u8>()),
                    PAYLOAD.len(),
                    PAYLOAD.len(),
                    BATCH,
                    &mut done,
                )
            };
            assert_eq!(rc, SUBETHA_OK);
            assert_eq!(done, BATCH);
            let rc = unsafe {
                subetha_ring_try_pop_many(handle, consumer, out.as_mut_ptr(), SUBETHA_RING_SLOT_BYTES, BATCH, &mut done)
            };
            assert_eq!(rc, SUBETHA_OK);
            assert_eq!(done, BATCH);
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// The list's nodes are built at an alignment of at least four, so the
/// bench declares the element that way and both sides agree.
const LIST_ELEMENT: ElementLayout = ElementLayout { slot_size: 16, alignment: 4, tag: 1 };
const C_LIST_ELEMENT: subetha_element_layout = subetha_element_layout { element_size: 16, alignment: 4, tag: 1 };

fn list_direct(c: &mut Criterion) {
    let path = bench_file("list-direct");
    let list = RawLinkedList::create(&path, 4, LIST_ELEMENT).expect("a file-backed list");
    let mut out = [0u8; 16];
    c.bench_function("list push_back+pop_front, direct Rust", |b| {
        b.iter(|| {
            list.push_back(black_box(&PAYLOAD)).expect("room past the head");
            let taken = list.pop_front(&mut out).expect("a buffer of the element size");
            black_box(taken);
        })
    });
    drop(list);
    std::fs::remove_file(&path).expect("the bench's list file is removed");
}

fn list_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = bench_file("list-abi");
    let c_path = c_path(&path);
    let mut handle: subetha_handle = 0;
    let rc = unsafe { subetha_list_create(c_path.as_ptr(), 4, &C_LIST_ELEMENT, SUBETHA_MODE_STRICT, &mut handle) };
    assert_eq!(rc, SUBETHA_OK);
    let mut out = [0u8; 16];
    let mut len = 0usize;
    let mut index = 0u32;
    c.bench_function("list push_back+pop_front, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_list_push_back(handle, black_box(PAYLOAD.as_ptr()), PAYLOAD.len(), &mut index) };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_list_pop_front(handle, out.as_mut_ptr(), out.len(), &mut len) };
            assert_eq!(rc, SUBETHA_OK);
            black_box(len);
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_list_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the list file");
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

fn atomic_direct(c: &mut Criterion) {
    let path = bench_file("atomic-direct");
    let counter = SharedAtomicU64::create(&path, 0).expect("a file-backed counter");
    c.bench_function("atomic fetch_add+load, direct Rust", |b| {
        b.iter(|| {
            let prev = counter.fetch_add(black_box(1), Ordering::SeqCst);
            let now = counter.load(Ordering::SeqCst);
            black_box((prev, now));
        })
    });
    drop(counter);
    std::fs::remove_file(&path).expect("the bench's counter file is removed");
}

/// What a call pays before it reaches the primitive, in steps that each
/// add one layer, so the difference between two rows names the layer
/// between them.
///
/// `abi_version` returns a computed constant: no init check, no panic
/// guard, no handle. It is the cost of crossing the boundary at all.
///
/// `handle_kind` adds `entry`'s `catch_unwind`, the initialized check and
/// the borrow with its guard, and writes one `uint32_t`.
///
/// `atomic_u64_load` is `with_kind`: the same borrow, its own
/// `catch_unwind`, the dispatch through `Object` and the family's own
/// closure, ending in one 64-bit load. The same load direct on the
/// primitive is the `atomic` row's direct figure.
///
/// With the `test-hooks` feature two more rows sit between those:
/// `entry` around nothing, which is the panic guard alone, and `with_kind`
/// around nothing, which is the borrow and its guard with no family
/// closure after them. A row that reads like `atomic_u64_load` names the
/// layer that costs.
fn boundary_layers(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = bench_file("layers");
    let c_path = c_path(&path);
    let mut handle: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_atomic_u64_create(c_path.as_ptr(), 0, SUBETHA_MODE_STRICT, &mut handle) },
        SUBETHA_OK,
    );

    c.bench_function("boundary: a call with no guard, through the C ABI", |b| {
        b.iter(|| black_box(subetha_abi_version()))
    });

    let mut kind = 0u32;
    c.bench_function("boundary: plus the panic guard and the slot lookup, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_handle_kind(black_box(handle), &mut kind) };
            assert_eq!(rc, SUBETHA_OK);
        })
    });

    let mut now = 0u64;
    c.bench_function("boundary: plus the borrow guard and the dispatch, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_atomic_u64_load(black_box(handle), &mut now) };
            assert_eq!(rc, SUBETHA_OK);
        })
    });

    // The order is passed through the explicit entry point rather than
    // fixed by the wrapper, so a cost that belongs to a runtime ordering
    // shows against the row above.
    c.bench_function("boundary: the atomic load with a relaxed order passed through, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_atomic_u64_load_explicit(black_box(handle), SUBETHA_ORDER_RELAXED, &mut now) };
            assert_eq!(rc, SUBETHA_OK);
        })
    });
    c.bench_function("boundary: the atomic load with a seq_cst order passed through, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_atomic_u64_load_explicit(black_box(handle), SUBETHA_ORDER_SEQ_CST, &mut now) };
            assert_eq!(rc, SUBETHA_OK);
        })
    });

    #[cfg(feature = "test-hooks")]
    c.bench_function("boundary: the panic guard around nothing, through the C ABI", |b| {
        b.iter(|| black_box(subetha_test_entry_only()))
    });
    #[cfg(feature = "test-hooks")]
    c.bench_function("boundary: the borrow and its guard around nothing, through the C ABI", |b| {
        b.iter(|| {
            let rc = subetha_test_borrow_only(black_box(handle), SUBETHA_KIND_ATOMIC);
            assert_eq!(rc, SUBETHA_OK);
        })
    });
    #[cfg(feature = "test-hooks")]
    c.bench_function("boundary: the atomic family's dispatch around nothing, through the C ABI", |b| {
        b.iter(|| {
            let rc = subetha_test_atomic_borrow_only(black_box(handle));
            assert_eq!(rc, SUBETHA_OK);
        })
    });

    eprintln!("registry at the boundary layers: {}", subetha_borrow_registry_len());
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_atomic_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the counter's file");
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

fn atomic_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = bench_file("atomic-abi");
    let c_path = c_path(&path);
    let mut handle: subetha_handle = 0;
    assert_eq!(unsafe { subetha_atomic_u64_create(c_path.as_ptr(), 0, SUBETHA_MODE_STRICT, &mut handle) }, SUBETHA_OK);
    let mut prev = 0u64;
    let mut now = 0u64;
    c.bench_function("atomic fetch_add+load, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_atomic_u64_fetch_add(handle, black_box(1), &mut prev) };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_atomic_u64_load(handle, &mut now) };
            assert_eq!(rc, SUBETHA_OK);
            black_box((prev, now));
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_atomic_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the counter's file");
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

fn region_direct(c: &mut Criterion) {
    let path = bench_file("region-direct");
    let region = RawRegion::create(&path, CAPACITY, ELEMENT).expect("a file-backed region");
    let mut out = [0u8; 16];
    c.bench_function("region allocate+free, direct Rust", |b| {
        b.iter(|| {
            let index = region.allocate(black_box(&PAYLOAD)).expect("a slot in an empty region");
            region.free(black_box(index), &mut out).expect("a buffer of the element size");
            black_box(&out);
        })
    });
    drop(region);
    std::fs::remove_file(&path).expect("the bench's region file is removed");
}

fn region_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = bench_file("region-abi");
    let c_path = c_path(&path);
    let mut handle: subetha_handle = 0;
    let rc = unsafe { subetha_region_create(c_path.as_ptr(), CAPACITY as u32, &C_ELEMENT, SUBETHA_MODE_STRICT, &mut handle) };
    assert_eq!(rc, SUBETHA_OK);
    let mut out = [0u8; 16];
    let mut len = 0usize;
    let mut index = 0u32;
    c.bench_function("region allocate+free, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_region_allocate(handle, black_box(PAYLOAD.as_ptr()), PAYLOAD.len(), &mut index) };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_region_free(handle, black_box(index), out.as_mut_ptr(), out.len(), &mut len) };
            assert_eq!(rc, SUBETHA_OK);
            black_box(len);
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_region_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the region file");
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// A hold, a permit and a pin are each a handle of their own, so taking
/// one issues a handle and releasing it destroys one. A destroy is not
/// free: it closes the slot, bumps the guard's global epoch and runs a
/// process-wide barrier before it lets the object go, so the question
/// these three pairs answer is whether that lands on every acquire and
/// release rather than only on the destroy of the object itself.
fn rwlock_direct(c: &mut Criterion) {
    let path = bench_file("rwlock-direct");
    let lock = SharedRWLock::create(&path).expect("a file-backed lock");
    c.bench_function("rwlock write acquire+release, direct Rust", |b| {
        b.iter(|| {
            let guard = lock.try_write_lock().expect("an unheld lock");
            black_box(&guard);
        })
    });
    drop(lock);
    std::fs::remove_file(&path).expect("the bench's lock file is removed");
}

fn rwlock_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = bench_file("rwlock-abi");
    let c_path = c_path(&path);
    let mut handle: subetha_handle = 0;
    let rc = unsafe { subetha_rwlock_create(c_path.as_ptr(), SUBETHA_MODE_STRICT, &mut handle) };
    assert_eq!(rc, SUBETHA_OK);
    let mut hold = 0u64;
    c.bench_function("rwlock write acquire+release, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_rwlock_try_write(handle, &mut hold) };
            assert_eq!(rc, SUBETHA_OK);
            assert_eq!(subetha_rwlock_unlock(handle, black_box(hold)), SUBETHA_OK);
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_rwlock_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the lock's file");
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

fn semaphore_direct(c: &mut Criterion) {
    let path = bench_file("sem-direct");
    let sem = SharedSemaphore::create(&path, 1, 1).expect("a file-backed semaphore");
    c.bench_function("semaphore acquire+release, direct Rust", |b| {
        b.iter(|| {
            let permit = sem.try_acquire().expect("the only permit");
            black_box(&permit);
        })
    });
    drop(sem);
    for suffix in ["count", "wakeup", "waiters"] {
        let mut file = path.clone();
        let stem = path.file_name().expect("a file name").to_string_lossy().to_string();
        file.set_file_name(format!("{stem}.{suffix}.bin"));
        std::fs::remove_file(&file).expect("the bench's semaphore file is removed");
    }
}

fn semaphore_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = bench_file("sem-abi");
    let c_path = c_path(&path);
    let mut handle: subetha_handle = 0;
    let rc = unsafe { subetha_semaphore_create(c_path.as_ptr(), 1, 1, SUBETHA_MODE_STRICT, &mut handle) };
    assert_eq!(rc, SUBETHA_OK);
    let mut permit = 0u64;
    c.bench_function("semaphore acquire+release, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_semaphore_try_acquire(handle, &mut permit) };
            assert_eq!(rc, SUBETHA_OK);
            assert_eq!(subetha_semaphore_release(handle, black_box(permit)), SUBETHA_OK);
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_semaphore_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 3, "the semaphore's three files");
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// A tick and a merge: what a participant does per event it records and
/// per message it receives. The slot is a plain index, so neither issues
/// nor closes anything and what is left is the boundary itself.
fn fence_clock_direct(c: &mut Criterion) {
    let path = bench_file("fence-direct");
    let clock = SharedFenceClock::create(&path, 4).expect("a file-backed fence clock");
    let slot = clock.register(std::process::id()).expect("a free slot");
    c.bench_function("fence clock tick+merge, direct Rust", |b| {
        b.iter(|| {
            let ours = clock.tick(slot);
            black_box(clock.merge(slot, ours));
        })
    });
    drop(clock);
    std::fs::remove_file(&path).expect("the bench's fence clock file is removed");
}

fn fence_clock_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = bench_file("fence-abi");
    let c_path = c_path(&path);
    let mut handle: subetha_handle = 0;
    let rc = unsafe { subetha_fence_clock_create(c_path.as_ptr(), 4, SUBETHA_MODE_STRICT, &mut handle) };
    assert_eq!(rc, SUBETHA_OK);
    let mut slot = 0u32;
    assert_eq!(unsafe { subetha_fence_clock_register(handle, std::process::id(), &mut slot) }, SUBETHA_OK);
    let mut ours = subetha_hlc::default();
    let mut merged = subetha_hlc::default();
    c.bench_function("fence clock tick+merge, through the C ABI", |b| {
        b.iter(|| {
            assert_eq!(unsafe { subetha_fence_clock_tick(handle, slot, &mut ours) }, SUBETHA_OK);
            assert_eq!(
                unsafe { subetha_fence_clock_merge(handle, slot, black_box(ours), &mut merged) },
                SUBETHA_OK,
            );
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_fence_clock_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the fence clock's file");
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// The same park and release straight onto the primitive, so the pair
/// with the row below is what the boundary costs rather than what a park
/// costs.
fn waker_direct(c: &mut Criterion) {
    let path = bench_file("waker-direct");
    let waker = CrossProcessWaker::create(&path, 8).expect("a file-backed waker");
    c.bench_function("waker park+release, direct Rust", |b| {
        b.iter(|| {
            let token = waker.try_park(black_box(1)).expect("a free slot");
            waker.release(token);
        })
    });
    drop(waker);
    std::fs::remove_file(&path).expect("the bench's waker file is removed");
}

/// A park taken and given back with nothing to wait for, so what is
/// measured is the slot's own cost and the boundary rather than a sleep.
/// The kernel's wait is what a park avoids paying when work is ready.
fn waker_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = bench_file("waker-abi");
    let c_waker = c_path(&path);
    let mut handle: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_waker_create(c_waker.as_ptr(), 8, SUBETHA_MODE_STRICT, &mut handle) },
        SUBETHA_OK,
    );
    let mut token = 0u64;
    c.bench_function("waker park+release, through the C ABI", |b| {
        b.iter(|| {
            assert_eq!(unsafe { subetha_waker_park(handle, black_box(1), &mut token) }, SUBETHA_OK);
            assert_eq!(subetha_waker_release(handle, black_box(token)), SUBETHA_OK);
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_waker_unlink(c_waker.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the waker's file");
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// The same published read straight onto the primitive, so the pair with
/// the row below is what the boundary costs rather than what a read
/// costs.
fn lazy_value_direct(c: &mut Criterion) {
    let path = bench_file("lazy-direct");
    let cell = SharedOnceCellDyn::create(&path, 8).expect("a file-backed lazy value");
    let mine = std::process::id();
    assert!(cell.claim(mine), "this process is the only claimant");
    assert!(cell.publish(mine, &[1u8, 2, 3, 4, 5, 6, 7, 8]), "the claim is the one standing");

    let mut out = [0u8; 8];
    c.bench_function("lazy value try_get, direct Rust", |b| {
        b.iter(|| {
            assert!(cell.try_get(black_box(&mut out)), "the value is published");
        })
    });
    drop(cell);
    std::fs::remove_file(&path).expect("the bench's lazy value file is removed");
}

/// Reading a value that is already published, which is what every caller
/// after the first does. The claim and the publish happen once in a
/// program's life and are not what a row should measure.
fn lazy_value_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = bench_file("lazy-abi");
    let c_lazy = c_path(&path);
    let mut handle: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_lazy_create(c_lazy.as_ptr(), 8, SUBETHA_MODE_STRICT, &mut handle) },
        SUBETHA_OK,
    );
    let mine = std::process::id();
    let mut won = false;
    assert_eq!(unsafe { subetha_lazy_claim(handle, mine, &mut won) }, SUBETHA_OK);
    assert!(won, "this process is the only claimant");
    let value = [1u8, 2, 3, 4, 5, 6, 7, 8];
    assert_eq!(
        unsafe { subetha_lazy_publish(handle, mine, value.as_ptr(), value.len()) },
        SUBETHA_OK,
    );

    let mut out = [0u8; 8];
    let mut len = 0usize;
    c.bench_function("lazy value try_get, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe {
                subetha_lazy_try_get(black_box(handle), out.as_mut_ptr(), out.len(), &mut len)
            };
            assert_eq!(rc, SUBETHA_OK);
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_lazy_unlink(c_lazy.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the lazy value's file");
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// One arrival and release with a single live peer, which is this
/// process: the round completes on its own arrival, so what is measured
/// is the barrier's own work and the boundary rather than any waiting.
fn epoch_barrier_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = bench_file("barrier-abi");
    let beats_path = bench_file("barrier-abi-beats");
    let c_barrier = c_path(&path);
    let c_beats = c_path(&beats_path);

    let mut beats: subetha_handle = 0;
    let mut handle: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_heartbeat_create(c_beats.as_ptr(), 4, SUBETHA_MODE_STRICT, &mut beats) },
        SUBETHA_OK,
    );
    let mut slot = 0u32;
    assert_eq!(unsafe { subetha_heartbeat_register(beats, std::process::id(), &mut slot) }, SUBETHA_OK);
    assert_eq!(
        unsafe { subetha_epoch_barrier_create(c_barrier.as_ptr(), beats, 0, SUBETHA_MODE_STRICT, &mut handle) },
        SUBETHA_OK,
    );

    let mut epoch = 0u32;
    c.bench_function("epoch barrier arrive+release, through the C ABI", |b| {
        b.iter(|| {
            assert_eq!(subetha_epoch_barrier_wait(handle, black_box(epoch)), SUBETHA_OK);
            epoch = epoch.wrapping_add(1);
        })
    });

    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(beats), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_epoch_barrier_unlink(c_barrier.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the barrier's state file");
    assert_eq!(unsafe { subetha_heartbeat_unlink(c_beats.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

fn epochs_direct(c: &mut Criterion) {
    let path = bench_file("epochs-direct");
    let epochs = SharedEpochs::create(&path, 8).expect("a file-backed epoch table");
    c.bench_function("epoch pin+release, direct Rust", |b| {
        b.iter(|| {
            let pin = epochs.pin().expect("a free pin slot");
            black_box(pin.epoch());
        })
    });
    drop(epochs);
    std::fs::remove_file(&path).expect("the bench's epoch file is removed");
}

fn epochs_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = bench_file("epochs-abi");
    let c_path = c_path(&path);
    let mut handle: subetha_handle = 0;
    let rc = unsafe { subetha_epochs_create(c_path.as_ptr(), 8, SUBETHA_MODE_STRICT, &mut handle) };
    assert_eq!(rc, SUBETHA_OK);
    let mut pin = 0u64;
    c.bench_function("epoch pin+release, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_epochs_pin(handle, &mut pin) };
            assert_eq!(rc, SUBETHA_OK);
            assert_eq!(subetha_pin_release(handle, black_box(pin)), SUBETHA_OK);
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_epochs_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the epoch table's file");
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// The uncontended rows above bound one half of a close and say nothing
/// about the other, which is why this arm exists.
///
/// A close bumps a global epoch and runs a process-wide barrier, paid
/// once however many threads exist; then it walks the borrow guard's
/// registry, which holds one word per thread that has ever been inside a
/// call and never shrinks. A single-threaded row walks one word, so it
/// cannot show a registry cost even if that cost is large.
///
/// Here seven threads run calls on a different object while the measured
/// thread acquires and releases, so nothing is ever waited for - what the
/// row adds is the barrier's price with threads running, plus the walk.
/// It does not separate those two on its own: `rwlock_after_contention`
/// below is what does that, by running this same call again once these
/// threads have left and their words remain.
fn rwlock_contended_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = bench_file("rwlock-contended");
    let c_lock = c_path(&path);
    let mut handle: subetha_handle = 0;
    let rc = unsafe { subetha_rwlock_create(c_lock.as_ptr(), SUBETHA_MODE_STRICT, &mut handle) };
    assert_eq!(rc, SUBETHA_OK);

    // A second object for the other threads to be inside, so they hold
    // registry words without ever contending for the lock being measured:
    // what this row adds is the walk, not lock contention.
    let other_path = bench_file("rwlock-contended-other");
    let c_other = c_path(&other_path);
    let mut other: subetha_handle = 0;
    let rc = unsafe { subetha_atomic_u64_create(c_other.as_ptr(), 0, SUBETHA_MODE_STRICT, &mut other) };
    assert_eq!(rc, SUBETHA_OK);

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut callers = Vec::new();
    for _ in 0..7 {
        let stop = std::sync::Arc::clone(&stop);
        callers.push(std::thread::spawn(move || {
            let mut seen = 0u64;
            while !stop.load(Ordering::Acquire) {
                let rc = unsafe { subetha_atomic_u64_load(other, &mut seen) };
                assert_eq!(rc, SUBETHA_OK);
            }
            black_box(seen);
        }));
    }

    let mut hold = 0u64;
    eprintln!("registry at rwlock contended: {}", subetha_borrow_registry_len());
    c.bench_function("rwlock write acquire+release, through the C ABI, seven threads inside calls", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_rwlock_try_write(handle, &mut hold) };
            assert_eq!(rc, SUBETHA_OK);
            assert_eq!(subetha_rwlock_unlock(handle, black_box(hold)), SUBETHA_OK);
        })
    });

    stop.store(true, Ordering::Release);
    for caller in callers {
        caller.join().expect("a caller thread finishes");
    }
    assert_eq!(subetha_handle_destroy(other), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_rwlock_unlink(c_lock.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the lock's file");
    assert_eq!(unsafe { subetha_atomic_unlink(c_other.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the second object's file");
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// Put at least `want` words in the borrow guard's registry, so a row
/// that cares about the walk length sets its own rather than inheriting
/// whatever ran before it.
///
/// A word is taken by a thread's first call through a handle and kept
/// when that thread ends, so spawning and joining is enough and the
/// threads need not stay alive. `handle` is any live object; the call
/// made on it does not matter, only that it borrows.
///
/// This exists because a row's cost otherwise depends on which other
/// rows ran, and criterion's filter decides that. The same call measures
/// about 322 ns with two words and about 563 ns with eight, so a filter
/// narrow enough to exclude the thread-spawning row moves an unrelated
/// row's number by three quarters. A row that warms its own registry
/// means the same thing whatever it is run alongside.
fn warm_registry(handle: subetha_handle, want: usize) {
    // The registry stands at the high-water mark of threads inside a call
    // at one moment - a thread's word goes back when it leaves, for the
    // next thread to take. So the threads have to be inside together:
    // spawning one and joining it hands its word straight back and moves
    // the mark not at all, which is a loop no number of rounds satisfies.
    let inside = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let release = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut warming = Vec::new();
    for _ in 0..want {
        let inside = std::sync::Arc::clone(&inside);
        let release = std::sync::Arc::clone(&release);
        warming.push(std::thread::spawn(move || {
            let mut seen = 0u64;
            let rc = unsafe { subetha_atomic_u64_load(handle, &mut seen) };
            assert_eq!(rc, SUBETHA_OK, "the warming call borrows its handle");
            inside.fetch_add(1, Ordering::AcqRel);
            // Keep calling until every thread has been inside once, so
            // their words are held at the same time.
            while !release.load(Ordering::Acquire) {
                let rc = unsafe { subetha_atomic_u64_load(handle, &mut seen) };
                assert_eq!(rc, SUBETHA_OK, "the warming call borrows its handle");
            }
            black_box(seen);
        }));
    }
    while inside.load(Ordering::Acquire) < want {
        std::hint::spin_loop();
    }
    release.store(true, Ordering::Release);
    for thread in warming {
        thread.join().expect("a warming thread finishes");
    }
    assert!(
        (subetha_borrow_registry_len() as usize) >= want,
        "the registry stands at {} after warming for {want}",
        subetha_borrow_registry_len(),
    );
}

/// The same call as `rwlock_through_the_abi`, run after the contended
/// arm has left its threads' words in the registry. One primitive, two
/// registry sizes, nothing else different.
///
/// This row exists because the obvious reading of the earlier numbers was
/// not sound. The semaphore and epoch rows came out near 590 ns against
/// the lock's 331, and the tempting conclusion was that the extra 260 ns
/// was the registry walk, since those rows happen to run after the
/// contended arm. But they also measure different primitives, whose
/// direct arms differ by nearly a factor of two - so that comparison has
/// two differences in it and one number, and cannot attribute the gap to
/// either. Holding the primitive fixed and varying only the registry is
/// what attributes it.
///
/// The threads are joined before this runs, so nothing is contending:
/// what is left of them is their words, which the registry keeps for the
/// next thread. That is the point. A registry cost is a cost paid forever
/// after a burst of threads, not only while they are running.
fn rwlock_after_contention_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = bench_file("rwlock-after-contention");
    let c_lock = c_path(&path);
    let mut handle: subetha_handle = 0;
    let rc = unsafe { subetha_rwlock_create(c_lock.as_ptr(), SUBETHA_MODE_STRICT, &mut handle) };
    assert_eq!(rc, SUBETHA_OK);

    let other_path = bench_file("rwlock-after-contention-warm");
    let c_other = c_path(&other_path);
    let mut other: subetha_handle = 0;
    let rc = unsafe { subetha_atomic_u64_create(c_other.as_ptr(), 0, SUBETHA_MODE_STRICT, &mut other) };
    assert_eq!(rc, SUBETHA_OK);
    warm_registry(other, 8);

    let mut hold = 0u64;
    eprintln!("registry at rwlock after contention: {}", subetha_borrow_registry_len());
    c.bench_function("rwlock write acquire+release, through the C ABI, after the contended arm", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_rwlock_try_write(handle, &mut hold) };
            assert_eq!(rc, SUBETHA_OK);
            assert_eq!(subetha_rwlock_unlock(handle, black_box(hold)), SUBETHA_OK);
        })
    });

    assert_eq!(subetha_handle_destroy(other), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_rwlock_unlink(c_lock.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the lock's file");
    assert_eq!(unsafe { subetha_atomic_unlink(c_other.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the warming object's file");
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// The three tier-2 families that shipped without a row. These are plain
/// calls with no handle issued per operation, so they measure the same
/// boundary the rest of that tier does.
fn btree_direct(c: &mut Criterion) {
    let path = bench_file("btree-direct");
    let map = RawBTreeMap::create(&path, 512, 4, 8, 0x494e4445583332).expect("a file-backed map");
    let mut out = [0u8; 8];
    c.bench_function("btree insert+get, direct Rust", |b| {
        b.iter(|| {
            map.insert(black_box(&7u32.to_be_bytes()), black_box(&[1u8; 8]), None).expect("room in the map");
            map.get(black_box(&7u32.to_be_bytes()), &mut out).expect("the entry just written");
            black_box(&out);
        })
    });
    drop(map);
    std::fs::remove_file(&path).expect("the bench's map file is removed");
}

fn btree_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = bench_file("btree-abi");
    let c_path = c_path(&path);
    let mut handle: subetha_handle = 0;
    let rc = unsafe {
        subetha_btree_create(c_path.as_ptr(), 512, 4, 8, 0x494e4445583332, SUBETHA_MODE_STRICT, &mut handle)
    };
    assert_eq!(rc, SUBETHA_OK);
    let key = 7u32.to_be_bytes();
    let value = [1u8; 8];
    let mut out = [0u8; 8];
    let mut len = 0usize;
    let mut previous = [0u8; 8];
    let mut previous_len = 0usize;
    let mut replaced = false;
    c.bench_function("btree insert+get, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe {
                subetha_btree_insert(
                    handle,
                    black_box(key.as_ptr()),
                    key.len(),
                    black_box(value.as_ptr()),
                    value.len(),
                    previous.as_mut_ptr(),
                    previous.len(),
                    &mut previous_len,
                    &mut replaced,
                )
            };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe {
                subetha_btree_get(handle, black_box(key.as_ptr()), key.len(), out.as_mut_ptr(), out.len(), &mut len)
            };
            assert_eq!(rc, SUBETHA_OK);
            black_box(len);
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_btree_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the map's file");
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

fn cell_direct(c: &mut Criterion) {
    let path = bench_file("cell-direct");
    let cell = RawCell::create(&path, 8).expect("a file-backed cell");
    let mut out = [0u8; 8];
    c.bench_function("cell set+get, direct Rust", |b| {
        b.iter(|| {
            cell.set(black_box(&[3u8; 8])).expect("a value of the declared size");
            cell.get(&mut out).expect("a buffer of the declared size");
            black_box(&out);
        })
    });
    drop(cell);
    std::fs::remove_file(&path).expect("the bench's cell file is removed");
}

fn cell_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = bench_file("cell-abi");
    let c_path = c_path(&path);
    let mut handle: subetha_handle = 0;
    let rc = unsafe { subetha_cell_create(c_path.as_ptr(), 8, SUBETHA_MODE_STRICT, &mut handle) };
    assert_eq!(rc, SUBETHA_OK);
    let value = [3u8; 8];
    let mut out = [0u8; 8];
    let mut len = 0usize;
    c.bench_function("cell set+get, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_cell_set(handle, black_box(value.as_ptr()), value.len()) };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_cell_get(handle, out.as_mut_ptr(), out.len(), &mut len) };
            assert_eq!(rc, SUBETHA_OK);
            black_box(len);
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_cell_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the cell's file");
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

fn frame_region_direct(c: &mut Criterion) {
    let path = bench_file("frames-direct");
    let region = FrameRegion::create(&path, 64, CAPACITY).expect("a file-backed frame region");
    let mut out = [0u8; 16];
    c.bench_function("frame region alloc+write+read+free, direct Rust", |b| {
        b.iter(|| {
            let block = region.alloc().expect("a block in an empty region");
            region.write_block(black_box(block), black_box(&PAYLOAD));
            region.read_block(black_box(block), PAYLOAD.len(), &mut out);
            region.free(block);
            black_box(&out);
        })
    });
    drop(region);
    std::fs::remove_file(&path).expect("the bench's frame region file is removed");
}

fn frame_region_through_the_abi(c: &mut Criterion) {
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = bench_file("frames-abi");
    let c_path = c_path(&path);
    let mut handle: subetha_handle = 0;
    let rc = unsafe { subetha_frame_region_create(c_path.as_ptr(), 64, CAPACITY as u32, SUBETHA_MODE_STRICT, &mut handle) };
    assert_eq!(rc, SUBETHA_OK);
    let mut out = [0u8; 16];
    let mut len = 0usize;
    let mut block = 0u32;
    c.bench_function("frame region alloc+write+read+free, through the C ABI", |b| {
        b.iter(|| {
            let rc = unsafe { subetha_frame_region_alloc(handle, &mut block) };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe { subetha_frame_region_write(handle, block, black_box(PAYLOAD.as_ptr()), PAYLOAD.len()) };
            assert_eq!(rc, SUBETHA_OK);
            let rc = unsafe {
                subetha_frame_region_read(handle, block, PAYLOAD.len(), out.as_mut_ptr(), out.len(), &mut len)
            };
            assert_eq!(rc, SUBETHA_OK);
            assert_eq!(subetha_frame_region_free(handle, black_box(block)), SUBETHA_OK);
            black_box(len);
        })
    });
    assert_eq!(subetha_handle_destroy(handle), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_frame_region_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the frame region's file");
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

criterion_group!(
    benches,
    rwlock_direct,
    rwlock_through_the_abi,
    rwlock_contended_through_the_abi,
    rwlock_after_contention_through_the_abi,
    semaphore_direct,
    semaphore_through_the_abi,
    boundary_layers,
    epochs_direct,
    epochs_through_the_abi,
    fence_clock_direct,
    fence_clock_through_the_abi,
    epoch_barrier_through_the_abi,
    waker_direct,
    waker_through_the_abi,
    lazy_value_direct,
    lazy_value_through_the_abi,
    btree_direct,
    btree_through_the_abi,
    cell_direct,
    cell_through_the_abi,
    frame_region_direct,
    frame_region_through_the_abi,
    ring_batch_through_the_abi,
    list_direct,
    list_through_the_abi,
    atomic_direct,
    atomic_through_the_abi,
    region_direct,
    region_through_the_abi,
    vec_direct,
    vec_through_the_abi,
    slab_direct,
    slab_through_the_abi,
    arena_direct,
    arena_through_the_abi,
    hashmap_direct,
    hashmap_through_the_abi,
    stack_direct,
    stack_through_the_abi,
    deque_direct,
    deque_through_the_abi,
    ordered_direct,
    ordered_through_the_abi,
    ring_direct,
    ring_through_the_abi,
    spsc_direct,
    spsc_through_the_abi,
    mpsc_direct,
    mpsc_through_the_abi,
    mpmc_direct,
    mpmc_through_the_abi,
    vyukov_direct,
    vyukov_through_the_abi,
    lamport_direct,
    lamport_through_the_abi,
    broadcast_direct,
    broadcast_through_the_abi,
    pubsub_direct,
    pubsub_through_the_abi,
    capacity_direct,
    capacity_through_the_abi,
    stamped_ring_direct,
    stamped_ring_through_the_abi
);
criterion_main!(benches);
