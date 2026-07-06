//! Bench: SharedLinkedList vs Mutex<VecDeque<T>> (FIFO baseline)
//! and Mutex<LinkedList<T>> (std DLL baseline).
//!
//! Architectural claim: handle-based O(1) removal is the unique
//! value. End-only push/pop ties with VecDeque; iteration ties
//! with std LinkedList; but `remove(handle)` is O(1) for us vs
//! O(N) scan for std::collections::LinkedList::remove (which
//! requires walking from head/tail to find the target).
//!
//! Workloads:
//! - push_back hot
//! - pop_front hot
//! - iter_forward 100-element list
//! - remove from middle by handle (vs scan-and-remove)

use std::collections::{LinkedList, VecDeque};
use std::hint::black_box;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};

use subetha_cxc::{NodeHandle, SharedLinkedList};

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-bench-linkedlist-{name}-{pid}.bin"));
    p
}

// =========================================================
// push_back hot
// =========================================================

fn push_back_hot(c: &mut Criterion) {
    let p = tmp("push-back");
    let l: SharedLinkedList<u32> = SharedLinkedList::create(&p, 1 << 20).unwrap();
    c.bench_function("linkedlist.push_back/mmf", |b| {
        b.iter(|| {
            match l.push_back(black_box(42)) {
                Ok(h) => black_box(h),
                Err(_) => { l.pop_front(); l.push_back(42).unwrap() }
            }
        });
    });
    drop(l);
    std::fs::remove_file(&p).ok();

    let v: Mutex<VecDeque<u32>> = Mutex::new(VecDeque::with_capacity(1 << 20));
    c.bench_function("linkedlist.push_back/mutex_vecdeque", |b| {
        b.iter(|| v.lock().unwrap().push_back(black_box(42)));
    });

    let s: Mutex<LinkedList<u32>> = Mutex::new(LinkedList::new());
    c.bench_function("linkedlist.push_back/mutex_std_linkedlist", |b| {
        b.iter(|| s.lock().unwrap().push_back(black_box(42)));
    });
}

// =========================================================
// pop_front hot (pre-filled)
// =========================================================

fn pop_front_hot(c: &mut Criterion) {
    c.bench_function("linkedlist.pop_front/mmf", |b| {
        let p = tmp("pop-front");
        let l: SharedLinkedList<u32> = SharedLinkedList::create(&p, 1 << 16).unwrap();
        b.iter(|| {
            if l.is_empty() {
                for i in 0..1000u32 { l.push_back(i).unwrap(); }
            }
            black_box(l.pop_front())
        });
        drop(l);
        std::fs::remove_file(&p).ok();
    });

    c.bench_function("linkedlist.pop_front/mutex_vecdeque", |b| {
        let v: Mutex<VecDeque<u32>> = Mutex::new(VecDeque::with_capacity(1 << 16));
        b.iter(|| {
            if v.lock().unwrap().is_empty() {
                let mut g = v.lock().unwrap();
                for i in 0..1000u32 { g.push_back(i); }
            }
            black_box(v.lock().unwrap().pop_front())
        });
    });
}

// =========================================================
// iter_forward over 100 elements
// =========================================================

fn iter_100(c: &mut Criterion) {
    let p = tmp("iter");
    let l: SharedLinkedList<u32> = SharedLinkedList::create(&p, 128).unwrap();
    for i in 0..100u32 { l.push_back(i).unwrap(); }
    c.bench_function("linkedlist.iter_100/mmf", |b| {
        b.iter(|| black_box(l.iter_forward()));
    });
    drop(l);
    std::fs::remove_file(&p).ok();

    let v: Mutex<VecDeque<u32>> = Mutex::new((0..100u32).collect());
    c.bench_function("linkedlist.iter_100/mutex_vecdeque", |b| {
        b.iter(|| {
            let g = v.lock().unwrap();
            black_box(g.iter().copied().collect::<Vec<u32>>())
        });
    });

    let s: Mutex<LinkedList<u32>> = Mutex::new((0..100u32).collect());
    c.bench_function("linkedlist.iter_100/mutex_std_linkedlist", |b| {
        b.iter(|| {
            let g = s.lock().unwrap();
            black_box(g.iter().copied().collect::<Vec<u32>>())
        });
    });
}

// =========================================================
// THE HEADLINE: remove from middle of 100-element list.
//
// SharedLinkedList: pass the handle directly to remove() = O(1).
// Mutex<LinkedList>: must scan to find the target = O(N/2 avg).
// Mutex<VecDeque>: pop_front + push_back doesn't apply (we need
//   to remove a SPECIFIC item, not just one end), so this would
//   be O(N) shift. Skipped because the comparison is unfair.
// =========================================================

fn remove_middle(c: &mut Criterion) {
    const N: usize = 100;

    let p = tmp("remove-middle");
    c.bench_function("linkedlist.remove_middle_100/mmf_handle", |b| {
        let l: SharedLinkedList<u32> = SharedLinkedList::create(&p, 256).unwrap();
        let mut handles: Vec<NodeHandle<u32>> = (0..N as u32)
            .map(|i| l.push_back(i).unwrap()).collect();
        let mut next_to_remove = N / 2;
        b.iter(|| {
            if l.is_empty() {
                // Refill.
                handles.clear();
                for i in 0..N as u32 { handles.push(l.push_back(i).unwrap()); }
                next_to_remove = N / 2;
            }
            // Remove the middle-ish handle.
            let idx = next_to_remove % handles.len();
            let h = handles.remove(idx);
            black_box(l.remove(h));
            next_to_remove = next_to_remove.saturating_sub(1);
        });
        drop(l);
    });
    std::fs::remove_file(&p).ok();

    c.bench_function("linkedlist.remove_middle_100/std_linkedlist_scan", |b| {
        // Std LinkedList has no O(1) remove-by-iterator (cursor is
        // unstable); use the legacy "split_off + collect" pattern
        // which is O(N).
        let mut l: LinkedList<u32> = (0..N as u32).collect();
        let mut next_idx = N / 2;
        b.iter(|| {
            if l.is_empty() {
                l = (0..N as u32).collect();
                next_idx = N / 2;
            }
            // O(N): split at the target index, take the head, pop
            // the target, splice the rest back. This is what callers
            // do when they need middle removal on std::LinkedList.
            let idx = next_idx % l.len();
            let mut after = l.split_off(idx);
            let removed = after.pop_front();
            l.append(&mut after);
            black_box(removed);
            next_idx = next_idx.saturating_sub(1);
        });
    });
}

// =========================================================
// Storage witness
// =========================================================

fn storage(c: &mut Criterion) {
    let mmf_node_size = std::mem::size_of::<subetha_cxc::LinkedListNode<u32>>();
    eprintln!("[storage] SharedLinkedList Node<u32> = {mmf_node_size} bytes (value + next + prev)");
    c.bench_function("linkedlist.storage_witness", |b| {
        b.iter(|| black_box(mmf_node_size));
    });
}

criterion_group!(benches,
    push_back_hot,
    pop_front_hot,
    iter_100,
    remove_middle,
    storage,
);
criterion_main!(benches);
