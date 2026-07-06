//! E2E: the real user-facing action. `AutoIpc::build_adaptive::<u64>()`
//! constructs an `AdaptiveIpc`; a `send_batch` of >= 2 items routes
//! through the KHL batched fast path (3 items per Release-store, gated
//! to the <= 16-byte payload), and `recv` drains it via the shared
//! surplus. Observes integrity end to end.
//!
//! Run: cargo run --release --example adaptive_khl_e2e -p subetha-cxc

use subetha_cxc::AutoIpc;

fn main() {
    let path = std::env::temp_dir()
        .join(format!("subetha-khl-e2e-{}.bin", std::process::id()));
    let ipc = AutoIpc::new(&path)
        .consumers(1)
        .capacity(256)
        .build_adaptive::<u64>()
        .expect("build_adaptive");

    const N: u64 = 100;
    let batch: Vec<u64> = (0..N).collect();
    ipc.send_batch(&batch).expect("send_batch routes the batch to KHL");

    let mut sum = 0u64;
    let mut got = 0u64;
    while got < N {
        match ipc.recv() {
            Ok(v) => {
                sum = sum.wrapping_add(v);
                got += 1;
            }
            Err(_) => std::hint::spin_loop(),
        }
    }
    let expected: u64 = (0..N).sum();
    assert_eq!(sum, expected, "every batched item round-tripped exactly once");
    println!(
        "E2E OK: {N} u64 items round-tripped via \
         AutoIpc::build_adaptive().send_batch() -> KHL -> recv (sum={sum}, expected={expected})"
    );
}
