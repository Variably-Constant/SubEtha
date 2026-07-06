//! E2E + A/B bench: the RLC transport over its auto-detected datagram backend.
//!
//! Runs a real `SensOMaticRlcSender` -> `SensOMaticRlcReceiver` transfer of `N` items and
//! verifies every item arrives in order, reporting which datagram backend the
//! transport resolved to. With `SUBETHA_DGRAM=udp` it forces the plain socket;
//! with `SUBETHA_DGRAM=iouring` it forces the io_uring backend; unset =
//! auto-detect (io_uring where available). Run both to compare throughput on
//! the real transport workload (not a loopback microbench).
//!
//! Run:
//!     SUBETHA_DGRAM=udp     cargo run --release --example rlc_transport_e2e -p subetha-cxc
//!     SUBETHA_DGRAM=iouring cargo run --release --example rlc_transport_e2e -p subetha-cxc

fn main() {
    use std::sync::mpsc;
    use std::time::{Duration, Instant};
    use subetha_cxc::sens_rlc::{SensOMaticRlcReceiver, SensOMaticRlcSender};

    const N: u64 = 20_000;
    const ITEM_LEN: usize = 1024;
    // symbol holds a 2-byte length prefix + the item; the datagram is
    // symbol_len + DATA_HDR, which must fit the backend's frame cap.
    const SYMBOL_LEN: usize = 1100;

    println!("=== RLC transport E2E over auto-detected datagram backend ===");

    let (addr_tx, addr_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let rx = std::thread::spawn(move || {
        let mut recv = SensOMaticRlcReceiver::bind("127.0.0.1:0", SYMBOL_LEN).expect("recv bind");
        addr_tx
            .send((recv.local_addr().unwrap(), format!("{:?}", recv.dgram_backend())))
            .unwrap();
        let mut got: Vec<u64> = Vec::new();
        let start = Instant::now();
        while (got.len() as u64) < N {
            if start.elapsed() > Duration::from_secs(120) {
                break;
            }
            for item in recv.poll().unwrap() {
                got.push(u64::from_le_bytes(item[..8].try_into().unwrap()));
            }
        }
        for _ in 0..50 {
            recv.poll().ok();
            std::thread::sleep(Duration::from_millis(2));
        }
        done_tx.send(()).ok();
        (got, recv.rlc_recovered())
    });

    let (recv_addr, rx_backend) = addr_rx.recv().unwrap();
    let t0 = Instant::now();
    let tx = std::thread::spawn(move || {
        let mut send =
            SensOMaticRlcSender::bind("127.0.0.1:0", recv_addr, 16, 2, 15, SYMBOL_LEN).expect("send bind");
        let backend = format!("{:?}", send.dgram_backend());
        for i in 0..N {
            let mut item = vec![0u8; ITEM_LEN];
            item[..8].copy_from_slice(&i.to_le_bytes());
            send.send_item(&item).expect("send_item");
        }
        send.drain_until_acked(N as u32, Duration::from_secs(120)).expect("drain");
        done_rx.recv_timeout(Duration::from_secs(120)).ok();
        backend
    });

    let (got, recovered) = rx.join().unwrap();
    let tx_backend = tx.join().unwrap();
    let elapsed = t0.elapsed();

    let expected: Vec<u64> = (0..N).collect();
    assert_eq!(got.len() as u64, N, "received {} of {N} items", got.len());
    assert_eq!(got, expected, "RLC transport must deliver every item in order");

    let mb = N as f64 * ITEM_LEN as f64 / 1e6;
    println!();
    println!("=== Result ===");
    println!("  backend:    sender={tx_backend} receiver={rx_backend}");
    println!("  items:      {N} x {ITEM_LEN}B delivered in order");
    println!("  recovered:  {recovered} symbols via RLC repair");
    println!("  elapsed:    {elapsed:?}");
    println!("  throughput: {:.1} MB/s", mb / elapsed.as_secs_f64());
    println!("  integrity:  PASS");
}
