//! Micro-bench: is the GF(256) RS parity encode the throughput ceiling?
use std::time::Instant;
use subetha_cxc::fec::RsCode;

fn main() {
    let (k, r, len) = (8usize, 2usize, 1408usize);
    let code = RsCode::new(k, r).expect("rs");
    let data: Vec<Vec<u8>> = (0..k).map(|i| vec![(i * 37 + 1) as u8; len]).collect();
    let drefs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut parity: Vec<Vec<u8>> = vec![vec![0u8; len]; r];
    let iters = 200_000u64;
    let t = Instant::now();
    for _ in 0..iters {
        let mut prefs: Vec<&mut [u8]> = parity.iter_mut().map(|v| v.as_mut_slice()).collect();
        code.encode(&drefs, &mut prefs).expect("encode");
    }
    let secs = t.elapsed().as_secs_f64();
    let data_gbit = iters as f64 * (k * len) as f64 * 8.0 / secs / 1e9;
    println!("scalar RS encode: {data_gbit:.2} Gbit/s of data, {:.0} blocks/s", iters as f64 / secs);
}
