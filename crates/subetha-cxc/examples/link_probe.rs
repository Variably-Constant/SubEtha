//! Live link-quality probe. Prints the platform link sensor's Wi-Fi MAC stats -
//! signal quality, normalized PHY rate (`mcs_norm`), MAC retry rate, and link
//! class - plus the derived `link_stress`, on a cadence. On a real Wi-Fi host
//! the radio shows it is struggling (the PHY rate adapting down, retries
//! climbing) before end-to-end loss appears, which is the feed-forward signal
//! the adaptive controller folds into `link_stress`.
//!
//!     link_probe [samples] [interval_ms]
//!
//! Run it alone to read the idle link, or alongside a saturating transfer
//! (e.g. a bridge_lan sens client) to watch the radio react under load.
use std::thread;
use std::time::Duration;
use subetha_cxc::link_sensor::platform_sensor;

fn main() {
    let mut s = platform_sensor(None);
    let samples: usize = std::env::args().nth(1).and_then(|a| a.parse().ok()).unwrap_or(10);
    let interval_ms: u64 = std::env::args().nth(2).and_then(|a| a.parse().ok()).unwrap_or(500);
    println!("link_probe backend={} samples={samples}", s.backend());
    for i in 0..samples {
        let snap = s.sample();
        println!(
            "[{i:02}] class={:?} signal={} mcs_norm={} retry_rate={} link_stress={:.3}",
            snap.class,
            opt_u8(snap.signal_quality),
            opt_f32(snap.mcs_norm),
            opt_f32(snap.retry_rate),
            snap.link_stress(),
        );
        thread::sleep(Duration::from_millis(interval_ms));
    }
}

fn opt_u8(v: Option<u8>) -> String {
    v.map(|x| x.to_string()).unwrap_or_else(|| "-".into())
}

fn opt_f32(v: Option<f32>) -> String {
    v.map(|x| format!("{x:.3}")).unwrap_or_else(|| "-".into())
}
