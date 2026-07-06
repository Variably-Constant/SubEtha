//! One-port QUIC + Sens-O-Matic demux: end-to-end proof.
//!
//! Brings up ONE UDP server port that speaks BOTH protocols, then drives a
//! vanilla QUIC transfer AND a unified Sens-O-Matic transfer (with the loss-
//! driven RLC <-> RS auto-switch) against it CONCURRENTLY. The server's
//! `DemuxQuicSocket` routes each inbound datagram by its first wire byte: QUIC
//! (fixed bit 0x40) to quinn, Sens (RS 1/4, RLC 10..=14, CODE_SWITCH 9) to the
//! unified receiver's queues. Both transfers are verified: the QUIC payload must
//! arrive byte-exact; the Sens stream must arrive in order with the right sum,
//! fully acked, and (under injected loss) must have switched codes.
//!
//! Run (VM loopback; do NOT run high-rate UDP loopback on a Windows host):
//!   cargo run --release --features quic-bridge --example one_port_e2e -- \
//!       --sens-items 4000 --quic-kb 512 --loss 30
//!
//! All addresses are 127.0.0.1; the three clients (one QUIC, one Sens) and the
//! one server live in a single process, so this is a self-contained binary whose
//! RESULT line is the observable effect.

use std::error::Error;
use std::time::{Duration, Instant};

use quinn::Endpoint;
use subetha_cxc::quic_bridge::{install_default_crypto_provider, make_self_signed_pair};
use subetha_cxc::sens_quic::one_port_server;
use subetha_cxc::sens_unified::{CodePolicy, UnifiedConfig, UnifiedSensSender};

type BoxErr = Box<dyn Error + Send + Sync>;

fn arg_val(argv: &[String], flag: &str) -> Option<String> {
    argv.iter().position(|a| a == flag).and_then(|i| argv.get(i + 1).cloned())
}

fn main() -> Result<(), BoxErr> {
    install_default_crypto_provider();
    let argv: Vec<String> = std::env::args().collect();
    // Default large enough that the stream outlives the 1s switch-warmup so the
    // loss-driven RLC -> RS crossover can actually fire under injected loss.
    let sens_items: u64 = arg_val(&argv, "--sens-items").and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let quic_kb: usize = arg_val(&argv, "--quic-kb").and_then(|s| s.parse().ok()).unwrap_or(512);
    let loss: u32 = arg_val(&argv, "--loss").and_then(|s| s.parse().ok()).unwrap_or(30);
    // --policy auto|rs|rlc: forces a code (rs/rlc) for isolation, default auto.
    let policy = arg_val(&argv, "--policy").unwrap_or_else(|| "auto".to_string());
    let item_bytes: usize = 64;
    let symbol_len = item_bytes + 8;
    let quic_bytes = quic_kb * 1024;

    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    rt.block_on(async move {
        // One shared server UDP port.
        let server_sock = std::net::UdpSocket::bind("127.0.0.1:0")?;
        let server_addr = server_sock.local_addr()?;
        let (server_cfg, client_cfg) = make_self_signed_pair("subetha")
            .map_err(|e| -> BoxErr { e.to_string().into() })?;

        // Server-side unified config carries the receive-path loss injector that
        // drives the auto-switch; the client never injects (loss is a channel
        // property, mirrored from sens_auto_client).
        let mut srv_unified = UnifiedConfig::new(symbol_len);
        // RS block geometry: k + r must stay <= 32 (the per-block shard bitmap is
        // a u32). k=16 leaves headroom for the receiver to provision r up to 16
        // (50% redundancy) under extreme loss without overflowing the bitmap.
        srv_unified.k = 16;
        srv_unified.r = 8;
        srv_unified.debug_loss = loss;
        srv_unified.seed = 42;
        srv_unified.policy = match policy.as_str() {
            "rs" => CodePolicy::ForceRs,
            "rlc" => CodePolicy::ForceRlc,
            _ => CodePolicy::default_auto(),
        };
        let mut cli_unified = srv_unified;
        cli_unified.debug_loss = 0;

        let (endpoint, mut sens_recv) = one_port_server(server_sock, server_cfg, srv_unified)
            .map_err(|e| -> BoxErr { e.to_string().into() })?;

        // Hold the endpoint in this scope so its driver (which polls the demux
        // socket and feeds the Sens queues) stays alive until BOTH transfers
        // finish; the server task gets a clone. If QUIC retired the only handle,
        // Sens routing would halt the moment the QUIC transfer completed.
        let ep_srv = endpoint.clone();
        // QUIC server: accept one connection + drain its uni stream, bounded so a
        // failed handshake cannot hang the run.
        let quic_srv = tokio::spawn(async move {
            let work = async {
                let inc = ep_srv.accept().await.ok_or("quic: endpoint closed")?;
                let conn = inc.await?;
                let mut recv = conn.accept_uni().await?;
                let mut got: Vec<u8> = Vec::new();
                let mut buf = vec![0u8; 64 * 1024];
                while let Some(n) = recv.read(&mut buf).await? {
                    got.extend_from_slice(&buf[..n]);
                }
                Ok::<Vec<u8>, BoxErr>(got)
            };
            match tokio::time::timeout(Duration::from_secs(120), work).await {
                Ok(r) => r,
                Err(_) => Err::<Vec<u8>, BoxErr>("quic server timed out".into()),
            }
        });

        // Sens receiver: poll on a sync thread (the unified receiver is sync; its
        // queues are fed by the QUIC socket's demux). Verify order + sum exactly
        // as bridge_lan's auto server does, then linger to flush final acks.
        let sens_rx = std::thread::spawn(move || {
            let mut got: u64 = 0;
            let mut expected: u64 = 0;
            let mut order_ok = true;
            let mut sum: u128 = 0;
            let mut last_progress = Instant::now();
            let mut next_mark: u64 = 20_000;
            while got < sens_items {
                let delivered = sens_recv.poll().unwrap_or_default();
                if delivered.is_empty() {
                    if last_progress.elapsed() > Duration::from_secs(45) {
                        eprintln!(
                            "[sens-rx] STALLED at {got}/{sens_items} (no delivery 45s, code={:?})",
                            sens_recv.active_code()
                        );
                        break;
                    }
                    std::thread::sleep(Duration::from_micros(200));
                    continue;
                }
                last_progress = Instant::now();
                for it in delivered {
                    let mut s = [0u8; 8];
                    s.copy_from_slice(&it[..8]);
                    let seq = u64::from_le_bytes(s);
                    if seq != expected {
                        order_ok = false;
                    }
                    expected += 1;
                    sum += seq as u128;
                    got += 1;
                }
                if got >= next_mark {
                    eprintln!("[sens-rx] delivered {got}/{sens_items} code={:?}", sens_recv.active_code());
                    next_mark += 20_000;
                }
            }
            let linger = Instant::now();
            while linger.elapsed() < Duration::from_millis(500) {
                sens_recv.poll().ok();
                std::thread::sleep(Duration::from_micros(500));
            }
            (got, order_ok, sum, sens_recv.switches(), format!("{:?}", sens_recv.active_code()))
        });

        // QUIC client: connect to the shared port + ship a deterministic payload.
        let quic_payload: Vec<u8> = (0..quic_bytes).map(|i| (i % 251) as u8).collect();
        let quic_expect = quic_payload.clone();
        let quic_cli = tokio::spawn(async move {
            let mut ep = Endpoint::client("127.0.0.1:0".parse().unwrap())?;
            ep.set_default_client_config(client_cfg);
            let conn = ep.connect(server_addr, "subetha")?.await?;
            let mut send = conn.open_uni().await?;
            send.write_all(&quic_payload).await?;
            send.finish()?;
            send.stopped().await?;
            Ok::<(), BoxErr>(())
        });

        // Sens client: connect to the SAME port + ship the u64 sequence with the
        // auto-switch live (sync UnifiedSensSender on its own thread).
        let sens_cli = std::thread::spawn(move || {
            let mut send = UnifiedSensSender::connect("0.0.0.0:0", server_addr, cli_unified)?;
            let mut buf = vec![0u8; item_bytes];
            let mut next_mark: u64 = 20_000;
            for seq in 0..sens_items {
                buf[..8].copy_from_slice(&seq.to_le_bytes());
                send.send_item(&buf)?;
                if seq + 1 >= next_mark {
                    eprintln!("[sens-tx] sent {}/{sens_items} code={:?}", seq + 1, send.active_code());
                    next_mark += 20_000;
                }
            }
            eprintln!("[sens-tx] all {sens_items} handed off, draining (code={:?})...", send.active_code());
            let acked = send.finish()?;
            eprintln!("[sens-tx] drain done acked={acked}");
            Ok::<(bool, u64, String), std::io::Error>((
                acked,
                send.switches(),
                format!("{:?}", send.active_code()),
            ))
        });

        // Join everything. Collect outcomes WITHOUT aborting early, so the RESULT
        // line always reports BOTH protocols even if one of them failed.
        let quic_cli_res = quic_cli.await.map_err(|e| -> BoxErr { e.to_string().into() })?;
        let quic_srv_res = quic_srv.await.map_err(|e| -> BoxErr { e.to_string().into() })?;
        let (sens_acked, sw_tx, code_tx) = sens_cli.join().map_err(|_| "sens client panicked")??;
        let (got, order_ok, sum, sw_rx, code_rx) =
            sens_rx.join().map_err(|_| "sens receiver panicked")?;
        // Both sides are done; the endpoint driver can retire.
        drop(endpoint);

        // Verify both protocols.
        let quic_got_len = quic_srv_res.as_ref().map(|g| g.len()).unwrap_or(0);
        let quic_ok = quic_cli_res.is_ok()
            && quic_srv_res.as_ref().map(|g| *g == quic_expect).unwrap_or(false);
        let quic_err = match (&quic_cli_res, &quic_srv_res) {
            (Err(e), _) => format!(" quic_cli_err=\"{e}\""),
            (_, Err(e)) => format!(" quic_srv_err=\"{e}\""),
            _ => String::new(),
        };
        let expected_sum = if sens_items > 0 {
            (sens_items as u128 - 1) * sens_items as u128 / 2
        } else {
            0
        };
        let sum_ok = sum == expected_sum;
        let sens_ok = got == sens_items && order_ok && sum_ok && sens_acked;
        // Under injected loss the stream must have crossed RLC -> RS at least once.
        let switched = sw_tx > 0 && sw_rx > 0;

        println!(
            "RESULT one_port quic_bytes={quic_got_len} quic_ok={quic_ok} sens_items={got}/{sens_items} \
             order_ok={order_ok} sum_ok={sum_ok} fully_acked={sens_acked} \
             switches_tx={sw_tx} switches_rx={sw_rx} final_code_tx={code_tx} \
             final_code_rx={code_rx} loss={loss}%{quic_err}",
        );

        if !quic_ok {
            return Err::<(), BoxErr>("QUIC payload mismatch on shared port".into());
        }
        if !sens_ok {
            return Err::<(), BoxErr>(
                format!("Sens transfer failed: got={got} order_ok={order_ok} sum_ok={sum_ok} acked={sens_acked}").into(),
            );
        }
        if policy == "auto" && loss >= 20 && !switched {
            return Err::<(), BoxErr>(
                format!("expected an RLC->RS switch at {loss}% loss (tx={sw_tx} rx={sw_rx})").into(),
            );
        }
        println!("PASS one-port demux: QUIC + Sens-O-Matic both delivered on one UDP port");
        Ok(())
    })
}
