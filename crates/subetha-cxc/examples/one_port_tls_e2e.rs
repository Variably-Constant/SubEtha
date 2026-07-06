//! One-port QUIC + ENCRYPTED Sens-O-Matic demux: end-to-end proof.
//!
//! Same shape as `one_port_e2e`, but the Sens half runs a TLS 1.3 handshake and
//! AEAD-seals every item, so the WHOLE one-port endpoint - QUIC and Sens - is
//! confidential on one UDP port. The server's `DemuxQuicSocket` routes inbound
//! datagrams by first wire byte: QUIC (fixed bit 0x40) to quinn, Sens (RS 1/4,
//! RLC 10..=14, CODE_SWITCH 9, crypto 15/16) to the unified receiver's queues -
//! and the Sens TLS handshake rides the demux'd handshake queue because the QUIC
//! endpoint owns the socket. Both transfers are verified: the QUIC payload must
//! arrive byte-exact; the Sens stream must arrive in order with the right sum,
//! fully acked, encrypted, and (under loss) must have switched codes.
//!
//! Run (VM loopback; do NOT run high-rate UDP loopback on a Windows host):
//!   cargo run --release --features "quic-bridge tls" --example one_port_tls_e2e -- \
//!       --sens-items 100000 --quic-kb 512 --loss 30

use std::error::Error;
use std::time::{Duration, Instant};

use quinn::Endpoint;
use subetha_cxc::quic_bridge::{install_default_crypto_provider, make_self_signed_pair};
use subetha_cxc::rlc_crypto;
use subetha_cxc::sens_quic::one_port_server_tls;
use subetha_cxc::sens_unified::{CodePolicy, UnifiedConfig, UnifiedSensSender};

type BoxErr = Box<dyn Error + Send + Sync>;

fn arg_val(argv: &[String], flag: &str) -> Option<String> {
    argv.iter().position(|a| a == flag).and_then(|i| argv.get(i + 1).cloned())
}

fn main() -> Result<(), BoxErr> {
    install_default_crypto_provider();
    let argv: Vec<String> = std::env::args().collect();
    let sens_items: u64 = arg_val(&argv, "--sens-items").and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let quic_kb: usize = arg_val(&argv, "--quic-kb").and_then(|s| s.parse().ok()).unwrap_or(512);
    let loss: u32 = arg_val(&argv, "--loss").and_then(|s| s.parse().ok()).unwrap_or(30);
    let policy = arg_val(&argv, "--policy").unwrap_or_else(|| "auto".to_string());
    let item_bytes: usize = 64;
    let symbol_len = item_bytes + 8;
    let quic_bytes = quic_kb * 1024;

    // One in-process Sens cert: the server authenticates with it, the client
    // trusts it (issued for rlc_crypto::SNI). Separate from the QUIC pair.
    let (sens_cert, sens_key) =
        rlc_crypto::self_signed_cert().map_err(|e| -> BoxErr { e.into() })?;
    let sens_server_cfg =
        rlc_crypto::server_config(&sens_cert, &sens_key).map_err(|e| -> BoxErr { e.into() })?;
    let sens_client_cfg =
        rlc_crypto::client_config(&sens_cert).map_err(|e| -> BoxErr { e.into() })?;

    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    rt.block_on(async move {
        let server_sock = std::net::UdpSocket::bind("127.0.0.1:0")?;
        let server_addr = server_sock.local_addr()?;
        let (server_cfg, client_cfg) = make_self_signed_pair("subetha")
            .map_err(|e| -> BoxErr { e.to_string().into() })?;

        let mut srv_unified = UnifiedConfig::new(symbol_len);
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

        // One UDP port speaking QUIC + ENCRYPTED Sens. The Sens handshake driver
        // runs on a thread fed by the demux'd handshake queue.
        let (endpoint, mut sens_recv) =
            one_port_server_tls(server_sock, server_cfg, srv_unified, sens_server_cfg)
                .map_err(|e| -> BoxErr { e.to_string().into() })?;

        let ep_srv = endpoint.clone();
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

        let sens_rx = std::thread::spawn(move || {
            let mut got: u64 = 0;
            let mut expected: u64 = 0;
            let mut order_ok = true;
            let mut sum: u128 = 0;
            let mut last_progress = Instant::now();
            while got < sens_items {
                let delivered = sens_recv.poll().unwrap_or_default();
                if delivered.is_empty() {
                    if last_progress.elapsed() > Duration::from_secs(60) {
                        eprintln!(
                            "[sens-rx] STALLED at {got}/{sens_items} (code={:?})",
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
            }
            let linger = Instant::now();
            while linger.elapsed() < Duration::from_millis(500) {
                sens_recv.poll().ok();
                std::thread::sleep(Duration::from_micros(500));
            }
            (got, order_ok, sum, sens_recv.switches(), format!("{:?}", sens_recv.active_code()))
        });

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

        // Sens client: connect_tls to the SAME port (the handshake rides the demux
        // on the server), then ship the u64 sequence AEAD-sealed with the switch live.
        let sens_cli = std::thread::spawn(move || {
            let mut send = UnifiedSensSender::connect_tls(
                "0.0.0.0:0",
                server_addr,
                cli_unified,
                sens_client_cfg,
            )?;
            let mut buf = vec![0u8; item_bytes];
            for seq in 0..sens_items {
                buf[..8].copy_from_slice(&seq.to_le_bytes());
                send.send_item(&buf)?;
            }
            let acked = send.finish()?;
            Ok::<(bool, u64, String), std::io::Error>((
                acked,
                send.switches(),
                format!("{:?}", send.active_code()),
            ))
        });

        let quic_cli_res = quic_cli.await.map_err(|e| -> BoxErr { e.to_string().into() })?;
        let quic_srv_res = quic_srv.await.map_err(|e| -> BoxErr { e.to_string().into() })?;
        let (sens_acked, sw_tx, code_tx) = sens_cli.join().map_err(|_| "sens client panicked")??;
        let (got, order_ok, sum, sw_rx, code_rx) =
            sens_rx.join().map_err(|_| "sens receiver panicked")?;
        drop(endpoint);

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
        let switched = sw_tx > 0 && sw_rx > 0;

        println!(
            "RESULT one_port_tls quic_bytes={quic_got_len} quic_ok={quic_ok} \
             sens_items={got}/{sens_items} order_ok={order_ok} sum_ok={sum_ok} \
             fully_acked={sens_acked} switches_tx={sw_tx} switches_rx={sw_rx} \
             final_code_tx={code_tx} final_code_rx={code_rx} loss={loss}%{quic_err}",
        );

        if !quic_ok {
            return Err::<(), BoxErr>("QUIC payload mismatch on shared port".into());
        }
        if !sens_ok {
            return Err::<(), BoxErr>(
                format!("encrypted Sens transfer failed: got={got} order_ok={order_ok} sum_ok={sum_ok} acked={sens_acked}").into(),
            );
        }
        if policy == "auto" && loss >= 20 && !switched {
            return Err::<(), BoxErr>(
                format!("expected an RLC->RS switch at {loss}% loss (tx={sw_tx} rx={sw_rx})").into(),
            );
        }
        println!("PASS one-port TLS demux: QUIC + encrypted Sens-O-Matic both delivered on one UDP port");
        Ok(())
    })
}
