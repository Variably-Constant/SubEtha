//! Optional TLS 1.3 record layer for the RLC transport (`tls` feature).
//!
//! The transport stays a datagram protocol; TLS only supplies the handshake and
//! the AEAD record keys. We drive a `rustls::quic` TLS 1.3 handshake - carrying
//! its `write_hs` / `read_hs` plaintext flights over the transport's own
//! reliable Crypto-frame exchange (one frame per encryption level, so `read_hs`
//! sees the same level boundaries) - and then seal / open every data datagram
//! with the derived 1-RTT [`rustls::quic::PacketKey`]. The FEC is computed over
//! cleartext source symbols (the whole datagram is sealed at the socket
//! boundary), so the "FEC over cleartext" ordering is preserved and the AEAD
//! protects the wire.
//!
//! Using `rustls::quic` (rather than a hand-rolled record layer) gets the TLS
//! 1.3 key schedule, the AEAD with per-packet nonces, and rustls's audited
//! handshake state machine for free. The handshake flights ride the transport's
//! reliability instead of QUIC packet protection, so the key agreement and
//! authentication are intact (the Finished MAC authenticates the transcript);
//! only QUIC's handshake-packet confidentiality is not reproduced.

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::quic::{ClientConnection, Connection, KeyChange, PacketKey, ServerConnection, Version};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// ALPN protocol id for the RLC TLS handshake (QUIC requires a non-empty ALPN).
const ALPN: &[u8] = b"subetha-rlc";
/// SNI the self-signed cert is issued for; clients pass it as the server name.
/// Matches the QUIC bridge's `generate_self_signed_cert`, so one `--gen-cert`
/// pair serves both transports.
pub const SNI: &str = "subetha-lan";

/// Install the ring crypto provider as the process default, so the rustls
/// config builders have a provider. Safe to call repeatedly: a second call
/// finds a provider already installed, which is the state this call exists to
/// establish.
pub fn install_provider() {
    match rustls::crypto::ring::default_provider().install_default() {
        Ok(()) => {}
        // The Err carries the provider that is already the process default,
        // installed by an earlier call here or by the embedding program.
        Err(already_installed) => debug_assert!(
            !already_installed.cipher_suites.is_empty(),
            "a provider with no cipher suites is the process default"
        ),
    }
}

/// Generate a self-signed cert + PKCS#8 key (DER), issued for [`SNI`].
pub fn self_signed_cert() -> Result<(Vec<u8>, Vec<u8>), String> {
    self_signed_cert_for(&[SNI])
}

/// Generate a self-signed cert + PKCS#8 key (DER) carrying `names` as its
/// subject alternative names. A client validating any one of them with
/// [`new_client_for`](CryptoState::new_client_for) accepts the cert; the
/// first name is the certificate subject.
pub fn self_signed_cert_for(names: &[&str]) -> Result<(Vec<u8>, Vec<u8>), String> {
    if names.is_empty() {
        return Err("a self-signed cert needs at least one name".to_string());
    }
    let subjects: Vec<String> = names.iter().map(|s| s.to_string()).collect();
    let ck = rcgen::generate_simple_self_signed(subjects).map_err(|e| e.to_string())?;
    Ok((ck.cert.der().to_vec(), ck.key_pair.serialize_der()))
}

/// Build a TLS server config (TLS 1.3, ring, RLC ALPN) from a cert + key DER.
pub fn server_config(cert_der: &[u8], key_der: &[u8]) -> Result<Arc<ServerConfig>, String> {
    install_provider();
    let cert = CertificateDer::from(cert_der.to_vec());
    let key = PrivateKeyDer::Pkcs8(key_der.to_vec().into());
    let mut cfg = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .map_err(|e| e.to_string())?;
    cfg.alpn_protocols = vec![ALPN.to_vec()];
    Ok(Arc::new(cfg))
}

/// Build a TLS client config trusting exactly the given cert DER (TLS 1.3, ring,
/// RLC ALPN). The cert is shared out-of-band, like the QUIC bridge's DER files.
pub fn client_config(cert_der: &[u8]) -> Result<Arc<ClientConfig>, String> {
    client_config_trusting(&[cert_der])
}

/// Build a TLS client config trusting every DER in `roots` (TLS 1.3, ring, RLC
/// ALPN). A CA root goes here to accept any leaf it signed, or several leaf
/// certs to accept exactly those; [`client_config`] is the one-leaf case.
pub fn client_config_trusting(roots: &[&[u8]]) -> Result<Arc<ClientConfig>, String> {
    install_provider();
    if roots.is_empty() {
        return Err("a client config needs at least one trusted root".to_string());
    }
    let mut store = RootCertStore::empty();
    for der in roots {
        store
            .add(CertificateDer::from(der.to_vec()))
            .map_err(|e| e.to_string())?;
    }
    let mut cfg = ClientConfig::builder()
        .with_root_certificates(store)
        .with_no_client_auth();
    cfg.alpn_protocols = vec![ALPN.to_vec()];
    Ok(Arc::new(cfg))
}

/// Bytes the seal appends to a datagram: the AEAD tag.
pub const TAG_LEN: usize = 16;

/// One endpoint's TLS state: the handshake connection while handshaking, then
/// the 1-RTT packet keys for sealing / opening data datagrams.
pub struct CryptoState {
    conn: Connection,
    local: Option<Box<dyn PacketKey>>,
    remote: Option<Box<dyn PacketKey>>,
    send_pn: AtomicU64,
}

impl CryptoState {
    /// Client side of the handshake, asserting [`SNI`] as the server name.
    pub fn new_client(cfg: Arc<ClientConfig>) -> Result<Self, String> {
        Self::new_client_for(cfg, SNI)
    }

    /// Client side of the handshake, asserting `server_name`. The server's
    /// cert must carry it as a subject alternative name, and `cfg` must trust
    /// the cert or its issuer. [`new_client`](Self::new_client) is the [`SNI`]
    /// case.
    pub fn new_client_for(cfg: Arc<ClientConfig>, server_name: &str) -> Result<Self, String> {
        let name = ServerName::try_from(server_name.to_string()).map_err(|e| e.to_string())?;
        let conn = ClientConnection::new(cfg, Version::V1, name, Vec::new())
            .map_err(|e| e.to_string())?;
        Ok(Self { conn: Connection::Client(conn), local: None, remote: None, send_pn: AtomicU64::new(0) })
    }

    /// Server side of the handshake.
    pub fn new_server(cfg: Arc<ServerConfig>) -> Result<Self, String> {
        let conn = ServerConnection::new(cfg, Version::V1, Vec::new()).map_err(|e| e.to_string())?;
        Ok(Self { conn: Connection::Server(conn), local: None, remote: None, send_pn: AtomicU64::new(0) })
    }

    /// Feed one received handshake flight (one encryption level) to the TLS state.
    pub fn read_handshake(&mut self, flight: &[u8]) -> Result<(), String> {
        self.conn.read_hs(flight).map_err(|e| e.to_string())
    }

    /// Drain all pending handshake output as a sequence of per-level flights to
    /// send (each `write_hs` chunk is one level). Captures the 1-RTT keys when
    /// the handshake reaches them; the handshake-level keys are not needed (the
    /// flights ride the transport's reliability, not QUIC packet protection).
    pub fn take_outgoing(&mut self) -> Vec<Vec<u8>> {
        let mut flights = Vec::new();
        loop {
            let mut buf = Vec::new();
            let kc = self.conn.write_hs(&mut buf);
            let had_data = !buf.is_empty();
            if had_data {
                flights.push(buf);
            }
            if let Some(KeyChange::OneRtt { keys, .. }) = kc {
                self.local = Some(keys.local.packet);
                self.remote = Some(keys.remote.packet);
            }
            // `write_hs` drains all data for the current level per call and
            // returns empty once the pending flight is exhausted.
            if !had_data {
                break;
            }
        }
        flights
    }

    /// Whether the handshake has produced the 1-RTT keys (sealing can begin).
    pub fn is_complete(&self) -> bool {
        self.local.is_some() && self.remote.is_some()
    }

    /// Whether the handshake has more to send or is still expecting peer data.
    pub fn handshake_wants_read(&self) -> bool {
        self.conn.is_handshaking()
    }

    /// Seal a datagram in place: encrypt the payload with the local 1-RTT key
    /// under a fresh packet number, append the AEAD tag, and return the packet
    /// number the receiver needs to open it.
    pub fn seal(&self, payload: &mut Vec<u8>) -> Result<u64, String> {
        let key = self.local.as_ref().ok_or("seal before handshake complete")?;
        let pn = self.send_pn.fetch_add(1, Ordering::Relaxed);
        let aad = pn.to_le_bytes();
        let tag = key.encrypt_in_place(pn, &aad, payload).map_err(|e| e.to_string())?;
        payload.extend_from_slice(tag.as_ref());
        Ok(pn)
    }

    /// Open a datagram in place (payload includes the tag): decrypt with the
    /// remote 1-RTT key under `pn`, returning the plaintext length.
    pub fn open(&self, pn: u64, payload: &mut [u8]) -> Result<usize, String> {
        let key = self.remote.as_ref().ok_or("open before handshake complete")?;
        let aad = pn.to_le_bytes();
        let pt = key.decrypt_in_place(pn, &aad, payload).map_err(|e| e.to_string())?;
        Ok(pt.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drive a full client <-> server handshake in memory by ferrying the
    /// flights between the two states, then confirm 1-RTT seal / open round-trips.
    #[test]
    fn handshake_completes_and_seals_round_trip() {
        let (cert, key) = self_signed_cert().expect("cert");
        let mut client = CryptoState::new_client(client_config(&cert).expect("cc")).expect("client");
        let mut server = CryptoState::new_server(server_config(&cert, &key).expect("sc")).expect("server");

        // Ferry flights until both sides have 1-RTT keys (a TLS 1.3 handshake is
        // ClientHello -> ServerHello+EE+Cert+Fin -> Fin, a few flights).
        for _ in 0..8 {
            for f in client.take_outgoing() {
                server.read_handshake(&f).expect("server read");
            }
            for f in server.take_outgoing() {
                client.read_handshake(&f).expect("client read");
            }
            if client.is_complete() && server.is_complete() {
                break;
            }
        }
        assert!(client.is_complete(), "client must reach 1-RTT keys");
        assert!(server.is_complete(), "server must reach 1-RTT keys");

        // Client seals, server opens (client.local == server.remote direction).
        let plaintext = b"the quick brown fox jumps over the lazy dog".to_vec();
        let mut wire = plaintext.clone();
        let pn = client.seal(&mut wire).expect("seal");
        assert_ne!(&wire[..plaintext.len()], &plaintext[..], "payload must be ciphertext");
        assert_eq!(wire.len(), plaintext.len() + TAG_LEN, "tag appended");
        let n = server.open(pn, &mut wire).expect("open");
        assert_eq!(&wire[..n], &plaintext[..], "decrypt must recover the plaintext");

        // A tampered tag must fail to open.
        let mut bad = plaintext.clone();
        let pn2 = client.seal(&mut bad).expect("seal2");
        *bad.last_mut().unwrap() ^= 0xff;
        assert!(server.open(pn2, &mut bad).is_err(), "tampered AEAD must not open");
    }

    /// A cert issued for a chosen name completes the handshake when the client
    /// asserts that name, and a client asserting a name the cert does not carry
    /// is refused.
    #[test]
    fn a_named_cert_validates_its_name_and_refuses_another() {
        let (cert, key) = self_signed_cert_for(&["replica-3.lan"]).expect("cert");
        let ccfg = client_config_trusting(&[&cert]).expect("cc");

        let mut ok_client =
            CryptoState::new_client_for(ccfg.clone(), "replica-3.lan").expect("named client");
        let mut server =
            CryptoState::new_server(server_config(&cert, &key).expect("sc")).expect("server");
        for _ in 0..8 {
            for f in ok_client.take_outgoing() {
                server.read_handshake(&f).expect("server read");
            }
            for f in server.take_outgoing() {
                ok_client.read_handshake(&f).expect("client read");
            }
            if ok_client.is_complete() && server.is_complete() {
                break;
            }
        }
        assert!(ok_client.is_complete(), "the asserted name matches a SAN, so it completes");

        // The client asserting a name the cert does not carry must fail to
        // validate: the server's Certificate does not satisfy the name, so the
        // client rejects it as it feeds the server's flight.
        let mut bad_client =
            CryptoState::new_client_for(ccfg, "replica-9.lan").expect("named client");
        let mut server2 =
            CryptoState::new_server(server_config(&cert, &key).expect("sc")).expect("server");
        let mut rejected = false;
        'drive: for _ in 0..8 {
            for f in bad_client.take_outgoing() {
                server2.read_handshake(&f).expect("server read");
            }
            for f in server2.take_outgoing() {
                if bad_client.read_handshake(&f).is_err() {
                    rejected = true;
                    break 'drive;
                }
            }
            if bad_client.is_complete() {
                break;
            }
        }
        assert!(rejected, "a wrong asserted name must be refused, not completed");
        assert!(!bad_client.is_complete(), "a refused handshake must not reach 1-RTT keys");
    }
}
