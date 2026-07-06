//! Cross-platform link-quality sensing: the radio / interface stats the
//! adaptive controller reads to anticipate loss before the in-band loss
//! estimate sees it.
//!
//! Each OS exposes a different best signal, normalized behind one trait:
//!
//!  - **Linux**: `/sys/class/net/<iface>/statistics` drop and error
//!    counters (delta-based drop rate). Works on any interface, wired or
//!    wireless - the relevant signal on a wired / virtual link where no
//!    RSSI exists.
//!  - **Windows**: both `WlanQueryInterface` connection signal quality
//!    (0..100, the RSSI-equivalent) on a Wi-Fi interface AND the
//!    `GetIfTable2` discard / error counters on ANY adapter (the Ethernet
//!    path, and a fallback where there is no Wi-Fi). The worse of the two
//!    wins, so wired and wireless links are both covered.
//!  - **macOS / other**: a stub returning "unknown" until a CoreWLAN
//!    backend lands.
//!
//! The controller fuses [`LinkSnapshot::link_stress`] (0..1) with the
//! loss / burstiness / delay sensors: a degrading link raises protection
//! pre-emptively.

/// The kind of link the local interface presents. A class change (Wi-Fi to
/// cellular, a wired uplink dropping to Wi-Fi) is a path event in its own right.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum LinkClass {
    /// Not yet determined / no usable interface.
    #[default]
    Unknown = 0,
    /// Software loopback.
    Loopback = 1,
    /// Wired Ethernet (no radio).
    Wired = 2,
    /// Wi-Fi (802.11) - the radio MAC stats below apply.
    Wifi = 3,
    /// Cellular (WWAN).
    Cellular = 4,
}

impl LinkClass {
    /// The wire code for the `Link` control frame.
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

/// A normalized link-quality reading. Every field is optional because no
/// single platform / interface exposes them all.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct LinkSnapshot {
    /// Signal quality, 0..=100 (Wi-Fi). `None` on wired links.
    pub signal_quality: Option<u8>,
    /// Fraction of recent packets dropped / errored at the interface,
    /// 0.0..=1.0. `None` if counters are unavailable.
    pub drop_rate: Option<f32>,
    /// Normalized current PHY rate (current TX rate / the best rate seen),
    /// 0.0..=1.0, Wi-Fi. A falling value is rate-adaptation backing off under
    /// poor radio conditions - an early loss predictor before frames drop.
    /// `None` off Wi-Fi.
    pub mcs_norm: Option<f32>,
    /// MAC-layer transmit retry rate (`tx_retries / tx_packets`), 0.0..=1.0,
    /// Wi-Fi. A climbing value is the radio struggling milliseconds before the
    /// loss reaches shard accounting. `None` where the OS does not expose it
    /// (Windows WLAN has no retry counter; Linux nl80211 does).
    pub retry_rate: Option<f32>,
    /// The raw first-hop PHY rate in kbit/s (Windows `ulTxRate`, Linux nl80211
    /// `tx_bitrate`). This is `nominal` - the rate a SINGLE Wi-Fi hop can carry -
    /// which the mesh-hop detector compares against the measured end-to-end
    /// `BtlBw`: each single-radio backhaul hop roughly halves throughput, so
    /// `round(log2(nominal / BtlBw))` is the backhaul-hop count. `None` off
    /// Wi-Fi.
    pub phy_rate_kbps: Option<u32>,
    /// The link's class (wired / Wi-Fi / cellular / loopback / unknown).
    pub class: LinkClass,
}

impl LinkSnapshot {
    /// Combined "link stress" in 0..=1: how degraded the link looks right now.
    /// Low signal quality, a high interface drop rate, a fallen PHY rate, and a
    /// climbing retry rate all push it up - the worst of the available signals
    /// wins. Used as a feed-forward term in the fusion controller.
    pub fn link_stress(&self) -> f32 {
        let from_signal = self
            .signal_quality
            .map(|q| (1.0 - q as f32 / 100.0).clamp(0.0, 1.0))
            .unwrap_or(0.0);
        let from_drops = self.drop_rate.unwrap_or(0.0).clamp(0.0, 1.0);
        let from_mcs = self
            .mcs_norm
            .map(|m| (1.0 - m).clamp(0.0, 1.0))
            .unwrap_or(0.0);
        let from_retry = self.retry_rate.unwrap_or(0.0).clamp(0.0, 1.0);
        from_signal.max(from_drops).max(from_mcs).max(from_retry)
    }
}

/// A pollable link-quality sensor. `sample` is called on the controller's
/// slow cadence (not per packet).
pub trait LinkSensor {
    /// Read the current link snapshot.
    fn sample(&mut self) -> LinkSnapshot;
    /// Backend identifier (for diagnostics).
    fn backend(&self) -> &'static str;
}

/// Construct the best link sensor for this platform. `iface` names the
/// interface to watch (Linux); `None` auto-detects the first non-loopback
/// up interface.
pub fn platform_sensor(iface: Option<String>) -> Box<dyn LinkSensor + Send> {
    #[cfg(target_os = "linux")]
    {
        Box::new(linux::SysfsSensor::new(iface))
    }
    #[cfg(target_os = "windows")]
    {
        drop(iface);
        Box::new(windows_net::WindowsSensor::new())
    }
    #[cfg(not(any(target_os = "linux", target_os = "windows")))]
    {
        drop(iface);
        Box::new(StubSensor)
    }
}

/// A sensor that knows nothing (macOS until CoreWLAN lands, and any other
/// target). Always returns an empty snapshot.
pub struct StubSensor;

impl LinkSensor for StubSensor {
    fn sample(&mut self) -> LinkSnapshot {
        LinkSnapshot::default()
    }
    fn backend(&self) -> &'static str {
        "stub"
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::{LinkClass, LinkSensor, LinkSnapshot};
    use std::fs;
    use std::path::PathBuf;

    /// Reads `/sys/class/net/<iface>/statistics` and derives a drop rate
    /// from the delta between samples.
    pub struct SysfsSensor {
        iface: Option<String>,
        prev: Option<(u64, u64)>, // (dropped+errors, packets)
    }

    impl SysfsSensor {
        pub fn new(iface: Option<String>) -> Self {
            let iface = iface.or_else(detect_iface);
            Self { iface, prev: None }
        }

        fn read_counter(&self, name: &str) -> Option<u64> {
            let iface = self.iface.as_ref()?;
            let mut p = PathBuf::from("/sys/class/net");
            p.push(iface);
            p.push("statistics");
            p.push(name);
            fs::read_to_string(p).ok()?.trim().parse().ok()
        }
    }

    /// First non-loopback interface whose `operstate` is `up`.
    fn detect_iface() -> Option<String> {
        let entries = fs::read_dir("/sys/class/net").ok()?;
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if name == "lo" {
                continue;
            }
            let state = fs::read_to_string(e.path().join("operstate"))
                .ok()
                .map(|s| s.trim().to_string())
                .unwrap_or_default();
            if state == "up" {
                return Some(name);
            }
        }
        None
    }

    /// The interface's class: Wi-Fi if it has a `wireless` sysfs node, else
    /// wired (loopback is excluded by `detect_iface`). A real radio E2E of the
    /// nl80211 retry/MCS read needs a Wi-Fi host; on a wired host this stays
    /// Wired and the drop-rate path carries the link signal.
    fn iface_class(iface: &str) -> LinkClass {
        let mut p = PathBuf::from("/sys/class/net");
        p.push(iface);
        p.push("wireless");
        if p.exists() {
            LinkClass::Wifi
        } else {
            LinkClass::Wired
        }
    }

    impl LinkSensor for SysfsSensor {
        fn sample(&mut self) -> LinkSnapshot {
            let dropped = self.read_counter("tx_dropped").unwrap_or(0)
                + self.read_counter("rx_dropped").unwrap_or(0)
                + self.read_counter("tx_errors").unwrap_or(0)
                + self.read_counter("rx_errors").unwrap_or(0);
            let packets = self.read_counter("tx_packets").unwrap_or(0)
                + self.read_counter("rx_packets").unwrap_or(0);
            let drop_rate = self.prev.map(|(pd, pp)| {
                let dd = dropped.saturating_sub(pd) as f32;
                let dp = packets.saturating_sub(pp).max(1) as f32;
                (dd / dp).clamp(0.0, 1.0)
            });
            self.prev = Some((dropped, packets));
            let class = self
                .iface
                .as_deref()
                .map(iface_class)
                .unwrap_or(LinkClass::Unknown);
            // On a Wi-Fi interface, the nl80211 station table carries the MAC
            // retry rate, PHY rate, and signal; on a wired interface there is no
            // radio, so those stay `None` and the drop rate is the signal.
            let radio = if class == LinkClass::Wifi {
                self.iface.as_deref().and_then(nl80211::station_stats)
            } else {
                None
            };
            let (signal_quality, mcs_norm, retry_rate, phy_rate_kbps) = match radio {
                Some(r) => (r.signal_quality, r.mcs_norm, r.retry_rate, r.phy_rate_kbps),
                None => (None, None, None, None),
            };
            LinkSnapshot {
                signal_quality,
                drop_rate,
                mcs_norm,
                retry_rate,
                phy_rate_kbps,
                class,
            }
        }
        fn backend(&self) -> &'static str {
            "linux-sysfs+nl80211"
        }
    }

    /// Wi-Fi MAC statistics from the nl80211 station table over generic
    /// netlink. The radio knows it is struggling - retries climbing, the PHY
    /// rate dropping - before the loss reaches shard accounting. Compile-clean
    /// everywhere; exercised only on a Wi-Fi host, since a wired interface never
    /// reaches it (see `iface_class`), so the VMs' Ethernet links short-circuit
    /// before this runs.
    pub mod nl80211 {
        use std::ffi::CString;
        use std::mem::size_of;

        /// One station's MAC-layer health.
        pub struct RadioStats {
            pub signal_quality: Option<u8>,
            pub mcs_norm: Option<f32>,
            pub retry_rate: Option<f32>,
            pub phy_rate_kbps: Option<u32>,
        }

        // Generic-netlink control family + nl80211 command / attribute ids
        // (linux/netlink.h, linux/genetlink.h, linux/nl80211.h). Stable kernel
        // ABI numbers, declared locally since libc does not expose nl80211.
        const GENL_ID_CTRL: u16 = 0x10;
        const CTRL_CMD_GETFAMILY: u8 = 3;
        const CTRL_ATTR_FAMILY_ID: u16 = 1;
        const CTRL_ATTR_FAMILY_NAME: u16 = 2;
        const NL80211_CMD_GET_STATION: u8 = 17;
        const NL80211_ATTR_IFINDEX: u16 = 3;
        const NL80211_ATTR_STA_INFO: u16 = 21;
        const STA_INFO_SIGNAL: u16 = 7;
        const STA_INFO_TX_BITRATE: u16 = 8;
        const STA_INFO_TX_PACKETS: u16 = 10;
        const STA_INFO_TX_RETRIES: u16 = 11;
        const RATE_INFO_BITRATE: u16 = 1; // u16, units of 100 kbps
        const RATE_INFO_BITRATE32: u16 = 5; // u32, units of 100 kbps
        const NLMSG_ERROR: u16 = 2;
        const NLMSG_DONE: u16 = 3;
        const GENL_HDRLEN: usize = 4; // genlmsghdr: cmd u8, version u8, reserved u16
        /// A representative high modern Wi-Fi PHY rate (Mbps) to normalize the
        /// current rate against, so `mcs_norm` is a 0..1 fraction without
        /// per-host calibration.
        const REF_RATE_MBPS: f32 = 866.0;

        /// `NLA_ALIGN`: netlink attributes are 4-byte aligned.
        fn nla_align(len: usize) -> usize {
            (len + 3) & !3
        }

        /// Read a `nlattr` header at `buf[pos..]`: `(nla_type, payload, next_pos)`.
        fn read_attr(buf: &[u8], pos: usize) -> Option<(u16, &[u8], usize)> {
            if pos + 4 > buf.len() {
                return None;
            }
            let nla_len = u16::from_ne_bytes([buf[pos], buf[pos + 1]]) as usize;
            let nla_type = u16::from_ne_bytes([buf[pos + 2], buf[pos + 3]]);
            if nla_len < 4 || pos + nla_len > buf.len() {
                return None;
            }
            let payload = &buf[pos + 4..pos + nla_len];
            Some((nla_type, payload, pos + nla_align(nla_len)))
        }

        /// Walk the nested `STA_INFO` attributes for the rate / packet / retry /
        /// signal counters and reduce them to a `RadioStats`.
        fn parse_sta_info(buf: &[u8]) -> RadioStats {
            let (mut signal, mut tx_packets, mut tx_retries, mut rate_100kbps) =
                (None, None, None, None);
            let mut pos = 0;
            while let Some((ty, val, next)) = read_attr(buf, pos) {
                match ty {
                    STA_INFO_SIGNAL => {
                        // Signal is i8 dBm; map [-100, -50] dBm to [0, 100].
                        if let Some(&b) = val.first() {
                            let dbm = b as i8 as f32;
                            signal = Some((((dbm + 100.0) * 2.0).clamp(0.0, 100.0)) as u8);
                        }
                    }
                    STA_INFO_TX_PACKETS if val.len() >= 4 => {
                        tx_packets = Some(u32::from_ne_bytes([val[0], val[1], val[2], val[3]]));
                    }
                    STA_INFO_TX_RETRIES if val.len() >= 4 => {
                        tx_retries = Some(u32::from_ne_bytes([val[0], val[1], val[2], val[3]]));
                    }
                    STA_INFO_TX_BITRATE => {
                        let mut rp = 0;
                        while let Some((rty, rval, rnext)) = read_attr(val, rp) {
                            if rty == RATE_INFO_BITRATE32 && rval.len() >= 4 {
                                rate_100kbps =
                                    Some(u32::from_ne_bytes([rval[0], rval[1], rval[2], rval[3]]));
                            } else if rty == RATE_INFO_BITRATE && rval.len() >= 2 && rate_100kbps.is_none()
                            {
                                rate_100kbps = Some(u16::from_ne_bytes([rval[0], rval[1]]) as u32);
                            }
                            rp = rnext;
                        }
                    }
                    _ => {}
                }
                pos = next;
            }
            let retry_rate = match (tx_retries, tx_packets) {
                (Some(r), Some(p)) if p > 0 => Some((r as f32 / p as f32).clamp(0.0, 1.0)),
                _ => None,
            };
            let mcs_norm = rate_100kbps
                .map(|r| ((r as f32 / 10.0) / REF_RATE_MBPS).clamp(0.0, 1.0));
            // 100-kbps units -> kbps for the raw first-hop PHY rate (`nominal`).
            let phy_rate_kbps = rate_100kbps.map(|r| r.saturating_mul(100));
            RadioStats {
                signal_quality: signal,
                mcs_norm,
                retry_rate,
                phy_rate_kbps,
            }
        }

        /// Resolve the nl80211 generic-netlink family id, dump the station table
        /// for `iface`, and reduce the first station to its MAC health. Returns
        /// `None` on any netlink error or if the interface has no associated
        /// station.
        pub fn station_stats(iface: &str) -> Option<RadioStats> {
            // SAFETY: the socket is a valid fd for its lifetime, every buffer
            // handed to send/recv outlives the call, the sockaddr is zeroed and
            // sized correctly, and the fd is closed before return.
            unsafe {
                let cname = CString::new(iface).ok()?;
                let ifindex = libc::if_nametoindex(cname.as_ptr());
                if ifindex == 0 {
                    return None;
                }
                let fd = libc::socket(libc::AF_NETLINK, libc::SOCK_RAW, libc::NETLINK_GENERIC);
                if fd < 0 {
                    return None;
                }
                let mut addr: libc::sockaddr_nl = std::mem::zeroed();
                addr.nl_family = libc::AF_NETLINK as u16;
                if libc::bind(
                    fd,
                    &addr as *const _ as *const libc::sockaddr,
                    size_of::<libc::sockaddr_nl>() as libc::socklen_t,
                ) < 0
                {
                    libc::close(fd);
                    return None;
                }
                let family = resolve_family(fd);
                let stats = family.and_then(|fam| dump_first_station(fd, fam, ifindex));
                libc::close(fd);
                stats
            }
        }

        /// Build and send a netlink request: a 16-byte `nlmsghdr`, a 4-byte
        /// genl header (`cmd`/version), then the supplied attributes (already
        /// `NLA_ALIGN`-padded). Returns the bytes written or `None` on error.
        ///
        /// # Safety
        /// `fd` must be a bound NETLINK_GENERIC socket.
        unsafe fn send_request(fd: i32, family: u16, flags: u16, cmd: u8, attrs: &[u8]) -> Option<()> {
            let total = 16 + GENL_HDRLEN + attrs.len();
            let mut msg = vec![0u8; total];
            msg[0..4].copy_from_slice(&(total as u32).to_ne_bytes());
            msg[4..6].copy_from_slice(&family.to_ne_bytes());
            msg[6..8].copy_from_slice(&flags.to_ne_bytes());
            // nlmsg_seq @ 8, nlmsg_pid @ 12 left 0 (kernel fills pid).
            msg[16] = cmd; // genlmsghdr.cmd
            msg[17] = 0; // version
            msg[20..].copy_from_slice(attrs);
            // SAFETY: msg is `total` valid bytes; fd is the bound socket.
            let n = unsafe { libc::send(fd, msg.as_ptr() as *const libc::c_void, total, 0) };
            (n as usize == total).then_some(())
        }

        /// Encode one `nlattr` (`type`, payload) with `NLA_ALIGN` padding.
        fn put_attr(out: &mut Vec<u8>, ty: u16, payload: &[u8]) {
            let len = 4 + payload.len();
            out.extend_from_slice(&(len as u16).to_ne_bytes());
            out.extend_from_slice(&ty.to_ne_bytes());
            out.extend_from_slice(payload);
            out.resize(nla_align(out.len()), 0);
        }

        /// CTRL_CMD_GETFAMILY("nl80211") -> the family id.
        ///
        /// # Safety
        /// `fd` must be a bound NETLINK_GENERIC socket.
        unsafe fn resolve_family(fd: i32) -> Option<u16> {
            const NLM_F_REQUEST: u16 = 1;
            let mut attrs = Vec::new();
            let name = b"nl80211\0";
            put_attr(&mut attrs, CTRL_ATTR_FAMILY_NAME, name);
            // SAFETY: forwarded; fd is the bound socket.
            unsafe { send_request(fd, GENL_ID_CTRL, NLM_F_REQUEST, CTRL_CMD_GETFAMILY, &attrs)? };
            let mut buf = vec![0u8; 8192];
            // SAFETY: buf is 8192 valid bytes; fd is the bound socket.
            let n = unsafe { libc::recv(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len(), 0) };
            if n <= 0 {
                return None;
            }
            let buf = &buf[..n as usize];
            // Skip nlmsghdr (16) + genlmsghdr (4); walk the control attributes.
            let mut pos = 16 + GENL_HDRLEN;
            while let Some((ty, val, next)) = read_attr(buf, pos) {
                if ty == CTRL_ATTR_FAMILY_ID && val.len() >= 2 {
                    return Some(u16::from_ne_bytes([val[0], val[1]]));
                }
                pos = next;
            }
            None
        }

        /// NL80211_CMD_GET_STATION dump for `ifindex` -> the first station's
        /// MAC health.
        ///
        /// # Safety
        /// `fd` must be a bound NETLINK_GENERIC socket.
        unsafe fn dump_first_station(fd: i32, family: u16, ifindex: u32) -> Option<RadioStats> {
            const NLM_F_REQUEST: u16 = 1;
            const NLM_F_DUMP: u16 = 0x300;
            let mut attrs = Vec::new();
            put_attr(&mut attrs, NL80211_ATTR_IFINDEX, &ifindex.to_ne_bytes());
            // SAFETY: forwarded; fd is the bound socket.
            unsafe {
                send_request(
                    fd,
                    family,
                    NLM_F_REQUEST | NLM_F_DUMP,
                    NL80211_CMD_GET_STATION,
                    &attrs,
                )?
            };
            let mut buf = vec![0u8; 16384];
            // SAFETY: buf is 16384 valid bytes; fd is the bound socket.
            let n = unsafe { libc::recv(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len(), 0) };
            if n <= 0 {
                return None;
            }
            let buf = &buf[..n as usize];
            // Walk the concatenated netlink messages; the first station's
            // STA_INFO is enough for a representative reading.
            let mut off = 0;
            while off + 16 <= buf.len() {
                let len = u32::from_ne_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
                    as usize;
                let mtype = u16::from_ne_bytes([buf[off + 4], buf[off + 5]]);
                if len < 16 || off + len > buf.len() {
                    break;
                }
                if mtype == NLMSG_DONE || mtype == NLMSG_ERROR {
                    break;
                }
                let body = &buf[off + 16 + GENL_HDRLEN..off + len];
                let mut pos = 0;
                while let Some((ty, val, next)) = read_attr(body, pos) {
                    if ty == NL80211_ATTR_STA_INFO {
                        return Some(parse_sta_info(val));
                    }
                    pos = next;
                }
                off += nla_align(len);
            }
            None
        }
    }
}

#[cfg(target_os = "windows")]
mod windows_net {
    use super::{LinkClass, LinkSensor, LinkSnapshot};
    use std::ptr;
    use windows_sys::Win32::NetworkManagement::IpHelper::{
        FreeMibTable, GetIfTable2, MIB_IF_TABLE2,
    };
    use windows_sys::Win32::NetworkManagement::WiFi::{
        wlan_intf_opcode_current_connection, WlanCloseHandle, WlanEnumInterfaces, WlanFreeMemory,
        WlanOpenHandle, WlanQueryInterface, WLAN_CONNECTION_ATTRIBUTES, WLAN_INTERFACE_INFO_LIST,
    };

    /// `WLAN_INTERFACE_STATE` value for a connected interface; only then are the
    /// association's signal quality and PHY rate meaningful.
    const WLAN_INTERFACE_STATE_CONNECTED: i32 = 1;

    /// `IF_OPER_STATUS` value for an interface that is up.
    const IF_OPER_STATUS_UP: i32 = 1;
    /// `IFTYPE` value for a software loopback interface (skipped).
    const IF_TYPE_SOFTWARE_LOOPBACK: u32 = 24;

    /// Reads the adapter's real-time health two ways and keeps the worse:
    /// `WlanQueryInterface` signal quality on Wi-Fi, and the `GetIfTable2`
    /// discard/error counters on ANY adapter (the Ethernet path, and a
    /// fallback when there is no Wi-Fi). The drop rate is a delta between
    /// samples on the busiest up, non-loopback interface.
    pub struct WindowsSensor {
        prev: Option<(u64, u64)>, // (discards+errors, packets)
        /// Best TX PHY rate seen (Kbps); the reference for `mcs_norm`, so a
        /// later rate below it reads as the radio rate-adapting down.
        max_tx_kbps: u32,
    }

    impl WindowsSensor {
        pub fn new() -> Self {
            Self {
                prev: None,
                max_tx_kbps: 0,
            }
        }

        /// Delta discard+error rate over the busiest up, non-loopback
        /// interface (Ethernet or Wi-Fi), via `GetIfTable2`.
        fn query_drop_rate(&mut self) -> Option<f32> {
            // SAFETY: GetIfTable2 allocates the table; every row is read
            // within `NumEntries`, the table is freed exactly once with
            // FreeMibTable, and no pointer outlives the call.
            unsafe {
                let mut table: *mut MIB_IF_TABLE2 = ptr::null_mut();
                if GetIfTable2(&mut table) != 0 || table.is_null() {
                    return None;
                }
                let n = (*table).NumEntries as usize;
                let rows = &raw const (*table).Table[0];
                let (mut best_pkts, mut best_drops, mut found) = (0u64, 0u64, false);
                for i in 0..n {
                    let row = &*rows.add(i);
                    if row.OperStatus != IF_OPER_STATUS_UP
                        || row.Type == IF_TYPE_SOFTWARE_LOOPBACK
                    {
                        continue;
                    }
                    let pkts = row.InUcastPkts.saturating_add(row.OutUcastPkts);
                    if !found || pkts > best_pkts {
                        best_pkts = pkts;
                        best_drops = row
                            .InDiscards
                            .saturating_add(row.OutDiscards)
                            .saturating_add(row.InErrors)
                            .saturating_add(row.OutErrors);
                        found = true;
                    }
                }
                FreeMibTable(table as *const core::ffi::c_void);
                if !found {
                    return None;
                }
                let rate = self.prev.map(|(pd, pp)| {
                    let dd = best_drops.saturating_sub(pd) as f32;
                    let dp = best_pkts.saturating_sub(pp).max(1) as f32;
                    (dd / dp).clamp(0.0, 1.0)
                });
                self.prev = Some((best_drops, best_pkts));
                rate
            }
        }
    }

    impl LinkSensor for WindowsSensor {
        fn sample(&mut self) -> LinkSnapshot {
            let drop_rate = self.query_drop_rate();
            // The current Wi-Fi association: signal quality and the TX PHY rate.
            // A rate below the best seen is rate-adaptation backing off under
            // poor radio conditions, an early loss predictor.
            let (signal_quality, mcs_norm, phy_rate_kbps, class) = match query_wlan() {
                Some((signal, tx_kbps)) => {
                    if tx_kbps > self.max_tx_kbps {
                        self.max_tx_kbps = tx_kbps;
                    }
                    let mcs_norm = (self.max_tx_kbps > 0)
                        .then(|| (tx_kbps as f32 / self.max_tx_kbps as f32).clamp(0.0, 1.0));
                    (Some(signal), mcs_norm, Some(tx_kbps), LinkClass::Wifi)
                }
                None => (
                    None,
                    None,
                    None,
                    if drop_rate.is_some() {
                        LinkClass::Wired
                    } else {
                        LinkClass::Unknown
                    },
                ),
            };
            LinkSnapshot {
                signal_quality,
                drop_rate,
                mcs_norm,
                retry_rate: None,
                phy_rate_kbps,
                class,
            }
        }
        fn backend(&self) -> &'static str {
            "windows-iftable+wlan"
        }
    }

    /// Open a WLAN handle, find the first interface, and read its current
    /// connection's signal quality and TX PHY rate (Kbps). Returns `None` if
    /// there is no Wi-Fi interface or it is not associated. The FFI is
    /// encapsulated and manages its own handles, so this is a safe wrapper.
    fn query_wlan() -> Option<(u8, u32)> {
        // SAFETY: every pointer the WLAN API hands back is checked for
        // null before use, each allocation is freed exactly once with
        // WlanFreeMemory, and the handle is closed before return.
        unsafe {
            let mut handle = ptr::null_mut();
            let mut negotiated = 0u32;
            // Client version 2 (Vista+).
            if WlanOpenHandle(2, ptr::null(), &mut negotiated, &mut handle) != 0 {
                return None;
            }
            let mut result = None;
            let mut list: *mut WLAN_INTERFACE_INFO_LIST = ptr::null_mut();
            if WlanEnumInterfaces(handle, ptr::null(), &mut list) == 0
                && !list.is_null()
                && (*list).dwNumberOfItems > 0
            {
                let guid = (*list).InterfaceInfo[0].InterfaceGuid;
                let mut size = 0u32;
                let mut data: *mut core::ffi::c_void = ptr::null_mut();
                let rc = WlanQueryInterface(
                    handle,
                    &guid,
                    wlan_intf_opcode_current_connection,
                    ptr::null(),
                    &mut size,
                    &mut data,
                    ptr::null_mut(),
                );
                if rc == 0 && !data.is_null() {
                    let attrs = data as *const WLAN_CONNECTION_ATTRIBUTES;
                    // Signal quality and rate are meaningful only when connected.
                    if (*attrs).isState == WLAN_INTERFACE_STATE_CONNECTED {
                        let assoc = (*attrs).wlanAssociationAttributes;
                        result = Some((assoc.wlanSignalQuality.min(100) as u8, assoc.ulTxRate));
                    }
                    WlanFreeMemory(data);
                }
            }
            if !list.is_null() {
                WlanFreeMemory(list as *mut core::ffi::c_void);
            }
            WlanCloseHandle(handle, ptr::null());
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn link_stress_from_low_signal() {
        let s = LinkSnapshot { signal_quality: Some(20), ..Default::default() };
        // 20% quality -> 0.8 stress.
        assert!((s.link_stress() - 0.8).abs() < 0.01);
    }

    #[test]
    fn link_stress_from_drops() {
        let s = LinkSnapshot { drop_rate: Some(0.3), ..Default::default() };
        assert!((s.link_stress() - 0.3).abs() < 0.01);
    }

    #[test]
    fn link_stress_takes_the_worse_signal() {
        let s = LinkSnapshot {
            signal_quality: Some(90),
            drop_rate: Some(0.4),
            ..Default::default()
        };
        // signal 90 -> 0.1 stress, drops 0.4 -> max is 0.4.
        assert!((s.link_stress() - 0.4).abs() < 0.01);
    }

    #[test]
    fn clean_link_has_zero_stress() {
        let s = LinkSnapshot {
            signal_quality: Some(100),
            drop_rate: Some(0.0),
            mcs_norm: Some(1.0),
            ..Default::default()
        };
        assert_eq!(s.link_stress(), 0.0);
    }

    #[test]
    fn link_stress_from_wifi_mac_signals() {
        // A fallen PHY rate (rate-adaptation backing off) raises stress.
        let slow = LinkSnapshot { mcs_norm: Some(0.3), ..Default::default() };
        assert!((slow.link_stress() - 0.7).abs() < 0.01, "mcs 0.3 -> 0.7 stress");
        // A climbing retry rate raises stress directly.
        let retry = LinkSnapshot { retry_rate: Some(0.4), ..Default::default() };
        assert!((retry.link_stress() - 0.4).abs() < 0.01, "retry 0.4 -> 0.4 stress");
        // A full-rate, no-retry Wi-Fi link is unstressed.
        let good = LinkSnapshot {
            signal_quality: Some(95),
            mcs_norm: Some(1.0),
            retry_rate: Some(0.0),
            class: LinkClass::Wifi,
            ..Default::default()
        };
        assert!(good.link_stress() < 0.06, "good wifi low stress: {}", good.link_stress());
    }

    #[test]
    fn platform_sensor_samples_without_panicking() {
        // Whatever backend this platform builds, sampling must be safe.
        let mut s = platform_sensor(None);
        let snap = s.sample();
        assert!(!s.backend().is_empty());
        assert!(snap.link_stress() >= 0.0, "stress is well-defined");
    }
}
