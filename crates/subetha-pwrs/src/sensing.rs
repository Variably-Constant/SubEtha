//! The link across a lossy network and the sensors a transport does
//! its arithmetic with.
//!
//! The sensors hold no shared memory and touch no network: they are fed
//! measurements and answer what they have worked out from them, which
//! is why they are worth having from a script whether or not a SubEtha
//! link produced the numbers. Every one of them answers `$null` rather
//! than a number until it has seen enough to say anything, which is a
//! different answer from zero.

use std::net::{SocketAddr, ToSocketAddrs};

use pwrs::prelude::*;

use subetha_cxc::burst_model_sensor::BurstModel;
use subetha_cxc::forecast_sensor::ArrivalForecast;
use subetha_cxc::loss_class_sensor::{LossClass, LossClassSensor};
use subetha_cxc::path_sensor::PathSensor;
use subetha_cxc::periodicity_sensor::PeriodicitySensor;
use subetha_cxc::rtt_shape_sensor::RttShape;
use subetha_cxc::sens_unified::{CodePolicy, SensCode, UnifiedConfig, UnifiedSensReceiver, UnifiedSensSender};
use subetha_cxc::temporal_sensor::TemporalSensor;
use subetha_cxc::wbest_sensor::WBestEstimator;

use crate::common::{arg_err, assert_send, bytes, op_err, out_bytes, size};

assert_send!(SensSender, SensReceiver, LossKind, LossBursts, Timing, RoundTripShape, Periodicity, Capacity, Forecast, PathChanges);

/// The way a link works out the extra traffic a reader rebuilds lost
/// items from.
#[psenum(name = "SubEtha.SensCode")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SensCodeKind {
    /// The sliding code, quicker while losses are light.
    #[default]
    Rlc,
    /// The block code, which carries heavy sustained loss for less
    /// extra traffic.
    Rs,
}

impl SensCodeKind {
    fn from_rust(code: SensCode) -> Self {
        match code {
            SensCode::Rlc => SensCodeKind::Rlc,
            SensCode::Rs => SensCodeKind::Rs,
        }
    }

    fn policy(code: Option<SensCodeKind>) -> CodePolicy {
        match code {
            None => CodePolicy::default_auto(),
            Some(SensCodeKind::Rlc) => CodePolicy::ForceRlc,
            Some(SensCodeKind::Rs) => CodePolicy::ForceRs,
        }
    }
}

/// An address as a host and a port.
#[psclass(name = "SubEtha.Endpoint")]
#[derive(Clone, Default)]
pub struct Endpoint {
    /// The host, as text.
    pub host: String,
    /// The port.
    pub port: u16,
}

impl Endpoint {
    fn of(addr: SocketAddr) -> Self {
        Self { host: addr.ip().to_string(), port: addr.port() }
    }
}

/// An item and the sender it came from.
#[psclass(name = "SubEtha.SourcedItem")]
#[derive(Clone, Default)]
pub struct SourcedItem {
    /// Which sender the item came from.
    pub source: u64,
    /// The item's bytes, a `byte[]`.
    pub bytes: PsObject,
}

/// One address from a host and a port, resolved here so a caller never
/// has to spell a socket address as one string.
fn socket_addr_of(host: &str, port: u16) -> PsResult<SocketAddr> {
    (host, port)
        .to_socket_addrs()
        .map_err(|e| arg_err(format!("{host}:{port} is not an address: {e}")))?
        .next()
        .ok_or_else(|| arg_err(format!("{host}:{port} named no address")))
}

/// The buffer one item travels in: the item itself plus the two bytes
/// that record how long it is, so a short item arrives short rather
/// than padded out to the full size.
fn symbol_len_for(max_item_size: u64) -> PsResult<usize> {
    if max_item_size < 1 {
        return Err(arg_err("an item is at least one byte"));
    }
    Ok(size(max_item_size, "the item size")? + 2)
}

/// The sending end of a link across a network that keeps working as the
/// network gets worse.
///
/// It sends more than the items themselves, so a reader can rebuild
/// what the network lost without asking for it again. There are two
/// ways of working out that extra, and which one is better depends on
/// how much is being lost: a sliding one is quicker while losses are
/// light, and a block one carries heavy sustained loss for less extra
/// traffic. The link measures the loss the reader reports and changes
/// between them while running, without either end reconnecting; Code
/// says which it is on now and Switches how many times it has changed.
///
/// The two differ in when a reader sees an item. Under the sliding code
/// an item can come out as soon as it arrives. Under the block code
/// items are grouped in eights, and a reader sees none of a group until
/// enough of it has arrived, so a run shorter than a group waits for
/// the rest of the group before any of it is delivered.
#[psclass(name = "SubEtha.SensSender", mode = proxy)]
pub struct SensSender {
    /// The largest an item may be on this link, fixed when the link is
    /// made and the same at both ends.
    pub max_item_size: u64,
    /// The address this sends to.
    pub peer: Endpoint,
    #[psfield(skip)]
    inner: UnifiedSensSender,
}

impl SensSender {
    fn fits(&self, item: &[u8]) -> PsResult<()> {
        if item.len() as u64 > self.max_item_size {
            return Err(arg_err(format!("an item on this link is at most {} bytes, got {}", self.max_item_size, item.len())));
        }
        Ok(())
    }
}

/// The operations of a `SubEtha.SensSender`.
#[psmethods]
impl SensSender {
    /// Sends one item, of anything up to the item size.
    pub fn send(&mut self, item: PsObject) -> PsResult<()> {
        let item = bytes(&item)?;
        self.fits(&item)?;
        self.inner.send_item(&item).map_err(|e| op_err("sending", e))
    }

    /// Sends a run of items in one call, which is the shape to reach
    /// for. Every item is checked for size before any is sent, so a run
    /// with one item too large sends none of them rather than stopping
    /// partway.
    pub fn send_many(&mut self, items: Vec<PsObject>) -> PsResult<u64> {
        let mut held = Vec::with_capacity(items.len());
        for item in &items {
            let item = bytes(item)?;
            self.fits(&item)?;
            held.push(item.to_vec());
        }
        for item in &held {
            self.inner.send_item(item).map_err(|e| op_err("sending", e))?;
        }
        Ok(held.len() as u64)
    }

    /// The address this sends from, which is worth reading when the
    /// port was left to the system to pick.
    pub fn local_addr(&self) -> PsResult<Endpoint> {
        self.inner.local_addr().map(Endpoint::of).map_err(|e| op_err("reading the address", e))
    }

    /// Which way of working out the extra traffic is in use now.
    pub fn code(&self) -> PsResult<SensCodeKind> {
        Ok(SensCodeKind::from_rust(self.inner.active_code()))
    }

    /// How many times the link has changed between the two.
    pub fn switches(&self) -> PsResult<u64> {
        Ok(self.inner.switches())
    }

    /// The share of what was sent that the reader reports never
    /// arrived, between zero and one, or `$null` before the reader has
    /// reported anything at all. `$null` and zero are different
    /// answers: nothing measured yet, and measured with nothing lost.
    /// This is the number the link watches when deciding to change
    /// code.
    pub fn loss(&self) -> PsResult<Option<f64>> {
        let measured = self.inner.raw_loss_estimate();
        // The Rust reports no sample yet as a negative share, which read
        // as a number would be a loss rate that cannot happen.
        Ok((measured >= 0.0).then_some(measured))
    }

    /// Datagrams sent on this link, which is more than the items,
    /// because the extra traffic is on the wire too.
    pub fn datagrams_sent(&self) -> PsResult<u64> {
        Ok(self.inner.raw_sent_recv().0)
    }

    /// Datagrams received on this link: the reader's reports.
    pub fn datagrams_received(&self) -> PsResult<u64> {
        Ok(self.inner.raw_sent_recv().1)
    }
}

/// Opens the sending end of a link from LocalHost and LocalPort to
/// PeerHost and PeerPort, carrying items of up to MaxItemSize bytes.
#[cmdlet(verb = "New", noun = "SubEthaSensSender", alias = "New-SESensSender", output = ["SubEtha.SensSender"])]
#[derive(Default)]
pub struct NewSubEthaSensSender {
    /// The host to send to.
    #[param(mandatory, position = 0)]
    pub peer_host: String,
    /// The port to send to.
    #[param(mandatory, position = 1)]
    pub peer_port: u16,
    /// The largest an item may be; the same at both ends.
    #[param(mandatory, position = 2)]
    pub max_item_size: u64,
    /// The host to send from; every address when absent.
    #[param]
    pub local_host: Option<String>,
    /// The port to send from; one the system picks when absent.
    #[param]
    pub local_port: Option<u16>,
    /// Pin the way of working out the extra traffic, instead of
    /// letting the link choose from the loss it measures.
    #[param]
    pub code: Option<SensCodeKind>,
}

impl Cmdlet for NewSubEthaSensSender {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let peer = socket_addr_of(&self.peer_host, self.peer_port)?;
        let mut config = UnifiedConfig::new(symbol_len_for(self.max_item_size)?);
        config.policy = SensCodeKind::policy(self.code);
        let local_host = self.local_host.clone().unwrap_or_else(|| "0.0.0.0".to_string());
        let inner = UnifiedSensSender::connect((local_host.as_str(), self.local_port.unwrap_or(0)), peer, config).map_err(|e| op_err("opening the link", e))?;
        ps.write(SensSender { max_item_size: self.max_item_size, peer: Endpoint::of(peer), inner })
    }
}

/// The reading end of a link across a network that keeps working as
/// the network gets worse.
///
/// Items do not arrive one at a time. Poll drives the link and answers
/// everything it could rebuild this time round, which may be nothing,
/// one item, or a run of them held back while a lost piece was being
/// recovered. A reader calls it in a loop.
#[psclass(name = "SubEtha.SensReceiver", mode = proxy)]
pub struct SensReceiver {
    /// The largest an item may be on this link, which must be the one
    /// the sender was made with.
    pub max_item_size: u64,
    #[psfield(skip)]
    inner: UnifiedSensReceiver,
}

/// The operations of a `SubEtha.SensReceiver`.
#[psmethods]
impl SensReceiver {
    /// Drives the link and answers every item it could rebuild this
    /// time round. An empty answer is ordinary and means nothing was
    /// ready, not that anything is wrong.
    pub fn poll(&mut self) -> PsResult<Vec<PsObject>> {
        let items = self.inner.poll().map_err(|e| op_err("reading", e))?;
        items.iter().map(|item| out_bytes(item)).collect()
    }

    /// As Poll, and also which sender each item came from, for a reader
    /// taking from several at once.
    pub fn poll_from(&mut self) -> PsResult<Vec<SourcedItem>> {
        let items = self.inner.poll_from().map_err(|e| op_err("reading", e))?;
        items.iter().map(|(source, item)| Ok(SourcedItem { source: *source, bytes: out_bytes(item)? })).collect()
    }

    /// The address this listens on, which is worth reading when the
    /// port was left to the system to pick.
    pub fn local_addr(&self) -> PsResult<Endpoint> {
        self.inner.local_addr().map(Endpoint::of).map_err(|e| op_err("reading the address", e))
    }

    /// Which way of working out the extra traffic is in use now.
    pub fn code(&self) -> PsResult<SensCodeKind> {
        Ok(SensCodeKind::from_rust(self.inner.active_code()))
    }

    /// How many times the link has changed between the two.
    pub fn switches(&self) -> PsResult<u64> {
        Ok(self.inner.switches())
    }

    /// Answers this end owed a sender and could not send: feedback,
    /// path checks, handshakes. Each one leaves a sender waiting, so a
    /// link that has gone quiet is worth reading this on.
    pub fn send_failures(&self) -> PsResult<u64> {
        Ok(self.inner.send_failures())
    }

    /// Whether the thread reading the socket is still running. A
    /// reader whose thread has stopped looks exactly like a healthy
    /// process nobody is sending to, which is why this is worth asking.
    pub fn alive(&self) -> PsResult<bool> {
        Ok(self.inner.demux_alive())
    }
}

/// Opens the reading end of a link on LocalHost and LocalPort, carrying
/// items of up to MaxItemSize bytes, which must be what the sender was
/// made with.
#[cmdlet(verb = "New", noun = "SubEthaSensReceiver", alias = "New-SESensReceiver", output = ["SubEtha.SensReceiver"])]
#[derive(Default)]
pub struct NewSubEthaSensReceiver {
    /// The port to listen on; zero lets the system pick one, which
    /// LocalAddr then reports.
    #[param(mandatory, position = 0)]
    pub local_port: u16,
    /// The largest an item may be; the same at both ends.
    #[param(mandatory, position = 1)]
    pub max_item_size: u64,
    /// The host to listen on; every address when absent.
    #[param]
    pub local_host: Option<String>,
    /// Pin the way of working out the extra traffic, instead of
    /// letting the link choose from the loss it measures.
    #[param]
    pub code: Option<SensCodeKind>,
}

impl Cmdlet for NewSubEthaSensReceiver {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let mut config = UnifiedConfig::new(symbol_len_for(self.max_item_size)?);
        config.policy = SensCodeKind::policy(self.code);
        let local_host = self.local_host.clone().unwrap_or_else(|| "0.0.0.0".to_string());
        let inner = UnifiedSensReceiver::bind((local_host.as_str(), self.local_port), config).map_err(|e| op_err("opening the link", e))?;
        ps.write(SensReceiver { max_item_size: self.max_item_size, inner })
    }
}

/// A measurement a sensor can use: a real number, not negative.
fn measurement(value: f64, what: &str) -> PsResult<f64> {
    if !value.is_finite() || value < 0.0 {
        return Err(arg_err(format!("{what} is a number of microseconds that is not negative")));
    }
    Ok(value)
}

/// What a loss was.
#[psenum(name = "SubEtha.LossClass")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum LossClassKind {
    /// The air was noisy; recover locally and do not slow down.
    #[default]
    Wireless,
    /// A queue overflowed; raise redundancy and slow the sender.
    Congestion,
}

/// Tells a loss that happened because the air was noisy from one that
/// happened because a queue overflowed.
///
/// The two want opposite responses. A wireless drop should be recovered
/// locally and must not be read as congestion, or the sender slows down
/// for no reason. A congestion drop should raise redundancy and slow
/// the sender. Telling them apart is done from the spacing between
/// arrivals: a gap at or above a quarter more than the smallest spacing
/// seen is queuing, and anything narrower is noise.
#[psclass(name = "SubEtha.LossKind", mode = proxy)]
pub struct LossKind {
    #[psfield(skip)]
    inner: LossClassSensor,
}

/// The operations of a `SubEtha.LossKind`.
#[psmethods]
impl LossKind {
    /// Feeds the spacing between two arrivals, in microseconds.
    pub fn observe_spacing(&mut self, microseconds: f64) -> PsResult<()> {
        self.inner.observe_interarrival(measurement(microseconds, "a spacing")?);
        Ok(())
    }

    /// Feeds a one-way delay, in microseconds.
    pub fn observe_delay(&mut self, microseconds: f64) -> PsResult<()> {
        self.inner.observe_owd(measurement(microseconds, "a delay")?);
        Ok(())
    }

    /// What a loss of `gap` items with this spacing was.
    pub fn classify(&mut self, gap: u32, spacing_microseconds: f64) -> PsResult<LossClassKind> {
        Ok(match self.inner.classify(gap, measurement(spacing_microseconds, "a spacing")?) {
            LossClass::Wireless => LossClassKind::Wireless,
            LossClass::Congestion => LossClassKind::Congestion,
        })
    }

    /// The share of recent losses that were congestion, between zero
    /// and one.
    pub fn congestion_share(&self) -> PsResult<f32> {
        Ok(self.inner.congestion_fraction())
    }

    /// The spread of recent delays, in microseconds.
    pub fn delay_spread(&self) -> PsResult<f64> {
        Ok(self.inner.recent_owd_spread_us())
    }
}

/// Builds a loss classifier.
#[cmdlet(verb = "New", noun = "SubEthaLossKind", alias = "New-SELossKind", output = ["SubEtha.LossKind"])]
#[derive(Default)]
pub struct NewSubEthaLossKind {}

impl Cmdlet for NewSubEthaLossKind {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        ps.write(LossKind { inner: LossClassSensor::new() })
    }
}

/// The two rates a burst model fits.
#[psclass(name = "SubEtha.BurstRates")]
#[derive(Clone, Default)]
pub struct BurstRates {
    /// The rate of entering a run of losses.
    pub entering: f64,
    /// The rate of leaving one.
    pub leaving: f64,
}

/// Whether losses arrive alone or in runs, and how long a run lasts.
///
/// A link that loses one item at a time and a link that loses twenty
/// together can lose the same share overall and need entirely different
/// redundancy. This fits a two-state model to what it is fed and reports
/// the average length of a run.
#[psclass(name = "SubEtha.LossBursts", mode = proxy)]
pub struct LossBursts {
    #[psfield(skip)]
    inner: BurstModel,
}

/// The operations of a `SubEtha.LossBursts`.
#[psmethods]
impl LossBursts {
    /// Feeds one item: true when it was lost.
    pub fn observe(&mut self, lost: bool) -> PsResult<()> {
        self.inner.observe(lost);
        Ok(())
    }

    /// Feeds a run of items in one call.
    pub fn observe_many(&mut self, losses: Vec<bool>) -> PsResult<u64> {
        for lost in &losses {
            self.inner.observe(*lost);
        }
        Ok(losses.len() as u64)
    }

    /// How many items a run of losses lasts on average, or `$null`
    /// before there is enough to say.
    pub fn mean_run_length(&self) -> PsResult<Option<f64>> {
        Ok(self.inner.mean_burst_len())
    }

    /// The share lost once the runs are accounted for, or `$null`
    /// before there is enough to say.
    pub fn steady_loss(&self) -> PsResult<Option<f64>> {
        Ok(self.inner.steady_loss())
    }

    /// The two rates the model fits, or `$null` before there is enough
    /// to say.
    pub fn transition_rates(&self) -> PsResult<Option<BurstRates>> {
        Ok(self.inner.fit().map(|(entering, leaving)| BurstRates { entering, leaving }))
    }

    /// How many items have been fed.
    pub fn samples(&self) -> PsResult<u64> {
        Ok(self.inner.samples())
    }
}

/// Builds a burst model.
#[cmdlet(verb = "New", noun = "SubEthaLossBursts", alias = "New-SELossBursts", output = ["SubEtha.LossBursts"])]
#[derive(Default)]
pub struct NewSubEthaLossBursts {}

impl Cmdlet for NewSubEthaLossBursts {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        ps.write(LossBursts { inner: BurstModel::new() })
    }
}

/// Jitter, spacing and whether delay is trending up.
///
/// Fed a send time and a receive time for each item, it reports the
/// variation between arrivals, the average spacing, and whether the
/// one-way delay is climbing, which is a queue filling before it
/// overflows. The two clocks need not agree: a trend is a change over
/// time, and a constant difference between two clocks cancels out of
/// it, which is what TrendDebiased makes explicit.
#[psclass(name = "SubEtha.Timing", mode = proxy)]
pub struct Timing {
    /// How many recent items the answers are taken over.
    pub window: u64,
    #[psfield(skip)]
    inner: TemporalSensor,
}

/// The operations of a `SubEtha.Timing`.
#[psmethods]
impl Timing {
    /// Feeds one item's send and receive times, in microseconds. The
    /// two clocks need not agree with each other.
    pub fn observe(&mut self, sent: u64, received: u64) -> PsResult<()> {
        self.inner.observe(sent, received);
        Ok(())
    }

    /// The variation between arrivals, in microseconds.
    pub fn jitter(&self) -> PsResult<f64> {
        Ok(self.inner.jitter_micros())
    }

    /// The average spacing between arrivals, in microseconds.
    pub fn spacing(&self) -> PsResult<f64> {
        Ok(self.inner.interarrival_micros())
    }

    /// Whether one-way delay is climbing. Above zero is a queue
    /// filling.
    pub fn trend(&self) -> PsResult<f64> {
        Ok(self.inner.owd_trend())
    }

    /// The same trend with a steady difference between the two clocks
    /// taken out, which is the one to read when the clocks are not
    /// synchronized.
    pub fn trend_debiased(&self) -> PsResult<f64> {
        Ok(self.inner.owd_trend_debiased())
    }

    /// How far the two clocks differ, in microseconds.
    pub fn clock_skew(&self) -> PsResult<f64> {
        Ok(self.inner.skew())
    }

    /// How many items have been fed.
    pub fn samples(&self) -> PsResult<u64> {
        Ok(self.inner.samples() as u64)
    }
}

/// Builds a timing sensor over a Window of recent items, sixty-four
/// when absent.
#[cmdlet(verb = "New", noun = "SubEthaTiming", alias = "New-SETiming", output = ["SubEtha.Timing"])]
#[derive(Default)]
pub struct NewSubEthaTiming {
    /// How many recent items the answers are taken over, at least two.
    #[param]
    pub window: Option<u64>,
}

impl Cmdlet for NewSubEthaTiming {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let window = self.window.unwrap_or(64);
        if window < 2 {
            return Err(arg_err("a window covers at least two items"));
        }
        ps.write(Timing { window, inner: TemporalSensor::new(size(window, "the window")?) })
    }
}

/// Whether round trips fall into two groups, which is what a wireless
/// link looks like.
///
/// A wired link's round trips cluster around one value. A wireless one
/// often has two: the quick ones and the ones that waited for a retry
/// in the radio. Two groups is evidence of the second.
#[psclass(name = "SubEtha.RoundTripShape", mode = proxy)]
pub struct RoundTripShape {
    #[psfield(skip)]
    inner: RttShape,
}

/// The operations of a `SubEtha.RoundTripShape`.
#[psmethods]
impl RoundTripShape {
    /// Feeds one round trip, in microseconds.
    pub fn observe(&mut self, microseconds: f64) -> PsResult<()> {
        self.inner.observe(measurement(microseconds, "a round trip")?);
        Ok(())
    }

    /// Feeds a run of round trips in one call.
    pub fn observe_many(&mut self, microseconds: Vec<f64>) -> PsResult<u64> {
        for one in &microseconds {
            measurement(*one, "a round trip")?;
        }
        for one in &microseconds {
            self.inner.observe(*one);
        }
        Ok(microseconds.len() as u64)
    }

    /// How strongly the round trips fall into two groups, or `$null`
    /// before there is enough to say.
    pub fn two_groups(&self) -> PsResult<Option<f64>> {
        Ok(self.inner.bimodality())
    }

    /// How much this looks like a wireless link, between zero and one.
    pub fn wireless_confidence(&self) -> PsResult<f32> {
        Ok(self.inner.wifi_confidence())
    }

    /// How many round trips have been fed.
    pub fn samples(&self) -> PsResult<u64> {
        Ok(self.inner.samples())
    }
}

/// Builds a round-trip shape sensor.
#[cmdlet(verb = "New", noun = "SubEthaRoundTripShape", alias = "New-SERoundTripShape", output = ["SubEtha.RoundTripShape"])]
#[derive(Default)]
pub struct NewSubEthaRoundTripShape {}

impl Cmdlet for NewSubEthaRoundTripShape {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        ps.write(RoundTripShape { inner: RttShape::new() })
    }
}

/// A beat found in the delays: its length and how strong it is.
#[psclass(name = "SubEtha.Beat")]
#[derive(Clone, Default)]
pub struct Beat {
    /// The length of the beat, in seconds.
    pub seconds: f64,
    /// How strong the beat is.
    pub strength: f64,
}

/// Whether delay spikes on a regular beat, and when the next one is
/// due.
///
/// Some interference is periodic: a radio that scans on a schedule, a
/// neighbor's traffic that arrives in a rhythm. Finding the beat means
/// a sender can raise redundancy just before the next spike rather than
/// reacting after it.
#[psclass(name = "SubEtha.Periodicity", mode = proxy)]
pub struct Periodicity {
    #[psfield(skip)]
    inner: PeriodicitySensor,
}

/// The operations of a `SubEtha.Periodicity`.
#[psmethods]
impl Periodicity {
    /// Feeds one delay, in microseconds, and when it was taken, also in
    /// microseconds.
    pub fn observe(&mut self, delay_microseconds: f64, at_microseconds: u64) -> PsResult<()> {
        self.inner.observe(measurement(delay_microseconds, "a delay")?, at_microseconds);
        Ok(())
    }

    /// The beat found, or `$null` when there is no beat to find.
    pub fn period(&self) -> PsResult<Option<Beat>> {
        Ok(self.inner.detected_period().map(|(seconds, strength)| Beat { seconds, strength }))
    }

    /// Seconds until the next spike is due, or `$null` when there is no
    /// beat to go on.
    pub fn seconds_to_next(&self) -> PsResult<Option<f64>> {
        Ok(self.inner.secs_to_next_spike())
    }
}

/// Builds a periodicity sensor.
#[cmdlet(verb = "New", noun = "SubEthaPeriodicity", alias = "New-SEPeriodicity", output = ["SubEtha.Periodicity"])]
#[derive(Default)]
pub struct NewSubEthaPeriodicity {}

impl Cmdlet for NewSubEthaPeriodicity {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        ps.write(Periodicity { inner: PeriodicitySensor::new() })
    }
}

/// How much of the path's capacity is free, worked out from probes sent
/// in pairs and in trains.
///
/// Two probes sent back to back arrive spread apart by the narrowest
/// link on the path, which gives its capacity. A longer train arrives
/// spread by what is left after the traffic already there, which gives
/// what is available.
#[psclass(name = "SubEtha.Capacity", mode = proxy)]
pub struct Capacity {
    /// How big each probe is, in bytes.
    pub probe_bytes: u64,
    #[psfield(skip)]
    inner: WBestEstimator,
}

/// The operations of a `SubEtha.Capacity`.
#[psmethods]
impl Capacity {
    /// Feeds the arrival of one probe of a pair, numbered within the
    /// pair, in microseconds.
    pub fn observe_pair(&mut self, index: u8, arrived_microseconds: f64) -> PsResult<()> {
        self.inner.on_pair_probe(index, measurement(arrived_microseconds, "an arrival")?);
        Ok(())
    }

    /// Feeds the arrival of one probe of a train, in microseconds.
    pub fn observe_train(&mut self, arrived_microseconds: f64) -> PsResult<()> {
        self.inner.on_train_probe(measurement(arrived_microseconds, "an arrival")?);
        Ok(())
    }

    /// The narrowest link's capacity, in bits a second, or `$null`
    /// before there is enough to say.
    pub fn link_capacity(&self) -> PsResult<Option<f64>> {
        Ok(self.inner.effective_capacity_bps())
    }

    /// What is left after the traffic already on the path, in bits a
    /// second, or `$null` before there is enough to say.
    pub fn available(&self) -> PsResult<Option<f64>> {
        Ok(self.inner.available_bps())
    }

    /// The rate the train arrived at, in bits a second, or `$null`.
    pub fn train_rate(&self) -> PsResult<Option<f64>> {
        Ok(self.inner.train_rate_bps())
    }

    /// How many pairs have been fed.
    pub fn pair_samples(&self) -> PsResult<u64> {
        Ok(self.inner.samples().0 as u64)
    }

    /// How many train probes have been fed.
    pub fn train_samples(&self) -> PsResult<u32> {
        Ok(self.inner.samples().1)
    }

    /// Forgets everything and starts again, which is what a caller does
    /// when the path may have changed.
    pub fn reset(&mut self) -> PsResult<()> {
        self.inner.reset();
        Ok(())
    }
}

/// Builds a capacity estimator over probes of ProbeBytes, 1400 when
/// absent.
#[cmdlet(verb = "New", noun = "SubEthaCapacity", alias = "New-SECapacity", output = ["SubEtha.Capacity"])]
#[derive(Default)]
pub struct NewSubEthaCapacity {
    /// How big each probe is, in bytes.
    #[param]
    pub probe_bytes: Option<u64>,
}

impl Cmdlet for NewSubEthaCapacity {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let probe_bytes = self.probe_bytes.unwrap_or(1400);
        if probe_bytes < 1 {
            return Err(arg_err("a probe is at least one byte"));
        }
        ps.write(Capacity { probe_bytes, inner: WBestEstimator::new(size(probe_bytes, "the probe size")?) })
    }
}

/// What the next interval's traffic is likely to be, from what the last
/// ones were.
#[psclass(name = "SubEtha.Forecast", mode = proxy)]
pub struct Forecast {
    #[psfield(skip)]
    inner: ArrivalForecast,
}

/// The operations of a `SubEtha.Forecast`.
#[psmethods]
impl Forecast {
    /// Feeds one interval: how many bytes arrived and how long it was,
    /// in seconds.
    pub fn observe(&mut self, bytes: u64, seconds: f64) -> PsResult<()> {
        if !seconds.is_finite() || seconds <= 0.0 {
            return Err(arg_err("an interval is a number of seconds above zero"));
        }
        self.inner.observe(bytes, seconds);
        Ok(())
    }

    /// The average rate so far, in bits a second.
    pub fn mean_rate(&self) -> PsResult<f64> {
        Ok(self.inner.mean_bps())
    }

    /// What the next interval is likely to carry, in bits a second.
    pub fn next_rate(&self) -> PsResult<f64> {
        Ok(self.inner.forecast_bps())
    }
}

/// Builds an arrival forecast.
#[cmdlet(verb = "New", noun = "SubEthaForecast", alias = "New-SEForecast", output = ["SubEtha.Forecast"])]
#[derive(Default)]
pub struct NewSubEthaForecast {}

impl Cmdlet for NewSubEthaForecast {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        ps.write(Forecast { inner: ArrivalForecast::new() })
    }
}

/// What the last item a path sensor saw carried.
#[psclass(name = "SubEtha.PathMark")]
#[derive(Clone, Default)]
pub struct PathMark {
    /// Its remaining time to live.
    pub ttl: u8,
    /// Its congestion marking.
    pub congestion_mark: u8,
    /// How many hops it took.
    pub hops: u8,
}

/// Whether the route changed under the traffic.
///
/// Every item carries how many hops it took and whether anything on the
/// way marked it as congested. A change in the hop count means the
/// route moved, which explains a sudden change in delay that would
/// otherwise look like congestion.
#[psclass(name = "SubEtha.PathChanges", mode = proxy)]
pub struct PathChanges {
    #[psfield(skip)]
    inner: PathSensor,
}

/// The operations of a `SubEtha.PathChanges`.
#[psmethods]
impl PathChanges {
    /// Feeds one item: its remaining time to live, its congestion
    /// marking, and how many hops it took.
    pub fn observe(&mut self, ttl: u8, congestion_mark: u8, hops: u8) -> PsResult<()> {
        self.inner.observe(ttl, congestion_mark, hops);
        Ok(())
    }

    /// How much the route has been moving, between zero and one.
    pub fn route_movement(&self) -> PsResult<f32> {
        Ok(self.inner.path_shift())
    }

    /// The share of items something on the way marked as congested,
    /// between zero and one.
    pub fn marked_share(&self) -> PsResult<f32> {
        Ok(self.inner.ecn_ce())
    }

    /// What the last item carried, or `$null` when nothing has been
    /// fed.
    pub fn last(&self) -> PsResult<Option<PathMark>> {
        Ok(self.inner.last().map(|(ttl, congestion_mark, hops)| PathMark { ttl, congestion_mark, hops }))
    }
}

/// Builds a path sensor.
#[cmdlet(verb = "New", noun = "SubEthaPathChanges", alias = "New-SEPathChanges", output = ["SubEtha.PathChanges"])]
#[derive(Default)]
pub struct NewSubEthaPathChanges {}

impl Cmdlet for NewSubEthaPathChanges {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        ps.write(PathChanges { inner: PathSensor::new() })
    }
}
