//! The cmdlets that carry items through the pipeline: one that sends
//! whatever is piped in to a structure, and one that reads a structure
//! out into the pipeline.
//!
//! Each takes the structure as an object the module returned and calls
//! that object's own methods through the engine, so the same two
//! cmdlets serve every structure that moves items. That dispatch costs
//! a binder call per item on top of the method, which is small beside
//! the pipeline's own per-record cost; a script moving bulk items calls
//! the object's many-item methods directly.

use pwrs::prelude::*;

use crate::common::arg_err;

/// How a structure moves items, read from its type name once so each
/// item costs one method call.
#[derive(Clone, Copy)]
enum Mover {
    /// `Push(item)` and `Pop()`.
    PushPop,
    /// `Send(item)` and `Recv()`.
    SendRecv,
    /// `Send(producer, item)` and `Recv(consumer)`.
    SendRecvNumbered,
    /// `Push(item)` and `Recv(consumer)`: a broadcast ring's producer is
    /// unnumbered and its consumers are numbered.
    Broadcast,
    /// `Publish(item)`; reading needs a subscriber, whose `Next()`.
    Publish,
    /// `Next()` only.
    Next,
    /// `Steal()` only, or `Pop()` for the owner.
    Deque,
}

impl Mover {
    fn of(type_name: &str) -> PsResult<Self> {
        Ok(match type_name {
            "SubEtha.SpscRing" | "SubEtha.Stack" | "SubEtha.LamportProducer" | "SubEtha.LamportConsumer" | "SubEtha.MpscProducer"
            | "SubEtha.MpscConsumer" | "SubEtha.MpmcProducer" | "SubEtha.MpmcConsumer" | "SubEtha.WorkQueue" => Mover::PushPop,
            "SubEtha.Channel" | "SubEtha.AdaptiveQueue" | "SubEtha.SensSender" => Mover::SendRecv,
            "SubEtha.Ring" | "SubEtha.CapacityRing" | "SubEtha.LocaleRing" => Mover::SendRecvNumbered,
            "SubEtha.BroadcastRing" => Mover::Broadcast,
            "SubEtha.PubSub" => Mover::Publish,
            "SubEtha.Subscriber" => Mover::Next,
            "SubEtha.Deque" => Mover::Deque,
            other => return Err(arg_err(format!("{other} is not a structure that moves items"))),
        })
    }
}

/// Sends each item piped in to To, a structure the module returned, and
/// writes each item the structure refused, so a full structure hands
/// the item back rather than dropping it. A ring that numbers its
/// producers takes the id in Producer.
///
/// # Examples
///
/// `Get-Content .\lines.txt | Send-SubEthaItem -To $ring -Producer $producer`
#[cmdlet(verb = "Send", noun = "SubEthaItem", alias = "Send-SEItem", output = ["System.Object"])]
#[derive(Default)]
pub struct SendSubEthaItem {
    /// The item, a `byte[]` or a string.
    #[param(mandatory, position = 0, value_from_pipeline)]
    pub item: PsObject,
    /// The structure to send to.
    #[param(mandatory)]
    pub to: PsObject,
    /// The producer id, for a ring that numbers its producers; zero
    /// when absent.
    #[param]
    pub producer: Option<u64>,
    mover: Option<Mover>,
}

impl Cmdlet for SendSubEthaItem {
    fn begin(&mut self, _ps: &Pipeline<'_>) -> PsResult<()> {
        let mover = Mover::of(&self.to.type_name()?)?;
        if matches!(mover, Mover::Next) {
            return Err(arg_err("a subscriber only reads; send to the SubEtha.PubSub it came from").terminating());
        }
        self.mover = Some(mover);
        Ok(())
    }

    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let mover = match self.mover {
            Some(m) => m,
            None => Mover::of(&self.to.type_name()?)?,
        };
        let item = self.item.clone();
        let accepted = match mover {
            Mover::PushPop | Mover::Deque => self.to.call("Push", &[item])?,
            Mover::SendRecv => self.to.call("Send", &[item])?,
            Mover::SendRecvNumbered => {
                let producer = self.producer.unwrap_or(0).into_ps()?;
                self.to.call("Send", &[producer, item])?
            }
            Mover::Broadcast => self.to.call("Push", &[item])?,
            Mover::Publish => {
                self.to.call("Publish", &[item])?;
                return Ok(());
            }
            Mover::Next => return Ok(()),
        };
        // A method that answers nothing accepted the item outright; one
        // that answers false is a full structure, whose item goes back
        // to the pipeline for the caller to keep.
        if !accepted.is_null() && !bool::from_ps(&accepted)? {
            ps.write_object(&self.item)?;
        }
        Ok(())
    }
}

/// Reads items out of From, a structure the module returned, into the
/// pipeline: everything waiting, or up to Count of them, stopping when
/// the structure runs empty. A ring that numbers its consumers takes
/// the id in Consumer; a deque is stolen from unless Owner is given.
///
/// # Examples
///
/// `Receive-SubEthaItem -From $ring -Consumer $consumer -Count 10`
#[cmdlet(verb = "Receive", noun = "SubEthaItem", alias = "Receive-SEItem", output = ["System.Byte[]"])]
#[derive(Default)]
pub struct ReceiveSubEthaItem {
    /// The structure to read from.
    #[param(mandatory, position = 0)]
    pub from: PsObject,
    /// How many items to read at most; everything waiting when absent.
    #[param]
    pub count: Option<u64>,
    /// The consumer id, for a ring that numbers its consumers; zero
    /// when absent.
    #[param]
    pub consumer: Option<u64>,
    /// Read a deque or work queue at the owner's end rather than
    /// stealing.
    #[param]
    pub owner: bool,
}

impl Cmdlet for ReceiveSubEthaItem {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let type_name = self.from.type_name()?;
        let mover = Mover::of(&type_name)?;
        let limit = self.count.unwrap_or(u64::MAX);
        let mut taken = 0;
        while taken < limit && !ps.stopping() {
            let item = match mover {
                Mover::PushPop => self.from.call("Pop", &[])?,
                Mover::Deque => {
                    if self.owner {
                        self.from.call("Pop", &[])?
                    } else {
                        self.from.call("Steal", &[])?
                    }
                }
                Mover::SendRecv => self.from.call("Recv", &[])?,
                Mover::SendRecvNumbered | Mover::Broadcast => {
                    self.from.call("Recv", &[self.consumer.unwrap_or(0).into_ps()?])?
                }
                Mover::Next => self.from.call("Next", &[])?,
                Mover::Publish => return Err(arg_err("a SubEtha.PubSub is read through a subscriber; receive from its Subscribe()").terminating()),
            };
            if item.is_null() {
                break;
            }
            ps.write_object(&item)?;
            taken += 1;
        }
        Ok(())
    }
}
