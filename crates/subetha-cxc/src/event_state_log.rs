//! `EventStateLog<Event, State>` - event-sourced state with
//! materialized view.
//!
//! Composes [`SharedRing`] (the durable event log)
//! with [`SharedCell`] (the materialized current
//! state). Producers `emit` events onto the ring; consumers
//! `drain_and_fold` to advance the materialized view; any process
//! can `read_current` at constant-time for the latest state
//! snapshot.
//!
//! # Architectural pattern
//!
//! This is the CQRS / event-sourcing shape used by Kafka +
//! materialized views, EventStore + projections, Akka Persistence -
//! lifted to shared memory at lock-free MMF cost. The ring file IS
//! the durable event log (flush() syncs to disk); the cell IS the
//! current-state cache.
//!
//! # Two files per log
//!
//! - `<base>.events.bin` - the SharedRing
//! - `<base>.state.bin`  - the SharedCell holding State
//!
//! Pass the BASE PATH (without extension) to `create` / `open`;
//! the wrapper appends the extensions.

use std::marker::PhantomData;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::shared_cell::{SharedCell, SharedCellError, PAYLOAD_BYTES as CELL_PAYLOAD};
use crate::shared_ring::{RingError, SharedRing, PAYLOAD_BYTES as RING_PAYLOAD};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventLogError {
    Cell(SharedCellError),
    Ring(RingError),
    EventTooLarge,
    StateTooLarge,
}

impl From<SharedCellError> for EventLogError {
    fn from(e: SharedCellError) -> Self { Self::Cell(e) }
}
impl From<RingError> for EventLogError {
    fn from(e: RingError) -> Self { Self::Ring(e) }
}

fn events_path(base: &Path) -> PathBuf {
    let mut p = base.to_path_buf();
    let stem = p.file_name().unwrap().to_string_lossy().to_string();
    p.set_file_name(format!("{stem}.events.bin"));
    p
}

fn state_path(base: &Path) -> PathBuf {
    let mut p = base.to_path_buf();
    let stem = p.file_name().unwrap().to_string_lossy().to_string();
    p.set_file_name(format!("{stem}.state.bin"));
    p
}

pub struct EventStateLog<Event: Copy + 'static, State: Copy + 'static> {
    ring: Arc<SharedRing>,
    state: Arc<SharedCell<State>>,
    _phantom_e: PhantomData<Event>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

impl<Event: Copy + Send + Sync + 'static, State: Copy + Send + Sync + 'static>
    subetha_sidecar::AdaptiveInstance for EventStateLog<Event, State>
{
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl<Event: Copy + 'static, State: Copy + 'static> EventStateLog<Event, State> {
    pub fn create(
        base_path: impl AsRef<Path>,
        ring_capacity: usize,
        initial_state: State,
    ) -> Result<Self, EventLogError> {
        if size_of::<Event>() > RING_PAYLOAD { return Err(EventLogError::EventTooLarge); }
        if size_of::<State>() > CELL_PAYLOAD { return Err(EventLogError::StateTooLarge); }
        let base = base_path.as_ref();
        let ring = Arc::new(SharedRing::create(events_path(base), ring_capacity)?);
        let state = Arc::new(SharedCell::<State>::create(state_path(base))?);
        state.set(initial_state);
        Ok(Self {
            ring, state, _phantom_e: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(
        base_path: impl AsRef<Path>,
        ring_capacity: usize,
    ) -> Result<Self, EventLogError> {
        if size_of::<Event>() > RING_PAYLOAD { return Err(EventLogError::EventTooLarge); }
        if size_of::<State>() > CELL_PAYLOAD { return Err(EventLogError::StateTooLarge); }
        let base = base_path.as_ref();
        let ring = Arc::new(SharedRing::open(events_path(base), ring_capacity)?);
        let state = Arc::new(SharedCell::<State>::open(state_path(base))?);
        Ok(Self {
            ring, state, _phantom_e: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Push an event onto the durable log. Returns `Err(Ring(Full))`
    /// when the ring is full; callers should drain or apply
    /// backpressure.
    pub fn emit(&self, event: Event) -> Result<(), EventLogError> {
        let bytes: [u8; RING_PAYLOAD] = {
            let mut buf = [0u8; RING_PAYLOAD];
            // SAFETY: Event is Copy + Sized, size_of::<Event>() <= RING_PAYLOAD
            // (checked at create/open). We memcpy the event's bytes
            // into the ring slot's payload region.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    &event as *const Event as *const u8,
                    buf.as_mut_ptr(),
                    size_of::<Event>(),
                );
            }
            buf
        };
        let r = self.ring.try_push(&bytes);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::event_log::OP_EMIT,
            if r.is_err() { 1 } else { 0 },
        );
        r?;
        Ok(())
    }

    /// Drain all pending events from the log and apply each to the
    /// current state via `fold`. The state cell is updated after
    /// all events are folded. Returns the number of events applied.
    pub fn drain_and_fold<F: FnMut(&mut State, &Event)>(
        &self,
        mut fold: F,
    ) -> usize {
        let mut state = self.state.get();
        let mut count = 0;
        let mut buf = [0u8; RING_PAYLOAD];
        loop {
            match self.ring.try_pop(&mut buf) {
                Ok(_) => {
                    // SAFETY: bytes were pushed by emit() with the
                    // same Event layout.
                    let event: Event = unsafe {
                        std::ptr::read_unaligned(buf.as_ptr() as *const Event)
                    };
                    fold(&mut state, &event);
                    count += 1;
                }
                Err(RingError::Empty) => break,
                Err(_) => break,
            }
        }
        if count > 0 {
            self.state.set(state);
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::event_log::OP_DRAIN_FOLD, 0);
        count
    }

    /// Read the current materialized state. O(1) - one SeqLock cell read.
    pub fn read_current(&self) -> State {
        let s = self.state.get();
        self.ring_sidecar
            .push_op(crate::sidecar_ops::event_log::OP_READ_CURRENT, 0);
        s
    }

    /// Approximate number of events waiting in the log.
    pub fn pending_events(&self) -> usize {
        self.ring.approx_len()
    }

    /// Force-set the materialized state (e.g., for checkpoint restore).
    pub fn set_state(&self, state: State) {
        self.state.set(state);
    }

    /// Sync both files to disk.
    pub fn flush(&self) -> Result<(), EventLogError> {
        self.ring.flush()?;
        self.state.flush()?;
        Ok(())
    }

    /// Non-blocking flush of both files. Delegates to the ring and
    /// the state cell's flush_async.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), EventLogError> {
        self.ring.flush_async()?;
        self.state.flush_async()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    fn tmp_base(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-eventlog-{name}-{pid}"));
        p
    }

    fn cleanup(base: &Path) {
        std::fs::remove_file(events_path(base)).ok();
        std::fs::remove_file(state_path(base)).ok();
    }

    #[test]
    fn emit_drain_fold_round_trip() {
        let base = tmp_base("rt");
        let log: EventStateLog<u32, u32> = EventStateLog::create(&base, 16, 0).unwrap();
        log.emit(10).unwrap();
        log.emit(20).unwrap();
        log.emit(30).unwrap();
        assert_eq!(log.pending_events(), 3);
        let n = log.drain_and_fold(|s, e| *s += *e);
        assert_eq!(n, 3);
        assert_eq!(log.read_current(), 60);
        assert_eq!(log.pending_events(), 0);
        cleanup(&base);
    }

    #[test]
    fn initial_state_is_visible_before_any_emit() {
        let base = tmp_base("initial");
        let log: EventStateLog<u32, u64> = EventStateLog::create(&base, 8, 1234).unwrap();
        assert_eq!(log.read_current(), 1234);
        cleanup(&base);
    }

    #[test]
    fn cross_handle_emit_and_drain() {
        let base = tmp_base("cross-handle");
        let producer: EventStateLog<u32, u32> = EventStateLog::create(&base, 16, 0).unwrap();
        let consumer: EventStateLog<u32, u32> = EventStateLog::open(&base, 16).unwrap();
        producer.emit(5).unwrap();
        producer.emit(7).unwrap();
        producer.emit(11).unwrap();
        // Consumer drains and folds independently of producer.
        let n = consumer.drain_and_fold(|s, e| *s += *e);
        assert_eq!(n, 3);
        assert_eq!(consumer.read_current(), 23);
        // Producer reads the consumer's update.
        assert_eq!(producer.read_current(), 23);
        cleanup(&base);
    }

    #[test]
    fn drain_with_empty_log_returns_zero() {
        let base = tmp_base("empty-drain");
        let log: EventStateLog<u32, u32> = EventStateLog::create(&base, 8, 100).unwrap();
        let n = log.drain_and_fold(|_, _| panic!("must not run on empty"));
        assert_eq!(n, 0);
        assert_eq!(log.read_current(), 100);  // unchanged
        cleanup(&base);
    }

    #[test]
    fn full_ring_returns_error_on_emit() {
        let base = tmp_base("full");
        let log: EventStateLog<u32, u32> = EventStateLog::create(&base, 4, 0).unwrap();
        for i in 0..4u32 { log.emit(i).unwrap(); }
        match log.emit(99) {
            Err(EventLogError::Ring(RingError::Full)) => {}
            other => panic!("expected Ring(Full), got {other:?}"),
        }
        cleanup(&base);
    }

    #[test]
    fn concurrent_producers_drain_correctly() {
        let base = tmp_base("concurrent");
        let log: Arc<EventStateLog<u32, u64>>
            = Arc::new(EventStateLog::create(&base, 256, 0).unwrap());
        let n_threads = 4;
        let per_thread = 50u32;
        let total = (n_threads as u32) * per_thread;
        let mut handles = vec![];
        for _ in 0..n_threads {
            let log = log.clone();
            handles.push(thread::spawn(move || {
                for i in 0..per_thread {
                    while log.emit(i).is_err() {
                        std::hint::spin_loop();
                    }
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        // Single-threaded drain to count.
        let n = log.drain_and_fold(|s, e| *s += *e as u64);
        assert_eq!(n, total as usize);
        // Sum = N * sum(0..per_thread)
        let expected = (n_threads as u64) * (0u64..per_thread as u64).sum::<u64>();
        assert_eq!(log.read_current(), expected);
        cleanup(&base);
    }

    #[test]
    fn disk_persistence_state_survives_reopen() {
        let base = tmp_base("disk");
        {
            let log: EventStateLog<u32, u32> = EventStateLog::create(&base, 8, 0).unwrap();
            log.emit(10).unwrap();
            log.emit(20).unwrap();
            log.drain_and_fold(|s, e| *s += *e);
            log.flush().unwrap();
        }
        let log2: EventStateLog<u32, u32> = EventStateLog::open(&base, 8).unwrap();
        assert_eq!(log2.read_current(), 30);
        cleanup(&base);
    }

    #[test]
    fn set_state_overrides_for_checkpoint_restore() {
        let base = tmp_base("set-state");
        let log: EventStateLog<u32, u32> = EventStateLog::create(&base, 8, 0).unwrap();
        log.emit(5).unwrap();
        log.drain_and_fold(|s, e| *s += *e);
        assert_eq!(log.read_current(), 5);
        log.set_state(9999);
        assert_eq!(log.read_current(), 9999);
        cleanup(&base);
    }

    #[test]
    fn struct_event_and_state_round_trip() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        #[repr(C)]
        struct Event { delta_x: i32, delta_y: i32 }
        #[derive(Clone, Copy, Debug, PartialEq)]
        #[repr(C)]
        struct State { x: i32, y: i32 }
        let base = tmp_base("struct");
        let log: EventStateLog<Event, State> = EventStateLog::create(
            &base, 16, State { x: 0, y: 0 },
        ).unwrap();
        log.emit(Event { delta_x: 3, delta_y: 4 }).unwrap();
        log.emit(Event { delta_x: -1, delta_y: 2 }).unwrap();
        let n = log.drain_and_fold(|s, e| {
            s.x += e.delta_x;
            s.y += e.delta_y;
        });
        assert_eq!(n, 2);
        assert_eq!(log.read_current(), State { x: 2, y: 6 });
        cleanup(&base);
    }
}
