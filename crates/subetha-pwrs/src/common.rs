//! What every family shares: the error shapes, path resolution against
//! the session's location, the byte payloads that cross as arrays, and
//! the small records methods hand back where Rust would use a tuple.

use std::ops::Deref;
use std::time::Duration;

use pwrs::prelude::*;
use pwrs::Pinned;

/// The error for a structure that could not be obtained at `path`.
pub(crate) fn open_err(what: &str, path: &str, e: impl std::fmt::Debug) -> PsError {
    PsError::new(ErrorCategory::OpenError, "SubEthaOpen", format!("cannot obtain {what} at {path}: {e:?}"))
}

/// The error for an operation the structure refused or could not do.
pub(crate) fn op_err(what: &str, e: impl std::fmt::Debug) -> PsError {
    PsError::new(ErrorCategory::InvalidOperation, "SubEthaOperation", format!("{what}: {e:?}"))
}

/// The error for an argument the structure cannot take.
pub(crate) fn arg_err(message: impl Into<String>) -> PsError {
    PsError::new(ErrorCategory::InvalidArgument, "SubEthaArgument", message.into())
}

/// The provider path `path` names, relative to the session's current
/// location rather than the process's working directory, whether or
/// not the file exists yet.
pub(crate) fn full_path(ps: &Pipeline<'_>, path: &str) -> PsResult<String> {
    let mut resolved = ps.resolve_path(path, true)?;
    match resolved.pop() {
        Some(p) => Ok(p),
        None => Err(arg_err(format!("{path} names no path"))),
    }
}

/// The bytes of an argument: a `byte[]` borrowed where it lies, or the
/// UTF-8 of a string, so a script can pass either.
pub(crate) enum Bytes<'a> {
    Pinned(Pinned<'a, u8>),
    Owned(Vec<u8>),
}

impl Deref for Bytes<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            Bytes::Pinned(p) => &p[..],
            Bytes::Owned(v) => v,
        }
    }
}

/// Borrows `obj` as bytes. A `byte[]` is pinned in place and copied
/// nowhere; a string is encoded; any other array of numbers, which is
/// what a script's `1, 2, 3` or an unrolled pipeline hands over, is
/// converted element by element; anything else is refused by name.
pub(crate) fn bytes(obj: &PsObject) -> PsResult<Bytes<'_>> {
    if obj.is_null() {
        return Err(arg_err("the bytes are null; pass a byte[] or a string"));
    }
    match obj.pin::<u8>() {
        Ok(pinned) => Ok(Bytes::Pinned(pinned)),
        Err(not_bytes) => {
            let name = obj.type_name()?;
            if name == "System.String" {
                return Ok(Bytes::Owned(String::from_ps(obj)?.into_bytes()));
            }
            if name.ends_with("[]") || name.contains("Collections") {
                return match Vec::<u8>::from_ps(obj) {
                    Ok(converted) => Ok(Bytes::Owned(converted)),
                    Err(e) => Err(arg_err(format!("a {name} is not bytes: {e}"))),
                };
            }
            Err(arg_err(format!("a {name} is not a byte[] or a string: {not_bytes}")))
        }
    }
}

/// A `byte[]` holding `data`, filled through one pin.
pub(crate) fn out_bytes(data: &[u8]) -> PsResult<PsObject> {
    PsObject::from_slice(data)
}

/// A timeout in seconds as a duration; a negative one is refused.
pub(crate) fn seconds(timeout: f64) -> PsResult<Duration> {
    if !(timeout >= 0.0) || !timeout.is_finite() {
        return Err(arg_err("the timeout must be a non-negative number of seconds"));
    }
    Ok(Duration::from_secs_f64(timeout))
}

/// A count or size argument as the `usize` the structure takes.
pub(crate) fn size(value: u64, what: &str) -> PsResult<usize> {
    usize::try_from(value).map_err(|_| arg_err(format!("{what} is more than this platform addresses")))
}

/// A capacity that must be a power of two and at least two, which the
/// ring arithmetic relies on.
pub(crate) fn power_of_two(capacity: u64) -> PsResult<usize> {
    if !capacity.is_power_of_two() || capacity < 2 {
        return Err(arg_err("the capacity must be a power of two and at least two"));
    }
    size(capacity, "the capacity")
}

/// The process id an argument names, this process's own when absent.
pub(crate) fn pid(pid: Option<u32>) -> u32 {
    pid.unwrap_or_else(std::process::id)
}

/// Fails the build when a class's value cannot leave the thread it was
/// made on, because the runtime frees a collected object's value from
/// the finalizer thread.
macro_rules! assert_send {
    ($($ty:ty),* $(,)?) => {
        $( const _: fn() = || { fn needs_send<T: Send>() {} needs_send::<$ty>(); }; )*
    };
}
pub(crate) use assert_send;

/// An item and the stamp its sender gave it.
#[psclass(name = "SubEtha.StampedItem")]
#[derive(Clone, Default)]
pub struct StampedItem {
    /// The stamp, which orders the item among everything its sender
    /// and the other senders made.
    pub stamp: u64,
    /// The item's bytes, a `byte[]`.
    pub bytes: PsObject,
}

impl StampedItem {
    pub(crate) fn new(stamp: u64, data: &[u8]) -> PsResult<Self> {
        Ok(Self { stamp, bytes: out_bytes(data)? })
    }
}

/// What the families holding a fixed-size value put in one of their
/// slots: a length and the bytes.
///
/// These families are generic over any value a Rust caller can copy,
/// and a script has no such type to offer. Bytes are the one thing
/// every caller has, and the length travels with them because a slot
/// is a fixed size: without it a value ending in zero bytes would come
/// back indistinguishable from a shorter one padded out.
#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct Payload<const N: usize> {
    len: u32,
    bytes: [u8; N],
}

impl<const N: usize> Default for Payload<N> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<const N: usize> Payload<N> {
    pub(crate) const MAX: usize = N;

    pub(crate) fn empty() -> Self {
        Self { len: 0, bytes: [0; N] }
    }

    pub(crate) fn from_bytes(value: &[u8]) -> PsResult<Self> {
        if value.len() > N {
            return Err(arg_err(format!("a value here is at most {N} bytes, got {}", value.len())));
        }
        let mut held = Self::empty();
        held.len = value.len() as u32;
        held.bytes[..value.len()].copy_from_slice(value);
        Ok(held)
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        let len = (self.len as usize).min(N);
        &self.bytes[..len]
    }

    /// The value as a `byte[]`.
    pub(crate) fn to_ps(&self) -> PsResult<PsObject> {
        out_bytes(self.as_bytes())
    }
}

/// A payload travels between processes as its length and then its
/// bytes, little-endian, which is the same in every address space.
///
/// # Safety
///
/// `marshal` writes exactly `4 + N` bytes and `unmarshal` reads
/// exactly that many. There are no pointers, handles or descriptors in
/// it: a length and a run of bytes mean the same thing wherever they
/// are read. A length larger than `N` cannot name real bytes, so it is
/// refused as an invalid encoding rather than trusted.
unsafe impl<const N: usize> subetha_core::Marshal for Payload<N> {
    const PAYLOAD_BYTES: usize = 4 + N;

    fn marshal(&self, dst: &mut [u8]) {
        dst[..4].copy_from_slice(&self.len.to_le_bytes());
        dst[4..4 + N].copy_from_slice(&self.bytes);
    }

    fn unmarshal(src: &[u8]) -> Result<Self, subetha_core::MarshalError> {
        if src.len() < 4 + N {
            return Err(subetha_core::MarshalError::ShortBuffer { expected: 4 + N, got: src.len() });
        }
        let len = u32::from_le_bytes([src[0], src[1], src[2], src[3]]);
        if len as usize > N {
            return Err(subetha_core::MarshalError::InvalidEncoding);
        }
        let mut held = Self::empty();
        held.len = len;
        held.bytes.copy_from_slice(&src[4..4 + N]);
        Ok(held)
    }
}

/// The most a lease's value can be. The lease's own region gives 48
/// bytes to the value; four of them record how long the value is, so
/// that a value ending in zero bytes comes back as it went in.
pub(crate) const LEASE_VALUE_BYTES: usize = 44;

/// What a lease holds, sized to the lease's own region.
pub(crate) type LeaseValue = Payload<LEASE_VALUE_BYTES>;

/// The most a value can be in the families whose slot is 56 bytes,
/// four of which record the length.
pub(crate) const SLOT_VALUE_BYTES: usize = 52;

/// What those families hold.
pub(crate) type SlotValue = Payload<SLOT_VALUE_BYTES>;

/// A reading of a hybrid logical clock: the physical microseconds and
/// the logical counter that breaks ties within a microsecond.
#[psclass(name = "SubEtha.ClockReading")]
#[derive(Clone, Default)]
pub struct ClockReading {
    /// Microseconds of physical time.
    pub physical_us: u64,
    /// The counter that orders events sharing a microsecond.
    pub logical: u64,
}
