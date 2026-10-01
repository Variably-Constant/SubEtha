//! The `shm:` drive: values under names, shared by every process of the
//! user and outliving the session, reached as `$shm:name`.
//!
//! PowerShell resolves `$drive:name` through a provider's content
//! operations, so `$shm:name = value` is a `set_content` and `$shm:name`
//! a `get_content`, and the item and container operations serve
//! `Get-Item`, `Set-Item`, `Remove-Item` and `Get-ChildItem` on the same
//! names. The values live in a [`SharedNamedValues`] store: the default
//! drive's files are `shm.map`, `shm.arena` and `shm.epochs` in the
//! user's SubEtha directory, the one the wait calibration caches in,
//! and `New-PSDrive -PSProvider SubEthaShm -Root <directory>` opens the
//! files of that name in another directory.
//!
//! # Values
//!
//! A boolean, an integer of any width, a single or a double, a string,
//! a byte array or a DateTime is stored as its bytes behind one tag byte
//! and comes back as the same type. Any other object, arrays and
//! hashtables included, is stored as the CLIXML the remoting serializer
//! writes at depth 2, and comes back as remoting returns it: exact for
//! the primitives, a property bag with a `Deserialized.` type name for
//! the rest. Assigning `$null` removes the name, as `$env:` does, and a
//! name the drive does not hold reads as `$null`.
//!
//! # Names
//!
//! A name is one path segment, of any length, compared without regard
//! to case, and listed as it was first written. Two names whose hashes
//! collide are refused, never merged.

use std::path::Path;

use pwrs::prelude::*;
use pwrs::sys::{
    PS_TYPE_BOOL, PS_TYPE_DATETIME, PS_TYPE_F32, PS_TYPE_F64, PS_TYPE_I16, PS_TYPE_I32, PS_TYPE_I64, PS_TYPE_I8,
    PS_TYPE_STRING, PS_TYPE_U16, PS_TYPE_U32, PS_TYPE_U64, PS_TYPE_U8,
};
use pwrs::values::{DateTimeKind, PsDateTime};
use pwrs::PsType;
use subetha_cxc::{NamedValuesError, NamedValuesLayout, SharedNamedValues};

/// Names the default drive holds.
pub const NAMES: usize = 4096;

/// Bytes of the default drive's arena.
pub const ARENA_BYTES: usize = 256 << 20;

/// The smallest block class: 64 bytes.
pub const MIN_CLASS: u32 = 6;

/// The largest block class: 32 MiB, which holds a 16 MiB value with its
/// name and the block header.
pub const MAX_CLASS: u32 = 25;

/// Readers at once, across every process.
pub const PINS: usize = 256;

/// The name the three files share.
pub const STEM: &str = "shm";

/// The default drive's store.
pub const LAYOUT: NamedValuesLayout =
    NamedValuesLayout { names: NAMES, arena_bytes: ARENA_BYTES, min_class: MIN_CLASS, max_class: MAX_CLASS, pins: PINS };

/// The tag byte ahead of a stored value.
mod tag {
    pub const BOOL: u8 = 1;
    pub const I8: u8 = 2;
    pub const I16: u8 = 3;
    pub const I32: u8 = 4;
    pub const I64: u8 = 5;
    pub const U8: u8 = 6;
    pub const U16: u8 = 7;
    pub const U32: u8 = 8;
    pub const U64: u8 = 9;
    pub const F32: u8 = 10;
    pub const F64: u8 = 11;
    pub const STRING: u8 = 12;
    pub const BYTES: u8 = 13;
    pub const DATETIME: u8 = 14;
    pub const CLIXML: u8 = 15;
}

const SERIALIZER: &str = "System.Management.Automation.PSSerializer";

/// The depth `PSSerializer.Serialize` is given: an object's properties
/// and those of the objects one level inside it.
const CLIXML_DEPTH: i32 = 2;

fn corrupt(what: impl Into<String>) -> PsError {
    PsError::new(ErrorCategory::InvalidData, "SubEthaShmValue", format!("the stored value cannot be read: {}", what.into()))
}

fn store_err(what: &str, e: NamedValuesError) -> PsError {
    let category = match &e {
        NamedValuesError::NameCollision { .. } => ErrorCategory::ResourceExists,
        NamedValuesError::TooManyNames { .. }
        | NamedValuesError::Full { .. }
        | NamedValuesError::TooLarge { .. }
        | NamedValuesError::NameTooLong { .. } => ErrorCategory::LimitsExceeded,
        NamedValuesError::Corrupt => ErrorCategory::InvalidData,
        NamedValuesError::Map(_) | NamedValuesError::Arena(_) | NamedValuesError::Epochs(_) => ErrorCategory::InvalidOperation,
    };
    PsError::new(category, "SubEthaShm", format!("{what}: {e}"))
}

/// The bytes a value is stored as: its tag, then its bytes.
fn encode(value: &PsObject) -> PsResult<Vec<u8>> {
    let mut out = Vec::new();
    match value.type_tag()? {
        PS_TYPE_BOOL => {
            out.push(tag::BOOL);
            out.push(u8::from(bool::from_ps(value)?));
        }
        PS_TYPE_I8 => {
            out.push(tag::I8);
            out.extend_from_slice(&i8::from_ps(value)?.to_le_bytes());
        }
        PS_TYPE_I16 => {
            out.push(tag::I16);
            out.extend_from_slice(&i16::from_ps(value)?.to_le_bytes());
        }
        PS_TYPE_I32 => {
            out.push(tag::I32);
            out.extend_from_slice(&i32::from_ps(value)?.to_le_bytes());
        }
        PS_TYPE_I64 => {
            out.push(tag::I64);
            out.extend_from_slice(&i64::from_ps(value)?.to_le_bytes());
        }
        PS_TYPE_U8 => {
            out.push(tag::U8);
            out.push(u8::from_ps(value)?);
        }
        PS_TYPE_U16 => {
            out.push(tag::U16);
            out.extend_from_slice(&u16::from_ps(value)?.to_le_bytes());
        }
        PS_TYPE_U32 => {
            out.push(tag::U32);
            out.extend_from_slice(&u32::from_ps(value)?.to_le_bytes());
        }
        PS_TYPE_U64 => {
            out.push(tag::U64);
            out.extend_from_slice(&u64::from_ps(value)?.to_le_bytes());
        }
        PS_TYPE_F32 => {
            out.push(tag::F32);
            out.extend_from_slice(&f32::from_ps(value)?.to_le_bytes());
        }
        PS_TYPE_F64 => {
            out.push(tag::F64);
            out.extend_from_slice(&f64::from_ps(value)?.to_le_bytes());
        }
        PS_TYPE_STRING => {
            out.push(tag::STRING);
            out.extend_from_slice(String::from_ps(value)?.as_bytes());
        }
        PS_TYPE_DATETIME => {
            let d = PsDateTime::from_ps(value)?;
            out.push(tag::DATETIME);
            out.extend_from_slice(&d.ticks.to_le_bytes());
            out.push(d.kind as u8);
        }
        _ => match value.type_name()?.as_str() {
            "System.Byte[]" => {
                out.push(tag::BYTES);
                out.extend_from_slice(&<u8 as FromPs>::vec_from_ps(value)?);
            }
            "System.DateTime" => {
                let d = PsDateTime::from_ps(value)?;
                out.push(tag::DATETIME);
                out.extend_from_slice(&d.ticks.to_le_bytes());
                out.push(d.kind as u8);
            }
            _ => {
                out.push(tag::CLIXML);
                out.extend_from_slice(serialize(value)?.as_bytes());
            }
        },
    }
    Ok(out)
}

/// The CLIXML of an object at [`CLIXML_DEPTH`].
fn serialize(value: &PsObject) -> PsResult<String> {
    let xml = PsType::from_name(SERIALIZER).call_static("Serialize", &[value.clone(), CLIXML_DEPTH.into_ps()?])?;
    String::from_ps(&xml)
}

fn deserialize(xml: &str) -> PsResult<PsObject> {
    PsType::from_name(SERIALIZER).call_static("Deserialize", &[xml.into_ps()?])
}

/// `body` as exactly `N` bytes.
fn fixed<const N: usize>(body: &[u8]) -> PsResult<[u8; N]> {
    match <[u8; N]>::try_from(body) {
        Ok(bytes) => Ok(bytes),
        Err(_other_length) => Err(corrupt(format!("{} bytes where {N} were expected", body.len()))),
    }
}

/// The value stored bytes hold.
fn decode(bytes: &[u8]) -> PsResult<PsObject> {
    let (tag, body) = match bytes.split_first() {
        Some(split) => split,
        None => return Err(corrupt("no bytes at all")),
    };
    match *tag {
        tag::BOOL => (fixed::<1>(body)?[0] != 0).into_ps(),
        tag::I8 => i8::from_le_bytes(fixed(body)?).into_ps(),
        tag::I16 => i16::from_le_bytes(fixed(body)?).into_ps(),
        tag::I32 => i32::from_le_bytes(fixed(body)?).into_ps(),
        tag::I64 => i64::from_le_bytes(fixed(body)?).into_ps(),
        tag::U8 => fixed::<1>(body)?[0].into_ps(),
        tag::U16 => u16::from_le_bytes(fixed(body)?).into_ps(),
        tag::U32 => u32::from_le_bytes(fixed(body)?).into_ps(),
        tag::U64 => u64::from_le_bytes(fixed(body)?).into_ps(),
        tag::F32 => f32::from_le_bytes(fixed(body)?).into_ps(),
        tag::F64 => f64::from_le_bytes(fixed(body)?).into_ps(),
        tag::STRING => match std::str::from_utf8(body) {
            Ok(s) => s.into_ps(),
            Err(_not_utf8) => Err(corrupt("a string that is not UTF-8")),
        },
        tag::BYTES => <u8 as IntoPs>::vec_into_ps(body.to_vec()),
        tag::DATETIME => {
            let stamp = fixed::<9>(body)?;
            let mut ticks = [0u8; 8];
            ticks.copy_from_slice(&stamp[..8]);
            let kind = match stamp[8] {
                0 => DateTimeKind::Unspecified,
                1 => DateTimeKind::Utc,
                2 => DateTimeKind::Local,
                other => return Err(corrupt(format!("DateTime kind {other}"))),
            };
            PsDateTime::new(i64::from_le_bytes(ticks), kind).into_ps()
        }
        tag::CLIXML => match std::str::from_utf8(body) {
            Ok(xml) => deserialize(xml),
            Err(_not_utf8) => Err(corrupt("CLIXML that is not UTF-8")),
        },
        other => Err(corrupt(format!("value tag {other}"))),
    }
}

/// A `SubEtha.ShmValue` item: the name and the value.
fn item_of(name: &str, value: PsObject) -> PsResult<Item> {
    let obj = pwrs::object::new_psobject("SubEtha.ShmValue");
    pwrs::object::add_note(&obj, "Name", name.into_ps()?)?;
    pwrs::object::add_note(&obj, "Value", value)?;
    Ok(Item::leaf(name, obj))
}

/// Values under names, shared by every process of the user.
#[provider(name = "SubEthaShm")]
pub struct ShmDrive {
    store: SharedNamedValues,
    /// The root the drive was registered with, which names the
    /// container and nothing in it.
    root: String,
}

impl ShmDrive {
    /// The store in the user's SubEtha directory.
    fn default_store() -> PsResult<SharedNamedValues> {
        let dir = subetha_cxc::per_user_dir()
            .map_err(|reason| PsError::new(ErrorCategory::OpenError, "SubEthaShm", format!("the user's SubEtha directory: {reason}")))?;
        Self::store_in(&dir)
    }

    fn store_in(dir: &Path) -> PsResult<SharedNamedValues> {
        SharedNamedValues::create(dir, STEM, LAYOUT).map_err(|e| store_err(&format!("obtaining the store in {}", dir.display()), e))
    }

    fn is_root(&self, path: &str) -> bool {
        let trimmed = path.trim_end_matches(['\\', '/']);
        trimmed.is_empty() || trimmed.eq_ignore_ascii_case(self.root.trim_end_matches(['\\', '/']))
    }

    /// The name a path addresses: its last segment.
    fn name_of(path: &str) -> PsResult<String> {
        let name = pwrs::provider::child_name(path);
        if name.is_empty() {
            return Err(PsError::new(ErrorCategory::InvalidArgument, "SubEthaShmName", "a value needs a name"));
        }
        Ok(name)
    }

    fn get_bytes(&self, name: &str) -> PsResult<Option<Vec<u8>>> {
        self.store.get(name).map_err(|e| store_err(&format!("reading {name}"), e))
    }

    fn put(&self, name: &str, value: &PsObject) -> PsResult<()> {
        let bytes = encode(value)?;
        self.store.set(name, &bytes).map_err(|e| store_err(&format!("writing {name}"), e))
    }

    fn take(&self, name: &str) -> PsResult<bool> {
        self.store.remove(name).map_err(|e| store_err(&format!("removing {name}"), e))
    }
}

impl Provider for ShmDrive {
    fn default_drives() -> PsResult<Vec<(Drive, ShmDrive)>> {
        let drive = ShmDrive { store: Self::default_store()?, root: String::new() };
        Ok(vec![(Drive { name: STEM.to_string(), root: String::new() }, drive)])
    }

    /// A drive over the `shm` files in the directory `root` names, or over
    /// the user's own when `root` is empty.
    fn new_drive(name: &str, root: &str) -> PsResult<(Drive, ShmDrive)> {
        let store = if root.is_empty() { Self::default_store()? } else { Self::store_in(Path::new(root))? };
        Ok((Drive { name: name.to_string(), root: root.to_string() }, ShmDrive { store, root: root.to_string() }))
    }

    fn is_valid_path(_path: &str) -> bool {
        true
    }

    fn item_exists(&mut self, path: &str) -> PsResult<bool> {
        if self.is_root(path) {
            return Ok(true);
        }
        Ok(self.get_bytes(&Self::name_of(path)?)?.is_some())
    }

    fn is_item_container(&mut self, path: &str) -> PsResult<bool> {
        Ok(self.is_root(path))
    }

    fn get_item(&mut self, path: &str) -> PsResult<Option<Item>> {
        if self.is_root(path) {
            return Ok(Some(Item::container(self.root.clone(), self.root.clone().into_ps()?)));
        }
        let name = Self::name_of(path)?;
        match self.get_bytes(&name)? {
            Some(bytes) => Ok(Some(item_of(&name, decode(&bytes)?)?)),
            None => Ok(None),
        }
    }

    fn set_item(&mut self, path: &str, value: PsObject) -> PsResult<Option<Item>> {
        let name = Self::name_of(path)?;
        if value.is_null() {
            self.take(&name)?;
            return Ok(None);
        }
        self.put(&name, &value)?;
        Ok(Some(item_of(&name, value)?))
    }

    fn clear_item(&mut self, path: &str) -> PsResult<()> {
        self.take(&Self::name_of(path)?)?;
        Ok(())
    }

    /// The names, under the root; a name itself holds nothing.
    fn get_child_items(&mut self, path: &str, _recurse: bool) -> PsResult<Vec<Item>> {
        if !self.is_root(path) {
            return Ok(Vec::new());
        }
        let entries = self.store.entries().map_err(|e| store_err("listing the names", e))?;
        entries.into_iter().map(|(name, bytes)| item_of(&name, decode(&bytes)?)).collect()
    }

    fn has_child_items(&mut self, path: &str) -> PsResult<bool> {
        Ok(self.is_root(path) && !self.store.is_empty())
    }

    fn new_item(&mut self, path: &str, _item_type: &str, value: PsObject) -> PsResult<Option<Item>> {
        self.set_item(path, value)
    }

    fn remove_item(&mut self, path: &str, _recurse: bool) -> PsResult<()> {
        let name = Self::name_of(path)?;
        if !self.take(&name)? {
            return Err(PsError::new(ErrorCategory::ObjectNotFound, "SubEthaShmName", format!("the drive holds no {name}")));
        }
        Ok(())
    }

    fn get_content(&mut self, path: &str) -> PsResult<Vec<PsObject>> {
        match self.get_bytes(&Self::name_of(path)?)? {
            Some(bytes) => Ok(vec![decode(&bytes)?]),
            None => Ok(Vec::new()),
        }
    }

    /// One object is the value; several are stored as one array, or as a
    /// byte array when every one is a byte, since the engine hands an
    /// assigned array over one element at a time; none, or one `$null`,
    /// removes the name.
    fn set_content(&mut self, path: &str, content: Vec<PsObject>) -> PsResult<()> {
        let name = Self::name_of(path)?;
        match content.len() {
            0 => {
                self.take(&name)?;
                Ok(())
            }
            1 if content[0].is_null() => {
                self.take(&name)?;
                Ok(())
            }
            1 => self.put(&name, &content[0]),
            _ => {
                let mut bytes = Vec::with_capacity(content.len());
                for item in &content {
                    if item.type_tag()? != PS_TYPE_U8 {
                        return self.put(&name, &PsArray(content).into_ps()?);
                    }
                    bytes.push(u8::from_ps(item)?);
                }
                self.put(&name, &<u8 as IntoPs>::vec_into_ps(bytes)?)
            }
        }
    }

    fn clear_content(&mut self, path: &str) -> PsResult<()> {
        self.take(&Self::name_of(path)?)?;
        Ok(())
    }
}
