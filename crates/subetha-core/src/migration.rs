//! Dual-stack migration protocol.
//!
//! Migration steps:
//! 1. Allocate new representation alongside old.
//! 2. Initialize new from a snapshot of old.
//! 3. Bump generation, swap strategy tag - both representations now live.
//! 4. Wait for old generation's in-flight count to drain to zero.
//! 5. Drop old representation.
//!
//! The [`MigrationGuard`] enforces steps 3-5 in scope.

use crate::handshake::HandshakeHeader;

/// Wraps the current generation captured at op entry.
///
/// Acts as a witness that the caller holds an in-flight slot. Drop
/// releases the slot.
#[must_use = "Generation captures an in-flight slot; drop or pass to exit_op"]
pub struct Generation<'a> {
    pub(crate) header: &'a HandshakeHeader,
    pub(crate) value: u32,
}

impl<'a> Generation<'a> {
    pub fn enter(header: &'a HandshakeHeader) -> Self {
        let value = header.enter_op();
        Self { header, value }
    }

    #[inline(always)]
    pub fn value(&self) -> u32 {
        self.value
    }
}

impl<'a> Drop for Generation<'a> {
    fn drop(&mut self) {
        self.header.exit_op(self.value);
    }
}

/// Guard for the migration coordinator. Wraps the migrate-then-drain
/// sequence: `begin` bumps the generation and swaps the tag,
/// `wait_quiescent` drains the old generation. The drain is explicit -
/// the guard does NOT drain on drop.
pub struct MigrationGuard<'a> {
    header: &'a HandshakeHeader,
    old_value: u32,
}

impl<'a> MigrationGuard<'a> {
    /// Begin a migration. `new_tag` is the strategy tag to install.
    pub fn begin(header: &'a HandshakeHeader, new_tag: u32) -> Self {
        let old_value = header.migrate(new_tag);
        Self { header, old_value }
    }

    /// Wait for in-flight ops on the old generation to complete.
    pub fn wait_quiescent(&self) {
        self.header.drain(self.old_value);
    }

    pub fn old_generation(&self) -> u32 {
        self.old_value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_captures_and_releases() {
        let h = HandshakeHeader::new();
        {
            let guard = Generation::enter(&h);
            assert_eq!(guard.value(), 0);
        }
        h.drain(0);
    }

    #[test]
    fn migration_drains_old_generation() {
        let h = HandshakeHeader::new();
        let m = MigrationGuard::begin(&h, 1);
        m.wait_quiescent();
        assert_eq!(m.old_generation(), 0);
        assert_eq!(h.tag(), 1);
    }
}
