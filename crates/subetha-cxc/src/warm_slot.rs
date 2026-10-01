//! The one-slot warm cache the capacity rings share: a backing built
//! ahead of the morph that will want it, filed under the key that morph
//! targets.
//!
//! The slot is one atomic pointer. A prewarm replaces whatever it holds,
//! so the last prewarm wins. A morph takes the backing only when the key
//! matches its target, with a compare-and-swap against the entry it read,
//! so two morphs never take one backing and a prewarm landing in between
//! stays cached for the next.

use std::sync::Arc;

use subetha_core::SwapCellOption;

/// A backing cached under the key of the morph expected to want it.
pub(crate) struct WarmSlot<K, B> {
    slot: SwapCellOption<(K, B)>,
}

impl<K: PartialEq, B: Clone> WarmSlot<K, B> {
    pub(crate) fn new() -> Self {
        Self { slot: SwapCellOption::empty() }
    }

    /// Whether the slot holds a backing filed under `key`.
    pub(crate) fn holds(&self, key: &K) -> bool {
        self.slot.load().is_some_and(|entry| entry.0 == *key)
    }

    /// The key of the cached backing, if any.
    pub(crate) fn key(&self) -> Option<K>
    where
        K: Clone,
    {
        self.slot.load().map(|entry| entry.0.clone())
    }

    /// File `backing` under `key`, replacing whatever the slot held.
    pub(crate) fn store(&self, key: K, backing: B) {
        self.slot.store(Some(Arc::new((key, backing))));
    }

    /// Take the cached backing when it is filed under `key`. A backing
    /// under another key stays cached.
    pub(crate) fn take(&self, key: &K) -> Option<B> {
        let entry = self.slot.load_full().filter(|entry| entry.0 == *key)?;
        match self.slot.compare_and_set(Some(&entry), None) {
            Ok(()) => Some(entry.1.clone()),
            // Another morph took the entry, or a prewarm replaced it, since
            // it was read; the empty value offered comes back unused.
            Err(_unplaced) => None,
        }
    }

    /// Empty the slot, dropping the cached backing.
    pub(crate) fn clear(&self) {
        self.slot.store(None);
    }
}
