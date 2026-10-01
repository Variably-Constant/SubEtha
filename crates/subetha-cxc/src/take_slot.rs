//! A value one caller at a time works on, taken by swapping its pointer
//! out of an `AtomicPtr` and put back by storing it again.
//!
//! A caller that finds the value already out is told so at once and goes
//! its own way: a socket reads `WouldBlock`, a sender takes another path,
//! an estimator update is skipped. Nobody waits on the holder, so a holder
//! that stalls or is descheduled delays no one, and a holder that unwinds
//! still puts the value back.

use std::marker::PhantomData;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicPtr, Ordering};

/// A value one caller at a time works on. See the module docs.
pub(crate) struct TakeSlot<T> {
    value: AtomicPtr<T>,
    _owns: PhantomData<Box<T>>,
}

// SAFETY: the slot hands its value to one caller at a time, so sharing the
// slot between threads moves the value between them and needs no more than
// `T: Send`.
unsafe impl<T: Send> Sync for TakeSlot<T> {}

impl<T> TakeSlot<T> {
    pub(crate) fn new(value: T) -> Self {
        Self { value: AtomicPtr::new(Box::into_raw(Box::new(value))), _owns: PhantomData }
    }

    /// Runs `f` on the value, or returns `None` when another caller has it
    /// out. The value goes back when `f` returns or unwinds.
    pub(crate) fn with<R>(&self, f: impl FnOnce(&mut T) -> R) -> Option<R> {
        struct Back<'a, T>(&'a AtomicPtr<T>, NonNull<T>);
        impl<T> Drop for Back<'_, T> {
            fn drop(&mut self) {
                self.0.store(self.1.as_ptr(), Ordering::Release);
            }
        }
        let taken = NonNull::new(self.value.swap(ptr::null_mut(), Ordering::Acquire))?;
        let back = Back(&self.value, taken);
        // SAFETY: the pointer came from `Box::into_raw` in `new`, and the swap
        // made this caller its only holder until `back` stores it.
        Some(f(unsafe { &mut *back.1.as_ptr() }))
    }
}

impl<T> Drop for TakeSlot<T> {
    fn drop(&mut self) {
        let p = *self.value.get_mut();
        if !p.is_null() {
            // SAFETY: the pointer came from `Box::into_raw` in `new`, and a
            // slot being dropped has no caller holding its value.
            drop(unsafe { Box::from_raw(p) });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TakeSlot;

    #[test]
    fn a_caller_finds_the_value_out_while_another_holds_it() {
        let slot = TakeSlot::new(1u32);
        let inner = slot.with(|v| {
            *v += 1;
            slot.with(|_| ())
        });
        assert_eq!(inner, Some(None), "the value is out while its holder works");
        assert_eq!(slot.with(|v| *v), Some(2), "the holder's write is there when it comes back");
    }

    #[test]
    fn the_value_goes_back_when_its_holder_unwinds() {
        let slot = TakeSlot::new(7u32);
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            slot.with(|_| -> u32 { std::panic::resume_unwind(Box::new("the holder unwinds")) })
        }));
        assert!(unwound.is_err(), "the holder's unwind reaches the caller");
        assert_eq!(slot.with(|v| *v), Some(7), "the next caller finds the value");
    }
}
