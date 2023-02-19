//! Abstractions to deal with global variables.

use std::{sync::atomic::{AtomicBool, Ordering}, ops::Deref};

#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};

/// A `Flag` is used to check for the initialization of a global value.
pub struct Flag(AtomicBool);

impl Flag {
    /// Creates a new global variable `Flag`.
    pub const fn new() -> Self {
        Self(AtomicBool::new(false))
    }

    /// Sets the global variable as initialized. 
    #[inline]
    pub fn set(&'static self) {
        self.0.store(true, Ordering::Release);
    }

    /// Sets the global variable as dropped. 
    #[inline]
    pub fn unset(&'static self) {
        self.0.store(false, Ordering::Release);
    }

    /// Checks if a global variable is initialized.
    #[inline]
    pub fn test(&'static self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// A `Global` represents a global variable.
///
/// Checking for initialization is thread safe, but dropping or
/// setting a value is unsafe, and should be done with caution.
pub struct Global<T> {
    flag: Flag,
    value: Option<T>,
}

impl<T: 'static> Global<T> {
    /// Creates a new global variable handle.
    pub const fn new() -> Self {
        Self {
            flag: Flag::new(),
            value: None,
        }
    }

    /// Initializes the global variable with a `value`.
    #[inline]
    pub fn set(&'static mut self, value: T) {
        self.value = Some(value);
        self.flag.set();
    }

    /// Drops the global variable.
    #[inline]
    pub fn drop(&'static mut self) {
        self.flag.unset();
        self.value.take();
    }
}

impl<T: Sync + 'static> Global<T> {
    /// Checks for the initialization of a global variable.
    ///
    /// This method is potentially unsafe to call, because the reference
    /// may dangle if we deinitialize the library, by dropping `InitGuard`.
    /// In practice, it should always be safe, since dropping the `InitGuard`
    /// is the last thing users of `febft` should do.
    #[inline]
    pub fn get(&'static self) -> Option<&'static T> {
        if self.flag.test() {
            self.value.as_ref()
        } else {
            None
        }
    }
}


#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct ReadOnly<T> {

    value: T

}

unsafe impl<T> Sync for ReadOnly<T> {}

impl<T> ReadOnly<T> {

    pub fn new(value: T) -> Self {
        Self { value }
    }

}

impl<T> From<T> for ReadOnly<T> {
    fn from(value: T) -> Self {
        ReadOnly::new(value)
    }
}

impl<T> Deref for ReadOnly<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}