//! Abstractions to deal with global variables.

use std::sync::atomic::{AtomicBool, Ordering};

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
    /// Returns a `GlobalGuard` for a `Global` variable.
    ///
    /// Calling `get()` on the returned value safely checks
    /// for the initialization of the global variable.
    #[inline]
    pub fn guard(&'static self) -> GlobalGuard<'static, T> {
        GlobalGuard { inner: self }
    }
}

/// A safe wrapper for a `Global`, which checks for initialization
/// before every access.
pub struct GlobalGuard<'a, T> {
    inner: &'a Global<T>,
}

impl<T: Sync + 'static> GlobalGuard<'static, T> {
    /// Returns a reference to the value contained in a global variable,
    /// if it is initialized.
    #[inline]
    pub fn get(&self) -> Option<&'static T> {
        if self.inner.flag.test() {
            self.inner.value.as_ref()
        } else {
            None
        }
    }
}
