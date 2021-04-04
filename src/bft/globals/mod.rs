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
    pub fn set(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Sets the global variable as dropped. 
    pub fn unset(&self) {
        self.0.store(false, Ordering::Release);
    }

    /// Checks if a global variable is initialized.
    pub fn test(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// A `Global` represents a global variable.
///
/// Checking for initialization is thread safe, but dropping or
/// setting a value is unsafe, and should be done with caution.
pub struct Global<T> {
    guard: Flag,
    value: Option<T>,
}

impl<T> Global<T> {
    /// Creates a new global variable handle.
    pub const fn new() -> Self {
        Self {
            guard: Flag::new(),
            value: None,
        }
    }

    /// Initializes the global variable with a `value`.
    pub fn set(&mut self, value: T) {
        self.value = Some(value);
        self.guard.set();
    }

    /// Drops the global variable.
    pub fn drop(&mut self) {
        self.guard.unset();
        self.value.take();
    }
}

impl<T: Sync> Global<T> {
    /// Checks for the initialization of a global variable.
    ///
    /// This method is potentially unsafe to call, because the reference
    /// may dangle if we deinitialize the library, by dropping `InitGuard`.
    /// In practice, it should always be safe, since dropping the `InitGuard`
    /// is the last thing users of `febft` should do.
    pub fn get(&self) -> Option<&T> {
        if self.guard.test() {
            self.value.as_ref()
        } else {
            None
        }
    }
}
