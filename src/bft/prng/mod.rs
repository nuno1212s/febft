//! Pseudo random number generator.
//!
//! The current implementation is based on [xoshiro128**](https://prng.di.unimi.it/xoshiro128starstar.c),
//! from David Blackman and Sebastiano Vigna. This source code is a one to one translation of their
//! C code, released to the public domain.

use rand_core::{RngCore, OsRng};

/// This type is a container for the 128-bit state of `xoshiro128**`.
pub struct State {
    s: [u32; 4],
}

impl State {
    /// Creates a new PRNG from a cryptographically secure random seed.
    pub fn new() -> Self {
        let mut seed = [0; 16];
        OsRng.fill_bytes(&mut seed);

        let s = unsafe { std::mem::transmute(seed) };
        let mut s = State { s };

        // the first random number is very predictable
        s.next();

        s
    }

    /// Returns a new 32-bit random number.
    #[inline]
    pub fn next_state(&mut self) -> u32 {
        let result = rotl(self.s[1] * 5, 7) * 9;
        let t = self.s[1] << 9;

        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];

        self.s[2] ^= t;

        self.s[3] = rotl(self.s[3], 11);

        result
    }
}

impl Iterator for State {
    type Item = u32;

    #[inline]
    fn next(&mut self) -> Option<u32> {
        Some(self.next_state())
    }
}

#[inline]
fn rotl(x: u32, k: u32) -> u32 {
	(x << k) | (x >> (32 - k))
}
