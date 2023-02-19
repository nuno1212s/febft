//! Pseudo random number generator.
//!
//! The current implementation is based on [xoshiro256**](https://prng.di.unimi.it/xoshiro256starstar.c),
//! from David Blackman and Sebastiano Vigna. This source code is a one-to-one translation of their
//! C code, released to the public domain.

use std::cell::RefCell;
use rand_core::{RngCore, OsRng};
use thread_local::ThreadLocal;

/// This type is a container for the 256-bit state of `xoshiro256**`.
pub struct State {
    s: [u64; 4],
}

pub struct ThreadSafePrng {
    rng: ThreadLocal<RefCell<State>>
}

impl ThreadSafePrng {

    pub fn new() -> Self {
        Self {
            rng: ThreadLocal::new()
        }
    }

    #[inline]
    pub fn next_state(&self) -> u64 {
        let state = self.rng.get_or(|| {
            RefCell::new(State::new())
        });

        state.borrow_mut().next_state()
    }

}

impl State {
    /// Creates a new PRNG from a cryptographically secure random seed.
    pub fn new() -> Self {
        let mut seed = [0; 32];
        OsRng.fill_bytes(&mut seed);

        let s = unsafe { std::mem::transmute(seed) };
        let mut s = State { s };

        s.long_jump();
        s
    }

    /// Returns a new 64-bit random number.
    #[inline]
    pub fn next_state(&mut self) -> u64 {
        let result = rotl(self.s[1].wrapping_mul(5), 7).wrapping_mul(9);
        let t = self.s[1] << 17;

        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];

        self.s[2] ^= t;

        self.s[3] = rotl(self.s[3], 45);

        result
    }

    #[inline]
    fn long_jump(&mut self) {
        const LONG_JUMP: [u64; 4] = [
            0x76e15d3efefdcbbf,
            0xc5004e441c522fb3,
            0x77710069854ee241,
            0x39109bb02acbe635,
        ];

        let mut s0 = 0;
        let mut s1 = 0;
        let mut s2 = 0;
        let mut s3 = 0;

        for &jmp_val in LONG_JUMP.iter() {
            for b in 0..64 {
                if jmp_val & 1 << b != 0 {
                    s0 ^= self.s[0];
                    s1 ^= self.s[1];
                    s2 ^= self.s[2];
                    s3 ^= self.s[3];
                }
                self.next_state();
            }
        }

        self.s[0] = s0;
        self.s[1] = s1;
        self.s[2] = s2;
        self.s[3] = s3;
    }
}

impl Iterator for State {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<u64> {
        Some(self.next_state())
    }
}

#[inline]
fn rotl(x: u64, k: u64) -> u64 {
	(x << k) | (x >> (64 - k))
}
