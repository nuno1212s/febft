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

        s.long_jump();
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

    #[inline]
    fn long_jump(&mut self) {
        const LONG_JUMP: [u32; 4] = [0xb523952e, 0x0b6f099f, 0xccf5a0ef, 0x1c580662];

        let mut s0 = 0;
        let mut s1 = 0;
        let mut s2 = 0;
        let mut s3 = 0;

        for &jmp_val in LONG_JUMP.iter() {
            for b in 0..32 {
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
