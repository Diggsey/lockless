/// This lock-free data structure allows concurrent allocation of IDs from a contiguous slab.

use std::sync::atomic::{AtomicUsize, Ordering};
use primitives::atomic_ext::AtomicExt;

#[derive(Debug)]
pub struct IndexAllocator {
    mask: Vec<AtomicUsize>,
    used: AtomicUsize,
    len: usize
}

fn div_up(a: usize, b: usize) -> usize {
    (a + b - 1) / b
}

fn word_bits() -> usize {
    0usize.trailing_zeros() as usize
}

impl IndexAllocator {
    pub fn new(len: usize) -> Self {
        let mut result = IndexAllocator {
            mask: Vec::new(),
            used: AtomicUsize::new(0),
            len: 0,
        };
        result.resize(len);
        result
    }
    pub fn allocate(&self) -> Option<usize> {
        let word_bits = word_bits();
        // If we can reserve space
        if self.used.try_update(|prev| {
            if prev < self.len {
                Ok(prev + 1)
            } else {
                Err(())
            }
        }).is_ok() {
            loop {
                for (index, m) in self.mask.iter().enumerate() {
                    if let Ok((prev, next)) = m.try_update(|prev| {
                        match (!prev).trailing_zeros() {
                            32 => Err(()),
                            other => Ok(prev | (1 << other))
                        }
                    }) {
                        return Some(index*word_bits + (next & !prev).trailing_zeros() as usize);
                    }
                }
            }
        } else {
            None
        }
    }
    pub fn free(&self, id: usize) {
        let word_bits = word_bits();
        assert!(id < self.len);
        let (index, offset) = (id / word_bits, id % word_bits);
        let bit_mask = 1 << offset;
        let prev = self.mask[index].fetch_and(!bit_mask, Ordering::Relaxed);
        assert!(prev & bit_mask != 0, "Double-free of index!")
    }
    pub fn resize(&mut self, new_len: usize) {
        assert!(new_len >= self.len);
        let word_bits = word_bits();
        let mask_words = self.mask.len();

        // Clear previous unused bits
        let unused_bits = mask_words*word_bits - self.len;
        if unused_bits > 0 {
            *self.mask[mask_words-1].get_mut() &= usize::max_value() >> unused_bits;
        }

        let new_mask_words = div_up(new_len, word_bits);
        self.mask.reserve_exact(new_mask_words - mask_words);
        for _ in mask_words..new_mask_words {
            self.mask.push(AtomicUsize::new(0))
        }

        // Set new unused bits
        let new_unused_bits = new_mask_words*word_bits - new_len;
        if new_unused_bits > 0 {
            *self.mask[new_mask_words-1].get_mut() |= !(usize::max_value() >> new_unused_bits);
        }

        self.len = new_len;
    }
    pub fn len(&self) -> usize {
        self.len
    }
}
