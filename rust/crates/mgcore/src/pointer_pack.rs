use std::sync::atomic::{AtomicU64, Ordering};

/// Stores a pointer with `N` flag bits in the low bits.
/// The pointed-to type must be ≥ (1<<N)-byte aligned.
///
/// Equivalent to C++ `utils::PointerPack<T, N>`.
#[derive(Debug)]
pub struct PointerPack<const N: u8> {
    storage: AtomicU64,
}

impl<const N: u8> Default for PointerPack<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: u8> PointerPack<N> {
    const FLAGS_MASK: u64 = (1u64 << N) - 1;
    const PTR_MASK: u64 = !Self::FLAGS_MASK;

    pub const fn new() -> Self {
        Self {
            storage: AtomicU64::new(0),
        }
    }

    pub fn new_with<T>(ptr: *mut T, flags: u64) -> Self {
        debug_assert!(flags <= Self::FLAGS_MASK);
        let addr = ptr as u64;
        debug_assert!(
            addr & Self::FLAGS_MASK == 0,
            "pointer must be properly aligned"
        );
        Self {
            storage: AtomicU64::new(addr | flags),
        }
    }

    pub fn get_ptr<T>(&self) -> *mut T {
        (self.storage.load(Ordering::Acquire) & Self::PTR_MASK) as *mut T
    }

    pub fn set_ptr<T>(&self, ptr: *mut T) {
        let flags = self.storage.load(Ordering::Relaxed) & Self::FLAGS_MASK;
        let addr = ptr as u64;
        debug_assert!(addr & Self::FLAGS_MASK == 0);
        self.storage.store(addr | flags, Ordering::Release);
    }

    pub fn get<const POS: u8, const SIZE: u8>(&self) -> u64 {
        debug_assert!(POS + SIZE <= N);
        let field_mask = ((1u64 << SIZE) - 1) << POS;
        (self.storage.load(Ordering::Acquire) & field_mask) >> POS
    }

    pub fn set<const POS: u8, const SIZE: u8>(&self, value: u64) {
        debug_assert!(POS + SIZE <= N);
        let field_mask = ((1u64 << SIZE) - 1) << POS;
        let old = self.storage.load(Ordering::Relaxed);
        let new = (old & !field_mask) | ((value << POS) & field_mask);
        self.storage.store(new, Ordering::Release);
    }
}
