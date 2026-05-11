use std::sync::atomic::{AtomicU32, Ordering};
use std::hint;

/// Reader-writer spin lock with writer preference.
///
/// Uses an AtomicU32 where:
///   bit 31 (WRITER_BIT): writer is holding or waiting
///   bits 0-29: reader count
///
/// Equivalent to C++ `utils::RWSpinLock`.
/// Writers have priority over readers to prevent write starvation.
#[derive(Debug)]
pub struct RwSpinLock {
    state: AtomicU32,
}

const WRITER_BIT: u32 = 1 << 31;
const READER_MASK: u32 = !WRITER_BIT;
const MAX_RETRIES: u32 = 1024;

impl RwSpinLock {
    pub const fn new() -> Self {
        Self {
            state: AtomicU32::new(0),
        }
    }

    /// Acquire exclusive (writer) lock. Spins until acquired.
    pub fn lock_exclusive(&self) {
        // Set WRITER_BIT to block new readers, then wait for existing readers to drain.
        let mut retries = 0;
        loop {
            let current = self.state.load(Ordering::Relaxed);
            // Try to claim the writer bit
            if current & WRITER_BIT == 0 {
                if self
                    .state
                    .compare_exchange_weak(
                        current,
                        current | WRITER_BIT,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    break;
                }
            }
            retries += 1;
            if retries > MAX_RETRIES {
                hint::spin_loop();
                retries = 0;
            }
        }

        // Wait for existing readers to drain
        retries = 0;
        while self.state.load(Ordering::Acquire) & READER_MASK != 0 {
            retries += 1;
            if retries > MAX_RETRIES {
                hint::spin_loop();
                retries = 0;
            }
        }
    }

    /// Release exclusive (writer) lock.
    pub fn unlock_exclusive(&self) {
        self.state.fetch_and(!WRITER_BIT, Ordering::Release);
    }

    /// Acquire shared (reader) lock. Spins while writer holds the lock.
    pub fn lock_shared(&self) {
        let mut retries = 0;
        loop {
            let current = self.state.load(Ordering::Relaxed);
            if current & WRITER_BIT == 0 {
                if self
                    .state
                    .compare_exchange_weak(
                        current,
                        current + 1,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return;
                }
            }
            retries += 1;
            if retries > MAX_RETRIES {
                hint::spin_loop();
                retries = 0;
            }
        }
    }

    /// Release shared (reader) lock.
    pub fn unlock_shared(&self) {
        self.state.fetch_sub(1, Ordering::Release);
    }
}

// Safety: RwSpinLock provides its own synchronization.
unsafe impl Send for RwSpinLock {}
unsafe impl Sync for RwSpinLock {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_exclusive_lock() {
        let lock = RwSpinLock::new();
        lock.lock_exclusive();
        lock.unlock_exclusive();
    }

    #[test]
    fn test_shared_lock() {
        let lock = RwSpinLock::new();
        lock.lock_shared();
        lock.lock_shared();
        lock.unlock_shared();
        lock.unlock_shared();
    }

    #[test]
    fn test_concurrent_readers() {
        let lock = Arc::new(RwSpinLock::new());
        let mut handles = vec![];

        for _ in 0..4 {
            let lock = lock.clone();
            handles.push(thread::spawn(move || {
                lock.lock_shared();
                // Simulate read
                std::thread::sleep(std::time::Duration::from_micros(100));
                lock.unlock_shared();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }
}
