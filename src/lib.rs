#![cfg_attr(test, feature(test))]

extern crate parking_lot;
extern crate futures;

pub mod primitives;
#[macro_use]
pub mod handle;
pub mod containers;
pub mod sync;

#[cfg(target_pointer_width = "16")] const POINTER_BITS: usize = 16;
#[cfg(target_pointer_width = "32")] const POINTER_BITS: usize = 32;
#[cfg(target_pointer_width = "64")] const POINTER_BITS: usize = 64;
#[cfg(target_pointer_width = "128")] const POINTER_BITS: usize = 128;

#[cfg(test)]
mod tests {
    extern crate test;

    use self::test::Bencher;
    use std::thread;
    use std::sync::{Mutex, Arc};

    fn atomic_cell_smoke() {
        use primitives::atomic_cell::AtomicCell;

        let results = Arc::new(Mutex::new(Vec::new()));
        let results0 = results.clone();
        let results1 = results.clone();
        let cell = AtomicCell::new(0);
        let cell0 = cell.clone();
        let cell1 = cell.clone();
        let thread0 = thread::spawn(move || {
            let mut result = Vec::new();
            for i in 1..1000000 {
                result.push(cell0.swap(i));
            }
            results0.lock().unwrap().extend(result.into_iter());
        });
        let thread1 = thread::spawn(move || {
            let mut result = Vec::new();
            for i in 1000000..2000000 {
                result.push(cell1.swap(i));
            }
            results1.lock().unwrap().extend(result.into_iter());
        });
        thread0.join().unwrap();
        thread1.join().unwrap();
        let mut v = results.lock().unwrap();
        v.push(cell.swap(0));
        v.sort();

        assert_eq!(v.len(), 2000000);
        for (a, &b) in v.iter().enumerate() {
            assert_eq!(a, b);
        }
    }

    fn bounded_atomic_cell_smoke() {
        use containers::atomic_cell::BoundedAtomicCell;

        let results = Arc::new(Mutex::new(Vec::new()));
        let results0 = results.clone();
        let results1 = results.clone();
        let mut cell = BoundedAtomicCell::new(5, 0);
        let mut cell0 = cell.clone();
        let mut cell1 = cell.clone();
        let thread0 = thread::spawn(move || {
            let mut result = Vec::new();
            for i in 1..1000000 {
                result.push(cell0.swap(i));
            }
            results0.lock().unwrap().extend(result.into_iter());
        });
        let thread1 = thread::spawn(move || {
            let mut result = Vec::new();
            for i in 1000000..2000000 {
                result.push(cell1.swap(i));
            }
            results1.lock().unwrap().extend(result.into_iter());
        });
        thread0.join().unwrap();
        thread1.join().unwrap();
        let mut v = results.lock().unwrap();
        v.push(cell.swap(0));
        v.sort();

        assert_eq!(v.len(), 2000000);
        for (a, &b) in v.iter().enumerate() {
            assert_eq!(a, b);
        }
    }

    fn resizing_atomic_cell_smoke() {
        use containers::atomic_cell::ResizingAtomicCell;

        let results = Arc::new(Mutex::new(Vec::new()));
        let results0 = results.clone();
        let results1 = results.clone();
        let mut cell = ResizingAtomicCell::new(5, 0);
        let mut cell0 = cell.clone();
        let mut cell1 = cell.clone();
        let thread0 = thread::spawn(move || {
            let mut result = Vec::new();
            for i in 1..1000000 {
                result.push(cell0.swap(i));
            }
            results0.lock().unwrap().extend(result.into_iter());
        });
        let thread1 = thread::spawn(move || {
            let mut result = Vec::new();
            for i in 1000000..2000000 {
                result.push(cell1.swap(i));
            }
            results1.lock().unwrap().extend(result.into_iter());
        });
        thread0.join().unwrap();
        thread1.join().unwrap();
        let mut v = results.lock().unwrap();
        v.push(cell.swap(0));
        v.sort();

        assert_eq!(v.len(), 2000000);
        for (a, &b) in v.iter().enumerate() {
            assert_eq!(a, b);
        }
    }

    fn bounded_mpsc_queue_smoke() {
        use containers::mpsc_queue::{BoundedMpscQueueSender, BoundedMpscQueueReceiver};
        use std::time::Duration;

        let sender_count = 20;
        let rapid_fire = 10;
        let iterations = 10;
        let mut threads = Vec::with_capacity(sender_count);
        let mut receiver = BoundedMpscQueueReceiver::new(sender_count, 50);
        for i in 0..sender_count {
            let mut sender = BoundedMpscQueueSender::new(&receiver);
            threads.push(thread::spawn(move || {
                for _ in 0..iterations {
                    for _ in 0..rapid_fire {
                        while sender.send(i).is_err() {
                            thread::sleep(Duration::from_millis(1));
                        }
                    }
                    thread::sleep(Duration::from_millis(1));
                }
            }));
        }
        let expected = rapid_fire*iterations*sender_count;
        let mut results = vec![0; sender_count];

        for _ in 0..expected {
            loop {
                if let Ok(v) = receiver.receive() {
                    results[v] += 1;
                    break;
                }
            }
        }

        for t in threads {
            let _ = t.join();
        }
        
        for r in results {
            assert!(r == rapid_fire*iterations);
        }
    }

    fn bounded_mpmc_queue_smoke() {
        use containers::mpmc_queue::{BoundedMpmcQueueSender, BoundedMpmcQueueReceiver, self};
        use std::time::Duration;

        let sender_count = 20;
        let rapid_fire = 10;
        let iterations = 1000;
        let mut threads = Vec::with_capacity(sender_count);
        let (sender, receiver): (BoundedMpmcQueueSender<_>, BoundedMpmcQueueReceiver<_>) = mpmc_queue::new(sender_count*2+2, 50);
        let results = Arc::new(Mutex::new(vec![0; sender_count]));
        for i in 0..sender_count {
            let mut sender = sender.clone();
            let mut receiver = receiver.clone();
            let results = results.clone();
            threads.push(thread::spawn(move || {
                for _ in 0..iterations {
                    for _ in 0..rapid_fire {
                        while sender.send(i).is_err() {
                            thread::sleep(Duration::from_millis(1));
                        }
                    }
                    thread::sleep(Duration::from_millis(1));
                }
            }));
            threads.push(thread::spawn(move || {
                let mut tmp = vec![0; sender_count];
                for _ in 0..iterations {
                    for _ in 0..rapid_fire {
                        loop {
                            if let Ok(i) = receiver.receive() {
                                tmp[i] += 1;
                                break;
                            }
                            thread::sleep(Duration::from_millis(1));
                        }
                    }
                    thread::sleep(Duration::from_millis(1));
                }

                let mut r = results.lock().unwrap();
                for index in 0..sender_count {
                    (*r)[index] += tmp[index];
                }
            }));
        }

        for t in threads {
            let _ = t.join();
        }
        
        for &r in &*results.lock().unwrap() {
            assert!(r == rapid_fire*iterations);
        }
    }

    #[bench]
    fn atomic_cell_bench(b: &mut Bencher) {
        b.iter(|| atomic_cell_smoke())
    }

    #[bench]
    fn bounded_atomic_cell_bench(b: &mut Bencher) {
        b.iter(|| bounded_atomic_cell_smoke())
    }

    #[bench]
    fn resizing_atomic_cell_bench(b: &mut Bencher) {
        b.iter(|| resizing_atomic_cell_smoke())
    }

    #[bench]
    fn bounded_mpsc_queue_bench(b: &mut Bencher) {
        b.iter(|| bounded_mpsc_queue_smoke())
    }

    #[bench]
    fn bounded_mpmc_queue_bench(b: &mut Bencher) {
        b.iter(|| bounded_mpmc_queue_smoke())
    }
}
