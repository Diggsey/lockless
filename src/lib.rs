#![feature(test)]
extern crate test;

extern crate parking_lot;

pub mod primitives;
pub mod handle;
pub mod containers;

#[cfg(test)]
mod tests {

    use test::Bencher;
    use std::thread;
    use std::sync::{Mutex, Arc};

    #[test]
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

    #[test]
    fn bounded_atomic_cell_smoke() {
        use containers::atomic_cell::BoundedAtomicCell;

        let results = Arc::new(Mutex::new(Vec::new()));
        let results0 = results.clone();
        let results1 = results.clone();
        let cell = BoundedAtomicCell::new(0, 5);
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

    #[test]
    fn resizing_atomic_cell_smoke() {
        use containers::atomic_cell::ResizingAtomicCell;

        let results = Arc::new(Mutex::new(Vec::new()));
        let results0 = results.clone();
        let results1 = results.clone();
        let cell = ResizingAtomicCell::new(0, 5);
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
}
