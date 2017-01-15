mod atomic_cell;
mod spsc;

pub use atomic_cell::AtomicCell;

#[cfg(test)]
mod tests {
    use std::thread;
    use std::sync::{Mutex, Arc};
    use super::*;

    #[test]
    fn atomic_cell_smoke() {
        let results = Arc::new(Mutex::new(Vec::new()));
        let results0 = results.clone();
        let results1 = results.clone();
        let cell = AtomicCell::new(0);
        let cell0 = cell.clone();
        let cell1 = cell.clone();
        let thread0 = thread::spawn(move || {
            let mut result = Vec::new();
            for i in 1..10000 {
                result.push(cell0.swap(i));
            }
            results0.lock().unwrap().extend(result.into_iter());
        });
        let thread1 = thread::spawn(move || {
            let mut result = Vec::new();
            for i in 10000..20000 {
                result.push(cell1.swap(i));
            }
            results1.lock().unwrap().extend(result.into_iter());
        });
        thread0.join().unwrap();
        thread1.join().unwrap();
        let mut v = results.lock().unwrap();
        v.push(cell.swap(0));
        v.sort();

        assert_eq!(v.len(), 20000);
        for (a, &b) in v.iter().enumerate() {
            assert_eq!(a, b);
        }
    }
}
