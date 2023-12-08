//#![no_std]

/// A Concurrent HashMap with the following constraints:
/// - Only usize keys
/// - Only Insertions (No updates or deletes)


extern crate alloc;
use core::{sync::atomic::{AtomicPtr, AtomicUsize, Ordering}};
use alloc::{boxed::Box};

extern crate xorshift;
use xorshift::Rng;


#[derive(Debug)]
pub struct Entry<V> {
    key: usize,
    val: V
}

pub enum HashMapErr<'a, V> {
    HashMapFull,
    ExistentEntry(&'a V)
}

pub type Bucket<V> = AtomicPtr<Entry<V>>;

pub struct HashMap<V, const N: usize> {
    /// The state of this table
    permutation: [usize; N],

    /// Number of entries in the Table
    entries:    AtomicUsize,

    /// The buckets in the table.
    buckets: [Bucket<V>; N],
}

impl<V, const N: usize> Drop for HashMap<V, N> {
    fn drop(&mut self) {
        for idx in 0..N {
            // Get the entry
            let ptr = self.buckets[idx].load(Ordering::SeqCst);

            if !ptr.is_null() {
                // Take ownership of the value to drop it
                let boxed_ptr = unsafe { Box::from_raw(ptr) };
                drop(boxed_ptr);
            }
        }
    }
}

impl<V, const N: usize> HashMap<V, N> {

    pub fn entries(&self) -> usize {
        self.entries.load(Ordering::Acquire)
    }

    fn scramble<T>(rng: &mut Rng, slice: &mut [T]) {
        // Fisher-Yates shuffle algorithm
        for i in (1..slice.len()).rev() {
            let j = rng.get_random(i);
            slice.swap(i, j);
        }
    }

    pub fn new() -> Self {
        let mut rng = Rng::new(1);
        
        for _ in 1..10 {
            rng.rand();
        }
       
        return Self::new_with_seed(rng.rand())        
    }

    pub fn new_with_seed(seed: usize) -> Self {
        let mut rng = Rng::new(seed);
        let mut permutation_table: Vec<usize> = (0..N).collect();        
        Self::scramble(&mut rng, &mut permutation_table);
        // println!("{:?}", &permutation_table);
        
        HashMap {
            permutation:   permutation_table.as_slice().try_into().unwrap(),   
            entries:       AtomicUsize::new(0),        
            buckets:       unsafe { core::mem::zeroed() }
        }
    }

    /// Returns a position inside the table 
    /// based on the permutation table and the key
    #[inline]
    fn get_idx(&self, key: usize) -> usize {
        self.permutation[key & (N - 1)]
    }

    // debug method
    fn print_map(&self) {
        for idx in 0..N {
            let bucket = &self.buckets[idx];
            let entry_ptr = bucket.load(Ordering::Acquire);
            if entry_ptr.is_null() {    
                println!("Idx:[{:x}: NULL", idx);
                continue;
            }

            let cur_key = unsafe { (*entry_ptr).key  };
            println!("Idx:[{:x}: {:x} -> {}", 
                idx, entry_ptr as usize, cur_key );
        }
    }

    pub fn lookup(&self, key: usize) -> Option<&V> {

        let start_idx = self.get_idx(key);
        let mut idx   = start_idx;

        let mut bucket = &self.buckets[idx];

        loop {
            let entry_ptr = bucket.load(Ordering::Acquire);
            
            if entry_ptr.is_null() {    
                return None;
            } 

            let cur_key = unsafe { (*entry_ptr).key };

            if key == cur_key {
                return Some( unsafe { &(*entry_ptr).val } )
            } else {
                // Collided keys,, probe linearly until the entry
                // is either found or a the entry is null
                // if the entry is null it means the value isn't in the map
                idx    = (idx + 1) & (N - 1);
                if idx == start_idx {
                    // We wrapped around, in the search and the entry did not appear
                    return None;
                }
                bucket = &self.buckets[idx];
            }
        }
    }


    /// Insert a entry into the table
    pub fn insert(&self, key: usize, value: V) -> Result<&V, HashMapErr<V>> {

        // Get index for the entry
        let start_idx = self.get_idx(key);
        let mut idx   = start_idx;
        
        //println!("idx: {}", idx);

        let mut bucket = &self.buckets[idx];
        
        // A pointer to Null
        let empty:    *mut Entry<V> =  0 as *mut Entry<V>;

        let new_entry_ptr = Box::into_raw(Box::new(Entry {key, val: value}));
        
        // We use CAS to place the entry if and only if the bucket is empty. Otherwise, we must
        // handle the respective cases.
        if bucket.compare_exchange(empty, new_entry_ptr, 
            Ordering::Release,
            Ordering::Acquire).is_ok() {

            self.entries.fetch_add(1, Ordering::Release);
            
            // CAS suceeded, return new inserted entry value reference;
            return Ok( unsafe { &(*new_entry_ptr).val });

        } else { 
            // The entry was already present, or we lost the race?

            let cur_entry_ptr = bucket.load(Ordering::Acquire);
            let cur_entry = unsafe { &*cur_entry_ptr };

            // Check if the key matches with ours, if so, return the existent
            if cur_entry.key == key {
                return Err(HashMapErr::ExistentEntry(&cur_entry.val));

            } else {
                // Keys were different, go linear probing  
                loop {
                    idx = (idx + 1) & (N - 1);

                    if idx == start_idx {
                        // Wrapped around, return
                        return Err(HashMapErr::HashMapFull);
                    }

                    bucket = &self.buckets[idx];

                    while bucket.load(Ordering::Acquire) != empty {
                        idx    = (idx + 1) & (N - 1);
                        bucket = &self.buckets[idx];

                        if self.entries.load(Ordering::Acquire) == N {
                            return Err(HashMapErr::HashMapFull);
                        }
                    }

                    // Empty bucket found, attempt to insert
                    if bucket.compare_exchange(empty, new_entry_ptr, 
                        Ordering::Release,
                        Ordering::Acquire).is_ok() {
            
                        self.entries.fetch_add(1, Ordering::Release);
                        
                        // CAS suceeded, return new inserted entry value reference;
                        return Ok( unsafe { &(*new_entry_ptr).val });
                    }
                }                    
            }

        }

    }

}




#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_1() {

        let map = HashMap::<u64, 1024>::new();
  
        let _ = map.insert(1337,41);
        let _ = map.insert(1338,0);
        let _ = map.insert(1,1337);
        let _ = map.insert(1012312,55);
        let _ = map.insert(11111,999999);

        assert_eq!(41, *map.lookup(1337).unwrap());
        assert_eq!(0, *map.lookup(1338).unwrap());
        assert_eq!(1337, *map.lookup(1).unwrap());
        assert_eq!(55, *map.lookup(1012312).unwrap());
        assert_eq!(999999, *map.lookup(11111).unwrap());

        assert_eq!(map.entries(), 5);
    }

    /// Test insertions that end up with the same index in the HashMap
    /// This ends in linear probing internally
    #[test]
    fn test_2() {

        let map = HashMap::<String, 8>::new_with_seed(12311);       
        
        let s1 = "first string".into();
        let s2 = "second string".into();
        let s3 = "third string".into();
        let s4 = "foasodfjiosdg".into();
        let s5 = "collision please?".into();
        let s6 = "fasdafzxvng".into();
        let s7 = "fixccccing".into();
        let s8 = "fi1231211111ng".into();
        
        let _  = map.insert(0, s1);
        let _  = map.insert(1, s2);
        let _  = map.insert(16, s3);
        let _  = map.insert(24, s4);
        let _  = map.insert(32, s5);
        let _  = map.insert(40, s6);
        let _  = map.insert(48, s7);
        let _  = map.insert(56, s8);

        assert_eq!(map.lookup(0).unwrap(), "first string");
        assert_eq!(map.lookup(32).unwrap(), "collision please?");

        assert_eq!(map.entries(), 8);
    }


    /// Tests that an insertion with the same key returns the
    /// first value inserted instead in a HashMapErr::ExistentEntry
    /// Lookup returns the first value as well
    /// Meaning the second value just doesn't get accepted in the HashMap
    #[test]
    fn test_3() {

        let map = HashMap::<String, 8>::new_with_seed(12311);       
        
        let s1 = "first string".into();
        let s2 = "second string".into();
        
        let _  = map.insert(0, s1);
        let v = map.insert(0, s2).err().unwrap();
        match v {
            HashMapErr::HashMapFull => assert!(false),
            HashMapErr::ExistentEntry(x) => {
                assert_eq!(x, "first string");        
            },
        }
        
        assert_eq!(map.lookup(0).unwrap(), "first string");

        assert_eq!(map.entries(), 1);
    }

    /// Test map iteration
    #[test]
    fn test_4() {

        let map = HashMap::<u64, 8>::new_with_seed(12311);       
        
        let _  = map.insert(0, 1337);
        let _  = map.insert(4, 2020);
        let _  = map.insert(8, 2023);
        let _  = map.insert(12, 1990);        
        
        map.print_map();

    }


    /// Test map iteration
    #[test]
    fn test_5() {

        let map = HashMap::<u64, 8>::new_with_seed(12311);       
        
        let _  = map.insert(0, 1337);
        let _  = map.insert(8, 2020);
        let _  = map.insert(16, 2023);
        let _  = map.insert(24, 1990);        
        
        map.print_map();

        assert_eq!(*map.lookup(24).unwrap(), 1990);
        assert_eq!(*map.lookup(0).unwrap(), 1337);
        assert_eq!(*map.lookup(16).unwrap(), 2023);
        assert_eq!(*map.lookup(8).unwrap(), 2020);
    }

    #[test]
    fn test_thrads_1() {

        let map = Arc::new(HashMap::<u64, 256>::new_with_seed(1337)); 

        let map_t1 = map.clone(); 
        let map_t2 = map.clone();

        let t1 = std::thread::spawn(move || {
            let mut rng = Rng::new(789678922);
            for _ in 0..rng.get_random(200) {
                let _ = map_t1.insert(rng.rand(), 
                    (rng.get_random(100000000) as u64) + 1).ok();                

            }

            for _ in 0..rng.get_random(200) {
                let v = map_t1.lookup(rng.rand());
                match v {
                    None => {},
                    Some(x) => assert!(*x >= 1)
                }                
            }
        });


        let t2 = std::thread::spawn(move || {
            let mut rng = Rng::new(789678922);
            for _ in 0..rng.get_random(200) {
                let v = map_t2.lookup(rng.rand());
                match v {
                    None => {},
                    Some(x) => assert!(*x >= 1)
                }                
            }

            for _ in 0..rng.get_random(50) {
                let _ = map_t2.insert(rng.rand(), 
                    (rng.get_random(100000000) as u64) + 1).ok();                

            }
        });

        let _ = t1.join();
        let _ = t2.join();

        map.print_map();
        
    }
}
