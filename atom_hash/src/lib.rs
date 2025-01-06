//#![no_std]

/// A Concurrent HashMap with the following constraints:
/// - Only usize keys
/// - Only Insertions (No updates or deletes)


extern crate alloc;
use core::{sync::atomic::{AtomicPtr, AtomicUsize, Ordering}};
use std::{alloc::{Layout, alloc_zeroed}};
use alloc::{boxed::Box};

extern crate xorshift;
use xorshift::Rng;


#[derive(Debug)]
pub struct Entry<V> {
    key         : usize,
    val         : V,
    next        : AtomicPtr<Entry<V>>
}

pub enum HashMapErr<'a, V> {
    HashMapFull,
    ExistentEntry(&'a V)
}

pub type Bucket<V> = AtomicPtr<Entry<V>>;

#[derive(Debug)]
pub struct HashMap<V, const N: usize> {

    /// Number of entries in the Table
    entries         : AtomicUsize,

    /// Number of collisions
    collisions      : AtomicUsize,

    /// The buckets in the table.
    buckets         : Box<[Bucket<V>; N]>,
}

impl<V, const N: usize> Drop for HashMap<V, N> {
    fn drop(&mut self) {
        for idx in 0..N {
            // Get the entry
            let mut ptr = self.buckets[idx].load(Ordering::SeqCst);

            // Remove all the chained list of items for that bucket
            while !ptr.is_null() {
                // Take ownership of the value to drop it
                let boxed_ptr = unsafe { Box::from_raw(ptr) };
                // Get the next item in the list
                ptr = boxed_ptr.next.load(Ordering::SeqCst);
                // drop the current
                drop(boxed_ptr);
            }
        }
    }
}

impl<V, const N: usize> HashMap<V, N> {

    pub fn entries(&self) -> usize {
        self.entries.load(Ordering::Relaxed)
    }

    pub fn collisions(&self) -> usize {
        self.collisions.load(Ordering::Relaxed)
    }

    pub fn new() -> Self {
        let layout = Layout::array::<Bucket<V>>(N)
            .expect("unable to allocate memory for buckets");

        let raw_buckets = unsafe { alloc_zeroed(layout) }
             as *mut [AtomicPtr<Entry<V>>; N];

        HashMap {
            //permutation:   permutation_table.into_boxed_slice().try_into().unwrap(),   
            entries:       AtomicUsize::new(0),        
            collisions:    AtomicUsize::new(0),
            buckets:       unsafe { Box::from_raw(raw_buckets) }
        }       
    }

    /// Returns a position inside the table 
    /// based on the permutation table and the key
    #[inline]
    fn get_idx(&self, key: usize) -> usize {     
        key & (N - 1)
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

        let idx     = self.get_idx(key);

        let bucket  = &self.buckets[idx];

        let mut entry_ptr = bucket.load(Ordering::Acquire);
        
        if entry_ptr.is_null() {    
            return None;
        } 

        let mut cur_entry = unsafe { &*entry_ptr };

        if key == cur_entry.key {
            return Some( &cur_entry.val );
        } else {
            // Collided keys,, walk the LL
            
            // A pointer to Null
            let empty:    *mut Entry<V> =  0 as *mut Entry<V>;

            loop {               
                let mut next_entry_ptr = cur_entry.next.load(Ordering::Acquire);

                while next_entry_ptr != empty {

                    entry_ptr  = next_entry_ptr;

                    cur_entry = unsafe { &*entry_ptr };
                    if cur_entry.key == key {
                        return Some( &cur_entry.val );
                    }

                    next_entry_ptr = cur_entry.next.load(Ordering::Acquire);
                }

                return None;
            }
        }
        
    }

    /// Insert a entry into the table
    pub fn insert(&self, key: usize, value: V) -> Result<&V, HashMapErr<V>> {
        
        // A pointer to Null
        let empty:    *mut Entry<V> =  0 as *mut Entry<V>;

        // Prepare the new entry ptr
        let new_entry_ptr = 
            Box::into_raw(
                Box::new(Entry {
                    key, val: value, next: AtomicPtr::new(core::ptr::null_mut())
                }));  

        // Get index for the entry
        let idx = self.get_idx(key);

        let bucket = &self.buckets[idx];
                
        // We use CAS to place the entry if and only if the bucket is empty. Otherwise, we must
        // handle the respective cases.
        match bucket.compare_exchange(empty, new_entry_ptr, 
            Ordering::Release,
            Ordering::Acquire) {

            Ok(_) => {

                self.entries.fetch_add(1, Ordering::Relaxed);
            
                // CAS suceeded, return new inserted entry value reference;
                return Ok( unsafe { &(*new_entry_ptr).val });
            }

            Err(mut cur_entry_ptr) => {

                // Hash Collision or Entry already existed

                let cur_entry = unsafe { &*cur_entry_ptr };

                // Check if the key matches with ours, if so, return the existent
                if cur_entry.key == key {
                    drop(unsafe { Box::from_raw(new_entry_ptr) });
                    return Err(HashMapErr::ExistentEntry(&cur_entry.val));

                } else {
                    // Keys were different, go test linked list                    
                    loop {
                        let cur_entry = unsafe { &*cur_entry_ptr };                        
                        let mut next_entry_ptr = cur_entry.next.load(Ordering::Acquire);
                        while next_entry_ptr != empty {

                            cur_entry_ptr  = next_entry_ptr;

                            let cur_entry = unsafe { &*cur_entry_ptr };
                            if cur_entry.key == key {
                                drop(unsafe { Box::from_raw(new_entry_ptr) });
                                return Err(HashMapErr::ExistentEntry(&cur_entry.val));
                            }

                            next_entry_ptr = cur_entry.next.load(Ordering::Acquire);
                        }

                        // Found empty slot in the linked list and keys haven't collided
                        // up to this point
                       
                        if cur_entry.next.compare_exchange(empty, new_entry_ptr, 
                            Ordering::Release,
                            Ordering::Acquire).is_ok() {
                
                            self.entries.fetch_add(1, Ordering::Relaxed);

                            self.collisions.fetch_add(1, Ordering::Relaxed);
                            
                            // CAS suceeded, return new inserted entry value reference;
                            return Ok( unsafe { &(*new_entry_ptr).val });
                        } 

                        // Failed Race,, re-start the loop from the point where 
                        // we were going to insert this

                    }                    
                }
            }

        } 

    }

}


pub struct Iter<'a, V> {
    buckets: &'a [Bucket<V>],
    current_bucket: usize,
    current_entry: Option<&'a Entry<V>>,
}

impl<'a, V, const N: usize> HashMap<V, N> {
    pub fn iter(&'a self) -> Iter<'a, V> {
        Iter {
            buckets: &self.buckets[..],
            current_bucket: 0,
            current_entry: None,
        }
    }
}

impl<'a, V> Iterator for Iter<'a, V> {
    type Item = (&'a usize, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        while self.current_bucket < self.buckets.len() {
            if let Some(entry) = self.current_entry {
                // Traverse the linked list
                let next_ptr = entry.next.load(Ordering::Acquire);
                if !next_ptr.is_null() {
                    self.current_entry = Some(unsafe { &*next_ptr });
                    if let Some(entry) = self.current_entry {
                        return Some((&entry.key, &entry.val));
                    }
                } else {
                    // End of linked list
                    self.current_entry = None; 
                }
            }

            // Move to the next bucket
            let bucket_ptr = self.buckets[self.current_bucket].load(Ordering::Acquire);
            self.current_bucket += 1;
            if !bucket_ptr.is_null() {
                self.current_entry = Some(unsafe { &*bucket_ptr });
                if let Some(entry) = self.current_entry {
                    return Some((&entry.key, &entry.val));
                }
            }
        }

        None
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

        let map = HashMap::<String, 8>::new();       
        
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

        let map = HashMap::<String, 8>::new();       
        
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

        let map = HashMap::<u64, 8>::new();       
        
        let _  = map.insert(0, 1337);
        let _  = map.insert(4, 2020);
        let _  = map.insert(8, 2023);
        let _  = map.insert(12, 1990);        
        
        //map.print_map();

    }


    /// Test map iteration
    #[test]
    fn test_5() {

        let map = HashMap::<u64, 8>::new();       
        
        let _  = map.insert(0, 1337);
        let _  = map.insert(8, 2020);
        let _  = map.insert(16, 2023);
        let _  = map.insert(24, 1990);        
        
        //map.print_map();

        assert_eq!(*map.lookup(24).unwrap(), 1990);
        assert_eq!(*map.lookup(0).unwrap(), 1337);
        assert_eq!(*map.lookup(16).unwrap(), 2023);
        assert_eq!(*map.lookup(8).unwrap(), 2020);
    }

    #[test]
    fn test_6_full() {

        let map = HashMap::<u64, 8>::new();       
        
        let _  = map.insert(0, 1337);
        let _  = map.insert(8, 2020);
        let _  = map.insert(16, 2023);
        let _  = map.insert(24, 1990);   
        let _  = map.insert(12123, 1990);
        let _  = map.insert(212354, 1990);
        let _  = map.insert(2354, 1990);
        let _  = map.insert(66345, 1990);     
        
        assert_eq!(map.entries(), 8);

        match map.insert(0, 2222) {            
            Err(HashMapErr::ExistentEntry(v)) => {
                assert_eq!(1337, *v);
            },            
            _ => assert!(false)
        }
    }

    #[test]
    fn test_thrads_1() {

        let map = Arc::new(HashMap::<u64, 256>::new()); 

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

        //map.print_map();
        
    }

    /// Two threads attempting to insert the same keys
    #[test]
    fn test_threads_2() {

        let map = Arc::new(HashMap::<u64, 256>::new()); 

        let map_t1 = map.clone(); 
        let map_t2 = map.clone();

        let t1 = std::thread::spawn(move || {
            let mut rng = Rng::new(789678922);
            for _ in 0..128 {
                let _ = map_t1.insert(rng.rand(), 
                    (rng.get_random(100000000) as u64) + 1).ok();                
            }
        });

        let t2 = std::thread::spawn(move || {
            let mut rng = Rng::new(789678922);

            for _ in 0..128 {
                let _ = map_t2.insert(rng.rand(), 
                    (rng.get_random(100000000) as u64) + 1).ok();               
            }
        });

        let _ = t1.join();
        let _ = t2.join();

        //map.print_map();

        assert_eq!(map.entries(), 128);

    }


   



    /// 10 threads attempting to insert the same keys
    #[test]
    fn test_threads_3() {

        let map = Arc::new(HashMap::<u64, 2048>::new()); 

        let handles: Vec<_> = (0..10).map(|x| {
            let map_tx = map.clone();
            std::thread::spawn(move || {
                let mut rng = Rng::new(789678922);
                for _ in 0..1024 {
                    let _ = map_tx.insert(rng.rand(), 
                        (rng.get_random(100000000) as u64) + 1).ok();                

                }          
            })
        }).collect();

        for h in handles {
            let _ = h.join();
        }

        assert_eq!(map.entries(), 1024);
        // if map.entries() > 1024 {
        //     map.print_map()
        // }

    }


        /// 10 threads attempting to insert the same keys
        #[test]
        fn test_threads_3x() {
    
            let map = Arc::new(HashMap::<u64, 16384>::new()); 
    
            let handles: Vec<_> = (0..10).map(|x| {
                let map_tx = map.clone();
                std::thread::spawn(move || {
                    let mut rng = Rng::new(789678922);
                    for _ in 0..12000 {
                        let _ = map_tx.insert(rng.rand(), 
                            (rng.get_random(100000000) as u64) + 1).ok();                
    
                    }          
                })
            }).collect();
    
            for h in handles {
                let _ = h.join();
            }
    
            assert_eq!(map.entries(), 12000);
        }


    #[test]
    fn test_vector_values() {

        let map = HashMap::<Vec<u8>, 8>::new();       
        
        let _  = map.insert(0, vec![0u8, 255]);
        let _  = map.insert(8, Vec::new());
        let _  = map.insert(16, vec![0u8, 1u8, 1u8, 1u8, 1u8]);

        let _  = map.insert(16, Vec::new());
        let _  = map.insert(16, Vec::new());
        let _  = map.insert(16, Vec::new());
        
        assert_eq!(map.entries(), 3);

        match map.insert(0, Vec::new()) {            
            Err(HashMapErr::ExistentEntry(v)) => {
                assert_eq!(255, v[1]);
            },            
            _ => assert!(false)
        }

        match map.insert(16, Vec::new()) {            
            Err(HashMapErr::ExistentEntry(v)) => {
                assert_eq!(0, v[0]);
            },            
            _ => assert!(false)
        }
    }

    #[test]
    fn test_collisions_1() {
        let map = HashMap::<Vec<u8>, 8>::new();       
        
        let _  = map.insert(0, vec![0u8, 255]);
        let _  = map.insert(8, Vec::new());
        let _  = map.insert(16, vec![0u8, 1u8, 1u8, 1u8, 1u8]);

        let _  = map.insert(24, Vec::new());
        let _  = map.insert(32, Vec::new());
        let _  = map.insert(2, Vec::new());
        let _  = map.insert(24, Vec::new()); // key exists already
        
        assert_eq!(map.entries(), 6);
        // the first entry is not a collision
        // THe next 4 entries collide with the entry 0
        assert_eq!(map.collisions(), 4); 
    }

    #[test]
    fn test_iter1() {
        let map = HashMap::<Vec<u8>, 8>::new();       
        
        let _  = map.insert(0, vec![0u8, 255]);
        let _  = map.insert(8, Vec::new());
        let _  = map.insert(16, vec![0u8, 1u8, 1u8, 1u8, 1u8]);

        let _  = map.insert(24, Vec::new());
        let _  = map.insert(32, vec![0u8, 2u8, 2u8, 2u8, 2u8]);        
        let _  = map.insert(2, Vec::new());
        let _  = map.insert(24, Vec::new()); // key exists already
        
        assert_eq!(map.collisions(), 4); 

        assert_eq!(6, map.iter().count())
            
    
    }
}
