
extern crate atomic_hashmap;
extern crate xorshift;

use atomic_hashmap::*;
use xorshift::Rng;
use std::sync::Arc;


use std::sync::Mutex;
extern crate hashbrown;
use hashbrown::HashMap as BrownHashMap;


use std::sync::RwLock;

use std::time::Instant;

const MAP_SIZE: usize = 30 * 1024 * 1024;

fn test_compare_perf() {

    let map: &'static _ = Box::leak(Box::new(
        HashMap::<u64, MAP_SIZE>::new_with_seed(1337)
    )); 

    let start = Instant::now();

    let handles: Vec<_> = (0..10).map(|x| {
        std::thread::spawn(move || {
            let mut rng = Rng::new(x + 12125125 );
            for _ in 0..MAP_SIZE/2 {
                let _ = map.insert(rng.rand(), 
                    (rng.get_random(100000000) as u64) + 1).ok();                

            }          
        })
    }).collect();

    for h in handles {
        let _ = h.join();
    }

    let elapsed = start.elapsed().as_secs_f64();

    println!("Elapsed time: {:10.6}", elapsed);
}

fn test_compare_perf_brown() {

    let map = Arc::new(Mutex::new(
        BrownHashMap::<usize, u64>::with_capacity(MAP_SIZE)
    )); 


    let start = Instant::now();

    let handles: Vec<_> = (0..10).map(|x| {
        let xmap = map.clone();
        std::thread::spawn(move || {
            let mut rng = Rng::new(x + 12125125 );
            for _ in 0..MAP_SIZE/2 {
                let _ = xmap.lock().unwrap().insert(rng.rand(), 
                    (rng.get_random(100000000) as u64) + 1);                

            }          
        })
    }).collect();

    for h in handles {
        let _ = h.join();
    }

    let elapsed = start.elapsed().as_secs_f64();

    println!("Elapsed time: {:10.6}", elapsed);
}


fn test_compare_perf_brown_rwlock() {

    let map = Arc::new(RwLock::new(
        BrownHashMap::<usize, u64>::with_capacity(MAP_SIZE)
    )); 

    let start = Instant::now();

    let handles: Vec<_> = (0..10).map(|x| {
        let locked_map = map.clone();
        std::thread::spawn(move || {
            let mut rng = Rng::new(x + 12125125 );
            for _ in 0..MAP_SIZE/2 {
                let mut xmap = locked_map.write().unwrap();
                let _ = xmap.insert(rng.rand(), 
                    (rng.get_random(100000000) as u64) + 1);                

            }          
        })
    }).collect();

    for h in handles {
        let _ = h.join();
    }

    let elapsed = start.elapsed().as_secs_f64();

    println!("Elapsed time: {:10.6}", elapsed);
}



fn test_high_write_contention() {
    /// This looked bad for our implementation 
    /// when linear probing was used
    println!("Our nice implementation:");
    test_compare_perf();

    println!("HashBrown with Mutex: :/");
    test_compare_perf_brown();

    println!("HashBrown with RwLock");
    test_compare_perf_brown_rwlock();
}


fn atomhash_lookup_test() {
    let map: &'static _ = Box::leak(Box::new(
        HashMap::<u64, MAP_SIZE>::new_with_seed(1337)
    )); 


    let handles: Vec<_> = (0..5).map(|x| {
        std::thread::spawn(move || {
            let mut rng = Rng::new(x + 12312545 );
            for _ in 0..MAP_SIZE/3 {
                let _ = map.insert(rng.rand(), 
                    (rng.get_random(100000000) as u64) + 1).ok();                

            }          
        })
    }).collect();

    for h in handles {
        let _ = h.join();
    }

    println!("Inserted entries: {}", map.entries());

    println!("Starting lookups");

    let start = Instant::now();

    let handles: Vec<_> = (0..5).map(|x| {
        std::thread::spawn(move || {
            let mut rng = Rng::new(x + 12312545 );
            for _ in 0..MAP_SIZE/3 {
                assert_eq!(*map.lookup(rng.rand()).unwrap(),
                     (rng.get_random(100000000) as u64) + 1);
               }          
        })
    }).collect();

    for h in handles {
        let _ = h.join();
    }

    let elapsed = start.elapsed().as_secs_f64();

    println!("Elapsed time: {:10.6}", elapsed);
}

fn hashbrown_lookup_test() {

    let map = Arc::new(Mutex::new(
        BrownHashMap::<usize, u64>::with_capacity(MAP_SIZE)
    )); 

    let handles: Vec<_> = (0..5).map(|x| {
        let map = map.clone();
        std::thread::spawn(move || {
            let mut rng = Rng::new(x + 12312545 );
            for _ in 0..MAP_SIZE/3 {
                let _ = map.lock().unwrap().insert(rng.rand(), 
                    (rng.get_random(100000000) as u64) + 1);                

            }          
        })
    }).collect();

    for h in handles {
        let _ = h.join();
    }

    println!("Inserted entries: {}", map.lock().unwrap().len());
    
    println!("Starting lookups");

    let start = Instant::now();

    let handles: Vec<_> = (0..5).map(|x| {
        let map = map.clone();
        std::thread::spawn(move || {
            let mut rng = Rng::new(x + 12312545 );
            for _ in 0..MAP_SIZE/3 {
                assert_eq!(*map.lock().unwrap().get(&rng.rand()).unwrap(),
                     (rng.get_random(100000000) as u64) + 1);
               }          
        })
    }).collect();

    for h in handles {
        let _ = h.join();
    }

    let elapsed = start.elapsed().as_secs_f64();

    println!("Elapsed time: {:10.6}", elapsed);
}

fn test_lookups() {
    atomhash_lookup_test();

    hashbrown_lookup_test();

}

fn main() {

    //test_high_write_contention();
    test_lookups();


}
