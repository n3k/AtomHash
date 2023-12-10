# AtomHash

This was an attempt of doing something with the Rust atomics but in its current state it doesn't really work when there are a lot of collisions.

The collision resolution algorithm needs to be changed to improve the overall performance.

If the map gets created with enough space to keep collisions low, then this should be faster than HashBrown with Locks. Otherwise use HashBrown with RwLock or Mutex instead? :/
