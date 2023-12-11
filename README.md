# AtomHash

Constraints: 
- Only insertions and lookups, no updates or deletions.
- Keys must be `usize`.

The original implementation used linear probing to find an available bucket when a hash collision occured. This was slow and unsable for high contention scenarios.

Changed the collision resolution algorithm for linked lists. This improved the performance of the overall structure because now collisions of keys are constrained to the set of collided keys without affecting other buckets. Performance is looking very good. It's quite faster than using HashBrown with a Mutex or RwLock.

TODO: add perfs comparisons

