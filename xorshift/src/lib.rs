#![no_std]

pub struct Rng {
    state: usize,
    iter:  usize
}

impl Rng {

    pub fn new(seed: usize) -> Self {
        Rng {
            state: seed,
            iter:  0,
        }
    }

    pub fn seed(&mut self, seed: usize) {
        self.state = seed;
    }

    pub fn get_state(&self) -> usize {
        self.state
    }

    pub fn get_iteration(&self) -> usize {
        self.iter
    }

    pub fn rand(&mut self) -> usize {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 17;
        self.state ^= self.state << 5;
        self.iter += 1;

        return self.state;
    }

    pub fn get_random(&mut self, top: usize) -> usize {
        return self.rand() % top;
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test1() {
        let mut rng1 = Rng::new(1);       
        let mut rng2 = Rng::new(2);     
        for _ in 1..1000 {
            assert_ne!(rng1.rand(), rng2.rand());
        }       
    }

    // #[test]
    // fn test2() {
    //     let rngs: Vec<_> = (1..100).map(
    //         |x| Rng::new(x)).collect();
        
    //     for _ in 1..1000 {
    //         assert_ne!(rng1.rand(), rng2.rand());
    //     }       
    // }
}
