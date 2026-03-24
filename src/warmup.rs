use std::time::{Duration, Instant};

pub struct WarmupGuard {
    start: Instant,
    duration: Duration,
}

impl WarmupGuard {
    pub fn new(warmup_secs: u64) -> Self {
        Self {
            start: Instant::now(),
            duration: Duration::from_secs(warmup_secs),
        }
    }

    pub fn is_warming_up(&self) -> bool {
        self.start.elapsed() < self.duration
    }

    pub fn remaining(&self) -> Duration {
        self.duration.saturating_sub(self.start.elapsed())
    }

    pub fn duration(&self) -> Duration {
        self.duration
    }
}
