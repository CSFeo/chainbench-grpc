use dashmap::{DashMap, DashSet};
use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
};
use tracing::info;

use crate::domain::timing::TransactionData;

/// Thread-safe concurrent comparator for multi-endpoint transaction tracking.
/// Adapted from GeyserBench's Comparator with extended timestamp support.
#[derive(Debug)]
pub struct Comparator {
    data: DashMap<String, HashMap<String, TransactionData>>,
    emitted: DashSet<String>,
}

impl Default for Comparator {
    fn default() -> Self {
        Self::new()
    }
}

impl Comparator {
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
            emitted: DashSet::new(),
        }
    }

    pub fn add_batch(&self, from: &str, transactions: HashMap<String, TransactionData>) {
        for (signature, data) in transactions {
            let mut entry = self.data.entry(signature).or_default();
            entry.insert(from.to_owned(), data);
        }
    }

    /// Record an observation from an endpoint. Returns Some(snapshot) when all
    /// expected producers have reported this signature (emitted only once).
    pub fn record_observation(
        &self,
        endpoint: &str,
        signature: &str,
        data: TransactionData,
        expected_producers: usize,
    ) -> Option<HashMap<String, TransactionData>> {
        if expected_producers == 0 {
            return None;
        }

        let mut entry = self.data.entry(signature.to_owned()).or_default();

        let mut updated = false;
        entry
            .entry(endpoint.to_owned())
            .and_modify(|existing| {
                if data.elapsed_since_start < existing.elapsed_since_start {
                    *existing = data.clone();
                    updated = true;
                }
            })
            .or_insert_with(|| {
                updated = true;
                data.clone()
            });

        if !updated {
            return None;
        }

        if entry.len() != expected_producers {
            return None;
        }

        let snapshot = entry.clone();
        drop(entry);

        if self.emitted.insert(signature.to_owned()) {
            Some(snapshot)
        } else {
            None
        }
    }

    pub fn iter(&self) -> dashmap::iter::Iter<'_, String, HashMap<String, TransactionData>> {
        self.data.iter()
    }
}

/// Per-endpoint dedup accumulator. Keeps earliest observation per signature.
#[derive(Default)]
pub struct TransactionAccumulator {
    entries: HashMap<String, TransactionData>,
}

impl TransactionAccumulator {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn record(&mut self, signature: String, data: TransactionData) -> bool {
        use std::collections::hash_map::Entry;
        match self.entries.entry(signature) {
            Entry::Vacant(entry) => {
                entry.insert(data);
                true
            }
            Entry::Occupied(mut entry) => {
                if data.elapsed_since_start < entry.get().elapsed_since_start {
                    entry.insert(data);
                    true
                } else {
                    false
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn into_inner(self) -> HashMap<String, TransactionData> {
        self.entries
    }
}

/// Progress tracker that logs at 5% increments.
#[derive(Debug)]
pub struct ProgressTracker {
    target: usize,
    next_checkpoint: AtomicUsize,
}

impl ProgressTracker {
    pub fn new(target: usize) -> Self {
        Self {
            target,
            next_checkpoint: AtomicUsize::new(5),
        }
    }

    pub fn record(&self, current: usize) {
        if self.target == 0 {
            return;
        }
        let percent = (current.saturating_mul(100)) / self.target.max(1);
        loop {
            let next = self.next_checkpoint.load(Ordering::Acquire);
            if next > 100 || percent < next {
                break;
            }
            if self
                .next_checkpoint
                .compare_exchange(next, next + 5, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let clamped = next.min(100);
                info!(
                    progress = %format!("{}%", clamped),
                    current,
                    target = self.target,
                );
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::timing::{TimestampSource, TransactionData};
    use std::time::Duration;

    fn tx(elapsed_ms: u64) -> TransactionData {
        TransactionData {
            timestamp_ms: 1_000_000.0,
            timestamp_source: TimestampSource::ServerCreatedAt,
            client_wallclock_ms: 1_000_000.0 + elapsed_ms as f64,
            elapsed_since_start: Duration::from_millis(elapsed_ms),
            start_wallclock_ms: 1_000_000.0,
        }
    }

    #[test]
    fn record_observation_emits_once_when_all_producers_report() {
        let c = Comparator::new();
        // First of two producers -> not complete yet.
        assert!(c.record_observation("a", "sig", tx(10), 2).is_none());
        // Second producer completes the set -> emits a snapshot once.
        let snap = c.record_observation("b", "sig", tx(20), 2);
        assert!(snap.is_some());
        assert_eq!(snap.unwrap().len(), 2);
        // Any further observation for the same sig does not re-emit.
        assert!(c.record_observation("a", "sig", tx(5), 2).is_none());
    }

    #[test]
    fn record_observation_single_producer_emits_immediately() {
        let c = Comparator::new();
        let snap = c.record_observation("solo", "sig", tx(10), 1);
        assert!(snap.is_some());
        assert_eq!(snap.unwrap().len(), 1);
    }

    #[test]
    fn record_observation_keeps_earliest_for_same_endpoint() {
        let c = Comparator::new();
        assert!(c.record_observation("a", "sig", tx(50), 1).is_some());
        // A slower duplicate from the same endpoint is not an update -> None.
        assert!(c.record_observation("a", "sig", tx(80), 1).is_none());
        // Faster duplicate is an update, but already emitted -> still None,
        // yet the stored value is the earliest.
        assert!(c.record_observation("a", "sig", tx(20), 1).is_none());
    }

    #[test]
    fn record_observation_zero_producers_is_noop() {
        let c = Comparator::new();
        assert!(c.record_observation("a", "sig", tx(10), 0).is_none());
    }

    #[test]
    fn accumulator_earliest_wins() {
        let mut acc = TransactionAccumulator::new();
        assert!(acc.is_empty());
        assert!(acc.record("sig".into(), tx(50))); // new -> true
        assert!(!acc.record("sig".into(), tx(80))); // slower -> false (no update)
        assert!(acc.record("sig".into(), tx(20))); // faster -> true (replaced)
        assert_eq!(acc.len(), 1);
        assert!(!acc.is_empty());
    }
}
