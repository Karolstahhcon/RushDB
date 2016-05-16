//! Planner value-distribution summaries: estimates only,
//! never query results.

/// Number of equi-width buckets in a [`Histogram`].
pub const VALUE_DISTRIBUTION_BUCKETS: usize = 64;

/// Value-domain shape of one `(kind, pid)` for planner estimates:
/// exact extrema and distinct count, plus an equi-width histogram over the
/// order-preserving payload space. Computed on demand from the value
/// dictionary; never a query result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Histogram {
    /// Smallest payload.
    pub min: Vec<u8>,
    /// Largest payload.
    pub max: Vec<u8>,
    /// Distinct payloads.
    pub distinct: u64,
    /// Owner-occurrence counts per fixed bucket.
    pub buckets: [u32; VALUE_DISTRIBUTION_BUCKETS],
    /// The walk that built this stopped at its budget: buckets, distinct and
    /// `max` cover only the walked (smallest) prefix of the value domain.
    pub partial: bool,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            min: Vec::new(),
            max: Vec::new(),
            distinct: 0,
            buckets: [0; VALUE_DISTRIBUTION_BUCKETS],
            partial: false,
        }
    }
}

impl Histogram {
    /// Fold one distinct payload held by `count` owner-occurrences.
    pub fn add(&mut self, payload: &[u8], count: u64) {
        if self.min.is_empty() || payload < self.min.as_slice() {
            self.min = payload.to_vec();
        }
        if payload > self.max.as_slice() {
            self.max = payload.to_vec();
        }
        self.distinct += 1;
        let bucket = &mut self.buckets[fixed_bucket_index(payload)];
        *bucket = bucket.saturating_add(u32::try_from(count).unwrap_or(u32::MAX));
    }

    /// Resident bytes, for cache accounting.
    #[must_use]
    pub fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.min.capacity() + self.max.capacity()
    }

    /// Total owner-occurrences across all buckets.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.buckets.iter().map(|&b| u64::from(b)).sum()
    }

    /// Estimated owners satisfying a range bound (`above` = `Gt`/`Gte`),
    /// interpolating uniformly within the bound's fixed bucket.
    #[must_use]
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss,
        reason = "selectivity estimate: a non-negative fraction of a u32 bucket count"
