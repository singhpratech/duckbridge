//! Range-splitting of large tables so a single table streams on several connections.
#[derive(Debug, Clone)]
pub struct Range {
    pub lo: Option<i128>, // inclusive
    pub hi: Option<i128>, // exclusive
}

impl Range {
    pub fn predicate(&self, col: &str) -> String {
        match (self.lo, self.hi) {
            (None, None) => "1=1".into(),
            (Some(a), None) => format!("{col} >= {a}"),
            (None, Some(b)) => format!("({col} < {b} OR {col} IS NULL)"),
            (Some(a), Some(b)) => format!("{col} >= {a} AND {col} < {b}"),
        }
    }
}

/// Split [min, max] into `n` evenly spaced ranges; first range is open below and the last open above,
/// so rows outside the sampled bounds (or NULL keys) are never lost.
pub fn ranges(min: i128, max: i128, n: usize) -> Vec<Range> {
    if n <= 1 || max <= min { return vec![Range { lo: None, hi: None }]; }
    let span = (max - min + 1) as f64;
    let mut cuts: Vec<i128> = (1..n).map(|i| min + (span * i as f64 / n as f64) as i128).collect();
    cuts.dedup();
    let mut out = vec![];
    let mut prev: Option<i128> = None;
    for c in cuts {
        out.push(Range { lo: prev, hi: Some(c) });
        prev = Some(c);
    }
    out.push(Range { lo: prev, hi: None });
    out
}
