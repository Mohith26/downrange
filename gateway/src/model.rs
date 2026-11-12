use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// One telemetry sample after ingest. `ing_us` is the gateway wall-clock time at ingest,
/// used to measure ingest-to-client latency.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Sample {
    pub ch: u32,
    /// Source timestamp, unix milliseconds.
    pub t: f64,
    /// Numeric value (NaN when the source value was not a number).
    pub v: f64,
    /// Signal status: 0 = good, 1 = loss of signal reported by the source.
    pub st: u8,
    pub ing_us: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Limits {
    pub lo_alarm: Option<f64>,
    pub lo_warn: Option<f64>,
    pub hi_warn: Option<f64>,
    pub hi_alarm: Option<f64>,
    /// Value must move this far back inside a threshold before the state clears.
    #[serde(default)]
    pub hyst: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Channel {
    pub id: String,
    pub desc: String,
    pub group: String,
    pub unit: String,
    #[serde(default)]
    pub limits: Option<Limits>,
    /// Per-channel staleness window. `None` means the channel only publishes on change,
    /// so its age alone says nothing about liveness (ISS channels work this way).
    #[serde(default)]
    pub stale_ms: Option<u64>,
}

pub fn now_us() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_micros() as u64).unwrap_or(0)
}

pub fn now_ms() -> f64 {
    now_us() as f64 / 1000.0
}

/// Days since 1970-01-01 for a civil date (Howard Hinnant's algorithm).
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

pub fn year_of_unix_ms(ms: f64) -> i64 {
    let days = (ms / 86_400_000.0).floor() as i64;
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    if m <= 2 { y + 1 } else { y }
}

/// ISSLIVE timestamps are fractional hours counted from 00:00 UTC on Dec 31 of the
/// previous year (so Jan 1 00:00 is hour 24). Convert to unix ms using the year of `now_ms`.
pub fn iss_hours_to_unix_ms(hours: f64, now_ms: f64) -> f64 {
    let y = year_of_unix_ms(now_ms);
    let base = days_from_civil(y - 1, 12, 31) as f64 * 86_400_000.0;
    let t = base + hours * 3_600_000.0;
    // Around new year the feed can still be on last year's clock.
    if t - now_ms > 180.0 * 86_400_000.0 {
        days_from_civil(y - 2, 12, 31) as f64 * 86_400_000.0 + hours * 3_600_000.0
    } else {
        t
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_round_trip() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(2026, 11, 12), 20769);
        assert_eq!(year_of_unix_ms(20769.0 * 86_400_000.0), 2026);
        assert_eq!(year_of_unix_ms(days_from_civil(2026, 12, 31) as f64 * 86_400_000.0), 2026);
        assert_eq!(year_of_unix_ms(days_from_civil(2027, 1, 1) as f64 * 86_400_000.0), 2027);
    }

    #[test]
    fn iss_hours_convert() {
        let now = days_from_civil(2026, 11, 12) as f64 * 86_400_000.0 + 3.0 * 3_600_000.0;
        // Nov 12 is day-of-year 316; hour count from Dec 31 = 316*24 + 3.
        let t = iss_hours_to_unix_ms(316.0 * 24.0 + 3.0, now);
        assert!((t - now).abs() < 1.0);
    }
}
