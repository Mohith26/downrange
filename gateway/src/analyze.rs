//! Offline pass over a recorded ISS session: runs every row through the same limit and
//! signal logic as the live gateway, on the recording's own clock.

use crate::limits::{AlarmKind, LimitEngine};
use crate::model::{Channel, Sample};
use crate::record::{RecLine, Recording};
use crate::sources::{iss_status, parse_value};
use serde_json::{json, Value};

pub fn analyze(rec: &Recording, channels: &[Channel]) -> Value {
    let map: Vec<Option<usize>> =
        rec.channels.iter().map(|id| channels.iter().position(|c| &c.id == id)).collect();
    let mut eng = LimitEngine::new(channels.to_vec(), Some("TIME_000001"), 10_000.0);
    let mut per_chan = vec![0u64; channels.len()];
    let mut bad_status = vec![0u64; channels.len()];
    let mut non_numeric = 0u64;
    let mut rows = 0u64;
    let mut disconnects = 0u64;
    let mut connects = 0u64;
    let mut events = vec![];
    let mut last_tick = 0.0f64;
    let mut los_ms = 0.0f64;
    let mut prev_t: Option<f64> = None;
    let mut first_off = None;
    let mut last_off = 0u64;
    let base = rec.start_unix_ms as f64;
    for line in &rec.lines {
        let off = match line {
            RecLine::Row { off_ms, .. } | RecLine::Event { off_ms, .. } => *off_ms,
        };
        let now = base + off as f64;
        // Run the periodic checks on the recording's clock.
        while last_tick + 250.0 <= now {
            last_tick = if last_tick == 0.0 { now } else { last_tick + 250.0 };
            eng.tick(last_tick, &mut events);
        }
        if let Some(p) = prev_t {
            if !eng.feed_aos() {
                los_ms += now - p;
            }
        }
        prev_t = Some(now);
        match line {
            RecLine::Event { text, .. } => {
                if text.starts_with("connected") {
                    connects += 1;
                } else if text.starts_with("disconnected") {
                    disconnects += 1;
                }
            }
            RecLine::Row { idx, value, status, .. } => {
                first_off.get_or_insert(off);
                last_off = off;
                let Some(Some(ch)) = map.get(*idx) else { continue };
                rows += 1;
                per_chan[*ch] += 1;
                let st = iss_status(status);
                if st != 0 {
                    bad_status[*ch] += 1;
                }
                let v = parse_value(value);
                if !v.is_finite() {
                    non_numeric += 1;
                }
                eng.feed(&Sample { ch: *ch as u32, t: now, v, st, ing_us: 0 }, now, &mut events);
            }
        }
    }
    let mut signal = vec![];
    let mut limit_transitions = 0u64;
    let mut alarm_entries = 0u64;
    for e in &events {
        match &e.kind {
            AlarmKind::Signal { aos, reason } => signal.push(json!({
                "t_offset_s": ((e.t - base) / 1000.0).round(),
                "aos": aos,
                "reason": reason,
            })),
            AlarmKind::Limit { to, .. } => {
                limit_transitions += 1;
                if to.severity() == 2 {
                    alarm_entries += 1;
                }
            }
            AlarmKind::Stale { .. } => {}
        }
    }
    let duration_min = (last_off - first_off.unwrap_or(0)) as f64 / 60000.0;
    let active = per_chan.iter().filter(|&&n| n > 0).count();
    let mut busiest: Vec<(usize, u64)> = per_chan.iter().copied().enumerate().collect();
    busiest.sort_by(|a, b| b.1.cmp(&a.1));
    json!({
        "start_unix_ms": rec.start_unix_ms,
        "minutes_recorded": (duration_min * 100.0).round() / 100.0,
        "channels_subscribed": rec.channels.len(),
        "channels_with_updates": active,
        "updates_received": rows,
        "updates_per_second_avg": ((rows as f64 / (duration_min * 60.0)) * 100.0).round() / 100.0,
        "non_numeric_values": non_numeric,
        "rows_with_source_status_not_nominal": bad_status.iter().sum::<u64>(),
        "channels_with_source_status_not_nominal": bad_status.iter().filter(|&&n| n > 0).count(),
        "feed_connects": connects,
        "feed_disconnects": disconnects,
        "los_events": eng.los_events,
        "seconds_in_los": (los_ms / 1000.0).round(),
        "signal_transitions": signal,
        "limit_transitions": limit_transitions,
        "alarm_level_entries": alarm_entries,
        "busiest_channels": busiest.iter().take(5).map(|(i, n)| json!({"id": channels[*i].id, "updates": n})).collect::<Vec<_>>(),
    })
}
