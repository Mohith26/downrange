//! Telemetry sources: live ISS, replay of a recorded ISS session, and a synthetic
//! generator that exists only for load testing.

use crate::app::{Input, IngestStats};
use crate::iss::{self, IssEvent};
use crate::model::{now_ms, now_us, Channel, Limits, Sample};
use crate::record::{RecLine, Recorder, Recording};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Status.Class 24 is what the ISSLIVE feed sends while the station has signal.
pub fn iss_status(status: &str) -> u8 {
    if status == "24" {
        0
    } else {
        1
    }
}

pub fn parse_value(v: &str) -> f64 {
    v.trim().parse::<f64>().unwrap_or(f64::NAN)
}

/// Live ISS feed. Optionally records every row to `record_path`.
pub async fn run_iss(channels: Vec<Channel>, tx: mpsc::Sender<Input>, record_path: Option<String>) {
    let ids: Vec<String> = channels.iter().map(|c| c.id.clone()).collect();
    let (rtx, mut rrx) = mpsc::unbounded_channel();
    tokio::spawn(iss::run(ids.clone(), rtx));
    let mut rec = record_path.and_then(|p| Recorder::create(&p, &ids, now_ms()).ok());
    let mut batch = Vec::new();
    loop {
        let ev = match rrx.recv().await {
            Some(e) => e,
            None => return,
        };
        let push = |ev: IssEvent, batch: &mut Vec<Sample>, rec: &mut Option<Recorder>| match ev {
            IssEvent::Row { item, timestamp, value, status, recv_ms } => {
                let (ts, v, st) = (timestamp.unwrap_or_default(), value.unwrap_or_default(), status.unwrap_or_default());
                if let Some(r) = rec.as_mut() {
                    let _ = r.row(recv_ms, item, &ts, &v, &st);
                }
                batch.push(Sample { ch: item as u32, t: recv_ms, v: parse_value(&v), st: iss_status(&st), ing_us: now_us() });
                None
            }
            IssEvent::Connected { session } => Some((true, format!("connected {session}"))),
            IssEvent::Disconnected { reason } => Some((false, reason)),
        };
        let mut links = vec![];
        if let Some(l) = push(ev, &mut batch, &mut rec) {
            links.push(l);
        }
        while let Ok(ev) = rrx.try_recv() {
            if let Some(l) = push(ev, &mut batch, &mut rec) {
                links.push(l);
            }
        }
        if let Some(r) = rec.as_mut() {
            for (up, reason) in &links {
                let _ = r.event(now_ms(), &format!("{} {reason}", if *up { "up" } else { "down" }));
            }
            let _ = r.flush();
        }
        if !batch.is_empty() && tx.send(Input::Samples(std::mem::take(&mut batch))).await.is_err() {
            return;
        }
        for (up, reason) in links {
            let _ = tx.send(Input::Link { up, reason }).await;
        }
    }
}

/// Replays a recording with its original timing divided by `speed`. Sample time is
/// shifted so the replay looks live (t = now at the moment of replay); the original
/// spacing between rows is preserved. Loops forever when `looped`.
pub async fn run_replay(rec: Recording, channels: &[Channel], speed: f64, looped: bool, tx: mpsc::Sender<Input>) {
    let map: Vec<Option<u32>> = rec
        .channels
        .iter()
        .map(|id| channels.iter().position(|c| &c.id == id).map(|i| i as u32))
        .collect();
    loop {
        let _ = tx.send(Input::Link { up: true, reason: "replay start".into() }).await;
        let start = tokio::time::Instant::now();
        let mut batch = Vec::new();
        for line in &rec.lines {
            let (off, row) = match line {
                RecLine::Row { off_ms, idx, value, status, .. } => (*off_ms, Some((*idx, value, status))),
                RecLine::Event { off_ms, .. } => (*off_ms, None),
            };
            let due = start + Duration::from_secs_f64(off as f64 / 1000.0 / speed);
            if tokio::time::Instant::now() < due {
                if !batch.is_empty() && tx.send(Input::Samples(std::mem::take(&mut batch))).await.is_err() {
                    return;
                }
                tokio::time::sleep_until(due).await;
            }
            if let Some((idx, value, status)) = row {
                if let Some(Some(ch)) = map.get(idx) {
                    batch.push(Sample { ch: *ch, t: now_ms(), v: parse_value(value), st: iss_status(status), ing_us: now_us() });
                }
            }
        }
        if !batch.is_empty() {
            let _ = tx.send(Input::Samples(batch)).await;
        }
        if !looped {
            return;
        }
    }
}

pub fn synth_channels(n: usize, hz: f64) -> Vec<Channel> {
    (0..n)
        .map(|i| Channel {
            id: format!("SYN{i:04}"),
            desc: format!("Synthetic load channel {i} (benchmark only)"),
            group: "Synthetic".into(),
            unit: "arb".into(),
            limits: if i < 8 {
                Some(Limits { lo_alarm: Some(10.0), lo_warn: Some(20.0), hi_warn: Some(80.0), hi_alarm: Some(90.0), hyst: 1.0 })
            } else {
                None
            },
            stale_ms: Some(((5000.0 / hz).max(500.0)) as u64),
        })
        .collect()
}

/// Synthetic generator: `n` channels at `hz` each, emitted in batches every `tick_ms`.
/// A full ingest queue counts as a stall.
pub async fn run_synth(n: usize, hz: f64, tick_ms: u64, stats: Arc<IngestStats>, tx: mpsc::Sender<Input>, stop: Arc<std::sync::atomic::AtomicBool>) {
    let start = std::time::Instant::now();
    let rate = n as f64 * hz;
    let mut emitted: u64 = 0;
    let mut iv = tokio::time::interval(Duration::from_millis(tick_ms));
    iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut rng_state: u64 = 0x9E3779B97F4A7C15;
    while !stop.load(Ordering::Relaxed) {
        let scheduled = iv.tick().await;
        let lag = scheduled.elapsed().as_micros() as u64;
        stats.max_sched_lag_us.fetch_max(lag, Ordering::Relaxed);
        let el = start.elapsed().as_secs_f64();
        let target = (el * rate) as u64;
        if target <= emitted {
            continue;
        }
        let count = (target - emitted) as usize;
        let mut batch = Vec::with_capacity(count);
        let t = now_ms();
        let ing = now_us();
        for k in 0..count {
            let idx = emitted + k as u64;
            let ch = (idx % n as u64) as u32;
            rng_state ^= rng_state << 13;
            rng_state ^= rng_state >> 7;
            rng_state ^= rng_state << 17;
            let noise = (rng_state % 1000) as f64 / 1000.0 - 0.5;
            let v = 50.0 + 20.0 * (el * 0.5 + ch as f64 * 0.37).sin() + noise;
            batch.push(Sample { ch, t, v, st: 0, ing_us: ing });
        }
        emitted = target;
        match tx.try_send(Input::Samples(batch)) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(b)) => {
                stats.stalls.fetch_add(1, Ordering::Relaxed);
                if tx.send(b).await.is_err() {
                    return;
                }
            }
            Err(_) => return,
        }
    }
}
