//! Measurements. Each run starts a gateway in-process on a loopback port and drives it
//! with real WebSocket clients, so every number goes through the same code path as the
//! console. The load comes from the synthetic generator, never from the ISS feed.

use crate::app::{serve, Gateway, Input};
use crate::command::synth_commands;
use crate::hub::HubConfig;
use crate::model::{now_ms, now_us, Sample};
use crate::sources::{run_synth, synth_channels};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

/// Resident memory of this process in bytes.
pub fn rss_bytes() -> u64 {
    #[cfg(target_os = "macos")]
    unsafe {
        let mut info: libc::proc_taskinfo = std::mem::zeroed();
        let size = std::mem::size_of::<libc::proc_taskinfo>() as i32;
        let r = libc::proc_pidinfo(libc::getpid(), libc::PROC_PIDTASKINFO, 0, &mut info as *mut _ as *mut libc::c_void, size);
        if r == size {
            return info.pti_resident_size;
        }
        0
    }
    #[cfg(target_os = "linux")]
    {
        let s = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
        let pages: u64 = s.split_whitespace().nth(1).and_then(|v| v.parse().ok()).unwrap_or(0);
        pages * 4096
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        0
    }
}

pub struct Running {
    pub gw: Arc<Gateway>,
    pub port: u16,
    pub stop: Arc<AtomicBool>,
}

/// Start a gateway with the synthetic source. `hz == 0` means no generator (tests feed it).
pub async fn start_synth(nchan: usize, hz: f64, hub: HubConfig) -> Running {
    let gw = Gateway::new(synth_channels(nchan, hz.max(1.0)), synth_commands(), None, "synthetic", hub, None);
    let (tx, rx) = mpsc::channel::<Input>(64);
    gw.spawn_ingest(rx);
    gw.spawn_ticker();
    let stop = Arc::new(AtomicBool::new(false));
    if hz > 0.0 {
        tokio::spawn(run_synth(nchan, hz, 2, gw.stats.clone(), tx, stop.clone()));
    } else {
        std::mem::forget(tx);
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(serve(gw.clone(), listener));
    Running { gw, port, stop }
}

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

pub async fn connect(port: u16, resume_from: Option<u64>) -> Ws {
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/ws")).await.expect("connect");
    let hello = json!({"type": "hello", "resume_from": resume_from}).to_string();
    ws.send(Message::Text(hello)).await.unwrap();
    ws
}

#[derive(Deserialize)]
struct TmFrame {
    #[serde(rename = "type")]
    kind: String,
    first: Option<u64>,
    seq: Option<u64>,
    #[serde(default)]
    s: Vec<(u32, f64, Option<f64>, u8, u64)>,
}

#[derive(Default, Clone)]
pub struct ClientResult {
    pub samples: u64,
    pub gaps: u64,
    pub coalesced_frames: u64,
    pub lat_us: Vec<u32>,
}

/// Read telemetry until `until`; latency recorded only after `measure_from`.
async fn fast_client(mut ws: Ws, measure_from: tokio::time::Instant, until: tokio::time::Instant) -> ClientResult {
    let mut r = ClientResult::default();
    let mut next_seq: Option<u64> = None;
    let mut k = 0u64;
    loop {
        let msg = tokio::select! {
            _ = tokio::time::sleep_until(until) => break,
            m = ws.next() => m,
        };
        let Some(Ok(Message::Text(t))) = msg else { break };
        if !t.starts_with("{\"type\":\"tm\"") && !t.starts_with("{\"type\":\"coalesced\"") && !t.starts_with("{\"type\":\"snapshot\"") {
            continue;
        }
        let Ok(f) = serde_json::from_str::<TmFrame>(&t) else { continue };
        let now = now_us();
        let measuring = tokio::time::Instant::now() >= measure_from;
        match f.kind.as_str() {
            "tm" => {
                let first = f.first.unwrap_or(0);
                if let Some(n) = next_seq {
                    if first != n && measuring {
                        r.gaps += 1;
                    }
                }
                next_seq = Some(first + f.s.len() as u64);
                if measuring {
                    r.samples += f.s.len() as u64;
                    for s in &f.s {
                        k += 1;
                        if k % 8 == 0 {
                            r.lat_us.push(now.saturating_sub(s.4).min(u32::MAX as u64) as u32);
                        }
                    }
                }
            }
            "coalesced" => {
                if measuring {
                    r.coalesced_frames += 1;
                }
                next_seq = f.seq.map(|s| s + 1);
            }
            _ => next_seq = f.seq.map(|s| s + 1),
        }
    }
    let _ = ws.close(None).await;
    r
}

fn pct(v: &mut [u32], p: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_unstable();
    let i = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[i] as f64 / 1000.0
}

/// One load point: `nchan` x `hz` fanned out to `clients` WebSocket clients.
pub async fn throughput_point(nchan: usize, hz: f64, clients: usize, secs: f64) -> Value {
    let run = start_synth(nchan, hz, HubConfig { ring_samples: 200_000, client_queue_samples: 200_000, client_control_msgs: 2000 }).await;
    let mut wss = vec![];
    for _ in 0..clients {
        wss.push(connect(run.port, None).await);
    }
    let start = tokio::time::Instant::now();
    let measure_from = start + Duration::from_secs(1);
    let until = measure_from + Duration::from_secs_f64(secs);
    let pub0 = Arc::new(AtomicU64::new(0));
    {
        let gw = run.gw.clone();
        let p = pub0.clone();
        tokio::spawn(async move {
            tokio::time::sleep_until(measure_from).await;
            p.store(gw.hub.published.load(Ordering::Relaxed), Ordering::Relaxed);
        });
    }
    let handles: Vec<_> = wss.into_iter().map(|ws| tokio::spawn(fast_client(ws, measure_from, until))).collect();
    let mut results = vec![];
    for h in handles {
        results.push(h.await.unwrap());
    }
    run.stop.store(true, Ordering::Relaxed);
    let published = run.gw.hub.published.load(Ordering::Relaxed) - pub0.load(Ordering::Relaxed);
    let stats = run.gw.stats_json();
    let mut all_lat: Vec<u32> = results.iter().flat_map(|r| r.lat_us.iter().copied()).collect();
    let min_recv = results.iter().map(|r| r.samples).min().unwrap_or(0);
    let offered = nchan as f64 * hz;
    let delivered_rate = min_recv as f64 / secs;
    let stalls = stats["ingest"]["stalls"].as_u64().unwrap_or(0);
    let coalesced: u64 = results.iter().map(|r| r.coalesced_frames).sum();
    let gaps: u64 = results.iter().map(|r| r.gaps).sum();
    let pass = stalls == 0 && coalesced == 0 && gaps == 0 && delivered_rate >= offered * 0.98;
    json!({
        "channels": nchan,
        "hz_per_channel": hz,
        "offered_samples_per_s": offered,
        "clients": clients,
        "measure_s": secs,
        "published_samples_in_window": published,
        "min_client_samples_per_s": delivered_rate.round(),
        "ingest_stalls": stalls,
        "max_sched_lag_ms": stats["ingest"]["max_sched_lag_us"].as_u64().unwrap_or(0) as f64 / 1000.0,
        "max_ingest_batch_ms": stats["ingest"]["max_batch_ns"].as_u64().unwrap_or(0) as f64 / 1e6,
        "coalesced_frames": coalesced,
        "seq_gaps": gaps,
        "latency_ms_p50": pct(&mut all_lat, 0.50),
        "latency_ms_p99": pct(&mut all_lat, 0.99),
        "latency_ms_max": pct(&mut all_lat, 1.0),
        "latency_samples": all_lat.len(),
        "pass": pass,
    })
}

/// Fast clients plus one client that completes the handshake and then never reads.
pub async fn slow_client(nchan: usize, hz: f64, fast: usize, stalled_n: usize, secs: f64, queue_cap: usize) -> Value {
    let run = start_synth(nchan, hz, HubConfig { ring_samples: 200_000, client_queue_samples: queue_cap, client_control_msgs: 2000 }).await;
    let rss_start = rss_bytes();
    let mut wss = vec![];
    for _ in 0..fast {
        wss.push(connect(run.port, None).await);
    }
    // The stalled client: socket stays open, nothing is ever read.
    let mut stalled = vec![];
    for _ in 0..stalled_n {
        stalled.push(connect(run.port, None).await);
    }
    let start = tokio::time::Instant::now();
    let measure_from = start + Duration::from_secs(1);
    let until = measure_from + Duration::from_secs_f64(secs);
    let handles: Vec<_> = wss.into_iter().map(|ws| tokio::spawn(fast_client(ws, measure_from, until))).collect();
    let timeline = Arc::new(Mutex::new(vec![]));
    {
        let gw = run.gw.clone();
        let tl = timeline.clone();
        tokio::spawn(async move {
            let t0 = tokio::time::Instant::now();
            while tokio::time::Instant::now() < until {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let cs = gw.hub.clients();
                let worst = cs.iter().map(|c| c.stats()).max_by_key(|s| s.dropped_samples).unwrap_or_default();
                tl.lock().unwrap().push(json!({
                    "t_s": (t0.elapsed().as_secs_f64() * 10.0).round() / 10.0,
                    "rss_mb": (rss_bytes() as f64 / 1048576.0 * 10.0).round() / 10.0,
                    "stalled_queued_samples": worst.queued_samples,
                    "stalled_dropped_samples": worst.dropped_samples,
                }));
            }
        });
    }
    let mut results = vec![];
    for h in handles {
        results.push(h.await.unwrap());
    }
    run.stop.store(true, Ordering::Relaxed);
    let stats = run.gw.stats_json();
    let clients = stats["clients"].as_array().cloned().unwrap_or_default();
    let worst = if stalled_n == 0 {
        json!({"max_queued_samples": null, "dropped_samples": null, "coalesce_events": null})
    } else {
        clients.iter().max_by_key(|c| c["dropped_samples"].as_u64().unwrap_or(0)).cloned().unwrap_or(json!({}))
    };
    let mut fast_lat: Vec<u32> = results.iter().flat_map(|r| r.lat_us.iter().copied()).collect();
    let offered = nchan as f64 * hz;
    let min_recv = results.iter().map(|r| r.samples).min().unwrap_or(0);
    drop(stalled);
    let tl = timeline.lock().unwrap().clone();
    let rss_vals: Vec<f64> = tl.iter().map(|p| p["rss_mb"].as_f64().unwrap_or(0.0)).collect();
    json!({
        "channels": nchan,
        "hz_per_channel": hz,
        "fast_clients": fast,
        "stalled_clients": stalled_n,
        "measure_s": secs,
        "client_queue_cap_samples": queue_cap,
        "stalled_client_max_queued_samples": worst["max_queued_samples"],
        "stalled_client_dropped_samples": worst["dropped_samples"],
        "stalled_client_coalesce_events": worst["coalesce_events"],
        "ingest_stalls": stats["ingest"]["stalls"],
        "fast_min_samples_per_s": (min_recv as f64 / secs).round(),
        "fast_delivery_ratio": ((min_recv as f64 / secs / offered) * 10000.0).round() / 10000.0,
        "fast_seq_gaps": results.iter().map(|r| r.gaps).sum::<u64>(),
        "fast_coalesced_frames": results.iter().map(|r| r.coalesced_frames).sum::<u64>(),
        "fast_latency_ms_p50": pct(&mut fast_lat, 0.5),
        "fast_latency_ms_p99": pct(&mut fast_lat, 0.99),
        "rss_mb_at_start": (rss_start as f64 / 1048576.0 * 10.0).round() / 10.0,
        "rss_mb_min_during": rss_vals.iter().cloned().fold(f64::INFINITY, f64::min),
        "rss_mb_max_during": rss_vals.iter().cloned().fold(0.0, f64::max),
        "timeline": tl,
    })
}

/// Small deterministic PRNG so fault patterns are reproducible from a seed.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn chance(&mut self, p: f64) -> bool {
        (self.next() % 10_000) as f64 / 10_000.0 < p
    }
}

async fn send(ws: &mut Ws, v: Value) {
    ws.send(Message::Text(v.to_string())).await.unwrap();
}

/// Wait for the command reply for `key`, skipping telemetry and broadcasts.
async fn reply_for(ws: &mut Ws, key: &str) -> Value {
    loop {
        let m = tokio::time::timeout(Duration::from_secs(5), ws.next()).await.expect("reply timeout");
        let Some(Ok(Message::Text(t))) = m else { panic!("socket closed") };
        if std::env::var("DR_DEBUG").is_ok() && !t.starts_with("{\"type\":\"tm\"") {
            eprintln!("rx {}", &t[..t.len().min(300)]);
        }
        if !t.starts_with("{\"type\":\"cmd_reply\"") {
            continue;
        }
        let v: Value = serde_json::from_str(&t).unwrap();
        if v["reply"]["key"] == key {
            return v["reply"].clone();
        }
    }
}

/// N commands through arm/confirm with injected faults: lost replies (client retries),
/// duplicate confirms, reconnects between arm and confirm, and interlock breaches.
pub async fn command_faults(n: usize, seed: u64) -> Value {
    let run = start_synth(4, 0.0, HubConfig::default()).await;
    let breach = Arc::new(AtomicBool::new(false));
    let applied = Arc::new(AtomicU64::new(0));
    {
        // Feeder keeps every channel fresh. During a breach SYN0000 reads 85 (warn high).
        let gw = run.gw.clone();
        let (b, a) = (breach.clone(), applied.clone());
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(Duration::from_millis(10));
            loop {
                iv.tick().await;
                let br = b.load(Ordering::SeqCst);
                let t = now_ms();
                let batch: Vec<Sample> = (0..4u32)
                    .map(|ch| Sample { ch, t, v: if ch == 0 && br { 85.0 } else { 50.0 }, st: 0, ing_us: now_us() })
                    .collect();
                gw.ingest(&batch);
                a.store(if br { 1 } else { 2 }, Ordering::SeqCst);
            }
        });
    }
    let mut rng = Rng(seed | 1);
    let mut ws = connect(run.port, None).await;
    if std::env::var("DR_DEBUG").is_ok() {
        eprintln!("connected");
    }
    let (mut lost_arm, mut lost_confirm, mut dup_confirm, mut reconnects) = (0u64, 0u64, 0u64, 0u64);
    let (mut interlock_rejections, mut dedup_replies, mut executed_replies) = (0u64, 0u64, 0u64);
    let wait_state = |want: u64| {
        let a = applied.clone();
        async move {
            a.store(0, Ordering::SeqCst);
            while a.load(Ordering::SeqCst) != want {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
    };
    // Make sure every channel has a fresh value before the first command.
    wait_state(2).await;
    for i in 0..n {
        if std::env::var("DR_DEBUG").is_ok() {
            eprintln!("command {i}");
        }
        let key = format!("cmd-{seed}-{i}");
        let (cmd, args) = if i % 2 == 0 {
            ("SIM_VALVE_OPEN", json!({"pct": (i % 100) as f64}))
        } else {
            ("SIM_HEATER_ON", json!({}))
        };
        if cmd == "SIM_VALVE_OPEN" && rng.chance(0.15) {
            breach.store(true, Ordering::SeqCst);
            wait_state(1).await;
            send(&mut ws, json!({"type": "arm", "key": key, "cmd": cmd, "args": args})).await;
            let r = reply_for(&mut ws, &key).await;
            assert_eq!(r["type"], "rejected", "breach must block arming: {r}");
            interlock_rejections += 1;
            breach.store(false, Ordering::SeqCst);
            wait_state(2).await;
        }
        send(&mut ws, json!({"type": "arm", "key": key, "cmd": cmd, "args": args})).await;
        let mut armed = reply_for(&mut ws, &key).await;
        if rng.chance(0.2) {
            // Reply "lost": the operator console retries the arm with the same key.
            lost_arm += 1;
            send(&mut ws, json!({"type": "arm", "key": key, "cmd": cmd, "args": args})).await;
            let again = reply_for(&mut ws, &key).await;
            assert_eq!(again["token"], armed["token"], "re-arm must return the same token");
            armed = again;
        }
        assert_eq!(armed["type"], "armed", "{armed}");
        let token = armed["token"].as_str().unwrap().to_string();
        if rng.chance(0.15) {
            reconnects += 1;
            let _ = ws.close(None).await;
            ws = connect(run.port, Some(run.gw.hub.last_seq())).await;
        }
        let confirm = json!({"type": "confirm", "key": key, "token": token});
        let mut expected_replies = 1;
        send(&mut ws, confirm.clone()).await;
        if rng.chance(0.2) {
            dup_confirm += 1;
            expected_replies += 1;
            send(&mut ws, confirm.clone()).await;
        }
        let mut replies = vec![reply_for(&mut ws, &key).await];
        if rng.chance(0.2) {
            // The confirm reply was "lost": retry with the same key and token.
            lost_confirm += 1;
            expected_replies += 1;
            send(&mut ws, confirm.clone()).await;
        }
        while replies.len() < expected_replies {
            replies.push(reply_for(&mut ws, &key).await);
        }
        for r in &replies {
            assert_eq!(r["type"], "executed", "{r}");
            if r["dedup"] == true {
                dedup_replies += 1;
            } else {
                executed_replies += 1;
            }
        }
    }
    let core = run.gw.core.lock().unwrap();
    let ex = &core.cmds.device.executions;
    let exactly_once = ex.values().filter(|&&c| c == 1).count();
    let more_than_once = ex.values().filter(|&&c| c > 1).count();
    json!({
        "commands": n,
        "seed": seed,
        "injected": {
            "lost_arm_replies": lost_arm,
            "lost_confirm_replies": lost_confirm,
            "duplicate_confirms": dup_confirm,
            "reconnects_between_arm_and_confirm": reconnects,
            "interlock_breaches": interlock_rejections,
        },
        "interlock_rejections": interlock_rejections,
        "device_executions_total": core.cmds.device.total_executions,
        "keys_executed_exactly_once": exactly_once,
        "keys_executed_more_than_once": more_than_once,
        "keys_never_executed": n - ex.len(),
        "dedup_replies": dedup_replies,
        "first_execution_replies": executed_replies,
        "audit_counters": core.cmds.counters,
    })
}
