//! Wires sources, limit checking, fan-out and the command path into one gateway, and
//! serves it over a small HTTP + WebSocket server.

use crate::command::{CommandDef, CommandEngine};
use crate::hub::{Hub, HubConfig, Outgoing};
use crate::limits::{AlarmEvent, LimitEngine};
use crate::model::{now_ms, Channel, Sample};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

pub struct Core {
    pub limits: LimitEngine,
    pub cmds: CommandEngine,
}

#[derive(Default)]
pub struct IngestStats {
    pub batches: AtomicU64,
    pub samples: AtomicU64,
    /// Longest time ingest spent on one batch (limits + fan-out), ns.
    pub max_batch_ns: AtomicU64,
    /// Times a source found the ingest queue full and had to wait.
    pub stalls: AtomicU64,
    /// Worst lag of the synthetic generator behind its schedule, us.
    pub max_sched_lag_us: AtomicU64,
}

pub enum Input {
    Samples(Vec<Sample>),
    Link { up: bool, reason: String },
}

pub struct Gateway {
    pub hub: Arc<Hub>,
    pub core: Arc<Mutex<Core>>,
    pub stats: Arc<IngestStats>,
    pub meta: Arc<str>,
    pub source: String,
    pub results_dir: Option<String>,
    pub static_dir: Option<String>,
}

impl Gateway {
    pub fn new(
        channels: Vec<Channel>,
        commands: Vec<CommandDef>,
        heartbeat: Option<&str>,
        source: &str,
        hub_cfg: HubConfig,
        audit_path: Option<&str>,
    ) -> Arc<Self> {
        let meta = serde_json::json!({
            "type": "meta",
            "source": source,
            "device": "loopback (simulated actuator, no real hardware)",
            "channels": channels,
            "commands": commands,
        })
        .to_string();
        let hub = Arc::new(Hub::new(channels.len(), hub_cfg));
        let limits = LimitEngine::new(channels, heartbeat, 10_000.0);
        let cmds = CommandEngine::new(commands, 10_000.0, audit_path);
        Arc::new(Self {
            hub,
            core: Arc::new(Mutex::new(Core { limits, cmds })),
            stats: Arc::new(IngestStats::default()),
            meta: meta.into(),
            source: source.into(),
            results_dir: None,
            static_dir: None,
        })
    }

    pub fn with_dirs(self: Arc<Self>, results: Option<String>, static_dir: Option<String>) -> Arc<Self> {
        let mut g = Arc::try_unwrap(self).ok().expect("configure before sharing");
        g.results_dir = results;
        g.static_dir = static_dir;
        Arc::new(g)
    }

    fn broadcast_alarms(&self, events: &[AlarmEvent]) {
        for e in events {
            let msg = format!("{{\"type\":\"alarm\",\"e\":{}}}", serde_json::to_string(e).unwrap());
            self.hub.broadcast(&msg);
        }
    }

    /// Process one batch synchronously: limits, then fan-out. Never awaits.
    pub fn ingest(&self, batch: &[Sample]) {
        let t0 = std::time::Instant::now();
        let now = now_ms();
        let mut events = Vec::new();
        {
            let mut core = self.core.lock().unwrap();
            for s in batch {
                core.limits.feed(s, now, &mut events);
            }
        }
        self.hub.publish(batch);
        self.broadcast_alarms(&events);
        self.stats.batches.fetch_add(1, Ordering::Relaxed);
        self.stats.samples.fetch_add(batch.len() as u64, Ordering::Relaxed);
        self.stats.max_batch_ns.fetch_max(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    pub fn spawn_ingest(self: &Arc<Self>, mut rx: mpsc::Receiver<Input>) {
        let gw = self.clone();
        tokio::spawn(async move {
            while let Some(input) = rx.recv().await {
                match input {
                    Input::Samples(b) => gw.ingest(&b),
                    Input::Link { up, reason } => {
                        let msg = serde_json::json!({"type": "link", "up": up, "reason": reason}).to_string();
                        gw.hub.broadcast(&msg);
                    }
                }
            }
        });
    }

    /// Staleness checks every 250 ms and interlock status for the UI every 500 ms.
    pub fn spawn_ticker(self: &Arc<Self>) {
        let gw = self.clone();
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(Duration::from_millis(250));
            let mut n = 0u64;
            loop {
                iv.tick().await;
                n += 1;
                let now = now_ms();
                let mut events = Vec::new();
                let report = {
                    let mut core = gw.core.lock().unwrap();
                    core.limits.tick(now, &mut events);
                    if n % 2 == 0 {
                        let r = core.cmds.interlock_report(&core.limits, now);
                        Some((core.limits.feed_aos(), r))
                    } else {
                        None
                    }
                };
                gw.broadcast_alarms(&events);
                if let Some((aos, r)) = report {
                    let cmds: Vec<_> = r
                        .into_iter()
                        .map(|(name, ok, st)| serde_json::json!({"name": name, "ok": ok, "checks": st}))
                        .collect();
                    let msg = serde_json::json!({"type": "interlocks", "aos": aos, "t": now, "cmds": cmds});
                    gw.hub.broadcast(&msg.to_string());
                }
            }
        });
    }

    pub fn stats_json(&self) -> serde_json::Value {
        let core = self.core.lock().unwrap();
        let clients: Vec<_> = self.hub.clients().iter().map(|c| c.stats()).collect();
        serde_json::json!({
            "source": self.source,
            "last_seq": self.hub.last_seq(),
            "published": self.hub.published.load(Ordering::Relaxed),
            "max_publish_ns": self.hub.max_publish_ns.load(Ordering::Relaxed),
            "ingest": {
                "batches": self.stats.batches.load(Ordering::Relaxed),
                "samples": self.stats.samples.load(Ordering::Relaxed),
                "max_batch_ns": self.stats.max_batch_ns.load(Ordering::Relaxed),
                "stalls": self.stats.stalls.load(Ordering::Relaxed),
                "max_sched_lag_us": self.stats.max_sched_lag_us.load(Ordering::Relaxed),
            },
            "limits": {
                "aos": core.limits.feed_aos(),
                "los_events": core.limits.los_events,
                "stale_events": core.limits.stale_events,
                "limit_events": core.limits.limit_events,
            },
            "commands": {
                "counters": core.cmds.counters,
                "device_executions": core.cmds.device.total_executions,
            },
            "clients": clients,
        })
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMsg {
    Hello { resume_from: Option<u64> },
    Arm { key: String, cmd: String, #[serde(default)] args: HashMap<String, f64> },
    Confirm { key: String, token: String },
    Ping { t: f64 },
}

async fn ws_session(gw: Arc<Gateway>, stream: TcpStream) {
    let Ok(ws) = tokio_tungstenite::accept_async(stream).await else { return };
    let (mut tx, mut rx) = ws.split();
    if tx.send(Message::Text(gw.meta.to_string())).await.is_err() {
        return;
    }
    // Wait for hello so we know where to resume from.
    let mut client = None;
    let mut pending: Vec<ClientMsg> = Vec::new();
    while client.is_none() {
        let Some(Ok(msg)) = rx.next().await else { return };
        let Message::Text(t) = msg else { continue };
        match serde_json::from_str::<ClientMsg>(&t) {
            Ok(ClientMsg::Hello { resume_from }) => client = Some(gw.hub.subscribe(resume_from).0),
            Ok(other) => pending.push(other),
            Err(_) => {}
        }
    }
    let client = client.unwrap();
    let writer_client = client.clone();
    let writer = tokio::spawn(async move {
        let mut out = Vec::new();
        loop {
            writer_client.drain(&mut out);
            for Outgoing::Text(t) in out.drain(..) {
                if tx.send(Message::Text(t.to_string())).await.is_err() {
                    writer_client.close();
                    return;
                }
            }
            if writer_client.is_closed() {
                let _ = tx.close().await;
                return;
            }
            writer_client.notify.notified().await;
        }
    });
    let handle = |m: ClientMsg| {
        let now = now_ms();
        if let ClientMsg::Ping { t } = m {
            client.push_control(format!("{{\"type\":\"pong\",\"t\":{t},\"server\":{now}}}").into());
            return;
        }
        let (reply, audit) = {
            let mut core = gw.core.lock().unwrap();
            let Core { limits, cmds } = &mut *core;
            match m {
                ClientMsg::Arm { key, cmd, args } => cmds.arm(client.id, &key, &cmd, args, limits, now),
                ClientMsg::Confirm { key, token } => cmds.confirm(client.id, &key, &token, limits, now),
                _ => return,
            }
        };
        let r = serde_json::to_value(&reply).unwrap();
        client.push_control(serde_json::json!({"type": "cmd_reply", "reply": r}).to_string().into());
        for e in audit {
            gw.hub.broadcast(&serde_json::json!({"type": "audit", "e": e}).to_string());
        }
    };
    for m in pending {
        handle(m);
    }
    while let Some(Ok(msg)) = rx.next().await {
        match msg {
            Message::Text(t) => {
                if let Ok(m) = serde_json::from_str::<ClientMsg>(&t) {
                    handle(m);
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    client.close();
    let _ = writer.await;
}

fn content_type(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" => "text/javascript",
        "css" => "text/css",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "rec" | "txt" => "text/plain",
        _ => "application/octet-stream",
    }
}

async fn http_reply(stream: &mut TcpStream, status: &str, ctype: &str, body: &[u8]) {
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Headers: content-type\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(body).await;
    let _ = stream.shutdown().await;
}

async fn handle_conn(gw: Arc<Gateway>, mut stream: TcpStream) {
    let _ = stream.set_nodelay(true);
    let mut buf = vec![0u8; 8192];
    let mut head_len = 0;
    for _ in 0..200 {
        let n = match stream.peek(&mut buf).await {
            Ok(n) => n,
            Err(_) => return,
        };
        if let Some(p) = buf[..n].windows(4).position(|w| w == b"\r\n\r\n") {
            head_len = p + 4;
            break;
        }
        if n == buf.len() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    if head_len == 0 {
        return;
    }
    let head = String::from_utf8_lossy(&buf[..head_len]).to_string();
    let lower = head.to_ascii_lowercase();
    if lower.contains("upgrade: websocket") {
        return ws_session(gw, stream).await;
    }
    let mut first = head.lines().next().unwrap_or("").split(' ');
    let method = first.next().unwrap_or("").to_string();
    let path = first.next().unwrap_or("/").split('?').next().unwrap_or("/").to_string();
    let clen: usize = lower
        .lines()
        .find_map(|l| l.strip_prefix("content-length:").map(|v| v.trim().parse().unwrap_or(0)))
        .unwrap_or(0);
    let mut req = vec![0u8; head_len + clen.min(1 << 20)];
    if stream.read_exact(&mut req).await.is_err() {
        return;
    }
    let body = &req[head_len..];
    match (method.as_str(), path.as_str()) {
        ("OPTIONS", _) => http_reply(&mut stream, "204 No Content", "text/plain", b"").await,
        ("GET", "/health") => http_reply(&mut stream, "200 OK", "text/plain", b"ok").await,
        ("GET", "/stats") => {
            let s = gw.stats_json().to_string();
            http_reply(&mut stream, "200 OK", "application/json", s.as_bytes()).await
        }
        ("POST", "/shutdown") => {
            http_reply(&mut stream, "200 OK", "text/plain", b"bye").await;
            tokio::spawn(async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                std::process::exit(0);
            });
        }
        ("POST", "/bench-result") => {
            let ok = match (&gw.results_dir, serde_json::from_slice::<serde_json::Value>(body)) {
                (Some(dir), Ok(v)) => {
                    std::fs::write(format!("{dir}/browser_render.json"), serde_json::to_string_pretty(&v).unwrap()).is_ok()
                }
                _ => false,
            };
            let status = if ok { "200 OK" } else { "400 Bad Request" };
            http_reply(&mut stream, status, "text/plain", if ok { b"saved" } else { b"rejected" }).await
        }
        ("GET", p) => {
            let Some(dir) = &gw.static_dir else {
                return http_reply(&mut stream, "404 Not Found", "text/plain", b"no static dir").await;
            };
            let rel = p.trim_start_matches('/');
            let rel = if rel.is_empty() || rel.contains("..") { "index.html" } else { rel };
            let full = format!("{dir}/{rel}");
            match tokio::fs::read(&full).await {
                Ok(b) => http_reply(&mut stream, "200 OK", content_type(&full), &b).await,
                Err(_) => match tokio::fs::read(format!("{dir}/index.html")).await {
                    Ok(b) => http_reply(&mut stream, "200 OK", "text/html; charset=utf-8", &b).await,
                    Err(_) => http_reply(&mut stream, "404 Not Found", "text/plain", b"not found").await,
                },
            }
        }
        _ => http_reply(&mut stream, "405 Method Not Allowed", "text/plain", b"").await,
    }
}

pub async fn serve(gw: Arc<Gateway>, listener: TcpListener) {
    loop {
        let Ok((stream, _)) = listener.accept().await else { continue };
        tokio::spawn(handle_conn(gw.clone(), stream));
    }
}
