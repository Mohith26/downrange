//! WebSocket fan-out core. Ingest calls `publish`, which never awaits: each client has a
//! bounded queue, and a client that falls behind is switched to "latest value per
//! channel" coalescing instead of slowing anyone else down.

use crate::model::Sample;
use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

#[derive(Debug, Clone)]
pub struct HubConfig {
    /// Samples kept for resume-from-seq.
    pub ring_samples: usize,
    /// Max samples waiting in one client's queue before it is coalesced.
    pub client_queue_samples: usize,
    /// Max control messages (alarms, command replies) queued per client.
    pub client_control_msgs: usize,
}

impl Default for HubConfig {
    fn default() -> Self {
        Self { ring_samples: 200_000, client_queue_samples: 20_000, client_control_msgs: 2_000 }
    }
}

/// A serialized batch of consecutive samples, shared by every client.
pub struct Frame {
    pub first: u64,
    pub last: u64,
    pub json: Arc<str>,
}

pub enum Outgoing {
    Text(Arc<str>),
}

pub struct ClientQueue {
    frames: VecDeque<Arc<Frame>>,
    queued_samples: usize,
    control: VecDeque<Arc<str>>,
    coalescing: bool,
    pending: Vec<Option<Sample>>,
    pending_last_seq: u64,
    pub closed: bool,
}

#[derive(Default, Debug, Clone, serde::Serialize)]
pub struct ClientStats {
    pub id: u64,
    pub max_queued_samples: usize,
    pub queued_samples: usize,
    pub dropped_samples: u64,
    pub coalesce_events: u64,
    pub control_dropped: u64,
    pub sent_frames: u64,
}

pub struct Client {
    pub id: u64,
    q: Mutex<ClientQueue>,
    stats: Mutex<ClientStats>,
    pub notify: Notify,
    cfg: HubConfig,
}

impl Client {
    pub fn stats(&self) -> ClientStats {
        let mut s = self.stats.lock().unwrap().clone();
        s.queued_samples = self.q.lock().unwrap().queued_samples;
        s
    }

    pub fn push_control(&self, msg: Arc<str>) {
        let mut q = self.q.lock().unwrap();
        if q.control.len() >= self.cfg.client_control_msgs {
            q.control.pop_front();
            self.stats.lock().unwrap().control_dropped += 1;
        }
        q.control.push_back(msg);
        drop(q);
        self.notify.notify_one();
    }

    fn push_frame(&self, f: &Arc<Frame>, samples: &[Sample]) {
        let mut q = self.q.lock().unwrap();
        if q.closed {
            return;
        }
        let n = samples.len();
        if q.coalescing || q.queued_samples + n > self.cfg.client_queue_samples {
            let mut st = self.stats.lock().unwrap();
            if !q.coalescing {
                st.coalesce_events += 1;
                q.coalescing = true;
            }
            st.dropped_samples += n as u64;
            drop(st);
            for s in samples {
                if let Some(slot) = q.pending.get_mut(s.ch as usize) {
                    *slot = Some(s.clone());
                }
            }
            q.pending_last_seq = f.last;
        } else {
            q.frames.push_back(f.clone());
            q.queued_samples += n;
            let mut st = self.stats.lock().unwrap();
            if q.queued_samples > st.max_queued_samples {
                st.max_queued_samples = q.queued_samples;
            }
        }
        drop(q);
        self.notify.notify_one();
    }

    /// Take everything ready to send, in order: control messages, queued frames, then a
    /// coalesced frame once the backlog has drained.
    pub fn drain(&self, out: &mut Vec<Outgoing>) {
        let mut q = self.q.lock().unwrap();
        while let Some(c) = q.control.pop_front() {
            out.push(Outgoing::Text(c));
        }
        let mut sent = 0;
        while let Some(f) = q.frames.pop_front() {
            q.queued_samples -= (f.last - f.first + 1) as usize;
            out.push(Outgoing::Text(f.json.clone()));
            sent += 1;
        }
        if q.coalescing {
            let mut json = String::with_capacity(64 + q.pending.len() * 48);
            let _ = write!(json, "{{\"type\":\"coalesced\",\"seq\":{},\"s\":[", q.pending_last_seq);
            let mut first = true;
            for s in q.pending.iter_mut().filter_map(|p| p.take()) {
                if !first {
                    json.push(',');
                }
                first = false;
                write_sample(&mut json, &s);
            }
            json.push_str("]}");
            q.coalescing = false;
            out.push(Outgoing::Text(json.into()));
            sent += 1;
        }
        drop(q);
        self.stats.lock().unwrap().sent_frames += sent;
    }

    pub fn close(&self) {
        self.q.lock().unwrap().closed = true;
        self.notify.notify_one();
    }

    pub fn is_closed(&self) -> bool {
        self.q.lock().unwrap().closed
    }
}

pub fn write_sample(out: &mut String, s: &Sample) {
    out.push('[');
    let _ = write!(out, "{},{},", s.ch, s.t);
    if s.v.is_finite() {
        let _ = write!(out, "{}", s.v);
    } else {
        out.push_str("null");
    }
    let _ = write!(out, ",{},{}]", s.st, s.ing_us);
}

struct HubInner {
    next_seq: u64,
    ring: VecDeque<Arc<Frame>>,
    ring_samples: usize,
    latest: Vec<Option<(u64, Sample)>>,
    clients: Vec<Arc<Client>>,
}

pub struct Hub {
    inner: Mutex<HubInner>,
    cfg: HubConfig,
    nchan: usize,
    next_client: AtomicU64,
    pub published: AtomicU64,
    pub max_publish_ns: AtomicU64,
}

impl Hub {
    pub fn new(nchan: usize, cfg: HubConfig) -> Self {
        Self {
            inner: Mutex::new(HubInner {
                next_seq: 1,
                ring: VecDeque::new(),
                ring_samples: 0,
                latest: vec![None; nchan],
                clients: Vec::new(),
            }),
            cfg,
            nchan,
            next_client: AtomicU64::new(1),
            published: AtomicU64::new(0),
            max_publish_ns: AtomicU64::new(0),
        }
    }

    pub fn last_seq(&self) -> u64 {
        self.inner.lock().unwrap().next_seq - 1
    }

    /// Assign sequence numbers, store for resume, and hand to every client. Never awaits.
    pub fn publish(&self, samples: &[Sample]) {
        if samples.is_empty() {
            return;
        }
        let t0 = std::time::Instant::now();
        let mut g = self.inner.lock().unwrap();
        let first = g.next_seq;
        let last = first + samples.len() as u64 - 1;
        g.next_seq = last + 1;
        let mut json = String::with_capacity(48 + samples.len() * 52);
        let _ = write!(json, "{{\"type\":\"tm\",\"first\":{first},\"s\":[");
        for (i, s) in samples.iter().enumerate() {
            if i > 0 {
                json.push(',');
            }
            write_sample(&mut json, s);
            if let Some(slot) = g.latest.get_mut(s.ch as usize) {
                *slot = Some((first + i as u64, s.clone()));
            }
        }
        json.push_str("]}");
        let frame = Arc::new(Frame { first, last, json: json.into() });
        g.ring.push_back(frame.clone());
        g.ring_samples += samples.len();
        while g.ring_samples > self.cfg.ring_samples {
            if let Some(f) = g.ring.pop_front() {
                g.ring_samples -= (f.last - f.first + 1) as usize;
            } else {
                break;
            }
        }
        g.clients.retain(|c| !c.is_closed());
        for c in &g.clients {
            c.push_frame(&frame, samples);
        }
        drop(g);
        self.published.fetch_add(samples.len() as u64, Ordering::Relaxed);
        let ns = t0.elapsed().as_nanos() as u64;
        self.max_publish_ns.fetch_max(ns, Ordering::Relaxed);
    }

    /// Send a control message (alarm, link state, audit entry) to every client.
    pub fn broadcast(&self, msg: &str) {
        let msg: Arc<str> = msg.into();
        let g = self.inner.lock().unwrap();
        for c in &g.clients {
            c.push_control(msg.clone());
        }
    }

    fn snapshot_json(g: &HubInner) -> String {
        let mut json = String::new();
        let _ = write!(json, "{{\"type\":\"snapshot\",\"seq\":{},\"s\":[", g.next_seq - 1);
        let mut first = true;
        for (_, s) in g.latest.iter().flatten() {
            if !first {
                json.push(',');
            }
            first = false;
            write_sample(&mut json, s);
        }
        json.push_str("]}");
        json
    }

    /// Register a client. With `resume_from = Some(seq)` the client gets every frame after
    /// `seq` from the ring if it is still there, otherwise a snapshot of latest values.
    /// Returns the client and whether the resume was gapless.
    pub fn subscribe(&self, resume_from: Option<u64>) -> (Arc<Client>, bool) {
        let id = self.next_client.fetch_add(1, Ordering::Relaxed);
        let client = Arc::new(Client {
            id,
            q: Mutex::new(ClientQueue {
                frames: VecDeque::new(),
                queued_samples: 0,
                control: VecDeque::new(),
                coalescing: false,
                pending: vec![None; self.nchan],
                pending_last_seq: 0,
                closed: false,
            }),
            stats: Mutex::new(ClientStats { id, ..Default::default() }),
            notify: Notify::new(),
            cfg: self.cfg.clone(),
        });
        let mut g = self.inner.lock().unwrap();
        let mut gapless = false;
        let oldest = g.ring.front().map(|f| f.first).unwrap_or(g.next_seq);
        match resume_from {
            Some(from) if from + 1 >= oldest && from < g.next_seq => {
                gapless = true;
                let mut q = client.q.lock().unwrap();
                for f in g.ring.iter().filter(|f| f.last > from) {
                    q.queued_samples += (f.last - f.first + 1) as usize;
                    q.frames.push_back(f.clone());
                }
            }
            Some(from) if from + 1 == g.next_seq => gapless = true,
            _ => {
                let snap = Self::snapshot_json(&g);
                client.q.lock().unwrap().control.push_back(snap.into());
            }
        }
        let resume = format!(
            "{{\"type\":\"resume\",\"requested\":{},\"gapless\":{gapless},\"seq\":{}}}",
            resume_from.map(|v| v.to_string()).unwrap_or("null".into()),
            g.next_seq - 1
        );
        client.q.lock().unwrap().control.push_front(resume.into());
        g.clients.push(client.clone());
        drop(g);
        client.notify.notify_one();
        (client, gapless)
    }

    pub fn clients(&self) -> Vec<Arc<Client>> {
        self.inner.lock().unwrap().clients.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(ch: u32, v: f64) -> Sample {
        Sample { ch, t: 0.0, v, st: 0, ing_us: 0 }
    }

    fn texts(c: &Client) -> Vec<String> {
        let mut out = vec![];
        c.drain(&mut out);
        out.into_iter().map(|Outgoing::Text(t)| t.to_string()).collect()
    }

    #[test]
    fn slow_client_is_coalesced_and_bounded() {
        let hub = Hub::new(4, HubConfig { ring_samples: 100, client_queue_samples: 10, client_control_msgs: 4 });
        let (c, _) = hub.subscribe(None);
        texts(&c);
        for i in 0..1000 {
            hub.publish(&[s((i % 4) as u32, i as f64), s(((i + 1) % 4) as u32, i as f64)]);
        }
        let st = c.stats();
        assert!(st.max_queued_samples <= 10);
        assert!(st.dropped_samples > 0);
        let t = texts(&c);
        let last = t.last().unwrap();
        assert!(last.contains("\"coalesced\""));
        assert!(last.contains("\"seq\":2000"));
        // Only latest value per channel survives.
        assert_eq!(last.matches('[').count(), 1 + 4);
    }

    #[test]
    fn resume_from_ring_is_gapless() {
        let hub = Hub::new(2, HubConfig { ring_samples: 50, client_queue_samples: 1000, client_control_msgs: 10 });
        for i in 0..10 {
            hub.publish(&[s(0, i as f64)]);
        }
        let (c, gapless) = hub.subscribe(Some(6));
        assert!(gapless);
        let t = texts(&c);
        assert!(t[0].contains("\"gapless\":true"));
        assert_eq!(t.len(), 1 + 4);
        assert!(t[1].contains("\"first\":7"));

        for i in 0..100 {
            hub.publish(&[s(1, i as f64)]);
        }
        let (c2, gapless2) = hub.subscribe(Some(3));
        assert!(!gapless2, "seq 3 fell out of the ring");
        let t2 = texts(&c2);
        assert!(t2[1].contains("\"snapshot\""));
    }
}
