//! Limit checking with hysteresis, plus channel freshness (staleness and loss of signal).

use crate::model::{Channel, Limits, Sample};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Level {
    Nominal,
    WarnLo,
    WarnHi,
    AlarmLo,
    AlarmHi,
}

impl Level {
    pub fn severity(self) -> u8 {
        match self {
            Level::Nominal => 0,
            Level::WarnLo | Level::WarnHi => 1,
            Level::AlarmLo | Level::AlarmHi => 2,
        }
    }
}

fn raw_level(v: f64, l: &Limits, shift: f64) -> Level {
    if let Some(t) = l.hi_alarm {
        if v >= t - shift {
            return Level::AlarmHi;
        }
    }
    if let Some(t) = l.lo_alarm {
        if v <= t + shift {
            return Level::AlarmLo;
        }
    }
    if let Some(t) = l.hi_warn {
        if v >= t - shift {
            return Level::WarnHi;
        }
    }
    if let Some(t) = l.lo_warn {
        if v <= t + shift {
            return Level::WarnLo;
        }
    }
    Level::Nominal
}

/// Escalation is immediate. De-escalation only happens once the value is back inside
/// the threshold by at least `hyst`, so a value hovering on a line does not chatter.
pub fn next_level(v: f64, l: &Limits, cur: Level) -> Level {
    let raw = raw_level(v, l, 0.0);
    if raw.severity() >= cur.severity() {
        return raw;
    }
    let held = raw_level(v, l, l.hyst);
    if held.severity() >= cur.severity() {
        cur
    } else {
        held
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AlarmKind {
    Limit { from: Level, to: Level, value: f64 },
    Stale { stale: bool },
    Signal { aos: bool, reason: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct AlarmEvent {
    /// Unix ms when the gateway detected it.
    pub t: f64,
    /// Channel index, or -1 for feed-level events.
    pub ch: i64,
    #[serde(flatten)]
    pub kind: AlarmKind,
}

#[derive(Debug, Clone)]
struct ChanState {
    level: Level,
    last_rx_ms: f64,
    last_value: f64,
    has_value: bool,
    source_bad: bool,
    stale: bool,
}

/// Tracks limit state and freshness for every channel.
pub struct LimitEngine {
    pub channels: Vec<Channel>,
    st: Vec<ChanState>,
    /// Index of the feed heartbeat channel (TIME_000001 for ISS), if any.
    heartbeat: Option<usize>,
    heartbeat_timeout_ms: f64,
    feed_aos: bool,
    feed_ever_seen: bool,
    pub los_events: u64,
    pub stale_events: u64,
    pub limit_events: u64,
}

impl LimitEngine {
    pub fn new(channels: Vec<Channel>, heartbeat: Option<&str>, heartbeat_timeout_ms: f64) -> Self {
        let n = channels.len();
        let hb = heartbeat.and_then(|id| channels.iter().position(|c| c.id == id));
        Self {
            channels,
            st: vec![
                ChanState {
                    level: Level::Nominal,
                    last_rx_ms: 0.0,
                    last_value: f64::NAN,
                    has_value: false,
                    source_bad: false,
                    stale: false,
                };
                n
            ],
            heartbeat: hb,
            heartbeat_timeout_ms,
            feed_aos: hb.is_none(),
            feed_ever_seen: false,
            los_events: 0,
            stale_events: 0,
            limit_events: 0,
        }
    }

    pub fn index_of(&self, id: &str) -> Option<usize> {
        self.channels.iter().position(|c| c.id == id)
    }

    pub fn feed_aos(&self) -> bool {
        self.feed_aos
    }

    fn set_aos(&mut self, aos: bool, reason: &str, now_ms: f64, out: &mut Vec<AlarmEvent>) {
        if aos == self.feed_aos {
            return;
        }
        self.feed_aos = aos;
        if !aos {
            self.los_events += 1;
        }
        out.push(AlarmEvent { t: now_ms, ch: -1, kind: AlarmKind::Signal { aos, reason: reason.into() } });
    }

    /// Feed one sample. Any limit or signal transitions are appended to `out`.
    pub fn feed(&mut self, s: &Sample, now_ms: f64, out: &mut Vec<AlarmEvent>) {
        let i = s.ch as usize;
        if i >= self.st.len() {
            return;
        }
        if Some(i) == self.heartbeat {
            self.feed_ever_seen = true;
            if s.st == 0 {
                self.set_aos(true, "heartbeat status nominal", now_ms, out);
            } else {
                self.set_aos(false, "source reports loss of signal", now_ms, out);
            }
        }
        let st = &mut self.st[i];
        st.last_rx_ms = now_ms;
        st.source_bad = s.st != 0;
        if st.stale {
            st.stale = false;
            out.push(AlarmEvent { t: now_ms, ch: i as i64, kind: AlarmKind::Stale { stale: false } });
        }
        if !s.v.is_finite() {
            return;
        }
        st.last_value = s.v;
        st.has_value = true;
        if let Some(l) = &self.channels[i].limits {
            let next = next_level(s.v, l, st.level);
            if next != st.level {
                out.push(AlarmEvent {
                    t: now_ms,
                    ch: i as i64,
                    kind: AlarmKind::Limit { from: st.level, to: next, value: s.v },
                });
                st.level = next;
                self.limit_events += 1;
            }
        }
    }

    /// Periodic check for heartbeat loss and per-channel staleness.
    pub fn tick(&mut self, now_ms: f64, out: &mut Vec<AlarmEvent>) {
        if let Some(hb) = self.heartbeat {
            let age = now_ms - self.st[hb].last_rx_ms;
            if self.feed_aos && (age > self.heartbeat_timeout_ms || !self.feed_ever_seen) {
                self.set_aos(false, "heartbeat timeout", now_ms, out);
            }
        }
        for i in 0..self.st.len() {
            let Some(stale_ms) = self.channels[i].stale_ms else { continue };
            let st = &mut self.st[i];
            if st.has_value && !st.stale && now_ms - st.last_rx_ms > stale_ms as f64 {
                st.stale = true;
                self.stale_events += 1;
                out.push(AlarmEvent { t: now_ms, ch: i as i64, kind: AlarmKind::Stale { stale: true } });
            }
        }
    }

    /// A channel is fresh when it has a value, the source flags it good, the feed has
    /// signal, and (if it has its own window) it updated recently.
    pub fn is_fresh(&self, i: usize, now_ms: f64) -> bool {
        let Some(st) = self.st.get(i) else { return false };
        if !st.has_value || st.source_bad || !self.feed_aos {
            return false;
        }
        match self.channels[i].stale_ms {
            Some(ms) => now_ms - st.last_rx_ms <= ms as f64,
            None => true,
        }
    }

    pub fn level(&self, i: usize) -> Level {
        self.st.get(i).map(|s| s.level).unwrap_or(Level::Nominal)
    }

    pub fn value(&self, i: usize) -> Option<f64> {
        self.st.get(i).filter(|s| s.has_value).map(|s| s.last_value)
    }

    /// Test and fault-injection hook: force a channel stale as if it stopped updating.
    pub fn force_stale(&mut self, i: usize) {
        if let Some(st) = self.st.get_mut(i) {
            st.last_rx_ms = f64::NEG_INFINITY;
            st.source_bad = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lim() -> Limits {
        Limits { lo_alarm: Some(10.0), lo_warn: Some(20.0), hi_warn: Some(80.0), hi_alarm: Some(90.0), hyst: 2.0 }
    }

    #[test]
    fn hysteresis_prevents_chatter() {
        let l = lim();
        let mut cur = Level::Nominal;
        cur = next_level(80.5, &l, cur);
        assert_eq!(cur, Level::WarnHi);
        // Dips just under the line: stays in warning because of hysteresis.
        cur = next_level(79.0, &l, cur);
        assert_eq!(cur, Level::WarnHi);
        cur = next_level(77.9, &l, cur);
        assert_eq!(cur, Level::Nominal);
        cur = next_level(95.0, &l, cur);
        assert_eq!(cur, Level::AlarmHi);
        cur = next_level(89.0, &l, cur);
        assert_eq!(cur, Level::AlarmHi);
        cur = next_level(85.0, &l, cur);
        assert_eq!(cur, Level::WarnHi);
        cur = next_level(5.0, &l, cur);
        assert_eq!(cur, Level::AlarmLo);
    }

    fn chans() -> Vec<Channel> {
        vec![
            Channel { id: "HB".into(), desc: String::new(), group: String::new(), unit: String::new(), limits: None, stale_ms: None },
            Channel { id: "P".into(), desc: String::new(), group: String::new(), unit: String::new(), limits: Some(lim()), stale_ms: Some(500) },
        ]
    }

    fn s(ch: u32, v: f64, st: u8) -> Sample {
        Sample { ch, t: 0.0, v, st, ing_us: 0 }
    }

    #[test]
    fn heartbeat_drives_los() {
        let mut e = LimitEngine::new(chans(), Some("HB"), 10_000.0);
        let mut out = vec![];
        e.feed(&s(0, 1.0, 0), 0.0, &mut out);
        e.feed(&s(1, 50.0, 0), 0.0, &mut out);
        assert!(e.feed_aos());
        assert!(e.is_fresh(1, 100.0));
        e.feed(&s(0, 2.0, 1), 1000.0, &mut out);
        assert!(!e.feed_aos());
        assert!(!e.is_fresh(1, 1000.0));
        assert_eq!(e.los_events, 1);
        e.feed(&s(0, 3.0, 0), 2000.0, &mut out);
        e.tick(13_000.0, &mut out);
        assert!(!e.feed_aos(), "heartbeat timeout should drop to LOS");
        assert_eq!(e.los_events, 2);
    }

    #[test]
    fn per_channel_staleness() {
        let mut e = LimitEngine::new(chans(), None, 0.0);
        let mut out = vec![];
        e.feed(&s(1, 50.0, 0), 0.0, &mut out);
        e.tick(400.0, &mut out);
        assert!(out.is_empty());
        e.tick(600.0, &mut out);
        assert_eq!(e.stale_events, 1);
        assert!(!e.is_fresh(1, 600.0));
        e.feed(&s(1, 95.0, 0), 700.0, &mut out);
        assert!(e.is_fresh(1, 700.0));
        assert_eq!(e.level(1), Level::AlarmHi);
    }
}
