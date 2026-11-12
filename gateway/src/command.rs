//! Guarded command path: arm, then confirm with a single-use expiring token. Idempotency
//! keys make retries safe, and interlocks are checked against live telemetry at both steps.
//!
//! The target is `LoopbackDevice`, an in-process simulated actuator. There is no real
//! hardware behind it; it exists so the safety semantics can be exercised and counted.

use crate::limits::{Level, LimitEngine};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::io::Write;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArgDef {
    pub name: String,
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Interlock {
    /// Channel must have a recent, source-good value with signal present.
    Fresh { ch: String },
    /// Channel must be fresh and inside its warning limits.
    InLimits { ch: String },
    /// Channel must be fresh and within [min, max].
    Range { ch: String, min: f64, max: f64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandDef {
    pub name: String,
    pub desc: String,
    pub args: Vec<ArgDef>,
    pub interlocks: Vec<Interlock>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InterlockStatus {
    pub ok: bool,
    pub detail: String,
}

pub fn eval_interlock(il: &Interlock, lim: &LimitEngine, now_ms: f64) -> InterlockStatus {
    let ch = match il {
        Interlock::Fresh { ch } | Interlock::InLimits { ch } | Interlock::Range { ch, .. } => ch,
    };
    let Some(i) = lim.index_of(ch) else {
        return InterlockStatus { ok: false, detail: format!("{ch}: unknown channel") };
    };
    if !lim.is_fresh(i, now_ms) {
        return InterlockStatus { ok: false, detail: format!("{ch}: stale or no signal") };
    }
    match il {
        Interlock::Fresh { .. } => InterlockStatus { ok: true, detail: format!("{ch}: fresh") },
        Interlock::InLimits { .. } => {
            let lvl = lim.level(i);
            InterlockStatus {
                ok: lvl == Level::Nominal,
                detail: format!("{ch}: {}", serde_json::to_value(lvl).unwrap().as_str().unwrap_or("")),
            }
        }
        Interlock::Range { min, max, .. } => {
            let v = lim.value(i).unwrap_or(f64::NAN);
            InterlockStatus { ok: v >= *min && v <= *max, detail: format!("{ch}: {v} in [{min}, {max}]") }
        }
    }
}

/// In-process stand-in for an actuator. Counts executions per idempotency key so tests
/// can prove exactly-once behaviour.
#[derive(Default)]
pub struct LoopbackDevice {
    pub executions: HashMap<String, u32>,
    pub total_executions: u64,
    pub state: HashMap<String, f64>,
}

impl LoopbackDevice {
    fn execute(&mut self, key: &str, cmd: &str, args: &HashMap<String, f64>) -> String {
        *self.executions.entry(key.to_string()).or_insert(0) += 1;
        self.total_executions += 1;
        for (k, v) in args {
            self.state.insert(format!("{cmd}.{k}"), *v);
        }
        format!("loopback ack #{}: {cmd}", self.total_executions)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AuditEntry {
    pub t: f64,
    pub client: u64,
    pub key: String,
    pub cmd: String,
    pub event: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Reply {
    Armed { key: String, token: String, expires_ms: f64 },
    Executed { key: String, result: String, dedup: bool },
    Rejected { key: String, reason: String },
}

struct Armed {
    key: String,
    cmd: String,
    args: HashMap<String, f64>,
    expires_ms: f64,
}

enum KeyState {
    Armed { token: String, cmd: String, args: HashMap<String, f64> },
    Executed { result: String, cmd: String, at_ms: f64 },
}

pub struct CommandEngine {
    pub defs: Vec<CommandDef>,
    tokens: HashMap<String, Armed>,
    keys: HashMap<String, KeyState>,
    pub device: LoopbackDevice,
    pub token_ttl_ms: f64,
    pub audit: VecDeque<AuditEntry>,
    audit_file: Option<std::io::BufWriter<std::fs::File>>,
    pub counters: HashMap<String, u64>,
}

impl CommandEngine {
    pub fn new(defs: Vec<CommandDef>, token_ttl_ms: f64, audit_path: Option<&str>) -> Self {
        let audit_file = audit_path.and_then(|p| {
            std::fs::OpenOptions::new().create(true).append(true).open(p).ok().map(std::io::BufWriter::new)
        });
        Self {
            defs,
            tokens: HashMap::new(),
            keys: HashMap::new(),
            device: LoopbackDevice::default(),
            token_ttl_ms,
            audit: VecDeque::new(),
            audit_file,
            counters: HashMap::new(),
        }
    }

    fn log(&mut self, e: AuditEntry) -> AuditEntry {
        *self.counters.entry(e.event.clone()).or_insert(0) += 1;
        if let Some(f) = self.audit_file.as_mut() {
            let _ = writeln!(f, "{}", serde_json::to_string(&e).unwrap());
            let _ = f.flush();
        }
        if self.audit.len() >= 500 {
            self.audit.pop_front();
        }
        self.audit.push_back(e.clone());
        e
    }

    pub fn interlock_report(&self, lim: &LimitEngine, now_ms: f64) -> Vec<(String, bool, Vec<InterlockStatus>)> {
        self.defs
            .iter()
            .map(|d| {
                let st: Vec<_> = d.interlocks.iter().map(|il| eval_interlock(il, lim, now_ms)).collect();
                (d.name.clone(), st.iter().all(|s| s.ok), st)
            })
            .collect()
    }

    fn check(&self, def: &CommandDef, lim: &LimitEngine, now_ms: f64) -> Result<(), String> {
        let failed: Vec<String> = def
            .interlocks
            .iter()
            .map(|il| eval_interlock(il, lim, now_ms))
            .filter(|s| !s.ok)
            .map(|s| s.detail)
            .collect();
        if failed.is_empty() {
            Ok(())
        } else {
            Err(format!("interlock: {}", failed.join("; ")))
        }
    }

    fn prune(&mut self, now_ms: f64) {
        self.tokens.retain(|_, a| a.expires_ms > now_ms - 60_000.0);
        self.keys.retain(|_, k| match k {
            KeyState::Executed { at_ms, .. } => *at_ms > now_ms - 3_600_000.0,
            KeyState::Armed { .. } => true,
        });
    }

    /// Step 1. Returns a token only if the command is valid and every interlock passes.
    pub fn arm(
        &mut self,
        client: u64,
        key: &str,
        cmd: &str,
        args: HashMap<String, f64>,
        lim: &LimitEngine,
        now_ms: f64,
    ) -> (Reply, Vec<AuditEntry>) {
        self.prune(now_ms);
        let mut audit = vec![];
        let entry = |event: &str, detail: String| AuditEntry {
            t: now_ms,
            client,
            key: key.into(),
            cmd: cmd.into(),
            event: event.into(),
            detail,
        };
        if let Some(KeyState::Executed { result, .. }) = self.keys.get(key) {
            let result = result.clone();
            audit.push(self.log(entry("dedup_arm", result.clone())));
            return (Reply::Executed { key: key.into(), result, dedup: true }, audit);
        }
        let reject = |s: &mut Self, audit: &mut Vec<AuditEntry>, reason: String| {
            audit.push(s.log(entry("arm_rejected", reason.clone())));
            (Reply::Rejected { key: key.into(), reason }, std::mem::take(audit))
        };
        if key.is_empty() || key.len() > 128 {
            return reject(self, &mut audit, "missing or oversized idempotency key".into());
        }
        let Some(def) = self.defs.iter().find(|d| d.name == cmd).cloned() else {
            return reject(self, &mut audit, format!("unknown command {cmd}"));
        };
        for a in &def.args {
            match args.get(&a.name) {
                Some(v) if v.is_finite() && *v >= a.min && *v <= a.max => {}
                _ => return reject(self, &mut audit, format!("arg {} must be in [{}, {}]", a.name, a.min, a.max)),
            }
        }
        if args.keys().any(|k| !def.args.iter().any(|a| &a.name == k)) {
            return reject(self, &mut audit, "unexpected argument".into());
        }
        if let Err(e) = self.check(&def, lim, now_ms) {
            return reject(self, &mut audit, e);
        }
        // Same key armed again (for example after a lost reply): hand back the same token.
        if let Some(KeyState::Armed { token, cmd: c0, args: a0 }) = self.keys.get(key) {
            if c0 != cmd || a0 != &args {
                return reject(self, &mut audit, "idempotency key reused for a different command".into());
            }
            if let Some(a) = self.tokens.get(token) {
                if a.expires_ms > now_ms {
                    let (token, expires_ms) = (token.clone(), a.expires_ms);
                    audit.push(self.log(entry("rearm_same_token", String::new())));
                    return (Reply::Armed { key: key.into(), token, expires_ms }, audit);
                }
            }
        }
        let mut b = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut b);
        let token: String = b.iter().map(|x| format!("{x:02x}")).collect();
        let expires_ms = now_ms + self.token_ttl_ms;
        self.tokens.insert(
            token.clone(),
            Armed { key: key.into(), cmd: cmd.into(), args: args.clone(), expires_ms },
        );
        self.keys.insert(key.into(), KeyState::Armed { token: token.clone(), cmd: cmd.into(), args });
        audit.push(self.log(entry("armed", format!("expires in {} ms", self.token_ttl_ms))));
        (Reply::Armed { key: key.into(), token, expires_ms }, audit)
    }

    /// Step 2. Executes at most once per idempotency key.
    pub fn confirm(
        &mut self,
        client: u64,
        key: &str,
        token: &str,
        lim: &LimitEngine,
        now_ms: f64,
    ) -> (Reply, Vec<AuditEntry>) {
        let mut audit = vec![];
        let cmd_name = match (self.tokens.get(token), self.keys.get(key)) {
            (Some(a), _) => a.cmd.clone(),
            (None, Some(KeyState::Executed { cmd, .. })) => cmd.clone(),
            _ => String::new(),
        };
        let entry = |event: &str, detail: String| AuditEntry {
            t: now_ms,
            client,
            key: key.into(),
            cmd: cmd_name.clone(),
            event: event.into(),
            detail,
        };
        if let Some(KeyState::Executed { result, .. }) = self.keys.get(key) {
            let result = result.clone();
            audit.push(self.log(entry("dedup_confirm", result.clone())));
            return (Reply::Executed { key: key.into(), result, dedup: true }, audit);
        }
        let reject = |s: &mut Self, audit: &mut Vec<AuditEntry>, reason: String| {
            audit.push(s.log(entry("confirm_rejected", reason.clone())));
            (Reply::Rejected { key: key.into(), reason }, std::mem::take(audit))
        };
        let Some(armed) = self.tokens.get(token) else {
            return reject(self, &mut audit, "unknown or already used token".into());
        };
        if armed.key != key {
            return reject(self, &mut audit, "token was issued for a different key".into());
        }
        if armed.expires_ms <= now_ms {
            self.tokens.remove(token);
            self.keys.remove(key);
            return reject(self, &mut audit, "token expired, re-arm".into());
        }
        let def = self.defs.iter().find(|d| d.name == armed.cmd).cloned().unwrap();
        if let Err(e) = self.check(&def, lim, now_ms) {
            self.tokens.remove(token);
            self.keys.remove(key);
            return reject(self, &mut audit, e);
        }
        let armed = self.tokens.remove(token).unwrap();
        let result = self.device.execute(key, &armed.cmd, &armed.args);
        self.keys.insert(key.into(), KeyState::Executed { result: result.clone(), cmd: armed.cmd.clone(), at_ms: now_ms });
        audit.push(self.log(entry("executed", result.clone())));
        (Reply::Executed { key: key.into(), result, dedup: false }, audit)
    }
}

/// Commands for the ISS console, shared with the browser console. All of them target
/// the loopback device; their interlocks read real ISS channels.
pub fn iss_commands() -> Vec<CommandDef> {
    serde_json::from_str(include_str!("../../channels/iss_commands.json")).expect("iss_commands.json")
}

/// Commands used with the synthetic source (benchmarks and tests).
pub fn synth_commands() -> Vec<CommandDef> {
    vec![
        CommandDef {
            name: "SIM_VALVE_OPEN".into(),
            desc: "Open a valve on the loopback device".into(),
            args: vec![ArgDef { name: "pct".into(), min: 0.0, max: 100.0 }],
            interlocks: vec![Interlock::InLimits { ch: "SYN0000".into() }, Interlock::Fresh { ch: "SYN0001".into() }],
        },
        CommandDef {
            name: "SIM_HEATER_ON".into(),
            desc: "Enable a heater on the loopback device".into(),
            args: vec![],
            interlocks: vec![Interlock::Fresh { ch: "SYN0002".into() }],
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Channel, Limits, Sample};

    fn engine() -> LimitEngine {
        let ch = |id: &str, limits: Option<Limits>| Channel {
            id: id.into(),
            label: id.into(),
            desc: String::new(),
            group: String::new(),
            unit: String::new(),
            limits,
            stale_ms: Some(1000),
        };
        let lim = Limits { lo_alarm: None, lo_warn: None, hi_warn: Some(80.0), hi_alarm: Some(90.0), hyst: 1.0 };
        let mut e = LimitEngine::new(vec![ch("SYN0000", Some(lim)), ch("SYN0001", None), ch("SYN0002", None)], None, 0.0);
        let mut out = vec![];
        for c in 0..3 {
            e.feed(&Sample { ch: c, t: 0.0, v: 10.0, st: 0, ing_us: 0 }, 0.0, &mut out);
        }
        e
    }

    fn args(p: f64) -> HashMap<String, f64> {
        HashMap::from([("pct".to_string(), p)])
    }

    #[test]
    fn arm_confirm_is_single_use_and_deduped() {
        let lim = engine();
        let mut c = CommandEngine::new(synth_commands(), 5000.0, None);
        let (r, _) = c.arm(1, "k1", "SIM_VALVE_OPEN", args(40.0), &lim, 10.0);
        let Reply::Armed { token, .. } = r else { panic!("{r:?}") };
        // Lost arm reply: arming again with the same key returns the same token.
        let (r2, _) = c.arm(1, "k1", "SIM_VALVE_OPEN", args(40.0), &lim, 20.0);
        assert_eq!(r2, Reply::Armed { key: "k1".into(), token: token.clone(), expires_ms: 5010.0 });
        let (r3, _) = c.confirm(1, "k1", &token, &lim, 30.0);
        assert!(matches!(r3, Reply::Executed { dedup: false, .. }));
        // Retried confirm after a lost reply: deduped, not executed again.
        let (r4, _) = c.confirm(1, "k1", &token, &lim, 40.0);
        assert!(matches!(r4, Reply::Executed { dedup: true, .. }));
        assert_eq!(c.device.executions["k1"], 1);
        // A different key cannot reuse the spent token.
        let (r5, _) = c.confirm(1, "k2", &token, &lim, 50.0);
        assert!(matches!(r5, Reply::Rejected { .. }));
    }

    #[test]
    fn expiry_and_interlocks() {
        let mut lim = engine();
        let mut c = CommandEngine::new(synth_commands(), 100.0, None);
        let (r, _) = c.arm(1, "k", "SIM_VALVE_OPEN", args(40.0), &lim, 0.0);
        let Reply::Armed { token, .. } = r else { panic!() };
        let (r, _) = c.confirm(1, "k", &token, &lim, 200.0);
        assert_eq!(r, Reply::Rejected { key: "k".into(), reason: "token expired, re-arm".into() });
        let mut out = vec![];
        lim.feed(&Sample { ch: 0, t: 0.0, v: 85.0, st: 0, ing_us: 0 }, 300.0, &mut out);
        let (r, _) = c.arm(1, "k", "SIM_VALVE_OPEN", args(40.0), &lim, 300.0);
        assert!(matches!(r, Reply::Rejected { ref reason, .. } if reason.contains("warn_hi")));
        // Stale channel blocks arming.
        let (r, _) = c.arm(1, "h", "SIM_HEATER_ON", HashMap::new(), &lim, 5000.0);
        assert!(matches!(r, Reply::Rejected { ref reason, .. } if reason.contains("stale")));
        let (r, _) = c.arm(1, "bad", "SIM_VALVE_OPEN", args(400.0), &lim, 300.0);
        assert!(matches!(r, Reply::Rejected { .. }));
        assert_eq!(c.device.total_executions, 0);
    }

    #[test]
    fn iss_command_file_parses() {
        let defs = iss_commands();
        assert_eq!(defs.len(), 4);
        assert!(defs.iter().all(|d| !d.interlocks.is_empty()));
    }

    #[test]
    fn interlock_rechecked_at_confirm() {
        let mut lim = engine();
        let mut c = CommandEngine::new(synth_commands(), 5000.0, None);
        let (r, _) = c.arm(1, "k", "SIM_VALVE_OPEN", args(10.0), &lim, 10.0);
        let Reply::Armed { token, .. } = r else { panic!() };
        lim.force_stale(1);
        let (r, _) = c.confirm(1, "k", &token, &lim, 20.0);
        assert!(matches!(r, Reply::Rejected { ref reason, .. } if reason.contains("SYN0001")));
        assert_eq!(c.device.total_executions, 0);
    }
}
