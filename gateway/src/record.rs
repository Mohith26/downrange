//! Append-only session log. Every row received from the feed is written as one
//! tab-separated line so a session can be replayed later with the original timing.
//!
//! ```text
//! #downrange-rec v1
//! #start_unix_ms <ms>
//! #chan <idx> <id>
//! <recv_offset_ms>\t<idx>\t<source_timestamp>\t<value>\t<status_class>
//! E\t<recv_offset_ms>\t<event text>
//! ```

use std::io::{BufRead, Write};

pub struct Recorder {
    out: std::io::BufWriter<std::fs::File>,
    start_ms: f64,
    pub rows: u64,
}

impl Recorder {
    pub fn create(path: &str, ids: &[String], start_ms: f64) -> std::io::Result<Self> {
        let f = std::fs::File::create(path)?;
        let mut out = std::io::BufWriter::new(f);
        writeln!(out, "#downrange-rec v1")?;
        writeln!(out, "#start_unix_ms {}", start_ms as u64)?;
        for (i, id) in ids.iter().enumerate() {
            writeln!(out, "#chan {i} {id}")?;
        }
        Ok(Self { out, start_ms, rows: 0 })
    }

    pub fn row(&mut self, recv_ms: f64, idx: usize, ts: &str, value: &str, status: &str) -> std::io::Result<()> {
        let off = (recv_ms - self.start_ms).max(0.0) as u64;
        writeln!(self.out, "{off}\t{idx}\t{}\t{}\t{}", compact(ts), compact(value), clean(status))?;
        self.rows += 1;
        Ok(())
    }

    pub fn event(&mut self, recv_ms: f64, text: &str) -> std::io::Result<()> {
        let off = (recv_ms - self.start_ms).max(0.0) as u64;
        writeln!(self.out, "E\t{off}\t{}", text.replace(['\t', '\n'], " "))
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.out.flush()
    }
}

fn clean(s: &str) -> String {
    s.replace(['\t', '\n'], " ")
}

/// The feed prints doubles with up to ~60 digits. Re-printing through f64 keeps the
/// exact same double (shortest round-trip form) and cuts the log size by about 3x.
pub fn compact(s: &str) -> String {
    match s.parse::<f64>() {
        Ok(v) if v.is_finite() && s.len() > 12 => format!("{v}"),
        _ => clean(s),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RecLine {
    Row { off_ms: u64, idx: usize, ts: String, value: String, status: String },
    Event { off_ms: u64, text: String },
}

#[derive(Debug, Default, Clone)]
pub struct Recording {
    pub start_unix_ms: u64,
    pub channels: Vec<String>,
    pub lines: Vec<RecLine>,
}

pub fn parse(text: &str) -> Recording {
    let mut rec = Recording::default();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("#start_unix_ms ") {
            rec.start_unix_ms = rest.trim().parse().unwrap_or(0);
        } else if let Some(rest) = line.strip_prefix("#chan ") {
            let mut p = rest.splitn(2, ' ');
            let idx: usize = p.next().and_then(|s| s.parse().ok()).unwrap_or(0);
            let id = p.next().unwrap_or("").to_string();
            if rec.channels.len() <= idx {
                rec.channels.resize(idx + 1, String::new());
            }
            rec.channels[idx] = id;
        } else if line.starts_with('#') || line.is_empty() {
            continue;
        } else if let Some(rest) = line.strip_prefix("E\t") {
            let mut p = rest.splitn(2, '\t');
            let off_ms = p.next().and_then(|s| s.parse().ok()).unwrap_or(0);
            rec.lines.push(RecLine::Event { off_ms, text: p.next().unwrap_or("").to_string() });
        } else {
            let p: Vec<&str> = line.split('\t').collect();
            if p.len() >= 5 {
                if let (Ok(off_ms), Ok(idx)) = (p[0].parse(), p[1].parse()) {
                    rec.lines.push(RecLine::Row {
                        off_ms,
                        idx,
                        ts: p[2].into(),
                        value: p[3].into(),
                        status: p[4].into(),
                    });
                }
            }
        }
    }
    rec
}

pub fn load(path: &str) -> std::io::Result<Recording> {
    let f = std::fs::File::open(path)?;
    let mut text = String::new();
    for l in std::io::BufReader::new(f).lines() {
        text.push_str(&l?);
        text.push('\n');
    }
    Ok(parse(&text))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_keeps_the_same_double() {
        let raw = "-0.53221115166807175800300910850637592375278472900390625";
        let c = compact(raw);
        assert!(c.len() < 22);
        assert_eq!(c.parse::<f64>().unwrap(), raw.parse::<f64>().unwrap());
        assert_eq!(compact("24"), "24");
    }

    #[test]
    fn round_trip() {
        let dir = std::env::temp_dir().join(format!("dr-rec-{}", std::process::id()));
        let path = dir.to_str().unwrap().to_string();
        let ids = vec!["TIME_000001".to_string(), "USLAB000058".to_string()];
        let mut r = Recorder::create(&path, &ids, 1000.0).unwrap();
        r.event(1000.0, "connected S1").unwrap();
        r.row(1250.0, 1, "7600.5", "14.69", "24").unwrap();
        r.flush().unwrap();
        let rec = load(&path).unwrap();
        std::fs::remove_file(&path).ok();
        assert_eq!(rec.channels, ids);
        assert_eq!(rec.start_unix_ms, 1000);
        assert_eq!(rec.lines.len(), 2);
        assert_eq!(
            rec.lines[1],
            RecLine::Row { off_ms: 250, idx: 1, ts: "7600.5".into(), value: "14.69".into(), status: "24".into() }
        );
    }
}
