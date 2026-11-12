//! Minimal TLCP-2.2.0 text protocol parsing (the Lightstreamer wire format used by
//! NASA's public ISSLIVE feed). Only the subset needed for a MERGE subscription.

/// One decoded line from the session stream.
#[derive(Debug, Clone, PartialEq)]
pub enum Msg {
    ConOk { session: String, control_link: String },
    SubOk { sub_id: u32, items: usize, fields: usize },
    Update { sub_id: u32, item: usize, fields: Vec<FieldDelta> },
    Loop,
    Probe,
    Sync(u64),
    End(String),
    Error(String),
    ConErr(String),
    Other(String),
}

/// A field in an update line. TLCP only sends what changed.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldDelta {
    Unchanged,
    Null,
    Value(String),
}

pub fn parse_line(line: &str) -> Msg {
    let line = line.trim_end_matches(['\r', '\n']);
    let (head, rest) = match line.find(',') {
        Some(i) => (&line[..i], &line[i + 1..]),
        None => (line, ""),
    };
    match head {
        "CONOK" => {
            let p: Vec<&str> = rest.split(',').collect();
            Msg::ConOk {
                session: p.first().unwrap_or(&"").to_string(),
                control_link: p.get(3).unwrap_or(&"*").to_string(),
            }
        }
        "SUBOK" => {
            let p: Vec<&str> = rest.split(',').collect();
            Msg::SubOk {
                sub_id: p.first().and_then(|s| s.parse().ok()).unwrap_or(0),
                items: p.get(1).and_then(|s| s.parse().ok()).unwrap_or(0),
                fields: p.get(2).and_then(|s| s.parse().ok()).unwrap_or(0),
            }
        }
        "U" => {
            let mut it = rest.splitn(3, ',');
            let sub_id = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
            let item = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
            let raw = it.next().unwrap_or("");
            Msg::Update { sub_id, item, fields: parse_fields(raw) }
        }
        "LOOP" => Msg::Loop,
        "PROBE" => Msg::Probe,
        "SYNC" => Msg::Sync(rest.parse().unwrap_or(0)),
        "END" => Msg::End(rest.to_string()),
        "ERROR" => Msg::Error(rest.to_string()),
        "CONERR" => Msg::ConErr(rest.to_string()),
        _ => Msg::Other(line.to_string()),
    }
}

/// Split the pipe-separated field list of an update. `^N` means N unchanged fields,
/// an empty field means unchanged, `#` is null, `$` is the empty string.
pub fn parse_fields(raw: &str) -> Vec<FieldDelta> {
    let mut out = Vec::new();
    for f in raw.split('|') {
        if f.is_empty() {
            out.push(FieldDelta::Unchanged);
        } else if f == "#" {
            out.push(FieldDelta::Null);
        } else if f == "$" {
            out.push(FieldDelta::Value(String::new()));
        } else if let Some(n) = f.strip_prefix('^').and_then(|s| s.parse::<usize>().ok()) {
            for _ in 0..n {
                out.push(FieldDelta::Unchanged);
            }
        } else {
            out.push(FieldDelta::Value(percent_decode(f)));
        }
    }
    out
}

pub fn percent_decode(s: &str) -> String {
    if !s.contains('%') {
        return s.to_string();
    }
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub fn form_encode(s: &str) -> String {
    let mut out = String::new();
    for c in s.bytes() {
        match c {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' => out.push(c as char),
            _ => out.push_str(&format!("%{:02X}", c)),
        }
    }
    out
}

/// MERGE-mode state for one subscription: applies deltas onto the last full row.
pub struct MergeState {
    rows: Vec<Vec<Option<String>>>,
}

impl MergeState {
    pub fn new(items: usize, fields: usize) -> Self {
        Self { rows: vec![vec![None; fields]; items] }
    }

    /// `item` is 1-based as on the wire. Returns the full current row after applying.
    pub fn apply(&mut self, item: usize, deltas: &[FieldDelta]) -> Option<&[Option<String>]> {
        let row = self.rows.get_mut(item.checked_sub(1)?)?;
        for (i, d) in deltas.iter().enumerate() {
            if i >= row.len() {
                break;
            }
            match d {
                FieldDelta::Unchanged => {}
                FieldDelta::Null => row[i] = None,
                FieldDelta::Value(v) => row[i] = Some(v.clone()),
            }
        }
        Some(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_conok() {
        let m = parse_line("CONOK,Sadb39a3b88bd2d1fMaa0T0825831,50000,0,*\r\n");
        assert_eq!(
            m,
            Msg::ConOk { session: "Sadb39a3b88bd2d1fMaa0T0825831".into(), control_link: "*".into() }
        );
    }

    #[test]
    fn parses_update_with_unchanged_fields() {
        let m = parse_line("U,1,3,7741.5|12.25|");
        match m {
            Msg::Update { sub_id, item, fields } => {
                assert_eq!((sub_id, item), (1, 3));
                assert_eq!(
                    fields,
                    vec![
                        FieldDelta::Value("7741.5".into()),
                        FieldDelta::Value("12.25".into()),
                        FieldDelta::Unchanged
                    ]
                );
            }
            _ => panic!("expected update"),
        }
    }

    #[test]
    fn caret_run_expands() {
        assert_eq!(parse_fields("^2|x").len(), 3);
        assert_eq!(parse_fields("#|$|a%20b")[2], FieldDelta::Value("a b".into()));
    }

    #[test]
    fn merge_keeps_unchanged_values() {
        let mut st = MergeState::new(2, 3);
        st.apply(2, &parse_fields("1|2|24"));
        let row = st.apply(2, &parse_fields("|5|")).unwrap().to_vec();
        assert_eq!(row, vec![Some("1".into()), Some("5".into()), Some("24".into())]);
        assert!(st.apply(3, &parse_fields("1")).is_none());
    }
}
