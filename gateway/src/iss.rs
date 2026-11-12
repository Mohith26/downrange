//! Live client for NASA's public ISS telemetry feed (Lightstreamer, adapter set ISSLIVE).
//! Plain HTTP streaming transport, written against TLCP-2.2.0 directly so the gateway
//! does not need a TLS stack or the vendor SDK.

use crate::model::now_ms;
use crate::tlcp::{form_encode, parse_line, MergeState, Msg};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedSender;

pub const HOST: &str = "push.lightstreamer.com";
pub const CID: &str = "mgQkwtwdysogQz2BJ4Ji kOj2Bg";

#[derive(Debug, Clone)]
pub enum IssEvent {
    /// Raw field row for item index `item` (0-based in the subscribed list).
    Row { item: usize, timestamp: Option<String>, value: Option<String>, status: Option<String>, recv_ms: f64 },
    Connected { session: String },
    Disconnected { reason: String },
}

async fn post(host: &str, path: &str, body: &str) -> std::io::Result<BodyLines> {
    let stream = TcpStream::connect((host, 80)).await?;
    stream.set_nodelay(true)?;
    let mut stream = stream;
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\nUser-Agent: downrange/0.1\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).await?;
    let mut r = BufReader::new(stream);
    let mut line = String::new();
    r.read_line(&mut line).await?;
    if !line.contains(" 200 ") {
        return Err(std::io::Error::other(format!("http status: {}", line.trim())));
    }
    let mut chunked = false;
    loop {
        line.clear();
        if r.read_line(&mut line).await? == 0 || line == "\r\n" {
            break;
        }
        let l = line.to_ascii_lowercase();
        if l.starts_with("transfer-encoding:") && l.contains("chunked") {
            chunked = true;
        }
    }
    Ok(BodyLines { r, chunked, buf: Vec::new(), done: false })
}

/// Line reader over an HTTP body, decoding chunked transfer encoding when present.
pub struct BodyLines {
    r: BufReader<TcpStream>,
    chunked: bool,
    buf: Vec<u8>,
    done: bool,
}

impl BodyLines {
    async fn fill(&mut self) -> std::io::Result<bool> {
        if self.done {
            return Ok(false);
        }
        if !self.chunked {
            let mut tmp = [0u8; 8192];
            let n = self.r.read(&mut tmp).await?;
            if n == 0 {
                self.done = true;
                return Ok(false);
            }
            self.buf.extend_from_slice(&tmp[..n]);
            return Ok(true);
        }
        let mut size_line = String::new();
        if self.r.read_line(&mut size_line).await? == 0 {
            self.done = true;
            return Ok(false);
        }
        let hex = size_line.trim().split(';').next().unwrap_or("");
        let size = usize::from_str_radix(hex, 16).map_err(|_| std::io::Error::other(format!("bad chunk size {hex:?}")))?;
        if size == 0 {
            self.done = true;
            return Ok(false);
        }
        let start = self.buf.len();
        self.buf.resize(start + size, 0);
        self.r.read_exact(&mut self.buf[start..]).await?;
        let mut crlf = [0u8; 2];
        self.r.read_exact(&mut crlf).await?;
        Ok(true)
    }

    pub async fn next_line(&mut self) -> std::io::Result<Option<String>> {
        loop {
            if let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = self.buf.drain(..=pos).collect();
                return Ok(Some(String::from_utf8_lossy(&line).trim_end().to_string()));
            }
            if !self.fill().await? {
                return Ok(None);
            }
        }
    }
}

async fn subscribe(session: &str, items: &[String]) -> std::io::Result<()> {
    let group = items.join(" ");
    let body = format!(
        "LS_reqId=1&LS_op=add&LS_subId=1&LS_mode=MERGE&LS_group={}&LS_schema={}&LS_snapshot=true",
        form_encode(&group),
        form_encode("TimeStamp Value Status.Class")
    );
    let path = format!("/lightstreamer/control.txt?LS_protocol=TLCP-2.2.0&LS_session={session}");
    let mut r = post(HOST, &path, &body).await?;
    let resp = r.next_line().await?.unwrap_or_default();
    if !resp.starts_with("REQOK") {
        return Err(std::io::Error::other(format!("subscribe failed: {}", resp.trim())));
    }
    Ok(())
}

/// Runs one session until it ends. Returns the reason.
async fn run_session(items: &[String], tx: &UnboundedSender<IssEvent>) -> std::io::Result<String> {
    let body = format!(
        "LS_adapter_set=ISSLIVE&LS_cid={}&LS_content_length=50000000&LS_keepalive_millis=5000",
        form_encode(CID)
    );
    let mut lines = post(HOST, "/lightstreamer/create_session.txt?LS_protocol=TLCP-2.2.0", &body).await?;
    let mut merge = MergeState::new(items.len(), 3);
    let mut session = String::new();
    loop {
        // Keepalives arrive every 5 s; 20 s of silence means the stream is dead.
        let line = match tokio::time::timeout(Duration::from_secs(20), lines.next_line()).await {
            Err(_) => return Ok("stream silent for 20s".into()),
            Ok(r) => r?,
        };
        let Some(line) = line else { return Ok("stream closed".into()) };
        match parse_line(&line) {
            Msg::ConOk { session: s, .. } => {
                session = s.clone();
                subscribe(&s, items).await?;
                let _ = tx.send(IssEvent::Connected { session: s });
            }
            Msg::Update { item, fields, .. } => {
                if let Some(row) = merge.apply(item, &fields) {
                    let _ = tx.send(IssEvent::Row {
                        item: item - 1,
                        timestamp: row[0].clone(),
                        value: row[1].clone(),
                        status: row[2].clone(),
                        recv_ms: now_ms(),
                    });
                }
            }
            Msg::Loop => {
                // Content length exhausted. Rebinding keeps the session but restarting is
                // simpler and the snapshot re-sends every value, so state stays correct.
                return Ok(format!("server LOOP on session {session}"));
            }
            Msg::End(m) => return Ok(format!("END {m}")),
            Msg::ConErr(m) | Msg::Error(m) => return Ok(format!("error {m}")),
            _ => {}
        }
    }
}

/// Connect forever with exponential backoff. Every row from the feed goes to `tx`.
pub async fn run(items: Vec<String>, tx: UnboundedSender<IssEvent>) {
    let mut backoff = 1u64;
    loop {
        let started = std::time::Instant::now();
        let reason = match run_session(&items, &tx).await {
            Ok(r) => r,
            Err(e) => format!("io: {e}"),
        };
        let _ = tx.send(IssEvent::Disconnected { reason });
        if tx.is_closed() {
            return;
        }
        if started.elapsed() > Duration::from_secs(60) {
            backoff = 1;
        }
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(30);
    }
}
