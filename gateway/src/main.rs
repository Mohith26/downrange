use downrange::iss::{self, IssEvent};
use downrange::model::now_ms;
use downrange::record::Recorder;

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1).cloned())
}

fn channel_ids(path: &str) -> Vec<String> {
    let text = std::fs::read_to_string(path).expect("read channel file");
    let v: serde_json::Value = serde_json::from_str(&text).expect("channel json");
    v.as_array().unwrap().iter().map(|c| c["id"].as_str().unwrap().to_string()).collect()
}

async fn record(args: &[String]) {
    let chans = arg(args, "--channels").unwrap_or("../channels/iss_channels.json".into());
    let out = arg(args, "--out").unwrap_or("../recordings/iss-session.rec".into());
    let minutes: f64 = arg(args, "--minutes").and_then(|s| s.parse().ok()).unwrap_or(60.0);
    let ids = channel_ids(&chans);
    let start = now_ms();
    let mut rec = Recorder::create(&out, &ids, start).expect("create recording");
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(iss::run(ids.clone(), tx));
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs_f64(minutes * 60.0);
    let mut last_flush = std::time::Instant::now();
    loop {
        let ev = tokio::select! {
            _ = tokio::time::sleep_until(deadline) => break,
            ev = rx.recv() => match ev { Some(e) => e, None => break },
        };
        match ev {
            IssEvent::Row { item, timestamp, value, status, recv_ms } => {
                rec.row(
                    recv_ms,
                    item,
                    timestamp.as_deref().unwrap_or(""),
                    value.as_deref().unwrap_or(""),
                    status.as_deref().unwrap_or(""),
                )
                .ok();
            }
            IssEvent::Connected { session } => {
                eprintln!("connected {session}");
                rec.event(now_ms(), &format!("connected {session}")).ok();
            }
            IssEvent::Disconnected { reason } => {
                eprintln!("disconnected: {reason}");
                rec.event(now_ms(), &format!("disconnected {reason}")).ok();
            }
        }
        if last_flush.elapsed().as_secs() >= 2 {
            rec.flush().ok();
            last_flush = std::time::Instant::now();
        }
    }
    rec.event(now_ms(), "end").ok();
    rec.flush().ok();
    eprintln!("recorded {} rows in {:.1} min to {out}", rec.rows, (now_ms() - start) / 60000.0);
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("record") => record(&args).await,
        _ => {
            eprintln!("usage: downrange record [--channels f] [--out f] [--minutes m]");
            std::process::exit(2);
        }
    }
}
