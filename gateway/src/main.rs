use downrange::app::{serve, Gateway, Input};
use downrange::command::{iss_commands, synth_commands};
use downrange::hub::HubConfig;
use downrange::iss::{self, IssEvent};
use downrange::model::{now_ms, Channel};
use downrange::record::{self, Recorder};
use downrange::sources::{run_iss, run_replay, run_synth, synth_channels};
use downrange::{analyze, bench};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1).cloned())
}

fn num<T: std::str::FromStr>(args: &[String], name: &str, default: T) -> T {
    arg(args, name).and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn load_channels(path: &str) -> Vec<Channel> {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    serde_json::from_str(&text).expect("channel json")
}

async fn record(args: &[String]) {
    let chans = load_channels(&arg(args, "--channels").unwrap_or("../channels/iss_channels.json".into()));
    let out = arg(args, "--out").unwrap_or("../recordings/session.rec".into());
    let minutes: f64 = num(args, "--minutes", 60.0);
    let ids: Vec<String> = chans.iter().map(|c| c.id.clone()).collect();
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
                let f = |o: Option<String>| o.unwrap_or_default();
                rec.row(recv_ms, item, &f(timestamp), &f(value), &f(status)).ok();
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

async fn serve_cmd(args: &[String]) {
    let source = arg(args, "--source").unwrap_or("replay".into());
    let port: u16 = num(args, "--port", 8787);
    let chan_path = arg(args, "--channels").unwrap_or("../channels/iss_channels.json".into());
    let rec_path = arg(args, "--rec").unwrap_or("../recordings/iss-pass.rec".into());
    let speed: f64 = num(args, "--speed", 1.0);
    let audit = arg(args, "--audit").unwrap_or("audit.jsonl".into());
    let (tx, rx) = tokio::sync::mpsc::channel::<Input>(256);
    let gw = match source.as_str() {
        "synth" => {
            let n: usize = num(args, "--synth-channels", 64);
            let hz: f64 = num(args, "--synth-hz", 50.0);
            let gw = Gateway::new(synth_channels(n, hz), synth_commands(), None, "synthetic", HubConfig::default(), Some(&audit));
            let gw = gw.with_dirs(arg(args, "--results"), arg(args, "--static"));
            tokio::spawn(run_synth(n, hz, 5, gw.stats.clone(), tx, Arc::new(AtomicBool::new(false))));
            gw
        }
        "iss" | "replay" => {
            let chans = load_channels(&chan_path);
            let label = if source == "iss" { "iss-live" } else { "iss-replay" };
            let gw = Gateway::new(chans.clone(), iss_commands(), Some("TIME_000001"), label, HubConfig::default(), Some(&audit));
            let gw = gw.with_dirs(arg(args, "--results"), arg(args, "--static"));
            if source == "iss" {
                tokio::spawn(run_iss(chans, tx, arg(args, "--record-to")));
            } else {
                let rec = record::load(&rec_path).expect("load recording");
                tokio::spawn(async move { run_replay(rec, &chans, speed, true, tx).await });
            }
            gw
        }
        other => panic!("unknown source {other}"),
    };
    gw.spawn_ingest(rx);
    gw.spawn_ticker();
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await.expect("bind");
    eprintln!("downrange gateway ({source}) on http://127.0.0.1:{port}  ws://127.0.0.1:{port}/ws");
    serve(gw, listener).await;
}

async fn bench_cmd(args: &[String]) {
    let what = args.get(2).cloned().unwrap_or_default();
    let secs: f64 = num(args, "--secs", 8.0);
    let out = match what.as_str() {
        "throughput" => {
            let clients: usize = num(args, "--clients", 4);
            let points: Vec<(usize, f64)> = arg(args, "--points")
                .unwrap_or("100x100,500x100,1000x100,1000x250,2000x250,2000x500,4000x500".into())
                .split(',')
                .map(|p| {
                    let (c, h) = p.split_once('x').unwrap();
                    (c.parse().unwrap(), h.parse().unwrap())
                })
                .collect();
            let mut res = vec![];
            for (c, h) in points {
                let r = bench::throughput_point(c, h, clients, secs).await;
                eprintln!("{r}");
                let pass = r["pass"].as_bool().unwrap_or(false);
                res.push(r);
                if !pass {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            serde_json::json!({ "points": res })
        }
        "slow" => {
            let (c, h, cap) = (num(args, "--channels", 1000), num(args, "--hz", 100.0), num(args, "--cap", 20_000));
            let baseline = bench::slow_client(c, h, 3, 0, secs, cap).await;
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let stalled = bench::slow_client(c, h, 3, 1, secs, cap).await;
            serde_json::json!({ "baseline_no_stalled_client": baseline, "with_stalled_client": stalled })
        }
        "commands" => bench::command_faults(num(args, "--n", 1000), num(args, "--seed", 7)).await,
        "analyze" => {
            let rec = record::load(&arg(args, "--rec").unwrap_or("../recordings/iss-pass.rec".into())).expect("rec");
            let chans = load_channels(&arg(args, "--channels").unwrap_or("../channels/iss_channels.json".into()));
            analyze::analyze(&rec, &chans)
        }
        _ => {
            eprintln!("usage: downrange bench <throughput|slow|commands|analyze> [--out file]");
            std::process::exit(2);
        }
    };
    let text = serde_json::to_string_pretty(&out).unwrap();
    match arg(args, "--out") {
        Some(p) => std::fs::write(&p, text).expect("write"),
        None => println!("{text}"),
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("record") => record(&args).await,
        Some("serve") => serve_cmd(&args).await,
        Some("bench") => bench_cmd(&args).await,
        _ => {
            eprintln!("usage: downrange <record|serve|bench> ...");
            std::process::exit(2);
        }
    }
}
