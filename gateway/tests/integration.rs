//! End-to-end tests over real loopback WebSockets.

use downrange::bench::{command_faults, connect, start_synth};
use downrange::hub::HubConfig;
use futures_util::StreamExt;
use serde_json::Value;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commands_execute_exactly_once_under_faults() {
    let r = command_faults(150, 11).await;
    let n = r["commands"].as_u64().unwrap();
    assert_eq!(r["keys_executed_exactly_once"].as_u64().unwrap(), n, "{r}");
    assert_eq!(r["keys_executed_more_than_once"].as_u64().unwrap(), 0);
    assert_eq!(r["device_executions_total"].as_u64().unwrap(), n);
    assert!(r["interlock_rejections"].as_u64().unwrap() > 0, "fault plan should include breaches");
    assert!(r["dedup_replies"].as_u64().unwrap() > 0, "retries should have been deduped");
    let inj = &r["injected"];
    assert!(inj["reconnects_between_arm_and_confirm"].as_u64().unwrap() > 0);
}

async fn next_json(ws: &mut (impl StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin)) -> Value {
    loop {
        let m = tokio::time::timeout(Duration::from_secs(5), ws.next()).await.expect("timeout").unwrap().unwrap();
        if let Message::Text(t) = m {
            return serde_json::from_str(&t).unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconnect_resumes_without_gaps() {
    let run = start_synth(50, 100.0, HubConfig::default()).await;
    let mut ws = connect(run.port, None).await;
    let mut last = 0u64;
    let mut frames = 0;
    while frames < 20 {
        let v = next_json(&mut ws).await;
        match v["type"].as_str().unwrap() {
            "tm" => {
                let first = v["first"].as_u64().unwrap();
                let n = v["s"].as_array().unwrap().len() as u64;
                if last != 0 {
                    assert_eq!(first, last + 1, "gap inside one connection");
                }
                last = first + n - 1;
                frames += 1;
            }
            "snapshot" => last = v["seq"].as_u64().unwrap(),
            _ => {}
        }
    }
    drop(ws);
    // Data keeps flowing while we are away.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut ws2 = connect(run.port, Some(last)).await;
    let mut saw_resume = false;
    loop {
        let v = next_json(&mut ws2).await;
        match v["type"].as_str().unwrap() {
            "resume" => {
                assert_eq!(v["gapless"], true);
                saw_resume = true;
            }
            "tm" => {
                let first = v["first"].as_u64().unwrap();
                assert!(saw_resume);
                assert!(first <= last + 1, "resume skipped data: first={first} last={last}");
                break;
            }
            _ => {}
        }
    }
    run.stop.store(true, std::sync::atomic::Ordering::Relaxed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_client_stays_bounded() {
    let cap = 5_000;
    let run = start_synth(200, 200.0, HubConfig { ring_samples: 50_000, client_queue_samples: cap, client_control_msgs: 100 }).await;
    let _stalled = connect(run.port, None).await;
    let mut fast = connect(run.port, None).await;
    let mut got = 0usize;
    let until = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < until {
        let v = next_json(&mut fast).await;
        if v["type"] == "tm" {
            got += v["s"].as_array().unwrap().len();
        }
    }
    let clients = run.gw.hub.clients();
    let worst = clients.iter().map(|c| c.stats()).max_by_key(|s| s.dropped_samples).unwrap();
    assert!(worst.dropped_samples > 0, "stalled client should have been coalesced");
    assert!(worst.max_queued_samples <= cap);
    assert_eq!(run.gw.stats.stalls.load(std::sync::atomic::Ordering::Relaxed), 0);
    // 200 x 200 Hz for ~3 s, the fast client should get nearly all of it.
    assert!(got > 100_000, "fast client starved: {got}");
    run.stop.store(true, std::sync::atomic::Ordering::Relaxed);
}

#[test]
fn analyze_counts_los_from_recording() {
    let text = "#downrange-rec v1\n#start_unix_ms 1000\n#chan 0 TIME_000001\n#chan 1 USLAB000058\n\
                0\t0\t1.0\t5\t24\n100\t1\t1.0\t754.1\t24\n1000\t0\t1.0\t6\t24\n2000\t0\t1.0\t7\t0\n\
                3000\t0\t1.0\t8\t24\n30000\t0\t1.0\t9\t24\n";
    let rec = downrange::record::parse(text);
    let chans: Vec<downrange::model::Channel> =
        serde_json::from_str(&std::fs::read_to_string("../channels/iss_channels.json").unwrap()).unwrap();
    let r = downrange::analyze::analyze(&rec, &chans);
    // One LOS from the status flag at t=2s, one from the 27 s heartbeat gap.
    assert_eq!(r["los_events"], 2, "{r}");
    assert_eq!(r["updates_received"], 6);
}
