"""Merge the raw measurement files into results/results.json."""
import glob
import json
import os
import platform
import subprocess
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = os.path.join(ROOT, "results", "raw")


def load(name):
    p = os.path.join(RAW, name)
    return json.load(open(p)) if os.path.exists(p) else None


def sh(*cmd):
    try:
        return subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return None


throughput = load("throughput.json") or {"points": []}
passing = [p for p in throughput["points"] if p.get("pass")]
best = max(passing, key=lambda p: p["offered_samples_per_s"]) if passing else None

render = {}
for f in sorted(glob.glob(os.path.join(RAW, "browser_render_*.json"))):
    key = os.path.basename(f)[len("browser_render_"):-len(".json")]
    render[key] = json.load(open(f))

results = {
    "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "machine": {
        "os": f"{platform.system()} {platform.release()}",
        "macos": sh("sw_vers", "-productVersion"),
        "arch": platform.machine(),
        "logical_cpus": os.cpu_count(),
        "rustc": sh("rustc", "--version"),
        "note": "gateway and all benchmark clients run on the same machine over loopback",
    },
    "iss_session": load("iss_session.json"),
    "fanout_throughput": {
        "load": "synthetic generator (benchmark only, not ISS data)",
        "clients": throughput["points"][0]["clients"] if throughput["points"] else None,
        "max_sustained": None
        if not best
        else {
            "channels": best["channels"],
            "hz_per_channel": best["hz_per_channel"],
            "samples_per_s": best["offered_samples_per_s"],
            "clients": best["clients"],
            "ingest_stalls": best["ingest_stalls"],
            "latency_ms_p50": best["latency_ms_p50"],
            "latency_ms_p99": best["latency_ms_p99"],
        },
        "pass_rule": "0 ingest stalls, 0 coalesced frames, 0 sequence gaps, every client receives >= 98% of offered rate",
        "points": throughput["points"],
    },
    "slow_client": load("slow_client.json"),
    "command_faults": load("commands.json"),
    "browser_render": render,
}

out = os.path.join(ROOT, "results", "results.json")
json.dump(results, open(out, "w"), indent=2)
print(f"wrote {out}")
