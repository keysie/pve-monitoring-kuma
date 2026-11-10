#!/usr/bin/env python3
"""kuma-ceph-push - Ceph + mesh health check reporting to an Uptime Kuma push monitor.

Run every minute via cron on each Ceph node. Pushes status=up when all checks
pass, otherwise status=down with a short list of problems. The 'ping' value
carries pool usage in %, so Kuma graphs it.

Needs only a read-only Ceph keyring (client.kuma) and outbound HTTP to Kuma.
No inbound ports, no credentials stored in Kuma.

Usage:  kuma-ceph-push.py            # check and push
        kuma-ceph-push.py --dry-run  # check and print, don't push
"""
import json, os, re, subprocess, sys, urllib.parse, urllib.request

CONF = "/etc/kuma-ceph-push.json"
STATE = "/var/lib/kuma-ceph-push/nic.json"

DEFAULTS = {
    "push_url": "",               # Kuma push URL (query string is ignored)
    "ceph_name": "client.kuma",   # read-only keyring
    "pool": "ceph-vm",
    "pool_warn_pct": 80,          # pool usage that triggers an alert
    "warn_is_down": True,         # HEALTH_WARN counts as a problem
    "ignore_checks": [],          # health checks to ignore, e.g. ["POOL_NO_REDUNDANCY"]
    "osd_commit_ms": 50,          # commit latency per OSD that triggers an alert
    "mesh_peers": [],             # mesh IPs of the other nodes
    "mesh_rtt_ms": 1.0,           # avg RTT that triggers an alert
    "mesh_ifaces": [],            # mesh NICs on this node
    "nic_speed": 10000,           # expected link speed in Mb/s (0 = don't check)
    "nic_err_delta": 10,          # new NIC errors per run that trigger an alert
}
NIC_COUNTERS = ["rx_errors", "rx_crc_errors", "rx_frame_errors", "rx_missed_errors", "tx_errors"]
BAD_PG_STATES = {"laggy", "degraded", "undersized", "stale", "peering", "down",
                 "inconsistent", "incomplete"}


def load_conf():
    cfg = dict(DEFAULTS)
    with open(CONF) as f:
        cfg.update(json.load(f))
    if not cfg["push_url"]:
        sys.exit(f"push_url missing in {CONF}")
    return cfg


def ceph(cfg, *args):
    r = subprocess.run(["timeout", "20", "ceph", "--name", cfg["ceph_name"], *args, "-f", "json"],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"ceph {' '.join(args)} failed ({r.returncode}): {r.stderr.strip()[:80]}")
    return json.loads(r.stdout)


def check_status(cfg, problems):
    s = ceph(cfg, "status")
    h = s.get("health", {})
    status = h.get("status", "UNKNOWN")
    checks = [c for c in h.get("checks", {}) if c not in cfg["ignore_checks"]]
    if status == "HEALTH_ERR" or (status == "HEALTH_WARN" and cfg["warn_is_down"] and checks):
        problems.append(f"{status}: {','.join(checks) or '?'}")
    states = set()
    for p in s.get("pgmap", {}).get("pgs_by_state", []):
        states.update(p["state_name"].split("+"))
    bad = states & BAD_PG_STATES
    if bad:
        problems.append("PGs " + ",".join(sorted(bad)))


def check_osd_perf(cfg, problems):
    d = ceph(cfg, "osd", "perf")
    infos = d.get("osdstats", d).get("osd_perf_infos", [])
    slow = [f"osd.{i['id']}={i['perf_stats'].get('commit_latency_ms', 0)}ms" for i in infos
            if i["perf_stats"].get("commit_latency_ms", 0) > cfg["osd_commit_ms"]]
    if slow:
        problems.append("slow " + " ".join(slow))


def check_pool(cfg, problems):
    for p in ceph(cfg, "df").get("pools", []):
        if p["name"] == cfg["pool"]:
            pct = p["stats"]["percent_used"] * 100   # Ceph reports a fraction (0-1)
            if pct >= cfg["pool_warn_pct"]:
                problems.append(f"{cfg['pool']} {pct:.1f}% full")
            return pct
    problems.append(f"pool {cfg['pool']} not found")
    return None


def check_mesh(cfg, problems):
    for peer in cfg["mesh_peers"]:
        r = subprocess.run(["ping", "-q", "-n", "-c", "5", "-i", "0.2", "-W", "1", peer],
                           capture_output=True, text=True)
        loss = re.search(r"([\d.]+)% packet loss", r.stdout)
        rtt = re.search(r"= [\d.]+/([\d.]+)/", r.stdout)
        if not loss or float(loss.group(1)) > 0:
            problems.append(f"{peer} loss {loss.group(1) if loss else '100'}%")
        elif rtt and float(rtt.group(1)) > cfg["mesh_rtt_ms"]:
            problems.append(f"{peer} rtt {rtt.group(1)}ms")


def read(path):
    with open(path) as f:
        return f.read().strip()


def check_nics(cfg, problems):
    try:
        with open(STATE) as f:
            prev = json.load(f)
    except (OSError, ValueError):
        prev = {}
    cur = {}
    for ifc in cfg["mesh_ifaces"]:
        base = f"/sys/class/net/{ifc}"
        if not os.path.isdir(base):
            problems.append(f"{ifc} missing")
            continue
        if read(f"{base}/operstate") != "up":
            problems.append(f"{ifc} down")
            continue
        if cfg["nic_speed"]:
            try:
                speed = int(read(f"{base}/speed"))
                if speed != cfg["nic_speed"]:
                    problems.append(f"{ifc} {speed}Mb/s")
            except (OSError, ValueError):
                pass
        for c in NIC_COUNTERS:
            try:
                v = int(read(f"{base}/statistics/{c}"))
            except (OSError, ValueError):
                continue
            key = f"{ifc}/{c}"
            cur[key] = v
            delta = v - prev.get(key, v)
            if delta > cfg["nic_err_delta"]:
                problems.append(f"{ifc} +{delta} {c}")
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    with open(STATE, "w") as f:
        json.dump(cur, f)


def main():
    dry = "--dry-run" in sys.argv
    cfg = load_conf()
    problems, pct = [], None
    for fn in (check_status, check_osd_perf, check_mesh, check_nics):
        try:
            fn(cfg, problems)
        except Exception as e:
            problems.append(f"{fn.__name__}: {e}"[:120])
    try:
        pct = check_pool(cfg, problems)
    except Exception as e:
        problems.append(f"check_pool: {e}"[:120])

    status = "down" if problems else "up"
    msg = "; ".join(problems) if problems else "OK" + (f", {cfg['pool']} {pct:.1f}%" if pct is not None else "")
    params = {"status": status, "msg": msg[:250]}
    if pct is not None:
        params["ping"] = round(pct, 1)
    url = cfg["push_url"].split("?")[0] + "?" + urllib.parse.urlencode(params)

    if dry:
        print(f"{status}: {msg}\n{url.split('/push/')[0]}/push/<token>?{url.split('?')[1]}")
        return
    try:
        urllib.request.urlopen(url, timeout=10).read()
    except Exception as e:
        sys.exit(f"push failed: {e}")


if __name__ == "__main__":
    main()
