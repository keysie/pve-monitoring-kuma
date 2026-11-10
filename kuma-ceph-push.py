#!/usr/bin/env python3
"""kuma-ceph-push - per-node Ceph + mesh health check for an Uptime Kuma push monitor.

One push monitor per node. Every run (cron, once a minute) sends:
  status  up for OK/ATTN, down for BAD (ATTN can count as down, see attn_is_down)
  msg     one line, max 100 chars, e.g.
          OK H:OK PG:ok P:12.3% OSD:3ms M:7=.071 8=.064 N:ok
  ping    RTT from this node to the Kuma host in ms (ICMP, HTTP as fallback)

Message fields:
  H    Ceph health (OK / WARN(checks) / ERR(checks); n/a = Ceph not reachable)
  PG   problematic PG states, or ok
  P    pool usage
  OSD  highest OSD commit latency (@id of the OSD when not OK)
  M    mesh RTT per peer in ms, peer = last octet of its IP
  N    mesh NICs: ok, or iface:down / iface:<speed>M / iface:+<n>err (new errors since last run)

Needs only a read-only Ceph keyring (client.kuma) and outbound HTTP to Kuma.

Usage:  kuma-ceph-push.py            # check and push
        kuma-ceph-push.py --dry-run  # check and print, don't push
"""
import json, os, re, subprocess, sys, time, urllib.error, urllib.parse, urllib.request

CONF = "/etc/kuma-ceph-push.json"
STATE = "/var/lib/kuma-ceph-push/nic.json"
OK, ATTN, BAD = 0, 1, 2
LEVEL = ["OK", "ATTN", "BAD"]
MAXLEN = 100

DEFAULTS = {
    "push_url": "",               # Kuma push URL (query string is ignored)
    "ceph_name": "client.kuma",   # read-only keyring
    "keyring": "",                # explicit keyring path; empty = ceph.conf default
    "pool": "ceph-vm",
    "pool_pct": [75, 85],         # thresholds [ATTN, BAD]
    "osd_commit_ms": [20, 100],   # thresholds [ATTN, BAD]
    "mesh_peers": [],             # mesh IPs of the other nodes
    "mesh_rtt_ms": [0.5, 2.0],    # thresholds [ATTN, BAD]; any packet loss = BAD
    "mesh_ifaces": [],            # mesh NICs on this node
    "nic_speed": 10000,           # expected link speed in Mb/s (0 = don't check)
    "nic_err_delta": [5, 100],    # new NIC errors per run, thresholds [ATTN, BAD]
    "ignore_checks": [],          # health checks to ignore, e.g. ["POOL_NO_REDUNDANCY"]
    "attn_is_down": False,        # True: ATTN also sets the monitor down
}
NIC_COUNTERS = ["rx_errors", "rx_crc_errors", "rx_frame_errors", "rx_missed_errors", "tx_errors"]
PG_ATTN = {"laggy", "degraded", "undersized", "peering", "backfill_toofull", "recovery_toofull"}
PG_BAD = {"stale", "down", "incomplete", "inconsistent"}


def load_conf():
    cfg = dict(DEFAULTS)
    with open(CONF) as f:
        cfg.update(json.load(f))
    if not cfg["push_url"]:
        sys.exit(f"push_url missing in {CONF}")
    return cfg


def grade(value, limits):
    return BAD if value >= limits[1] else ATTN if value >= limits[0] else OK


def read(path):
    with open(path) as f:
        return f.read().strip()


def ceph(cfg, *args):
    cmd = ["timeout", "20", "ceph", "--name", cfg["ceph_name"]]
    if cfg["keyring"]:
        cmd += ["--keyring", cfg["keyring"]]
    r = subprocess.run(cmd + [*args, "-f", "json"], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"ceph {' '.join(args)} failed ({r.returncode}): {r.stderr.strip()[:200]}")
    return json.loads(r.stdout)


def ping_avg(host, count=5):
    r = subprocess.run(["ping", "-q", "-n", "-c", str(count), "-i", "0.2", "-W", "1", host],
                       capture_output=True, text=True)
    loss = re.search(r"([\d.]+)% packet loss", r.stdout)
    rtt = re.search(r"= [\d.]+/([\d.]+)/", r.stdout)
    return (float(loss.group(1)) if loss else 100.0), (float(rtt.group(1)) if rtt else None)


def fmt_ms(v):
    return f"{v:.3f}".lstrip("0") if v < 1 else f"{v:.1f}"


# Each check returns a list of (level, token, short_token)

def check_status(cfg):
    s = ceph(cfg, "status")
    h = s.get("health", {})
    st = h.get("status", "HEALTH_UNKNOWN").replace("HEALTH_", "")
    checks = [c for c in h.get("checks", {}) if c not in cfg["ignore_checks"]]
    if st == "OK" or (st == "WARN" and not checks):
        hl, htok, hshort = OK, "H:OK", "H:OK"
    else:
        hl = ATTN if st == "WARN" else BAD
        htok = f"H:{st}" + (f"({','.join(checks)})" if checks else "")
        hshort = f"H:{st}" + (f"({len(checks)})" if checks else "")

    states = set()
    for p in s.get("pgmap", {}).get("pgs_by_state", []):
        states.update(p["state_name"].split("+"))
    bad, attn = sorted(states & PG_BAD), sorted(states & PG_ATTN)
    pl = BAD if bad else ATTN if attn else OK
    ptok = "PG:" + (",".join(bad + attn) or "ok")
    return [(hl, htok, hshort), (pl, ptok, ptok)]


def check_osd(cfg):
    d = ceph(cfg, "osd", "perf")
    infos = d.get("osdstats", d).get("osd_perf_infos", [])
    if not infos:
        return [(ATTN, "OSD:?", "OSD:?")]
    worst = max(infos, key=lambda i: i["perf_stats"].get("commit_latency_ms", 0))
    ms = worst["perf_stats"].get("commit_latency_ms", 0)
    lvl = grade(ms, cfg["osd_commit_ms"])
    tok = f"OSD:{ms}ms" + (f"@{worst['id']}" if lvl else "")
    return [(lvl, tok, tok)]


def check_pool(cfg):
    for p in ceph(cfg, "df").get("pools", []):
        if p["name"] == cfg["pool"]:
            pct = p["stats"]["percent_used"] * 100   # Ceph reports a fraction (0-1)
            tok = f"P:{pct:.1f}%"
            return [(grade(pct, cfg["pool_pct"]), tok, tok)]
    return [(ATTN, "P:?", "P:?")]


def check_mesh(cfg):
    parts, lvl = [], OK
    for peer in cfg["mesh_peers"]:
        tag = peer.rsplit(".", 1)[-1]
        loss, rtt = ping_avg(peer)
        if loss > 0 or rtt is None:
            l = BAD
            parts.append(f"{tag}=loss{loss:.0f}%")
        else:
            l = grade(rtt, cfg["mesh_rtt_ms"])
            parts.append(f"{tag}={fmt_ms(rtt)}")
        lvl = max(lvl, l)
    tok = "M:" + " ".join(parts)
    return [(lvl, tok, tok)] if parts else []


def check_nics(cfg):
    try:
        with open(STATE) as f:
            prev = json.load(f)
    except (OSError, ValueError):
        prev = {}
    cur, issues, lvl = {}, [], OK
    for ifc in cfg["mesh_ifaces"]:
        base = f"/sys/class/net/{ifc}"
        if not os.path.isdir(base):
            issues.append(f"{ifc}:gone"); lvl = BAD
            continue
        if read(f"{base}/operstate") != "up":
            issues.append(f"{ifc}:down"); lvl = BAD
            continue
        if cfg["nic_speed"]:
            try:
                speed = int(read(f"{base}/speed"))
            except (OSError, ValueError):
                speed = -1
            if speed != cfg["nic_speed"]:
                issues.append(f"{ifc}:{speed}M"); lvl = BAD
        new = 0
        for c in NIC_COUNTERS:
            try:
                v = int(read(f"{base}/statistics/{c}"))
            except (OSError, ValueError):
                continue
            key = f"{ifc}/{c}"
            cur[key] = v
            new += max(0, v - prev.get(key, v))
        l = grade(new, cfg["nic_err_delta"]) if new else OK
        if l:
            issues.append(f"{ifc}:+{new}err"); lvl = max(lvl, l)
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    with open(STATE, "w") as f:
        json.dump(cur, f)
    tok = "N:" + (" ".join(issues) or "ok")
    return [(lvl, tok, tok)] if cfg["mesh_ifaces"] else []


def kuma_rtt(url):
    """RTT from this node to the Kuma host: ICMP first, HTTP round trip as fallback."""
    u = urllib.parse.urlparse(url)
    _, rtt = ping_avg(u.hostname, 3)
    if rtt is not None:
        return rtt
    t = time.monotonic()
    try:
        urllib.request.urlopen(f"{u.scheme}://{u.netloc}/", timeout=5).read(0)
    except urllib.error.HTTPError:
        pass                      # an HTTP error still is a round trip
    except Exception:
        return None
    return (time.monotonic() - t) * 1000


def build_msg(level, tokens):
    head = [LEVEL[level]]
    msg = " ".join(head + [t[1] for t in tokens])
    if len(msg) > MAXLEN:
        msg = " ".join(head + [t[2] for t in tokens])
    return msg if len(msg) <= MAXLEN else msg[:MAXLEN - 1] + "~"


def main():
    dry = "--dry-run" in sys.argv
    cfg = load_conf()
    tokens, errors = [], []
    for name, fn in (("H", check_status), ("OSD", check_osd), ("P", check_pool),
                     ("M", check_mesh), ("N", check_nics)):
        try:
            tokens += fn(cfg)
        except Exception as e:
            tokens.append((BAD, f"{name}:n/a", f"{name}:n/a"))
            errors.append(f"{fn.__name__}: {e}")

    level = max((t[0] for t in tokens), default=OK)
    msg = build_msg(level, tokens)
    down = level == BAD or (level == ATTN and cfg["attn_is_down"])
    params = {"status": "down" if down else "up", "msg": msg}
    rtt = kuma_rtt(cfg["push_url"])
    if rtt is not None:
        params["ping"] = round(rtt, 2)
    url = cfg["push_url"].split("?")[0] + "?" + urllib.parse.urlencode(params)

    if dry:
        print(f"status: {params['status']}   ping: {params.get('ping')} ms   len: {len(msg)}")
        print(f"msg:    {msg}")
        for e in errors:
            print(f"error:  {e}")
        return
    for e in errors:
        print(e, file=sys.stderr)
    try:
        urllib.request.urlopen(url, timeout=10).read()
    except Exception as e:
        sys.exit(f"push failed: {e}")


if __name__ == "__main__":
    main()
