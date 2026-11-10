#!/usr/bin/env python3
"""kuma-ceph-push - per-node detector for low-level Ceph/mesh problems, reports to an
Uptime Kuma push monitor. Aimed at what the Proxmox UI doesn't show: degrading mesh
links (loss, latency, TCP retransmits, NIC errors, link speed) and slow local OSDs.

One push monitor per node, cron once a minute. Each run sends:
  status  up for OK/ATTN, down for BAD (see attn_is_down)
  msg     one line, max 60 chars, one verdict (Good/Warn/Bad) per area, e.g.
            Links: Good, NICs: Good, OSD-Latency: Good, Ceph: Good
            Links: Bad (01->03), NICs: Warn, OSD-Latency: Good, Ceph: Warn
          Details (values, reasons) are printed by --dry-run.
  ping    RTT from this node to the Kuma host in ms

Checks:
  mesh peers   packet loss and RTT (ping), TCP retransmit rate of all
               connections to the peer (mostly Ceph traffic, from `ss`)
  mesh NICs    link down, wrong speed, new rx/tx/CRC errors since last run
  local OSDs   highest commit latency, absolute and relative to cluster median
  Ceph         slow ops, laggy / stale / down PGs

Needs only a read-only Ceph keyring (client.kuma) and outbound HTTP to Kuma.

Usage:  kuma-ceph-push.py            # check and push
        kuma-ceph-push.py --dry-run  # check and print, don't push
"""
import json, os, re, socket, statistics, subprocess, sys, time
import urllib.error, urllib.parse, urllib.request

CONF = "/etc/kuma-ceph-push.json"
STATE = "/var/lib/kuma-ceph-push/state.json"
OK, ATTN, BAD = 0, 1, 2
WORD = ["Good", "Warn", "Bad"]
AREAS = ["Links", "NICs", "OSD-Latency", "Ceph"]
MAXLEN = 60

DEFAULTS = {
    "push_url": "",               # Kuma push URL (query string is ignored)
    "ceph_name": "client.kuma",   # read-only keyring
    "keyring": "",                # explicit keyring path; empty = ceph.conf default
    "host": "",                   # CRUSH host name; empty = short hostname
    "mesh_peers": [],             # mesh IPs of the other nodes
    "node_names": {},             # mesh IP -> short name for link hints, e.g. {"192.168.33.6": "01"}
    "mesh_ifaces": [],            # mesh NICs on this node
    "nic_speed": 10000,           # expected link speed in Mb/s (0 = don't check)
    "rtt_ms": [0.5, 2.0],         # mesh ping RTT, thresholds [ATTN, BAD]; any loss = BAD
    "retrans_pct": [0.05, 1.0],   # TCP retransmitted bytes per peer, [ATTN, BAD]
    "retrans_min_mb": 10,         # min traffic per run to judge the retransmit rate
    "nic_err_delta": [5, 100],    # new NIC errors per run, [ATTN, BAD]
    "osd_ms": [20, 100],          # local OSD commit latency, [ATTN, BAD]
    "osd_outlier": 4,             # ATTN if a local OSD is >= this x cluster median (min 5 ms)
    "attn_is_down": False,        # True: ATTN also sets the monitor down
}
NIC_COUNTERS = ["rx_errors", "rx_crc_errors", "rx_frame_errors", "rx_missed_errors", "tx_errors"]


def load_conf():
    cfg = dict(DEFAULTS)
    with open(CONF) as f:
        cfg.update(json.load(f))
    if not cfg["push_url"]:
        sys.exit(f"push_url missing in {CONF}")
    return cfg


def load_state():
    try:
        with open(STATE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def save_state(state):
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    with open(STATE, "w") as f:
        json.dump(state, f)


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


def tcp_counters(peer):
    """Sum of bytes sent / retransmitted over all TCP connections to peer."""
    r = subprocess.run(["ss", "-tinH", "dst", peer], capture_output=True, text=True)
    sent = sum(int(x) for x in re.findall(r"bytes_sent:(\d+)", r.stdout))
    retr = sum(int(x) for x in re.findall(r"bytes_retrans:(\d+)", r.stdout))
    return sent, retr


def check_ceph(cfg, issues):
    s = ceph(cfg, "status")
    if "SLOW_OPS" in s.get("health", {}).get("checks", {}):
        issues.append(("Ceph", ATTN, "", "slow ops"))
    states = set()
    for p in s.get("pgmap", {}).get("pgs_by_state", []):
        states.update(p["state_name"].split("+"))
    for st in ("down", "stale", "incomplete"):
        if st in states:
            issues.append(("Ceph", BAD, "", f"PGs {st}"))
    if "laggy" in states:
        issues.append(("Ceph", ATTN, "", "laggy PGs"))

    d = ceph(cfg, "osd", "perf")
    infos = d.get("osdstats", d).get("osd_perf_infos", [])
    lat = {i["id"]: i["perf_stats"].get("commit_latency_ms", 0) for i in infos}
    host = cfg["host"] or socket.gethostname().split(".")[0]
    local = [i for i in ceph(cfg, "osd", "ls-tree", host) if i in lat]
    if not local:
        issues.append(("OSD-Latency", ATTN, "", f"no OSDs found for host {host}"))
        return
    med = statistics.median(lat.values())
    worst = max(local, key=lat.get)
    ms = lat[worst]
    lvl = grade(ms, cfg["osd_ms"])
    if lvl == OK and ms >= 5 and ms >= cfg["osd_outlier"] * max(med, 1):
        lvl = ATTN
    issues.append(("OSD-Latency", lvl, f"osd.{worst}" if lvl else "",
                   f"slowest local osd.{worst} {ms}ms (cluster median {med}ms)"))


def local_name(cfg):
    """Short name of this node: the node_names entry whose IP is configured locally."""
    r = subprocess.run(["ip", "-o", "addr", "show"], capture_output=True, text=True)
    local_ips = set(re.findall(r"inet6? ([0-9a-fA-F:.]+)/", r.stdout))
    for ip, name in cfg["node_names"].items():
        if ip in local_ips:
            return name
    return socket.gethostname().split(".")[0]


def check_mesh(cfg, issues, prev, state):
    me = local_name(cfg)
    for peer in cfg["mesh_peers"]:
        tag = f"{me}->{cfg['node_names'].get(peer, peer)}"
        loss, rtt = ping_avg(peer)
        if loss > 0 or rtt is None:
            issues.append(("Links", BAD, tag, f"{peer}: ping loss {loss:.0f}%"))
        else:
            lvl = grade(rtt, cfg["rtt_ms"])
            issues.append(("Links", lvl, tag if lvl else "", f"{peer}: ping {rtt:.3f}ms"))

        sent, retr = tcp_counters(peer)
        state[f"tcp/{peer}"] = [sent, retr]
        old = prev.get(f"tcp/{peer}")
        if old:
            ds, dr = sent - old[0], retr - old[1]
            if ds >= cfg["retrans_min_mb"] * 1e6 and dr >= 0:
                pct = 100 * dr / ds
                lvl = grade(pct, cfg["retrans_pct"])
                issues.append(("Links", lvl, tag if lvl else "",
                               f"{peer}: {pct:.3f}% TCP retrans ({ds / 1e6:.0f} MB sent)"))
            else:
                issues.append(("Links", OK, "", f"{peer}: retrans not judged (too little traffic)"))


def check_nics(cfg, issues, prev, state):
    for ifc in cfg["mesh_ifaces"]:
        base = f"/sys/class/net/{ifc}"
        if not os.path.isdir(base):
            issues.append(("NICs", BAD, ifc, f"{ifc} missing"))
            continue
        if read(f"{base}/operstate") != "up":
            issues.append(("NICs", BAD, ifc, f"{ifc} down"))
            continue
        if cfg["nic_speed"]:
            try:
                speed = int(read(f"{base}/speed"))
            except (OSError, ValueError):
                speed = -1
            if speed != cfg["nic_speed"]:
                issues.append(("NICs", BAD, ifc, f"{ifc} at {speed}Mb/s"))
        new = 0
        for c in NIC_COUNTERS:
            try:
                v = int(read(f"{base}/statistics/{c}"))
            except (OSError, ValueError):
                continue
            key = f"nic/{ifc}/{c}"
            state[key] = v
            new += max(0, v - prev.get(key, v))
        lvl = grade(new, cfg["nic_err_delta"]) if new else OK
        issues.append(("NICs", lvl, ifc if lvl else "", f"{ifc}: {new} new errors"))


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


def build_msg(issues):
    """One verdict per area, with a short hint (which peer/NIC/OSD) when space allows."""
    level = {a: OK for a in AREAS}
    hints = {a: [] for a in AREAS}
    for area, lvl, hint, _ in issues:
        level[area] = max(level[area], lvl)
        if lvl and hint and hint not in hints[area]:
            hints[area].append(hint)

    def render(with_hints):
        parts = []
        for a in AREAS:
            h = hints[a] if a in with_hints and level[a] else []
            parts.append(f"{a}: {WORD[level[a]]}" + (f" ({','.join(h)})" if h else ""))
        return ", ".join(parts)

    for with_hints in (AREAS, ["Links", "OSD-Latency"], ["Links"], []):
        msg = render(with_hints)
        if len(msg) <= MAXLEN:
            return msg
    return msg[:MAXLEN]


def main():
    dry = "--dry-run" in sys.argv
    cfg = load_conf()
    prev, state = load_state(), {}
    issues, errors = [], []

    for fn, area, args in ((check_mesh, "Links", (prev, state)), (check_nics, "NICs", (prev, state)),
                           (check_ceph, "Ceph", ())):
        try:
            fn(cfg, issues, *args)
        except Exception as e:
            issues.append((area, BAD, "", f"{fn.__name__} failed"))
            if fn is check_ceph:
                issues.append(("OSD-Latency", BAD, "", "not checked, Ceph not reachable"))
            errors.append(f"{fn.__name__}: {e}")
    save_state(state)

    level = max((l for _, l, _, _ in issues), default=OK)
    msg = build_msg(issues)
    down = level == BAD or (level == ATTN and cfg["attn_is_down"])
    params = {"status": "down" if down else "up", "msg": msg}
    rtt = kuma_rtt(cfg["push_url"])
    if rtt is not None:
        params["ping"] = round(rtt, 2)
    url = cfg["push_url"].split("?")[0] + "?" + urllib.parse.urlencode(params)

    if dry:
        print(msg)
        print(f"  -> status {params['status']}, ping {params.get('ping')} ms, {len(msg)} chars")
        for area, lvl, _, detail in issues:
            print(f"  {WORD[lvl]:<4}  {area:<11}  {detail}")
        for e in errors:
            print(f"  error: {e}")
        return
    for e in errors:
        print(e, file=sys.stderr)
    try:
        urllib.request.urlopen(url, timeout=10).read()
    except Exception as e:
        sys.exit(f"push failed: {e}")


if __name__ == "__main__":
    main()
