#!/usr/bin/env python3
"""kuma-node-push - per-node detector for subtle hardware problems, reports to an
Uptime Kuma push monitor. Aimed at what the Proxmox UI/API doesn't show: rising
ECC error rates, ZFS vdev error counters and slow I/O, SMART deltas, PCIe AER
errors and link downtraining, BMC events, kernel hangs/lockups, clock drift.

One push monitor per node, cron once a minute. Each run sends:
  status  up for Good/Warn, down for Bad (see warn_is_down)
  msg     one line, max 60 chars, one verdict per area, e.g.
            ECC: Good, ZFS: Good, Disks: Good, HW: Good, System: Good
            ECC: Bad (mc0/dimm2), ZFS: Warn (slow I/O), rest: Good
  ping    RTT from this node to the Kuma host in ms

Areas:
  ECC     EDAC: new corrected errors per DIMM, any uncorrected error
  ZFS     pool/vdev state, read/write/checksum counters, data errors,
          scrub age/result, new zpool events (slow I/O, checksum, deadman ...)
  Disks   SMART/NVMe deltas: media errors, spare, wear, realloc/pending,
          CRC (cable), temperature time, self-test; plus kernel I/O errors
          (polled every smart_interval_min, cached in between)
  HW      PCIe AER counters, PCIe link downtraining vs. baseline, CPU thermal
          throttling, machine checks, IPMI sensors, SEL events and SEL fill level
          (IPMI polled every ipmi_interval_min)
  System  kernel log: hung tasks, lockups, RCU stalls, OOM kills, NIC link
          drops; chrony offset / sync state

Problems found from counters or the log are held for hold_minutes, so a single
event stays visible (and a Bad stays down) long enough to be noticed.

Usage:  kuma-node-push.py            # check and push (run by cron)
        kuma-node-push.py --dry-run  # check everything fresh, print details,
                                     # don't push, don't touch saved state
"""
import copy, fcntl, glob, json, os, re, subprocess, sys, time
import urllib.error, urllib.parse, urllib.request

CONF = "/etc/kuma-node-push.json"
STATE = "/var/lib/kuma-node-push/state.json"
LOCK = "/run/kuma-node-push.lock"
SYS = "/sys"
OK, WARN, BAD = 0, 1, 2
WORD = ["Good", "Warn", "Bad"]
ALL_AREAS = ["ECC", "ZFS", "Disks", "HW", "System"]
MAXLEN = 60
HINTLEN = 12

DEFAULTS = {
    "push_url": "",                 # Kuma push URL (query string is ignored)
    "areas": {a: True for a in ALL_AREAS},   # set an area to false to disable it
    "hold_minutes": 60,             # how long counter/log findings stay visible
    "warn_is_down": False,          # True: Warn also sets the monitor down
    "ignore": [],                   # regexes matched against "Area: hint: detail"
    # ECC
    "ecc_ce_delta": [1, 10],        # new corrected errors per DIMM per run [Warn, Bad]
    # ZFS
    "zfs_err_delta": [1, 10],       # new read+write+cksum errors per vdev per run
    "zfs_scrub_max_days": 35,
    # Disks
    "smart_interval_min": 10,
    "nvme_wear_pct": [80, 95],      # NVMe percentage_used [Warn, Bad]
    "ata_crc_delta": [1, 50],       # new UDMA CRC errors (cable/backplane)
    # HW
    "ipmi_interval_min": 5,
    "aer_cor_delta": [1, 100],      # new corrected PCIe errors per device per run
    "throttle_delta": [1, 1000],    # new CPU thermal throttle events per run
    "sel_full_pct": 80,
    # System
    "time_offset_ms": [10, 50],     # chrony system offset (Ceph allows 50 ms)
    "klog_ignore_ifaces": "^(tap|fwpr|fwln|fwbr|veth)",
}

# kernel log patterns: (regex, area, level, label, hint group index or fixed hint)
KLOG_RULES = [
    (r"mce: \[Hardware Error\]|Machine check events logged|\[Hardware Error\]",
     "HW", BAD, "machine check", "MCE"),
    (r"PCIe Bus Error|AER: (?:Corrected|Uncorrected|Multiple)", "HW", WARN, "PCIe AER error", "PCIe"),
    (r"blocked for more than \d+ seconds", "System", BAD, "hung task", "hung task"),
    (r"soft lockup|hard LOCKUP|watchdog: BUG", "System", BAD, "CPU lockup", "lockup"),
    (r"rcu: INFO: rcu_\w+ (?:self-)?detected stall|rcu_\w+ self-detected stall",
     "System", BAD, "RCU stall", "RCU stall"),
    (r"Out of memory: Killed process|oom-kill:", "System", WARN, "OOM kill", "OOM"),
    (r"(nvme\d+): (?:I/O \d+ (?:\(\w+\) )?QID \d+ timeout|controller is down|"
     r"resetting controller|Device not ready|Abort status)", "Disks", BAD, "NVMe timeout/reset", 1),
    (r"I/O error, dev (?!sr\d|loop)(\w+)", "Disks", WARN, "I/O error", 1),
    (r"(ata\d+)(?:\.\d+)?: (?:hard resetting link|SError|failed command|exception Emask)",
     "Disks", WARN, "SATA link error", 1),
    (r"([\w.-]+): (?:NIC )?Link is Down", "System", WARN, "NIC link down", 1),
    (r"WARNING: Pool '([^']+)' has encountered an uncorrectable I/O failure",
     "ZFS", BAD, "pool I/O failure", 1),
    (r"zio pool=(\S+) vdev=\S+ error=\d+", "ZFS", WARN, "ZFS I/O error", 1),
]

ZFS_EVENTS = {
    "ereport.fs.zfs.delay": (WARN, "slow I/O"),
    "ereport.fs.zfs.deadman": (BAD, "hung I/O"),
    "ereport.fs.zfs.checksum": (WARN, "checksum"),
    "ereport.fs.zfs.io": (WARN, "I/O error"),
    "ereport.fs.zfs.data": (BAD, "data error"),
    "ereport.fs.zfs.probe_failure": (BAD, "probe fail"),
    "ereport.fs.zfs.authentication": (BAD, "auth error"),
}
ZFS_GROUP = re.compile(r"^(mirror|raidz\d?|draid\d?\S*|spare|replacing|indirect)(-\d+)?$")

IPMI_STATUS = {"nc": WARN, "lnc": WARN, "unc": WARN,
               "cr": BAD, "lcr": BAD, "ucr": BAD, "nr": BAD, "lnr": BAD, "unr": BAD}
IPMI_BAD_TEXT = re.compile(r"failure detected|ac lost|redundancy lost|predictive failure|"
                           r"uncorrectable|non-recoverable|thermal trip|ierr", re.I)
IPMI_WARN_TEXT = re.compile(r"non-redundant|degraded|correctable", re.I)
SEL_BAD = re.compile(r"memory|ecc|uncorrect|power supply|\bps\d|psu|ac lost|failure|critical|"
                     r"non-recoverable|processor|ierr|machine check|thermal trip|watchdog|"
                     r"redundancy lost|bus (?:fatal|uncorrectable)|pci", re.I)
SEL_SKIP = re.compile(r"log area reset|cleared", re.I)


# ---------------------------------------------------------------- helpers

def run(cmd, timeout=30):
    """Run a command with C locale. Returns (rc, stdout, stderr); rc 127 = not installed."""
    env = dict(os.environ, LC_ALL="C", LANG="C")
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
        return r.returncode, r.stdout, r.stderr
    except FileNotFoundError:
        return 127, "", f"{cmd[0]} not found"
    except subprocess.TimeoutExpired:
        return 124, "", f"{cmd[0]} timed out after {timeout}s"


def readf(path):
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return None


def readint(path):
    v = readf(path)
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def grade(value, limits):
    return BAD if value >= limits[1] else WARN if value >= limits[0] else OK


def short(text, n=HINTLEN):
    text = str(text).strip()
    return text if len(text) <= n else text[:n - 1] + "~"


def dev_name(name):
    """Map a vdev / by-id name to the kernel disk name (nvme0n1, sda ...)."""
    for cand in (f"/dev/disk/by-id/{name}", f"/dev/{name}", name):
        if os.path.exists(cand):
            name = os.path.basename(os.path.realpath(cand))
            break
    name = re.sub(r"-part\d+$", "", name)
    m = re.match(r"(nvme\d+n\d+)p\d+$|((?:sd|vd|hd|xvd)[a-z]+)\d+$", name)
    if m:
        return m.group(1) or m.group(2)
    if len(name) > HINTLEN and "_" in name:          # unresolved by-id name: use the serial
        return name.rsplit("_", 1)[-1][-HINTLEN:]
    return name


class Ctx:
    def __init__(self, cfg, state, now, fresh):
        self.cfg, self.state, self.now, self.fresh = cfg, state, now, fresh
        self.issues = []

    def add(self, area, lvl, detail, hint="", hold=None):
        self.issues.append({"area": area, "lvl": lvl, "detail": detail,
                            "hint": short(hint) if hint else "", "hold": hold})

    def delta(self, key, value):
        """Increase of a counter since the last run; 0 on first sight or reset."""
        c = self.state.setdefault("counters", {})
        old = c.get(key)
        c[key] = value
        if old is None or value < old:
            return 0
        return value - old

    def cached(self, name, interval_min, fn):
        """Run fn every interval_min minutes; reuse its findings in between."""
        cache = self.state.setdefault("cache", {})
        entry = cache.get(name)
        if not self.fresh and entry and self.now - entry["ts"] < interval_min * 60 - 5:
            self.issues.extend(entry["issues"])
            return
        start = len(self.issues)
        fn(self)
        cache[name] = {"ts": self.now, "issues": self.issues[start:]}


# ---------------------------------------------------------------- ECC

def check_ecc(ctx):
    mcs = sorted(glob.glob(f"{SYS}/devices/system/edac/mc/mc[0-9]*"))
    if not mcs:
        ctx.add("ECC", WARN, "no EDAC memory controller found (driver not loaded?)", "no EDAC")
        return
    n_dimms = total_ce = 0
    for mc in mcs:
        m = os.path.basename(mc)
        dimms = sorted(glob.glob(f"{mc}/dimm[0-9]*")) or sorted(glob.glob(f"{mc}/rank[0-9]*"))
        dimm_ue = False
        for d in dimms:
            b = os.path.basename(d)
            size = readint(f"{d}/size")
            if size == 0:
                continue                                  # empty slot
            n_dimms += 1
            label = readf(f"{d}/dimm_label") or ""
            hint = label if label and len(label) <= HINTLEN else f"{m}/{b}"
            name = f"{label} ({m}/{b})" if label else f"{m}/{b}"
            ce, ue = readint(f"{d}/dimm_ce_count"), readint(f"{d}/dimm_ue_count")
            if ue:
                dimm_ue = True
                ctx.add("ECC", BAD, f"{name}: {ue} uncorrected errors since boot", hint)
            if ce is not None:
                total_ce += ce
                dc = ctx.delta(f"ecc/{m}/{b}", ce)
                if dc:
                    ctx.add("ECC", grade(dc, ctx.cfg["ecc_ce_delta"]),
                            f"{name}: +{dc} corrected errors ({ce} since boot)", hint,
                            hold=f"ecc/{m}/{b}")
        ue = readint(f"{mc}/ue_count")
        if ue and not dimm_ue:
            ctx.add("ECC", BAD, f"{m}: {ue} uncorrected errors since boot", m)
        for f, lvl in (("ce_noinfo_count", WARN), ("ue_noinfo_count", BAD)):
            v = readint(f"{mc}/{f}")
            if v is not None:
                d = ctx.delta(f"ecc/{m}/{f}", v)
                if d:
                    ctx.add("ECC", lvl, f"{m}: +{d} {f.replace('_count', '')} errors", m,
                            hold=f"ecc/{m}/{f}")
    ctx.add("ECC", OK, f"{len(mcs)} controller(s), {n_dimms} DIMMs, {total_ce} CE since boot")


# ---------------------------------------------------------------- ZFS

def parse_num(s):
    m = re.match(r"^([\d.]+)([KMGTP]?)$", s)
    if not m:
        return None
    mult = {"": 1, "K": 1e3, "M": 1e6, "G": 1e9, "T": 1e12, "P": 1e15}[m.group(2)]
    return int(float(m.group(1)) * mult)


def check_zfs(ctx):
    rc, out, err = run(["zpool", "status", "-p"])
    if rc == 127:
        ctx.add("ZFS", OK, "zpool not installed")
        return
    if "no pools available" in out + err:
        ctx.add("ZFS", OK, "no pools")
        return
    if rc:
        raise RuntimeError(f"zpool status failed: {err.strip()[:100]}")

    pools = re.split(r"\n(?=\s*pool: )", "\n" + out)
    for block in pools:
        pm = re.search(r"^\s*pool: (\S+)", block, re.M)
        if not pm:
            continue
        pool = pm.group(1)
        st = re.search(r"^\s*state: (\S+)", block, re.M)
        state = st.group(1) if st else "?"
        if state != "ONLINE":
            ctx.add("ZFS", BAD, f"pool {pool} is {state}", pool)

        # scrub
        sm = re.search(r"^\s*scan: (.*)$", block, re.M)
        scan = sm.group(1) if sm else "none requested"
        if "in progress" in scan:
            ctx.add("ZFS", OK, f"{pool}: {scan}")
        elif "none requested" in scan:
            ctx.add("ZFS", WARN, f"{pool}: never scrubbed", f"{pool} scrub")
        elif scan.startswith("scrub canceled"):
            ctx.add("ZFS", WARN, f"{pool}: last scrub was canceled", f"{pool} scrub")
        elif scan.startswith("scrub repaired"):
            m = re.match(r"scrub repaired (\S+) in .* with (\d+) errors on (.+)$", scan)
            if m:
                repaired, errs, when = m.group(1), int(m.group(2)), m.group(3).strip()
                try:
                    age = (ctx.now - time.mktime(time.strptime(when, "%a %b %d %H:%M:%S %Y"))) / 86400
                except ValueError:
                    age = None
                if errs:
                    ctx.add("ZFS", BAD, f"{pool}: last scrub found {errs} errors", f"{pool} scrub")
                elif repaired not in ("0", "0B"):
                    ctx.add("ZFS", WARN, f"{pool}: last scrub repaired {repaired}", f"{pool} scrub")
                if age is not None and age > ctx.cfg["zfs_scrub_max_days"]:
                    ctx.add("ZFS", WARN, f"{pool}: last scrub {age:.0f} days ago", f"{pool} scrub")
                else:
                    ctx.add("ZFS", OK, f"{pool}: last scrub {when}, {errs} errors")
        else:
            ctx.add("ZFS", OK, f"{pool}: {scan}")

        # vdev counters
        cfgsec = block.split("config:", 1)[1].split("errors:", 1)[0] if "config:" in block else ""
        for line in cfgsec.splitlines():
            t = line.split()
            if len(t) < 5:
                continue
            nums = [parse_num(x) for x in t[2:5]]
            if None in nums or t[0] == pool or ZFS_GROUP.match(t[0]):
                continue
            vname, vstate = t[0], t[1]
            disk = dev_name(vname)
            hint = disk
            if vstate not in ("ONLINE", "AVAIL", "INUSE"):
                ctx.add("ZFS", BAD, f"{pool}: {vname} is {vstate}", hint)
            r, w, c = nums
            if r or w or c:
                ctx.add("ZFS", WARN, f"{pool}/{disk}: {r} read, {w} write, {c} cksum errors "
                        f"(zpool clear once fixed)", hint)
            d = ctx.delta(f"zfs/{pool}/{vname}", r + w + c)
            if d:
                ctx.add("ZFS", grade(d, ctx.cfg["zfs_err_delta"]),
                        f"{pool}/{disk}: +{d} new read/write/cksum errors", hint,
                        hold=f"zfs/{pool}/{vname}")

        em = re.search(r"^errors: (.*)$", block, re.M)
        if em and "No known data errors" not in em.group(1):
            ctx.add("ZFS", BAD, f"{pool}: {em.group(1)}", f"{pool} data")

    check_zfs_events(ctx)


def check_zfs_events(ctx):
    rc, out, err = run(["zpool", "events", "-H"])
    if rc:
        ctx.add("ZFS", WARN, f"zpool events failed: {err.strip()[:80]}", "events")
        return
    last = ctx.state.get("zfs_event_ts")
    newest, counts = last or 0, {}
    for line in out.splitlines():
        m = re.match(r"^(\w{3}\s+\d+\s+\d{4}\s+\d\d:\d\d:\d\d)\.(\d+)\s+(\S+)", line)
        if not m:
            continue
        try:
            ts = time.mktime(time.strptime(m.group(1), "%b %d %Y %H:%M:%S")) + float("0." + m.group(2))
        except ValueError:
            continue
        newest = max(newest, ts)
        if last is not None and ts > last and m.group(3) in ZFS_EVENTS:
            counts[m.group(3)] = counts.get(m.group(3), 0) + 1
    ctx.state["zfs_event_ts"] = newest if newest else ctx.now
    for cls, n in counts.items():
        lvl, label = ZFS_EVENTS[cls]
        ctx.add("ZFS", lvl, f"{n}x {cls} since last run", label, hold=f"zfsev/{cls}")


# ---------------------------------------------------------------- Disks

def check_disks(ctx):
    rc, out, err = run(["smartctl", "--scan-open", "-j"])
    if rc == 127:
        ctx.add("Disks", WARN, "smartctl not installed", "smartctl")
        return
    try:
        devices = json.loads(out).get("devices", [])
    except ValueError:
        raise RuntimeError(f"smartctl --scan-open failed: {err.strip()[:100]}")
    if not devices:
        ctx.add("Disks", OK, "no SMART devices found")
    for dv in devices:
        path, dtype = dv.get("name", ""), dv.get("type", "auto")
        hint = dtype if path.startswith("/dev/bus") else os.path.basename(path)
        rc, out, err = run(["smartctl", "-a", "-j", "-n", "standby", "-d", dtype, path], timeout=20)
        try:
            d = json.loads(out)
        except ValueError:
            ctx.add("Disks", WARN, f"{path}: smartctl output unreadable (rc {rc})", hint)
            continue
        if d.get("power_mode", "") in ("STANDBY", "SLEEP") or "standby" in json.dumps(
                d.get("smartctl", {}).get("messages", [])).lower():
            ctx.add("Disks", OK, f"{path}: in standby, skipped")
            continue
        if rc & 0b11:
            ctx.add("Disks", WARN, f"{path}: smartctl could not read device (rc {rc})", hint)
            continue
        check_smart(ctx, path, hint, d)


def check_smart(ctx, path, hint, d):
    cfg, key = ctx.cfg, f"smart/{path}"
    model = d.get("model_name", "")
    if d.get("smart_status", {}).get("passed") is False:
        ctx.add("Disks", BAD, f"{path}: SMART overall health FAILED", hint)
    temp = d.get("temperature", {}).get("current")
    info = [model] + ([f"{temp}C"] if temp is not None else [])

    nv = d.get("nvme_smart_health_information_log")
    if nv:
        cw = nv.get("critical_warning", 0)
        if cw:
            ctx.add("Disks", BAD, f"{path}: NVMe critical warning 0x{cw:02x}", hint)
        spare, thr = nv.get("available_spare"), nv.get("available_spare_threshold")
        if spare is not None:
            if thr is not None and spare <= thr:
                ctx.add("Disks", BAD, f"{path}: available spare {spare}% <= threshold {thr}%", hint)
            drop = ctx.delta(f"{key}/spare_used", 100 - spare)
            if drop:
                ctx.add("Disks", WARN, f"{path}: available spare dropped by {drop}% to {spare}%",
                        hint, hold=f"{key}/spare")
        pu = nv.get("percentage_used")
        if pu is not None:
            lvl = grade(pu, cfg["nvme_wear_pct"])
            if lvl:
                ctx.add("Disks", lvl, f"{path}: wear {pu}% used", hint)
            info.append(f"{pu}% worn")
        for field, lvl, label in (("media_errors", BAD, "media/data integrity errors"),
                                  ("warning_temp_time", WARN, "minutes above warning temp"),
                                  ("critical_comp_time", BAD, "minutes above critical temp")):
            v = nv.get(field)
            if v is not None:
                dv = ctx.delta(f"{key}/{field}", v)
                if dv:
                    ctx.add("Disks", lvl, f"{path}: +{dv} {label} ({v} total)", hint,
                            hold=f"{key}/{field}")

    attrs = {a.get("id"): a.get("raw", {}).get("value")
             for a in d.get("ata_smart_attributes", {}).get("table", [])}
    if attrs:
        for aid, label in ((197, "pending sectors"), (198, "offline uncorrectable sectors")):
            v = attrs.get(aid)
            if v:
                ctx.add("Disks", WARN, f"{path}: {v} {label}", hint)
        for aid, label, limits in ((5, "reallocated sectors", [1, 10]),
                                   (187, "reported uncorrectable errors", [1, 10]),
                                   (197, "pending sectors", [1, 10]),
                                   (198, "offline uncorrectable sectors", [1, 10]),
                                   (199, "UDMA CRC errors (cable/backplane)", cfg["ata_crc_delta"])):
            v = attrs.get(aid)
            if v is not None:
                dv = ctx.delta(f"{key}/{aid}", v)
                if dv:
                    ctx.add("Disks", grade(dv, limits), f"{path}: +{dv} {label} ({v} total)", hint,
                            hold=f"{key}/{aid}")

    gd = d.get("scsi_grown_defect_list")
    if gd is not None:
        dv = ctx.delta(f"{key}/grown_defects", gd)
        if dv:
            ctx.add("Disks", WARN, f"{path}: +{dv} grown defects ({gd} total)", hint,
                    hold=f"{key}/grown_defects")

    tests = d.get("ata_smart_self_test_log", {}).get("standard", {}).get("table", [])
    if tests and tests[0].get("status", {}).get("passed") is False:
        ctx.add("Disks", WARN, f"{path}: last self-test failed "
                f"({tests[0].get('status', {}).get('string', '')})", hint)

    ctx.add("Disks", OK, f"{path}: " + ", ".join(x for x in info if x))


# ---------------------------------------------------------------- HW

def pci_label(dev):
    for sub in ("net", "nvme"):
        try:
            names = os.listdir(f"{dev}/{sub}")
            if names:
                return names[0]
        except OSError:
            pass
    addr = os.path.basename(dev)
    return addr[5:] if addr.startswith("0000:") else addr


def check_pcie(ctx):
    base = ctx.state.setdefault("pcie_base", {})
    n_aer = 0
    for dev in sorted(glob.glob(f"{SYS}/bus/pci/devices/*")):
        addr, label = os.path.basename(dev), None
        for fname, total, lvl in (("aer_dev_correctable", "TOTAL_ERR_COR", None),
                                  ("aer_dev_nonfatal", "TOTAL_ERR_NONFATAL", BAD),
                                  ("aer_dev_fatal", "TOTAL_ERR_FATAL", BAD)):
            txt = readf(f"{dev}/{fname}")
            m = re.search(rf"^{total}\s+(\d+)", txt or "", re.M)
            if not m:
                continue
            n_aer += 1
            d = ctx.delta(f"aer/{addr}/{fname}", int(m.group(1)))
            if d:
                label = label or pci_label(dev)
                ctx.add("HW", lvl if lvl is not None else grade(d, ctx.cfg["aer_cor_delta"]),
                        f"{label} ({addr}): +{d} {fname.replace('aer_dev_', '')} PCIe errors",
                        label, hold=f"aer/{addr}/{fname}")

        # link downtraining vs. baseline (NVMe, NICs, storage controllers)
        cls = readf(f"{dev}/class") or ""
        if not cls.startswith(("0x0108", "0x0200", "0x0104", "0x0107", "0x0106")):
            continue
        sp = re.match(r"[\d.]+", readf(f"{dev}/current_link_speed") or "")
        wd = readint(f"{dev}/current_link_width")
        if not sp or not wd:
            continue
        cur = [float(sp.group(0)), wd]
        if addr not in base:
            base[addr] = cur
            continue
        if cur[0] < base[addr][0] or cur[1] < base[addr][1]:
            label = label or pci_label(dev)
            ctx.add("HW", WARN, f"{label} ({addr}): PCIe link {cur[0]:g}GT/s x{cur[1]}, "
                    f"was {base[addr][0]:g}GT/s x{base[addr][1]}", label)
    ctx.add("HW", OK, f"PCIe: {n_aer} AER counters, {len(base)} links watched")


def check_throttle(ctx):
    total = 0
    found = False
    for f in glob.glob(f"{SYS}/devices/system/cpu/cpu[0-9]*/thermal_throttle/*_throttle_count"):
        v = readint(f)
        if v is not None:
            total += v
            found = True
    if not found:
        return
    d = ctx.delta("cpu/throttle", total)
    lvl = grade(d, ctx.cfg["throttle_delta"]) if d else OK
    if lvl:
        ctx.add("HW", lvl, f"CPU: +{d} thermal throttle events", "CPU throttle", hold="cpu/throttle")


def check_ipmi(ctx):
    rc, out, err = run(["ipmitool", "sdr", "elist"], timeout=45)
    if rc:
        ctx.add("HW", WARN, f"ipmitool sdr failed (rc {rc}): {err.strip()[:80]}", "BMC")
        return
    n = 0
    for line in out.splitlines():
        cols = [c.strip() for c in line.split("|")]
        if len(cols) < 5:
            continue
        name, status, reading = cols[0], cols[2].lower(), cols[4]
        n += 1
        lvl = IPMI_STATUS.get(status, OK)
        if lvl == OK and status in ("ok", ""):
            if IPMI_BAD_TEXT.search(reading):
                lvl = BAD
            elif IPMI_WARN_TEXT.search(reading):
                lvl = WARN
        if lvl:
            ctx.add("HW", lvl, f"IPMI sensor {name}: {reading} [{status}]", name)

    rc, out, err = run(["ipmitool", "sel", "info"], timeout=30)
    if rc == 0:
        m = re.search(r"Percent Used\s*:\s*(\d+)%", out)
        if m and int(m.group(1)) >= ctx.cfg["sel_full_pct"]:
            ctx.add("HW", WARN, f"SEL {m.group(1)}% full, new events may be lost", "SEL full")
        if re.search(r"Overflow\s*:\s*true", out):
            ctx.add("HW", WARN, "SEL overflow, events were lost", "SEL full")

    rc, out, err = run(["ipmitool", "sel", "elist", "last", "100"], timeout=45)
    if rc:
        ctx.add("HW", WARN, f"ipmitool sel failed (rc {rc}): {err.strip()[:80]}", "BMC")
        return
    entries = []
    for line in out.splitlines():
        cols = [c.strip() for c in line.split("|")]
        try:
            entries.append((int(cols[0], 16), cols))
        except (ValueError, IndexError):
            continue
    last = ctx.state.get("sel_last_id")
    top = max((e[0] for e in entries), default=0)
    if last is not None and top < last:
        ctx.add("HW", OK, "SEL was cleared, new baseline")
        last = None
    if last is not None:
        for eid, cols in entries:
            if eid <= last:
                continue
            text = " | ".join(cols[3:])
            if SEL_SKIP.search(text) or (cols[-1].lower() == "deasserted"):
                continue
            sensor = re.sub(r"\s*#0x[0-9a-fA-F]+", "", cols[3]) if len(cols) > 3 else "SEL"
            lvl = BAD if SEL_BAD.search(text) else WARN
            ctx.add("HW", lvl, f"SEL #{eid}: {text}", sensor, hold=f"sel/{sensor}")
    ctx.state["sel_last_id"] = top if entries else (last or 0)
    ctx.add("HW", OK, f"IPMI: {n} sensors read, SEL last id {top}")


# ---------------------------------------------------------------- System

def check_klog(ctx):
    since = ctx.state.get("klog_ts") or ctx.now - 120
    since = max(since, ctx.now - 3600)
    rc, out, err = run(["journalctl", "-k", "-q", "--no-pager", "-o", "short-unix",
                        "--since", f"@{int(since)}"])
    if rc:
        raise RuntimeError(f"journalctl failed: {err.strip()[:100]}")
    newest, hits = since, {}
    ign = re.compile(ctx.cfg["klog_ignore_ifaces"]) if ctx.cfg["klog_ignore_ifaces"] else None
    for line in out.splitlines():
        m = re.match(r"^(\d+\.\d+)\s+\S+\s+kernel: (.*)$", line)
        if not m:
            continue
        ts, msg = float(m.group(1)), m.group(2)
        if ts <= since:
            continue
        newest = max(newest, ts)
        for rx, area, lvl, label, h in KLOG_RULES:
            rm = re.search(rx, msg)
            if not rm:
                continue
            hint = rm.group(h) if isinstance(h, int) else h
            if label == "NIC link down" and ign and ign.search(hint):
                break
            k = (area, label, hint)
            if k not in hits:
                hits[k] = [lvl, 0, msg]
            hits[k][1] += 1
            break
    ctx.state["klog_ts"] = newest
    for (area, label, hint), (lvl, n, msg) in hits.items():
        ctx.add(area, lvl, f"{n}x {label}: {msg[:90]}", hint, hold=f"klog/{area}/{label}/{hint}")


def check_time(ctx):
    rc, out, err = run(["chronyc", "-c", "tracking"])
    if rc == 0 and out.count(",") >= 13:
        f = out.strip().split(",")
        offset_ms, leap = abs(float(f[4])) * 1000, f[13]
        if leap.lower().startswith("not"):
            ctx.add("System", BAD, f"chrony: {leap}", "time")
            return
        lvl = grade(offset_ms, ctx.cfg["time_offset_ms"])
        ctx.add("System", lvl, f"time offset {offset_ms:.3f}ms (stratum {f[2]}, {f[1]})",
                "time" if lvl else "")
        return
    rc, out, _ = run(["timedatectl", "show", "-p", "NTPSynchronized", "--value"])
    if out.strip() == "no":
        ctx.add("System", BAD, "time not synchronised (timedatectl)", "time")
    elif out.strip() == "yes":
        ctx.add("System", OK, "time synchronised (no chrony, offset unknown)")
    else:
        ctx.add("System", WARN, "time sync state unknown", "time")


# ---------------------------------------------------------------- core

CHECKS = [("ECC", check_ecc, None), ("ZFS", check_zfs, None),
          ("Disks", check_disks, "smart_interval_min"),
          ("HW", check_pcie, None), ("HW", check_throttle, None),
          ("HW", check_ipmi, "ipmi_interval_min"),
          ("System", check_klog, None), ("System", check_time, None)]


def run_checks(cfg, state, now, fresh=False):
    """Run all enabled checks; returns the final issue list (including held ones)."""
    ctx = Ctx(cfg, state, now, fresh)
    areas = [a for a in ALL_AREAS if cfg["areas"].get(a, True)]
    for area, fn, interval in CHECKS:
        if area not in areas and fn is not check_klog:
            continue
        try:
            if interval:
                ctx.cached(fn.__name__, cfg[interval], fn)
            else:
                fn(ctx)
        except Exception as e:
            ctx.add(area, WARN, f"{fn.__name__} failed: {e}"[:150], "check failed")

    ign = [re.compile(x) for x in cfg["ignore"]]
    issues = [i for i in ctx.issues if i["area"] in areas and
              not any(r.search(f"{i['area']}: {i['hint']}: {i['detail']}") for r in ign)]

    # holds: keep counter/log findings visible for hold_minutes
    holds = state.setdefault("holds", {})
    for i in issues:
        if i["hold"] and i["lvl"]:
            h = holds.get(i["hold"])
            lvl = max(i["lvl"], h["lvl"]) if h and h["until"] > now else i["lvl"]
            holds[i["hold"]] = dict(i, lvl=lvl, seen=now, until=now + cfg["hold_minutes"] * 60)
    current = {i["hold"]: i["lvl"] for i in issues if i["hold"]}
    for k, h in list(holds.items()):
        if h["until"] <= now:
            del holds[k]
        elif h["area"] in areas and h["lvl"] > current.get(k, -1) and h["seen"] < now:
            issues.append(dict(h, detail=h["detail"] + time.strftime(
                " [seen %H:%M]", time.localtime(h["seen"]))))

    # dedupe (cached findings may repeat)
    seen, out = set(), []
    for i in issues:
        k = (i["area"], i["lvl"], i["hint"], i["detail"])
        if k not in seen:
            seen.add(k)
            out.append(i)
    return out, areas


def build_msg(issues, areas):
    """One verdict per area, with short hints when space allows."""
    level = {a: OK for a in areas}
    hints = {a: [] for a in areas}
    for i in issues:
        a = i["area"]
        level[a] = max(level[a], i["lvl"])
    for i in issues:
        a = i["area"]
        if i["lvl"] == level[a] and i["lvl"] and i["hint"] and i["hint"] not in hints[a]:
            hints[a].append(i["hint"])

    def render(hinted, only_problems, first_only=False):
        parts, good = [], 0
        for a in areas:
            if only_problems and not level[a]:
                good += 1
                continue
            h = (hints[a][:1] if first_only else hints[a]) if a in hinted else []
            parts.append(f"{a}: {WORD[level[a]]}" + (f" ({','.join(h)})" if h else ""))
        if good:
            parts.append(f"rest: {WORD[OK]}")
        return ", ".join(parts)

    # hint priority: Bad areas first, then in area order
    problem = sorted([a for a in areas if level[a]], key=lambda a: (-level[a], areas.index(a)))
    attempts = [(problem, False), (problem, True)]
    attempts += [(problem[:n], True, True) for n in range(len(problem), 0, -1)]
    attempts += [([], True), ([], False)]
    for att in attempts:
        msg = render(*att)
        if len(msg) <= MAXLEN:
            return msg
    return msg[:MAXLEN]


def kuma_rtt(url):
    """RTT from this node to the Kuma host: ICMP first, HTTP round trip as fallback."""
    u = urllib.parse.urlparse(url)
    rc, out, _ = run(["ping", "-q", "-n", "-c", "3", "-i", "0.2", "-W", "1", u.hostname], timeout=10)
    m = re.search(r"= [\d.]+/([\d.]+)/", out)
    if m:
        return float(m.group(1))
    t = time.monotonic()
    try:
        urllib.request.urlopen(f"{u.scheme}://{u.netloc}/", timeout=5).read(0)
    except urllib.error.HTTPError:
        pass
    except Exception:
        return None
    return (time.monotonic() - t) * 1000


def load_conf():
    cfg = copy.deepcopy(DEFAULTS)
    with open(CONF) as f:
        user = json.load(f)
    cfg["areas"].update(user.pop("areas", {}))
    cfg.update(user)
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
    tmp = STATE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, STATE)


def main():
    dry = "--dry-run" in sys.argv
    cfg = load_conf()
    lock = open(LOCK, "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        sys.exit("previous run still active")

    state = load_state()
    if dry:
        state = copy.deepcopy(state)              # never persist a dry run
    issues, areas = run_checks(cfg, state, time.time(), fresh=dry)
    if not dry:
        save_state(state)

    level = max((i["lvl"] for i in issues), default=OK)
    msg = build_msg(issues, areas)
    down = level == BAD or (level == WARN and cfg["warn_is_down"])
    params = {"status": "down" if down else "up", "msg": msg}
    rtt = kuma_rtt(cfg["push_url"])
    if rtt is not None:
        params["ping"] = round(rtt, 2)
    url = cfg["push_url"].split("?")[0] + "?" + urllib.parse.urlencode(params)

    if dry:
        print(msg)
        print(f"  -> status {params['status']}, ping {params.get('ping')} ms, {len(msg)} chars")
        for i in sorted(issues, key=lambda x: (areas.index(x["area"]), -x["lvl"])):
            print(f"  {WORD[i['lvl']]:<4}  {i['area']:<6}  {i['detail']}")
        return
    try:
        urllib.request.urlopen(url, timeout=10).read()
    except Exception as e:
        sys.exit(f"push failed: {e}")


if __name__ == "__main__":
    main()
