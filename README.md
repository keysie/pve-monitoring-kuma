# pve-monitoring-kuma

Low-level health monitors for Proxmox VE nodes that report to
[Uptime Kuma](https://github.com/louislam/uptime-kuma) push monitors.

## Idea

The Proxmox UI and API already cover the high-level view: services, Ceph
health, pool usage, SMART "PASSED". These monitors look for the subtle
problems that stay invisible there until something breaks, for example a
mesh link that only collapses under load, a slowly rising ECC error rate,
ZFS checksum errors on a still ONLINE pool, or an SSD whose real wear is far
higher than the UI shows.

Each node runs the scripts from cron once a minute. They push one line per
node to Kuma, so no inbound ports or credentials are needed on the Kuma side:

```
Links: Good, NICs: Good, OSD-Latency: Good, Ceph: Good
ECC: Good, ZFS: Warn (rpool scrub), rest: Good
```

`Bad` sets the Kuma monitor down (and alerts). `Warn` stays up but is visible
in the message. The ping value is the RTT from the node to the Kuma host.

| Script | Areas |
|---|---|
| `kuma-ceph-push.py` | **Links**: mesh ping loss/RTT, TCP retransmit rate of the live Ceph traffic per peer. **NICs**: link state, speed, CRC/rx/tx errors. **OSD-Latency**: local OSDs, absolute and relative to the cluster median. **Ceph**: slow ops, laggy/stale/down PGs. |
| `kuma-node-push.py` | **ECC**: corrected/uncorrected errors per DIMM. **ZFS**: vdev error counters, scrub age, slow I/O events. **Disks**: SMART/NVMe deltas, real wear and wear projection, self-tests, write volume of the OS pool. **HW**: PCIe AER and link downtraining, CPU throttling, IPMI sensors and SEL events. **System**: kernel log (hung tasks, lockups, OOM, link drops), clock offset, expected settings. |

The node monitor holds findings from counters or logs for 60 minutes, so a
single event stays visible long enough to be noticed.

## Install

On each node, as root:

```bash
git clone https://github.com/keysie/pve-monitoring-kuma.git
cd pve-monitoring-kuma
./deploy.sh                    # dry run: shows exactly what would be done, changes nothing
./deploy.sh --apply --keyring  # do it; --keyring creates the read-only Ceph user client.kuma (once per cluster)
```

`deploy.sh` only shows its plan unless you add `--apply`. With `--apply` it
copies the scripts to `/usr/local/bin`, the example configs to
`/etc/kuma-*-push.json` (only if none exist) and enables cron once the
config holds a real push URL. It is plain bash and short; read it before
running it.

Then, per node:

1. In Kuma, create one **Push** monitor per script and node, heartbeat
   interval 60 s, 1–2 retries.
2. Edit `/etc/kuma-ceph-push.json` and `/etc/kuma-node-push.json`: set
   `push_url` (with that node's token) and the node-specific values
   (`mesh_peers`, `mesh_ifaces`, `node_names` for Ceph).
3. Run `./deploy.sh --apply` again. It now enables cron and does a test run.

To update later: `git pull && ./deploy.sh && ./deploy.sh --apply`. Configs and state are kept.
For an existing config, `deploy.sh` reports new options (their defaults
apply until you add them), keys the script no longer knows, and values of
the wrong type. It never changes the config; edit it by hand.

## Usage

```bash
kuma-node-push.py --dry-run    # all values and reasons, one line per check
kuma-ceph-push.py --dry-run
env -i /usr/local/bin/kuma-node-push.py --dry-run   # exactly like cron
```

A dry run never pushes and never changes the saved state. The node script
also never clears the SEL in a dry run.

## Configuration

All options with defaults and comments are at the top of each script
(`DEFAULTS`). Thresholds are `[Warn, Bad]` pairs. Useful options:

- `ignore` (node): regexes for known, accepted findings, matched against
  `"Area: hint: detail"`.
- `areas` (node): disable whole areas, e.g. `{"ZFS": false}`.
- `expect` (node): settings that must stay in place, e.g. a sysfs value, a
  line in a file or a ZFS property.
- `warn_is_down` / `attn_is_down`: let Warn also set the monitor down.

## SMART self-tests

Proxmox does not schedule self-tests by default. Add `-s` to the
`DEVICESCAN` line in `/etc/smartd.conf`, with a different weekday per node:

```
DEVICESCAN -d removable -n standby -m root -M exec /usr/share/smartmontools/smartd-runner -s (S/../.././02|L/../../6/03)
```

`S/../.././02` is a short test daily at 02:00, `L/../../6/03` a long test on
Saturdays (1 = Monday … 7 = Sunday) at 03:00. Then
`systemctl restart smartmontools`.

## Files

| Path | Content |
|---|---|
| `/usr/local/bin/kuma-*-push.py` | the scripts |
| `/etc/kuma-*-push.json` | configs (contain the push token, mode 600) |
| `/etc/cron.d/kuma-*-push` | cron entries |
| `/var/lib/kuma-*-push/` | state (counters, baselines); `sel-archive/` holds saved SELs |

## Troubleshooting

- **Kuma still shows an old message.** The event list only records up/down
  changes; hover the latest beat in the heartbeat bar for the current one.
- **After replacing hardware**, delete `/var/lib/kuma-node-push/state.json`
  to reset the PCIe link baselines. Replaced drives are recognised by serial
  number automatically.

## Uninstall

```bash
rm /etc/cron.d/kuma-ceph-push /etc/cron.d/kuma-node-push
rm /usr/local/bin/kuma-ceph-push.py /usr/local/bin/kuma-node-push.py
rm -f /etc/kuma-*-push.json*
rm -rf /var/lib/kuma-ceph-push /var/lib/kuma-node-push
ceph auth del client.kuma && rm /etc/pve/priv/ceph.client.kuma.keyring
```

## License

Apache 2.0, see [LICENSE](LICENSE).
