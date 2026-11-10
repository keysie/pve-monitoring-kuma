#!/usr/bin/env bash
# deploy.sh - install the Uptime Kuma push monitors on a Proxmox VE host.
#
# What it does, and nothing else:
#   1. copies kuma-<name>-push.py to /usr/local/bin/              (mode 700)
#   2. copies the example config to /etc/kuma-<name>-push.json    (mode 600)
#      only if there is no config yet. An existing config is never touched;
#      the current example is put next to it as *.json.example instead.
#   3. writes /etc/cron.d/kuma-<name>-push (runs every minute), but only once
#      the config holds a real push URL (no KUMA-IP / TOKEN placeholder).
#   4. with --keyring only: creates the read-only Ceph user client.kuma
#      (mon 'allow r', mgr 'allow r') in /etc/pve/priv/ if it does not exist.
#
# Usage:  ./deploy.sh [--ceph] [--node] [--keyring] [--dry-run]
#   --ceph / --node   install only that monitor (default: both)
#   --keyring         also create the Ceph keyring for client.kuma
#   --dry-run         only print what would be done
#
# Re-run it after editing the config (to enable cron) or after a git pull
# (to update the scripts). Your configs and state are kept.
set -euo pipefail

SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${ROOT:-}"                # install prefix, for testing only (ROOT=/tmp/x ./deploy.sh)
KEYRING_FILE="$ROOT/etc/pve/priv/ceph.client.kuma.keyring"
DRY=0
KEYRING=0
COMPONENTS=()

for arg in "$@"; do
    case "$arg" in
        --ceph)    COMPONENTS+=(ceph) ;;
        --node)    COMPONENTS+=(node) ;;
        --keyring) KEYRING=1 ;;
        --dry-run) DRY=1 ;;
        -h|--help) awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "$0"; exit 0 ;;
        *)         echo "unknown option: $arg (see --help)" >&2; exit 2 ;;
    esac
done
[ ${#COMPONENTS[@]} -eq 0 ] && COMPONENTS=(ceph node)

# ------------------------------------------------------------------ helpers

say()  { echo "==> $*"; }
info() { echo "    $*"; }
warn() { echo "!!  $*"; }

# run <command...>: print the command (shell-quoted, copy-pasteable), execute it unless --dry-run
run() {
    info "$(printf '%q ' "$@")"
    [ "$DRY" -eq 1 ] || "$@"
}

# write_file <path> <line>: print, write unless --dry-run
write_file() {
    info "write $1: $2"
    [ "$DRY" -eq 1 ] || printf '%s\n' "$2" > "$1"
}

# config_ready <config>: true if the push URL is set and not a placeholder
config_ready() {
    [ -f "$1" ] &&
    grep -Eq '"push_url": *"https?://' "$1" &&
    ! grep -Eq '"push_url": *"[^"]*(KUMA-IP|/push/TOKEN)' "$1"
}

need_tools() {
    local t
    for t in "$@"; do
        command -v "$t" >/dev/null 2>&1 || PATH="$PATH:/usr/sbin:/sbin" command -v "$t" >/dev/null 2>&1 ||
            warn "'$t' not found - the related checks will report Warn"
    done
}

# ------------------------------------------------------------------ preflight

if [ "$DRY" -eq 0 ] && [ -z "$ROOT" ] && [ "$(id -u)" -ne 0 ]; then
    echo "please run as root (or with --dry-run)" >&2
    exit 1
fi
command -v python3 >/dev/null 2>&1 || { echo "python3 is required" >&2; exit 1; }
[ "$DRY" -eq 1 ] && say "dry run - nothing is changed"

# ------------------------------------------------------------------ per component

install_component() {
    local comp="$1"
    local name="kuma-$comp-push"
    local bin="$ROOT/usr/local/bin/$name.py"
    local conf="$ROOT/etc/$name.json"
    local cron="$ROOT/etc/cron.d/$name"

    say "$name"

    # component specific checks
    if [ "$comp" = ceph ]; then
        if ! command -v ceph >/dev/null 2>&1; then
            warn "ceph CLI not found - skipping (install this monitor on Ceph nodes only)"
            return
        fi
        need_tools ss ip ping
        if [ ! -e "$KEYRING_FILE" ]; then
            if [ "$KEYRING" -eq 1 ]; then
                run ceph auth get-or-create client.kuma mon 'allow r' mgr 'allow r' -o "$KEYRING_FILE"
            else
                warn "no keyring for client.kuma - run again with --keyring, or create it by hand:"
                info "ceph auth get-or-create client.kuma mon 'allow r' mgr 'allow r' -o $KEYRING_FILE"
            fi
        fi
    else
        need_tools smartctl zpool ipmitool chronyc journalctl
        if ! grep -Eqs '^[^#]*[[:space:]]-s[[:space:]]' /etc/smartd.conf; then
            warn "smartd schedules no self-tests - see README, section 'SMART self-tests'"
        fi
    fi

    # 1. script
    run install -D -m 700 "$SRC/$name.py" "$bin"

    # 2. config: never overwrite
    if [ -e "$conf" ]; then
        info "config $conf exists - left unchanged"
        run install -m 600 "$SRC/$name.json" "$conf.example"
    else
        run install -D -m 600 "$SRC/$name.json" "$conf"
    fi

    # 3. cron: only with a real push URL
    if config_ready "$conf"; then
        run mkdir -p "$(dirname "$cron")"
        write_file "$cron" "* * * * * root /usr/local/bin/$name.py"
        if [ "$DRY" -eq 0 ] && [ -z "$ROOT" ]; then
            info "test run:"
            "$bin" --dry-run | sed 's/^/      /' || warn "test run failed, see output above"
        fi
    else
        warn "cron NOT enabled: edit /etc/$name.json (push_url etc.), then run ./deploy.sh again"
    fi
}

for comp in "${COMPONENTS[@]}"; do
    install_component "$comp"
done

say "done"
