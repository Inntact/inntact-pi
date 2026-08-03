#!/usr/bin/env python3
"""
Inntact Property Monitor
========================
Runs on each Raspberry Pi in a holiday let property.

What this script does:
- Connects to Zigbee2MQTT via MQTT and reads all paired sensors
- Monitors temperature, humidity, water leaks, and smart plug state
- Sends a heartbeat to InfluxDB every 60 seconds
- Runs LAYERED connectivity checks (link / gateway / WAN / DNS / AP clients)
- Owns 4G failover AND failback directly (no external failover.sh)
- Buffers data locally in SQLite when the VPS is unreachable
- Sends email alerts based on GUEST IMPACT, not raw network state
- All credentials are loaded from .env; all tunables from monitor_config.yaml

--------------------------------------------------------------------------
v3 rewrite (network reliability)
--------------------------------------------------------------------------
This version replaces the split-brain design (monitor.py observing a flag
written by /opt/inntact/failover.sh) with a single state machine that owns
detection, the route/NAT swap, and the event log. `inntact-failover.service`
must be disabled on deploy.

FIXES:
  BUG 1  FAILBACK. The old failover.sh deleted eth0's default route on
         failover, so every eth0-bound probe (`ping -I eth0`) had no route to
         its target and failed forever -> failback never triggered (stuck on 4G
         for 16-36h). Fix: POLICY ROUTING — keep a primary-WAN default in a
         dedicated table (default 100) with `ip rule oif <primary> table 100`,
         so an interface-bound probe always reaches the primary WAN even while
         4G holds the main default, WITHOUT hijacking forwarded guest traffic.
         (A /32 in the MAIN table — the previous approach — blackholes a guest
         whose DNS is 1.1.1.1/8.8.8.8 during the very outage failover exists to
         survive. Proven in netns.) Fail over fast (~90s), fail back slow.

  BUG 2  ONE SIGNAL. Connectivity was a single ping to a single target. Now
         five independent signals are collected and logged separately: link
         (eth0 carrier), gateway reachable, WAN reachable (2-3 targets), DNS
         resolving, and AP client association on wlan0.

  BUG 3  EVENT LOG. Events are paired open/close sharing an event_id, stamped
         at EVENT time (not influx-write time), so restores can never precede
         the outage they resolve and every transition is recorded.

  BUG 4  ALERTS ON GUEST IMPACT. A successful failover with guests online is
         log-only. On 4G > 2h is informational. Only a FAILED failover with
         guests actually offline is a real alarm.

  BUG 5  SENSOR WATCHDOG. Driven by Z2M bridge/devices + availability, covering
         EXPECTED inventory (expected_counts), so a never-paired leak sensor is
         flagged as missing instead of being silently indistinguishable from a
         dry house.

  BUG 6  AUTO-RESTART RACE. The router smart-plug power-cycle is hard-gated: it
         only fires for a genuinely wedged router (primary gateway unreachable
         on the LAN), never for a WAN/ISP outage that 4G already covers, never
         within the failback window, with a cooldown. It can no longer reboot a
         healthy or just-restored line.

Preserved sensor fixes from v2:
  - Humidity scaling correction for Tuya TS0201 (x/10 readings).
  - water_leak written as string "true"/"false" for the Grafana dashboard.
  - Paho MQTT v2 reason_code handled as a ReasonCode object.
"""

import os
import ssl
import sys
import json
import time
import uuid
import socket
import logging
import sqlite3
import smtplib
import threading
import subprocess
import config_sync
from datetime import datetime, timezone
from email.mime.text import MIMEText
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv

import yaml
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.exceptions import InfluxDBError

# =============================================================
# SECRETS — loaded from .env file
# =============================================================

load_dotenv('/opt/inntact/.env')

PROPERTY_SLUG    = os.getenv('PROPERTY_SLUG')
AGENCY_ID        = os.getenv('AGENCY_ID')
INFLUX_HOST      = os.getenv('INFLUX_HOST')
INFLUX_ORG       = os.getenv('INFLUX_ORG')
INFLUX_BUCKET    = os.getenv('INFLUX_BUCKET')
INFLUX_TOKEN     = os.getenv('INFLUX_TOKEN')
MQTT_HOST        = os.getenv('MQTT_HOST')
MQTT_PORT        = int(os.getenv('MQTT_PORT', 8883))
MQTT_USERNAME    = os.getenv('MQTT_USERNAME')
MQTT_PASSWORD    = os.getenv('MQTT_PASSWORD')
SMTP_HOST        = os.getenv('SMTP_HOST')
SMTP_PORT        = int(os.getenv('SMTP_PORT', 587))
SMTP_USER        = os.getenv('SMTP_USER')
SMTP_PASSWORD    = os.getenv('SMTP_PASSWORD')
ALERT_FROM       = os.getenv('ALERT_FROM')
ALERT_TO         = os.getenv('ALERT_TO')
TEMP_LOW         = float(os.getenv('TEMP_LOW', 10))
TEMP_HIGH        = float(os.getenv('TEMP_HIGH', 30))
CONFIG_API_URL   = os.getenv('CONFIG_API_URL')
CONFIG_API_TOKEN = os.getenv('CONFIG_API_TOKEN')

# Fixed paths
SQLITE_PATH      = '/opt/inntact/buffer.db'
ALERT_STATE_PATH = '/opt/inntact/alert_state.json'
FAILOVER_STATE_FILE = '/tmp/inntact_failover_active'  # kept for external compatibility
CONFIG_PATH      = os.getenv('MONITOR_CONFIG', '/opt/inntact/monitor_config.yaml')

LOG_FILE    = '/var/log/inntact/monitor.log'
LOG_MAX     = 10 * 1024 * 1024   # 10 MB
LOG_BACKUPS = 3

# =============================================================
# LOGGING SETUP
# =============================================================

os.makedirs('/var/log/inntact', exist_ok=True)

logger = logging.getLogger('inntact')
logger.setLevel(logging.INFO)

file_handler = RotatingFileHandler(
    LOG_FILE, maxBytes=LOG_MAX, backupCount=LOG_BACKUPS
)
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)s] %(message)s'
))

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# =============================================================
# CONFIG LOADER — monitor_config.yaml with safe defaults
# =============================================================

# Documented defaults. Every default here is also documented in
# monitor_config.yaml. If a key is missing from the YAML we fall back to these
# and log a warning naming the key — never a silent guess.
_DEFAULTS = {
    'interfaces': {
        'primary':  {'name': 'eth0', 'gateway': None},   # None -> auto-detect (P8)
        'failover': {'name': 'eth1', 'gateway': '192.168.8.1'},
        'ap':       {'name': 'wlan0'},
    },
    'probing': {
        'wan_targets': ['1.1.1.1', '8.8.8.8', '9.9.9.9'],
        'wan_targets_required': 1,
        'ping_count': 2,
        'ping_timeout_seconds': 3,
        'policy_table': 100,
        'policy_rule_priority': 100,
        'dns': {'resolver': '1.1.1.1', 'hostname': 'dashboard.inntact.co.uk',
                'timeout_seconds': 5},
        'signals': {'link': True, 'gateway': True, 'wan': True,
                    'dns': True, 'ap_clients': True},
    },
    'failover': {
        'enabled': True,
        'probe_interval_seconds': 30,
        'fail_threshold': 3,
        'failback': {'probe_interval_seconds': 60, 'clean_probes_required': 10},
        'verify_failover_gateway': True,
        'refuse_if_legacy_service_enabled': True,   # P11
    },
    'router_reboot': {
        'enabled': True,
        'smart_plug_topic': None,
        'require_gateway_down': True,
        'forbid_while_on_4g': False,
        'min_gateway_down_seconds': 900,
        'forbid_within_failback_window': True,
        'off_seconds': 30,
        'post_reboot_wait_seconds': 180,
        'cooldown_seconds': 3600,
    },
    'sensors': {
        'watchdog': {
            'enabled': True,
            'check_interval_seconds': 300,
            'startup_grace_seconds': 900,
            'use_z2m_availability': True,
            'default_stale_seconds': 108000,   # 30h (P6) — sleepy end devices go quiet
            'type_thresholds': {'leak': 108000, 'temperature': 5400,
                                'humidity': 5400, 'smart_plug': 3600},
            'expected_counts': {'leak': 2, 'climate': 2},
            'expected_devices': [],
        },
    },
    'alerts': {
        'email_enabled': True,
        'cooldown_seconds': 14400,   # fallback if a severity isn't listed in 'cooldowns'
        # Re-send interval for an UNRESOLVED alert, chosen by severity. The first
        # email of any incident always goes out immediately; these only throttle
        # the reminders while the condition persists.
        'cooldowns': {
            'CRITICAL': 21600,   # 6h  — leak / guests offline / frost: keep surfacing
            'WARNING':  86400,   # 24h — sensor maintenance: at most once a day
            'INFO':     604800,  # 7d  — e.g. "on 4G, guests fine": effectively once
        },
        'info_after_seconds': 7200,
        'guests_online': {'source': 'ap_clients', 'min_stations': 1},
    },
    'influx': {'events_measurement': 'events', 'write_event_timestamps': True},
    'heartbeat': {'interval_seconds': 60},
    'speedtest': {'interval_seconds': 1800, 'first_run_delay_seconds': 30,
                  'skip_on_4g': True, 'interval_on_4g_seconds': 21600},
}


class Config:
    """Nested config with defaults and a warn-on-missing accessor."""

    def __init__(self, data, defaults):
        self._data = data or {}
        self._defaults = defaults

    def get(self, path, default=None):
        """Fetch a dotted path, e.g. 'failover.failback.clean_probes_required'.

        Resolution order: the user's YAML, then the built-in DEFAULTS tree
        (descended fully to the leaf), then the explicit `default` argument.

        The previous version broke out of the descent on the first key missing
        from the YAML, so it returned the partial DEFAULTS *subtree* (a dict)
        instead of the leaf value — e.g. get('heartbeat.interval_seconds')
        returned {'interval_seconds': 60} rather than 60, which then blew up any
        caller doing time.sleep()/int()/+ on the result. This only surfaced on a
        Pi with NO monitor_config.yaml (pure defaults). Fix: walk the user data
        and the defaults tree independently, each all the way to the leaf.
        """
        keys = path.split('.')

        # 1. Try the user's config, descending as far as it goes.
        node = self._data
        found = True
        for k in keys:
            if isinstance(node, dict) and k in node:
                node = node[k]
            else:
                found = False
                break
        if found:
            return node

        # 2. Fall back to the DEFAULTS tree — descend ALL keys to the leaf.
        d_node = self._defaults
        for k in keys:
            if isinstance(d_node, dict) and k in d_node:
                d_node = d_node[k]
            else:
                # Not in defaults either — a genuinely unknown key. Warn loudly.
                logger.warning("Config key '%s' not found in defaults — using %r",
                               path, default)
                return default
        # Found a documented default. Normal on a defaults-only Pi, so log at
        # DEBUG rather than WARNING to avoid flooding the journal every cycle.
        logger.debug("Config key '%s' not set — using default %r", path, d_node)
        return d_node


def load_config():
    try:
        with open(CONFIG_PATH, 'r') as f:
            data = yaml.safe_load(f)
        logger.info("Loaded config from %s", CONFIG_PATH)
    except FileNotFoundError:
        logger.warning("Config file %s not found — using built-in defaults", CONFIG_PATH)
        data = {}
    except Exception as e:
        logger.error("Failed to parse %s (%s) — using built-in defaults", CONFIG_PATH, e)
        data = {}
    return Config(data, _DEFAULTS)


CFG = load_config()

# =============================================================
# SQLITE LOCAL BUFFER
# =============================================================

def init_sqlite():
    """Create the local buffer + state tables if they don't already exist."""
    conn = sqlite3.connect(SQLITE_PATH)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS buffer (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            measurement TEXT NOT NULL,
            tags        TEXT NOT NULL,
            fields      TEXT NOT NULL,
            timestamp   INTEGER NOT NULL,
            synced      INTEGER DEFAULT 0,
            UNIQUE(measurement, tags, timestamp)
        )
    ''')
    # Durable monitor state (P3): survives restart AND reboot, unlike /tmp.
    conn.execute('''
        CREATE TABLE IF NOT EXISTS monitor_state (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("SQLite buffer + state initialised at %s", SQLITE_PATH)


def state_set(key, value):
    """Persist a JSON-serialisable value in the monitor_state table."""
    conn = sqlite3.connect(SQLITE_PATH)
    try:
        conn.execute('INSERT OR REPLACE INTO monitor_state (key, value) VALUES (?, ?)',
                     (key, json.dumps(value)))
        conn.commit()
    except Exception as e:
        logger.error("state_set(%s) failed: %s", key, e)
    finally:
        conn.close()


def state_get(key, default=None):
    """Read a persisted value from monitor_state (returns default if absent)."""
    conn = sqlite3.connect(SQLITE_PATH)
    try:
        row = conn.execute('SELECT value FROM monitor_state WHERE key=?', (key,)).fetchone()
        return json.loads(row[0]) if row is not None else default
    except Exception as e:
        logger.error("state_get(%s) failed: %s", key, e)
        return default
    finally:
        conn.close()


def buffer_write(measurement, tags, fields, timestamp_ns):
    """Save a data point to the local SQLite buffer."""
    conn = sqlite3.connect(SQLITE_PATH)
    try:
        conn.execute(
            'INSERT OR IGNORE INTO buffer (measurement, tags, fields, timestamp) VALUES (?,?,?,?)',
            (measurement, json.dumps(tags), json.dumps(fields), timestamp_ns)
        )
        conn.commit()
    except Exception as e:
        logger.error("SQLite buffer write failed: %s", e)
    finally:
        conn.close()


def sync_buffer(write_api):
    """Try to flush buffered readings to InfluxDB."""
    conn = sqlite3.connect(SQLITE_PATH)
    rows = conn.execute(
        'SELECT id, measurement, tags, fields, timestamp FROM buffer WHERE synced=0 LIMIT 50'
    ).fetchall()

    if not rows:
        conn.close()
        return

    logger.info("Syncing %d buffered points to InfluxDB...", len(rows))
    synced_ids = []

    for row in rows:
        row_id, measurement, tags_json, fields_json, ts = row
        tags   = json.loads(tags_json)
        fields = json.loads(fields_json)

        point = Point(measurement).time(ts, WritePrecision.NS)
        for k, v in tags.items():
            point = point.tag(k, v)
        for k, v in fields.items():
            point = point.field(k, v)

        try:
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            synced_ids.append(row_id)
        except Exception as e:
            logger.warning("Buffer sync failed for id %s: %s", row_id, e)
            break

    if synced_ids:
        conn.execute(
            f"UPDATE buffer SET synced=1 WHERE id IN ({','.join('?'*len(synced_ids))})",
            synced_ids
        )
        conn.commit()
        logger.info("Synced %d buffered points", len(synced_ids))

    conn.close()

# =============================================================
# INFLUXDB WRITER
# =============================================================

def write_point(write_api, measurement, tags, fields, ts_ns=None):
    """
    Write a single point to InfluxDB, buffering locally on failure.
    If ts_ns is given the point is stamped at that (event) time; otherwise now.
    """
    if ts_ns is None:
        ts_ns = time.time_ns()

    tags = dict(tags)
    tags['property_slug'] = PROPERTY_SLUG
    tags['agency_id']     = AGENCY_ID

    point = Point(measurement).time(ts_ns, WritePrecision.NS)
    for k, v in tags.items():
        point = point.tag(k, v)
    for k, v in fields.items():
        point = point.field(k, v)

    try:
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        logger.debug("Wrote to InfluxDB: %s %s", measurement, fields)
        sync_buffer(write_api)
    except Exception as e:
        logger.warning("InfluxDB write failed, buffering locally: %s", e)
        buffer_write(measurement, tags, fields, ts_ns)


# Backwards-compatible alias used by sensor handlers.
def write_to_influx(write_api, measurement, tags, fields):
    write_point(write_api, measurement, tags, fields, ts_ns=None)

# =============================================================
# EMAIL ALERTING
# =============================================================

def _load_alert_state():
    try:
        with open(ALERT_STATE_PATH, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_alert_state(state):
    try:
        with open(ALERT_STATE_PATH, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logger.error("Failed to save alert state: %s", e)


def send_alert(severity, subject, body):
    """
    Send an email alert with a per-subject cooldown (persisted across restarts).
    Honours alerts.email_enabled.
    """
    if not CFG.get('alerts.email_enabled', True):
        logger.info("Email disabled — would have sent [%s] %s", severity, subject)
        return

    # Per-severity re-send interval; fall back to the flat cooldown_seconds if a
    # severity isn't configured (keeps old configs working unchanged).
    cooldowns = CFG.get('alerts.cooldowns', {}) or {}
    cooldown = cooldowns.get(severity, CFG.get('alerts.cooldown_seconds', 14400))
    now = time.time()
    alert_state = _load_alert_state()

    if subject in alert_state:
        elapsed = now - alert_state[subject]
        if elapsed < cooldown:
            logger.info("Alert suppressed (cooldown): %s (%.0fs remaining)",
                        subject, cooldown - elapsed)
            return

    detection_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    full_subject = f"[{severity}] {subject} at {PROPERTY_SLUG}"
    full_body = (
        f"Property:     {PROPERTY_SLUG}\n"
        f"Severity:     {severity}\n"
        f"Detected at:  {detection_time}\n"
        f"\n"
        f"{body}"
    )

    msg = MIMEText(full_body)
    msg['Subject'] = full_subject
    msg['From']    = ALERT_FROM
    msg['To']      = ALERT_TO

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(SMTP_USER, SMTP_PASSWORD)
            smtp.sendmail(ALERT_FROM, [ALERT_TO], msg.as_string())
        alert_state[subject] = now
        _save_alert_state(alert_state)
        logger.info("Alert sent [%s]: %s", severity, subject)
    except Exception as e:
        logger.error("Failed to send alert email: %s", e)

# =============================================================
# LOW-LEVEL NETWORK PROBES  (Bug 2 — independent signals)
# =============================================================

def _run(cmd, timeout):
    """Run a command, return CompletedProcess or None on error/timeout."""
    try:
        return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              timeout=timeout)
    except Exception:
        return None


def iface_link_up(name):
    """True if the interface has carrier (cable/link present)."""
    try:
        with open(f'/sys/class/net/{name}/carrier', 'r') as f:
            return f.read().strip() == '1'
    except OSError:
        return False


def ping_via(target, iface, count, timeout):
    """Ping a target bound to a specific interface. True on success."""
    r = _run(['ping', '-c', str(count), '-W', str(timeout), '-I', iface, target],
             timeout=count * (timeout + 1) + 2)
    return bool(r) and r.returncode == 0


def wan_reachable(iface, targets, required, count, timeout):
    """
    Ping all targets via iface CONCURRENTLY; True if >= `required` answer (P5).
    Concurrency matters during an outage: with no target answering there is no
    early success to short-circuit on, so sequential pings stack up to
    ~timeout*len(targets) and blow the 30s cadence. Returns (ok, hits).
    """
    results = {}
    threads = []
    for t in targets:
        th = threading.Thread(
            target=lambda tt=t: results.__setitem__(tt, ping_via(tt, iface, count, timeout)))
        th.start()
        threads.append(th)
    deadline = count * (timeout + 1) + 3
    for th in threads:
        th.join(timeout=deadline)
    hits = sum(1 for t in targets if results.get(t))
    return hits >= required, hits


def dns_ok(resolver, hostname, timeout):
    """Resolve hostname (prefer the given resolver). True on success."""
    r = _run(['nslookup', f'-timeout={int(timeout)}', hostname, resolver], timeout=timeout + 2)
    if r is not None:
        return r.returncode == 0
    # Fallback if nslookup isn't installed: system resolver only.
    try:
        socket.setdefaulttimeout(timeout)
        socket.getaddrinfo(hostname, None)
        return True
    except Exception:
        return False


def ap_client_count(iface):
    """Number of associated stations on the guest AP (wlan0)."""
    r = _run(['iw', 'dev', iface, 'station', 'dump'], timeout=5)
    if not r or r.returncode != 0:
        return 0
    return sum(1 for line in r.stdout.decode('utf-8', 'ignore').splitlines()
               if line.startswith('Station'))


def hostapd_active():
    r = _run(['systemctl', 'is-active', 'hostapd'], timeout=5)
    return bool(r) and r.stdout.decode('utf-8', 'ignore').strip() == 'active'

# =============================================================
# ROUTE / NAT MANAGEMENT  (Bug 1 — owns what failover.sh used to do)
# =============================================================

def _policy_rule_exists(primary_if, table, priority):
    """True if an `oif <primary_if> lookup <table>` rule is already present."""
    r = _run(['ip', 'rule', 'show'], timeout=5)
    if not r:
        return False
    text = r.stdout.decode('utf-8', 'ignore')
    for line in text.splitlines():
        if f"oif {primary_if}" in line and f"lookup {table}" in line:
            return True
    return False


def setup_policy_routing(primary_if, primary_gw, table, priority, log=True):
    """
    Make the primary WAN probeable WITHOUT hijacking forwarded guest traffic.

    Proven in netns: an interface-bound probe (`ping -I eth0`) carries oif=eth0,
    matches `ip rule oif eth0 lookup <table>`, and uses the primary default in
    <table>. Forwarded guest packets carry no oif, miss the rule, and follow the
    MAIN table (which points at 4G during failover). The old /32-in-main routes
    blackholed guests whose DNS was one of our probe targets.

    Idempotent — safe to call at startup and re-assert every cycle:
      - `ip route replace default via <gw> dev <primary_if> table <table>`
      - add the oif rule only if it isn't already there.
    Table <table> is set up ONCE and never touched by failover/failback, so the
    primary WAN stays probeable and the gateway stays discoverable in any state.
    """
    if primary_gw:
        _run(['ip', 'route', 'replace', 'default', 'via', primary_gw,
              'dev', primary_if, 'table', str(table)], timeout=5)
    if not _policy_rule_exists(primary_if, table, priority):
        _run(['ip', 'rule', 'add', 'oif', primary_if, 'table', str(table),
              'priority', str(priority)], timeout=5)
    if log:
        logger.info("Policy routing ready: table %s default via %s dev %s; "
                    "rule oif %s -> table %s (priority %s)",
                    table, primary_gw, primary_if, primary_if, table, priority)


def ensure_policy_route(primary_if, primary_gw, table):
    """Cheap, quiet per-cycle re-assert of just the table-<table> default."""
    if primary_gw:
        _run(['ip', 'route', 'replace', 'default', 'via', primary_gw,
              'dev', primary_if, 'table', str(table)], timeout=5)


def detect_transport(primary_if, failover_if):
    """
    Determine the ACTUAL transport from the kernel's MAIN-table default route
    (P3). This is ground truth after a restart — never assume 'primary'.
    Returns 'primary', 'failover', or None if no usable default is found.
    Note: `ip route show default` reads the main table only; table 100 is hidden.
    """
    r = _run(['ip', 'route', 'show', 'default'], timeout=5)
    if not r:
        return None
    for line in r.stdout.decode('utf-8', 'ignore').splitlines():
        line = line.strip()
        if not line.startswith('default') or 'dev' not in line.split():
            continue
        parts = line.split()
        dev = parts[parts.index('dev') + 1]
        if dev == failover_if:
            return 'failover'
        if dev == primary_if:
            return 'primary'
    return None


def detect_gateway_from_lease(iface):
    """
    Read the DHCP-provided gateway for <iface> from its dhcpcd lease.

    The route-based lookup only works while <iface> holds the MAIN default
    route. On a Pi whose first boot is on 4G at a network it has never seen,
    that never happens, so the gateway is never learned and failback stays
    blocked forever. The lease still knows the router, regardless of which
    interface currently owns the default route. Returns the IP or None.
    """
    r = _run(['dhcpcd', '-U', iface], timeout=5)
    if r and r.returncode == 0:
        for line in r.stdout.decode('utf-8', 'ignore').splitlines():
            line = line.strip()
            if line.startswith('routers='):
                gw = line.split('=', 1)[1].strip().strip('\'"').split()
                if gw:
                    return gw[0]
    return None


def detect_gateway(iface):
    """
    Auto-detect the gateway on <iface> (P8). Tries the MAIN-table default route
    first (authoritative while on broadband), then falls back to the dhcpcd
    lease so the gateway is still discoverable while 4G holds the default.
    Returns the gateway IP or None.
    """
    r = _run(['ip', 'route', 'show', 'default', 'dev', iface], timeout=5)
    if r:
        for line in r.stdout.decode('utf-8', 'ignore').splitlines():
            parts = line.split()
            if 'via' in parts:
                return parts[parts.index('via') + 1]
    return detect_gateway_from_lease(iface)


def assert_legacy_failover_disabled():
    """
    P11: two brains must never fight over the default route. If the legacy
    inntact-failover.service is still enabled or active, REFUSE TO START.
    """
    if not CFG.get('failover.refuse_if_legacy_service_enabled', True):
        return
    name = 'inntact-failover'
    en = _run(['systemctl', 'is-enabled', name], timeout=5)
    ac = _run(['systemctl', 'is-active', name], timeout=5)
    en_s = en.stdout.decode('utf-8', 'ignore').strip() if en else ''
    ac_s = ac.stdout.decode('utf-8', 'ignore').strip() if ac else ''
    if en_s == 'enabled' or ac_s == 'active':
        logger.critical(
            "REFUSING TO START: legacy %s.service is enabled=%s active=%s. It "
            "fights this process over the default route (the split-brain bug). "
            "Disable it first:  sudo systemctl disable --now %s",
            name, en_s or 'no', ac_s or 'no', name)
        sys.exit(1)
    logger.info("Legacy failover service check: %s enabled=%s active=%s — OK",
                name, en_s or 'no', ac_s or 'no')


def _ipt(rule, table=None):
    return ['iptables'] + (['-t', table] if table else []) + rule


def _ipt_ensure(rule, table=None):
    """Add an iptables rule only if it isn't already present (P10 idempotency)."""
    chk = _run(_ipt(['-C'] + rule, table), timeout=5)
    if not chk or chk.returncode != 0:
        _run(_ipt(['-A'] + rule, table), timeout=5)


def _ipt_remove(rule, table=None):
    """Remove ALL copies of an iptables rule (P10 — clears any accumulation)."""
    for _ in range(6):
        chk = _run(_ipt(['-C'] + rule, table), timeout=5)
        if not chk or chk.returncode != 0:
            break
        _run(_ipt(['-D'] + rule, table), timeout=5)


def apply_route_failover(primary_if, failover_if, failover_gw):
    """
    Point the MAIN default route + NAT at the 4G interface. Idempotent (P10):
    `ip route replace` + `-C`-guarded iptables, so a restart in the failover
    state never stacks duplicate FORWARD/MASQUERADE rules.
    """
    _run(['ip', 'route', 'replace', 'default', 'via', failover_gw, 'dev', failover_if], timeout=5)
    _ipt_remove(['POSTROUTING', '-o', primary_if, '-j', 'MASQUERADE'], table='nat')
    _ipt_ensure(['POSTROUTING', '-o', failover_if, '-j', 'MASQUERADE'], table='nat')
    _ipt_ensure(['FORWARD', '-i', 'wlan0', '-o', failover_if, '-j', 'ACCEPT'])
    _ipt_ensure(['FORWARD', '-i', failover_if, '-o', 'wlan0',
                 '-m', 'state', '--state', 'RELATED,ESTABLISHED', '-j', 'ACCEPT'])
    try:
        open(FAILOVER_STATE_FILE, 'w').close()
    except OSError:
        pass


def apply_route_failback(primary_if, failover_if, primary_gw):
    """Restore the MAIN default route + NAT to the primary interface (idempotent)."""
    if primary_gw:
        _run(['ip', 'route', 'replace', 'default', 'via', primary_gw, 'dev', primary_if], timeout=5)
    _ipt_remove(['POSTROUTING', '-o', failover_if, '-j', 'MASQUERADE'], table='nat')
    _ipt_ensure(['POSTROUTING', '-o', primary_if, '-j', 'MASQUERADE'], table='nat')
    _ipt_remove(['FORWARD', '-i', 'wlan0', '-o', failover_if, '-j', 'ACCEPT'])
    _ipt_remove(['FORWARD', '-i', failover_if, '-o', 'wlan0',
                 '-m', 'state', '--state', 'RELATED,ESTABLISHED', '-j', 'ACCEPT'])
    try:
        os.remove(FAILOVER_STATE_FILE)
    except OSError:
        pass

# =============================================================
# NETWORK MONITOR — single owner of state, probing, failover, events
# =============================================================

class NetworkMonitor:
    def __init__(self, write_api, mqtt_getter):
        self.write_api = write_api
        self._mqtt_getter = mqtt_getter          # callable -> mqtt client (may be None early)
        self.lock = threading.Lock()

        # Interfaces
        self.primary_if   = CFG.get('interfaces.primary.name', 'eth0')
        self.primary_gw   = CFG.get('interfaces.primary.gateway')   # None -> auto-detect (P8)
        self.failover_if  = CFG.get('interfaces.failover.name', 'eth1')
        self.failover_gw  = CFG.get('interfaces.failover.gateway', '192.168.8.1')
        self.ap_if        = CFG.get('interfaces.ap.name', 'wlan0')

        # Probe params
        self.targets      = CFG.get('probing.wan_targets', ['1.1.1.1', '8.8.8.8', '9.9.9.9'])
        self.req          = CFG.get('probing.wan_targets_required', 1)
        self.pcount       = CFG.get('probing.ping_count', 2)
        self.ptimeout     = CFG.get('probing.ping_timeout_seconds', 3)

        # Failover params
        self.probe_primary_iv = CFG.get('failover.probe_interval_seconds', 30)
        self.fail_threshold   = CFG.get('failover.fail_threshold', 3)
        self.probe_4g_iv      = CFG.get('failover.failback.probe_interval_seconds', 60)
        self.clean_required   = CFG.get('failover.failback.clean_probes_required', 10)
        self.verify_4g_gw     = CFG.get('failover.verify_failover_gateway', True)
        self.failover_enabled = CFG.get('failover.enabled', True)

        # Policy-routing (Bug 1 / P2)
        self.policy_table     = CFG.get('probing.policy_table', 100)
        self.policy_priority  = CFG.get('probing.policy_rule_priority', 100)

        # State
        self.transport        = 'primary'    # 'primary' | 'failover'
        self.wan_fail_streak  = 0
        self.wan_clean_streak = 0
        self.failover_since   = None
        self.gateway_down_since = None
        self.last_router_reboot = 0.0
        self.info_4g_sent     = False
        self.in_failback_window = False
        self.open_events      = {}            # key -> event_id
        # Non-blocking router reboot (P9): a state, not a 210s time.sleep.
        self.reboot_state       = None        # None | 'off' | 'settling'
        self.reboot_phase_since = 0.0
        # One-shot log guards for the per-cycle gateway re-resolve.
        self._gw_fallback_warned = False
        self._gw_unknown_warned  = False

    def mqtt(self):
        return self._mqtt_getter()

    # ---- durable state (P3) ----
    def _persist_state(self):
        state_set('transport', self.transport)
        state_set('failover_since', self.failover_since)
        with self.lock:
            state_set('open_events', dict(self.open_events))

    def resolve_primary_gateway(self):
        """
        Determine the primary gateway (P8): a config override wins; otherwise
        auto-detect from the routing table; otherwise fall back to the last
        persisted value. Runs at startup (before policy routing) and again after
        a router reboot, so a property we've never visited needs no hand-edited IP.
        """
        override = CFG.get('interfaces.primary.gateway')
        if override:
            if self.primary_gw != override:
                logger.info("Primary gateway (config override): %s", override)
            self.primary_gw = override
            state_set('primary_gw', override)
            return
        gw = detect_gateway(self.primary_if)
        if gw:
            if gw != self.primary_gw:
                logger.info("Primary gateway auto-detected: %s via %s", gw, self.primary_if)
            self.primary_gw = gw
            state_set('primary_gw', gw)
            # Detection works again — allow one more warning if we later lose it.
            self._gw_fallback_warned = False
            self._gw_unknown_warned = False
            return
        persisted = state_get('primary_gw')
        if persisted:
            # Runs every cycle, so only log when the value actually changes —
            # otherwise this warning floods the log while we sit on 4G.
            if self.primary_gw != persisted or not self._gw_fallback_warned:
                logger.warning("Primary gateway not detectable now (on 4G?) — using persisted %s",
                               persisted)
                self._gw_fallback_warned = True
            self.primary_gw = persisted
            return
        # Also once-only: this now runs every cycle.
        if not self._gw_unknown_warned:
            logger.error("Could not determine primary gateway and none persisted; "
                         "policy routing / probing may fail until broadband returns")
            self._gw_unknown_warned = True

    def reconcile_on_startup(self):
        """
        The 16-36h bug rebuilt: if the service restarts while failed over, the
        routes still point at 4G but an in-memory 'primary' assumption means we
        probe the primary, see it clean, and never fail back. Fix: adopt the
        ACTUAL transport from the routing table, and restore open events from
        SQLite so a mid-outage restart still produces a paired log.
        """
        actual = detect_transport(self.primary_if, self.failover_if)
        persisted_transport = state_get('transport')
        persisted_since     = state_get('failover_since')
        persisted_open      = state_get('open_events') or {}
        with self.lock:
            self.open_events = dict(persisted_open)

        if actual is None:
            actual = persisted_transport or 'primary'
            logger.warning("Reconcile: no default route found; assuming transport=%s", actual)

        if actual == 'failover':
            self.transport = 'failover'
            self.failover_since = persisted_since or time.time()
            if not self.open_events:
                # Routes say 4G but we have no open events (e.g. failed over then
                # the persist was lost). Open them now so failback can close them.
                logger.warning("Reconcile: on 4G with no open events — reconstructing")
                self.open_event('wan_outage', 'wan_outage',
                                'Primary WAN down (reconstructed at startup)', transport='failover')
                self.open_event('failover', 'failover',
                                'On 4G at startup (reconstructed)', reconstructed=1)
            logger.warning("Reconcile: adopted transport=FAILOVER "
                           "(failover_since=%s, %d open event(s))",
                           self.failover_since, len(self.open_events))
        else:
            self.transport = 'primary'
            self.failover_since = None
            # Close any orphan events left by a crash during/after failback so the
            # log never shows an unpaired open (the trial-screenshot symptom).
            if self.open_events:
                logger.warning("Reconcile: on primary with %d orphan open event(s) — closing",
                               len(self.open_events))
                for key in list(self.open_events.keys()):
                    self.close_event(key, key, 'Closed by startup reconciliation', 'reconciled')
            logger.info("Reconcile: adopted transport=primary")

        self._persist_state()

    # ---- paired event log (Bug 3) ----
    def open_event(self, key, etype, message, **fields):
        event_id = uuid.uuid4().hex
        with self.lock:
            self.open_events[key] = event_id
            snapshot = dict(self.open_events)
        state_set('open_events', snapshot)   # persist so a restart can still close it
        ts = time.time_ns()
        f = {'phase': 'open', 'message': message, 'open': 1}
        f.update(fields)
        write_point(self.write_api, CFG.get('influx.events_measurement', 'events'),
                    {'event_id': event_id, 'type': etype, 'phase': 'open'}, f, ts_ns=ts)
        logger.info("EVENT open [%s] %s — %s", etype, event_id[:8], message)
        return event_id

    def close_event(self, key, etype, message, outcome, **fields):
        with self.lock:
            event_id = self.open_events.pop(key, uuid.uuid4().hex)
            snapshot = dict(self.open_events)
        state_set('open_events', snapshot)   # persist the now-closed set
        ts = time.time_ns()
        f = {'phase': 'close', 'message': message, 'outcome': outcome, 'open': 0}
        f.update(fields)
        write_point(self.write_api, CFG.get('influx.events_measurement', 'events'),
                    {'event_id': event_id, 'type': etype, 'phase': 'close'}, f, ts_ns=ts)
        logger.info("EVENT close [%s] %s — %s (%s)", etype, event_id[:8], message, outcome)
        return event_id

    def transition_event(self, etype, message, **fields):
        """A standalone state-transition marker (no open/close pairing)."""
        ts = time.time_ns()
        f = {'phase': 'mark', 'message': message}
        f.update(fields)
        write_point(self.write_api, CFG.get('influx.events_measurement', 'events'),
                    {'event_id': uuid.uuid4().hex, 'type': etype, 'phase': 'mark'}, f, ts_ns=ts)
        logger.info("EVENT mark [%s] — %s", etype, message)

    # ---- guest impact ----
    def guests_online(self):
        src = CFG.get('alerts.guests_online.source', 'ap_clients')
        minst = CFG.get('alerts.guests_online.min_stations', 1)
        if src == 'ap_clients':
            return ap_client_count(self.ap_if) >= minst
        return False

    # ---- layered signals (Bug 2) ----
    def collect_signals(self):
        """
        Collect all enabled signals CONCURRENTLY and log each independently (P5).
        Running the probes in parallel keeps a fully-failing cycle down to roughly
        one probe's worth of time (~ping/dns timeout) instead of the sum, so the
        loop can actually honour the 30s cadence the failover trigger assumes.
        """
        sig = CFG.get('probing.signals', {})
        results = {}
        threads = []

        def spawn(fn):
            th = threading.Thread(target=fn)
            th.start()
            threads.append(th)

        if sig.get('link', True):
            results['link'] = iface_link_up(self.primary_if)   # cheap file read, inline
        if sig.get('gateway', True):
            if self.primary_gw:
                spawn(lambda: results.__setitem__(
                    'gateway', ping_via(self.primary_gw, self.primary_if, 1, self.ptimeout)))
            else:
                # Gateway UNKNOWN, not down: with no address there is nothing to
                # ping, and reporting a hard "down" would fake a wedged router.
                results['gateway'] = None
        if sig.get('wan', True):
            def _wan():
                ok, hits = wan_reachable(self.primary_if, self.targets, self.req,
                                         self.pcount, self.ptimeout)
                results['wan'] = ok
                results['wan_hits'] = hits
            spawn(_wan)
        if sig.get('dns', True):
            spawn(lambda: results.__setitem__(
                'dns', dns_ok(CFG.get('probing.dns.resolver', '1.1.1.1'),
                              CFG.get('probing.dns.hostname', 'dashboard.inntact.co.uk'),
                              CFG.get('probing.dns.timeout_seconds', 5))))
        if sig.get('ap_clients', True):
            def _ap():
                results['ap_clients'] = ap_client_count(self.ap_if)
                results['hostapd_active'] = hostapd_active()
            spawn(_ap)

        cap = max(self.pcount * (self.ptimeout + 1) + 3,
                  CFG.get('probing.dns.timeout_seconds', 5) + 3, 8)
        for th in threads:
            th.join(timeout=cap)

        # Log each signal independently so causes stay distinguishable.
        fields = {
            'transport': self.transport,
            'link': 1 if results.get('link') else 0,
            # -1 = unknown (no gateway address yet); 1 = up, 0 = down.
            'gateway': -1 if results.get('gateway') is None else (1 if results['gateway'] else 0),
            'wan': 1 if results.get('wan') else 0,
            'wan_hits': int(results.get('wan_hits', 0)),
            'dns': 1 if results.get('dns') else 0,
            'ap_clients': int(results.get('ap_clients', 0)),
            'hostapd_active': 1 if results.get('hostapd_active') else 0,
        }
        write_point(self.write_api, 'connectivity', {}, fields)
        return results

    # ---- failover / failback (Bug 1) ----
    def do_failover(self):
        guests = self.guests_online()
        # Perform the switch.
        if self.verify_4g_gw and not ping_via(self.failover_gw, self.failover_if, 1, self.ptimeout):
            logger.error("4G gateway %s unreachable — failover may not carry traffic", self.failover_gw)
        apply_route_failover(self.primary_if, self.failover_if, self.failover_gw)
        with self.lock:
            self.transport = 'failover'
            self.failover_since = time.time()
            self.wan_clean_streak = 0
            self.info_4g_sent = False

        # Did 4G actually deliver internet?
        fo_ok, _ = wan_reachable(self.failover_if, self.targets, self.req, self.pcount, self.ptimeout)

        self.open_event('wan_outage', 'wan_outage',
                        'Primary broadband WAN down', transport='failover')
        self.open_event('failover', 'failover',
                        'Switched to 4G backup', guests_online=1 if guests else 0,
                        failover_ok=1 if fo_ok else 0)

        # Bug 4 alert policy.
        if fo_ok and guests:
            logger.info("Failover succeeded, guests online via 4G — log only, no notification")
        elif fo_ok and not guests:
            logger.info("Failover succeeded, no guests currently online — log only")
        else:
            # Failover FAILED and guests can't get online — the only real alarm.
            send_alert('CRITICAL', 'Guests offline — failover failed',
                       "Broadband is down AND the 4G backup did not provide connectivity.\n"
                       "Guests at this property currently have no internet.\n\n"
                       "Immediate attention required: check the 4G dongle / SIM and broadband.")

        self._persist_state()   # transport=failover survives a restart (P3)

    def do_failback(self):
        apply_route_failback(self.primary_if, self.failover_if, self.primary_gw)
        with self.lock:
            was_since = self.failover_since
            self.transport = 'primary'
            self.failover_since = None
            self.wan_fail_streak = 0
            self.info_4g_sent = False
        dur = (time.time() - was_since) if was_since else 0
        self.close_event('failover', 'failover',
                         'Broadband restored — switched back from 4G', 'auto_resolved',
                         duration_seconds=float(dur))
        self.close_event('wan_outage', 'wan_outage',
                         'Primary broadband WAN restored', 'auto_resolved',
                         duration_seconds=float(dur))
        self._persist_state()   # transport=primary, no open events (P3)
        # Happy path: guests never went offline -> no notification (acceptance test).
        logger.info("Failback complete after %.0fs on 4G — no notification (guests unaffected)", dur)

    def maybe_info_4g(self):
        if self.transport != 'failover' or self.failover_since is None or self.info_4g_sent:
            return
        info_after = CFG.get('alerts.info_after_seconds', 7200)
        if (time.time() - self.failover_since) >= info_after:
            hrs = (time.time() - self.failover_since) / 3600
            send_alert('INFO', 'Broadband down — running on 4G',
                       f"Broadband at this property has been down for {hrs:.1f} hours and the "
                       f"property is running on the 4G backup.\n\n"
                       f"Guests are online; this is not an outage. It may be worth raising a "
                       f"ticket with the broadband provider.")
            self.info_4g_sent = True

    # ---- router reboot (Bug 6 — hard-gated; P9 non-blocking) ----
    def _router_plug_topic(self):
        return CFG.get('router_reboot.smart_plug_topic', None) or ROUTER_PLUG_TOPIC[0]

    def _plug_publish(self, state):
        topic = self._router_plug_topic()
        client = self.mqtt()
        if not topic or client is None:
            logger.warning("Router plug action '%s' but no topic/MQTT client known", state)
            return False
        client.publish(f"{topic}/set", json.dumps({"state": state}))
        return True

    def maybe_reboot_router(self, signals):
        if not CFG.get('router_reboot.enabled', True):
            return
        # A reboot already in progress — don't start another (P9).
        if self.reboot_state is not None:
            return
        # Gateway UNKNOWN: we cannot judge router health without an address, and
        # the fix for a missing gateway is resolution, never a power-cycle.
        if not self.primary_gw:
            self.gateway_down_since = None
            return
        forbid_4g = CFG.get('router_reboot.forbid_while_on_4g', False)
        min_down  = CFG.get('router_reboot.min_gateway_down_seconds', 900)
        forbid_fb = CFG.get('router_reboot.forbid_within_failback_window', True)
        cooldown  = CFG.get('router_reboot.cooldown_seconds', 3600)

        gateway_up = signals.get('gateway', False)

        # Track how long the primary gateway has been unreachable on the LAN.
        if gateway_up:
            self.gateway_down_since = None
            return
        if self.gateway_down_since is None:
            self.gateway_down_since = time.time()

        # Gates: only a genuinely wedged router (gateway down), never an ISP
        # outage 4G already covers, never mid-failback, and at most once/hour.
        if forbid_4g and self.transport == 'failover':
            return
        if forbid_fb and self.in_failback_window:
            return
        if (time.time() - self.gateway_down_since) < min_down:
            return
        if (time.time() - self.last_router_reboot) < cooldown:
            return

        self._start_router_reboot()

    def _start_router_reboot(self):
        """Begin a power-cycle without blocking the loop (P9). advance_reboot() finishes it."""
        if not self._router_plug_topic() or self.mqtt() is None:
            logger.warning("Router reboot wanted but no smart-plug topic / MQTT client known")
            return
        self.last_router_reboot = time.time()
        logger.warning("Router wedged (gateway down) — power-cycling via %s (non-blocking)",
                       self._router_plug_topic())
        self.transition_event('router_reboot',
                              'Primary gateway unreachable — power-cycling router',
                              transport=self.transport)
        if self._plug_publish("OFF"):
            self.reboot_state = 'off'
            self.reboot_phase_since = time.time()
            logger.info("Router plug OFF — will power ON in %ds",
                        CFG.get('router_reboot.off_seconds', 30))

    def advance_reboot(self):
        """Progress the non-blocking router reboot each cycle — never blocks (P9)."""
        if self.reboot_state is None:
            return
        off_s  = CFG.get('router_reboot.off_seconds', 30)
        wait_s = CFG.get('router_reboot.post_reboot_wait_seconds', 180)
        now = time.time()
        if self.reboot_state == 'off' and (now - self.reboot_phase_since) >= off_s:
            self._plug_publish("ON")
            self.reboot_state = 'settling'
            self.reboot_phase_since = now
            logger.info("Router plug ON — settling for %ds before re-evaluating", wait_s)
        elif self.reboot_state == 'settling' and (now - self.reboot_phase_since) >= wait_s:
            self.reboot_state = None
            logger.info("Router reboot settle window complete")
            # Router may have returned with a new DHCP lease — re-detect (P8).
            self.resolve_primary_gateway()
            ensure_policy_route(self.primary_if, self.primary_gw, self.policy_table)

    # ---- main loop ----
    def loop(self):
        logger.info("Network monitor loop started (owns failover/failback)")
        # Resolve the primary gateway first (P8): config override, else detect,
        # else persisted — so policy routing has a nexthop even at a new property.
        self.resolve_primary_gateway()
        # Startup: policy routing so the primary WAN is always probeable without
        # hijacking forwarded guest traffic. Set up once; never touched by
        # failover/failback (which only swap the MAIN-table default + NAT).
        setup_policy_routing(self.primary_if, self.primary_gw,
                             self.policy_table, self.policy_priority)
        # Adopt the ACTUAL transport from the routing table (P3) — never assume.
        self.reconcile_on_startup()

        while True:
            cycle_start = time.monotonic()
            interval = self.probe_primary_iv
            try:
                signals = self.collect_signals()
                # Re-derive the gateway EVERY cycle, not just at startup: a Pi that
                # booted while broadband was down (or with a stale persisted value
                # from another network) would otherwise never learn the real gateway,
                # leaving table 100 unpopulated and failback permanently blocked.
                self.resolve_primary_gateway()
                # Cheap re-assert of the table-100 default (survive any churn).
                ensure_policy_route(self.primary_if, self.primary_gw, self.policy_table)

                wan_up = signals.get('wan', False)

                if self.transport == 'primary':
                    if wan_up:
                        self.wan_fail_streak = 0
                    else:
                        self.wan_fail_streak += 1
                        logger.warning("Primary WAN probe failed (%d/%d)",
                                       self.wan_fail_streak, self.fail_threshold)
                        if self.failover_enabled and self.wan_fail_streak >= self.fail_threshold:
                            logger.warning("Failover threshold reached — switching to 4G")
                            self.do_failover()
                    interval = self.probe_primary_iv

                else:  # on failover (4G)
                    self.maybe_info_4g()
                    if wan_up:
                        self.wan_clean_streak += 1
                        self.in_failback_window = True
                        logger.info("Primary WAN clean while on 4G (%d/%d)",
                                    self.wan_clean_streak, self.clean_required)
                        if self.wan_clean_streak >= self.clean_required:
                            logger.info("Failback hysteresis satisfied — restoring broadband")
                            self.do_failback()
                            self.in_failback_window = False
                    else:
                        if self.wan_clean_streak > 0:
                            logger.info("Primary WAN dropped again — failback counter reset")
                        self.wan_clean_streak = 0
                        self.in_failback_window = False
                    interval = self.probe_4g_iv

                # Router reboot is non-blocking (P9): advance any in-progress
                # power-cycle, then evaluate whether to start one. Runs in every
                # transport — a wedged router forces us to 4G, so recovery must be
                # reachable there; the gateway-down gate keeps it off ISP outages.
                self.advance_reboot()
                self.maybe_reboot_router(signals)

            except Exception as e:
                logger.error("Network monitor loop error: %s", e)
                interval = self.probe_primary_iv

            # P5: the cadence is the target PERIOD, not sleep-on-top-of-probe-time.
            # sleep only the remainder so 3 failing cycles land near 90s, not ~170s.
            elapsed = time.monotonic() - cycle_start
            sleep_for = max(0, interval - elapsed)
            logger.info("Probe cycle %.1fs (target %ss, transport=%s) — sleeping %.1fs",
                        elapsed, interval, self.transport, sleep_for)
            time.sleep(sleep_for)

# =============================================================
# HEARTBEAT
# =============================================================

def heartbeat_loop(write_api):
    """Background thread: sends heartbeat every N seconds."""
    interval = CFG.get('heartbeat.interval_seconds', 60)
    while True:
        try:
            now_iso = datetime.now(timezone.utc).isoformat()
            write_to_influx(write_api, 'heartbeat', {},
                {'status': 'online', 'last_seen': now_iso})
            logger.info("Heartbeat sent successfully")
        except Exception as e:
            logger.error("Heartbeat thread error: %s", e)
        time.sleep(interval)

# =============================================================
# SENSOR WATCHDOG  (Bug 5 — inventory + last_seen from Z2M)
# =============================================================

# last_seen (unix seconds) per device, from any message we receive.
device_last_seen = {}
# availability state per device from zigbee2mqtt/<device>/availability.
device_availability = {}
# inventory from zigbee2mqtt/bridge/devices: name -> {'type':..., 'model':..., 'exposes':[...]}
z2m_inventory = {}
# last known leak state per device (see FIX 5 note in handler).
water_leak_last_state = {}
# one-shot flag: warn loudly if Z2M reports no last_seen at all (P7)
_z2m_last_seen_warned = [False]


def _classify_device(name, meta):
    """
    Classify a device by what it EXPOSES first (P4), falling back to name/model.

    Exposes is authoritative: a device exposing `water_leak` IS a leak sensor
    regardless of its friendly name or model string. This is what makes the
    watchdog correct on hardware whose model carries no keyword — e.g. SONOFF
    SNZB-02P (temp/humidity), SNZB-05P (leak), S60ZBTPF (plug) — which the old
    substring-only matcher classified as 'other', firing "missing sensor" alarms
    forever on a fully-paired property.
    """
    meta = meta or {}
    exposes = set(meta.get('exposes') or [])

    # 1. Exposes-based (authoritative)
    if 'water_leak' in exposes:
        return 'leak'
    if 'temperature' in exposes or 'humidity' in exposes:
        return 'climate'
    if 'state' in exposes and (exposes & {'power', 'energy', 'current', 'voltage'}):
        return 'smart_plug'

    # 2. Name / model fallback (incl. SONOFF model strings)
    text = (name or '').lower() + ' ' + (meta.get('model') or '').lower()
    if 'leak' in text or 'water' in text or 'snzb-05' in text or 'sq510' in text or 'ts0207' in text:
        return 'leak'
    if 'plug' in text or 'socket' in text or 's60' in text or 'ts011f' in text:
        return 'smart_plug'
    if ('temp' in text or 'humid' in text or 'climate' in text
            or 'snzb-02' in text or 'ts0201' in text):
        return 'climate'
    return 'other'


def _stale_threshold_for(dtype):
    tt = CFG.get('sensors.watchdog.type_thresholds', {}) or {}
    if dtype == 'climate':
        # climate maps to the tighter of temperature/humidity thresholds
        return min(tt.get('temperature', 5400), tt.get('humidity', 5400))
    return tt.get(dtype, CFG.get('sensors.watchdog.default_stale_seconds', 108000))


def get_expected_counts():
    """
    Expected sensor counts per property.

    TODO(seam): these should arrive from the create-property endpoint alongside
    the rest of the Pi config (the same channel guest-WiFi uses via config_sync),
    so a property's inventory is never hand-edited in YAML — that's the stale-.env
    problem wearing a new hat (P12). Until that endpoint exists, read the YAML.
    """
    return CFG.get('sensors.watchdog.expected_counts', {}) or {}


def evaluate_staleness(name, now):
    """
    Decide whether a device is stale (P7). Returns (stale, reason, dtype).
    A device we have NEVER heard from (last_seen absent) is treated as stale once
    past the startup grace — otherwise a silent/never-paired sensor looks exactly
    like a healthy dry house.
    """
    meta = z2m_inventory.get(name, {})
    dtype = _classify_device(name, meta)
    threshold = _stale_threshold_for(dtype)
    last = device_last_seen.get(name)
    offline = device_availability.get(name) == 'offline'

    if offline:
        return True, "has reported offline", dtype
    if last is None:
        return True, "has never reported (no last_seen)", dtype
    if (now - last) > threshold:
        return True, f"has been silent for {(now - last) / 3600:.1f} hours", dtype
    return False, None, dtype


def sensor_watchdog_loop():
    """
    Two checks, both configurable:
      1. STALENESS — a known device silent beyond its per-type threshold.
      2. MISSING INVENTORY — fewer devices of a type than expected_counts, or a
         named expected_device absent entirely (the never-paired case).
    """
    if not CFG.get('sensors.watchdog.enabled', True):
        logger.info("Sensor watchdog disabled by config")
        return

    grace = CFG.get('sensors.watchdog.startup_grace_seconds', 900)
    interval = CFG.get('sensors.watchdog.check_interval_seconds', 300)
    time.sleep(grace)

    while True:
        now = time.time()

        # 1. Staleness for every device we know about (via last_seen/availability).
        for name in list(set(list(device_last_seen.keys()) + list(z2m_inventory.keys()))):
            stale, reason, dtype = evaluate_staleness(name, now)
            if stale:
                logger.warning("Watchdog: device '%s' (%s) %s", name, dtype, reason)
                send_alert('WARNING', f'Sensor not reporting: {name}',
                           f"The {dtype} sensor '{name}' {reason}.\n\n"
                           f"This may be a dead battery, a lost Zigbee connection, or a "
                           f"sensor that never finished pairing. "
                           f"Please check the sensor at {PROPERTY_SLUG}.")

        # 2. Missing inventory — catches never-paired hardware.
        expected_counts = get_expected_counts()
        present_by_type = {}
        for name, meta in z2m_inventory.items():
            t = _classify_device(name, meta)
            present_by_type[t] = present_by_type.get(t, 0) + 1
        for dtype, want in expected_counts.items():
            have = present_by_type.get(dtype, 0)
            if have < want:
                logger.warning("Watchdog: expected %d %s device(s), only %d paired",
                               want, dtype, have)
                send_alert('WARNING', f'Missing {dtype} sensor(s)',
                           f"Expected {want} {dtype} sensor(s) at this property but only "
                           f"{have} are paired with the hub.\n\n"
                           f"A sensor may have never paired or has dropped off the network. "
                           f"Please check pairing at {PROPERTY_SLUG}.")

        expected_devices = CFG.get('sensors.watchdog.expected_devices', []) or []
        for name in expected_devices:
            if name not in z2m_inventory and name not in device_last_seen:
                logger.warning("Watchdog: expected device '%s' has never been seen", name)
                send_alert('WARNING', f'Sensor never paired: {name}',
                           f"The expected sensor '{name}' has never reported to the hub.\n\n"
                           f"Please check it is powered and paired at {PROPERTY_SLUG}.")

        time.sleep(interval)

# =============================================================
# ZIGBEE / MQTT SENSOR HANDLING
# =============================================================

# Auto-detected router smart-plug topic (list so nested funcs can mutate it).
ROUTER_PLUG_TOPIC = [None]

temp_breach_start = {}


def _extract_exposes(definition):
    """
    Flatten a Z2M definition's exposes into a set of property names.
    Simple exposes carry 'property'; composite ones (e.g. a 'switch') carry
    'features', each with its own 'property'. We want e.g. {'water_leak',
    'temperature', 'state', 'power', 'battery'}.
    """
    props = set()
    if not definition:
        return props
    for exp in definition.get('exposes', []) or []:
        if not isinstance(exp, dict):
            continue
        if exp.get('property'):
            props.add(exp['property'])
        for feat in exp.get('features', []) or []:
            if isinstance(feat, dict) and feat.get('property'):
                props.add(feat['property'])
    return props


def handle_bridge_message(topic, payload):
    """Consume zigbee2mqtt/bridge/* for device inventory and availability."""
    if topic.endswith('/bridge/devices'):
        # payload is a list of device dicts.
        try:
            count = 0
            saw_last_seen = False
            for dev in payload:
                name = dev.get('friendly_name')
                if not name or name == 'Coordinator':
                    continue
                definition = dev.get('definition') or {}
                z2m_inventory[name] = {
                    'type': dev.get('type'),
                    'model': definition.get('model'),
                    'exposes': sorted(_extract_exposes(definition)),
                }
                ls = dev.get('last_seen')
                if ls:
                    saw_last_seen = True
                    # last_seen may be ms epoch or ISO — accept both.
                    try:
                        device_last_seen[name] = float(ls) / 1000.0 if str(ls).isdigit() else \
                            datetime.fromisoformat(str(ls).replace('Z', '+00:00')).timestamp()
                    except Exception:
                        pass
                count += 1
            logger.info("Z2M inventory updated: %d device(s) known", count)

            # P7: if Z2M reports no last_seen at all, the watchdog is half-blind.
            if count > 0 and not saw_last_seen and not _z2m_last_seen_warned[0]:
                _z2m_last_seen_warned[0] = True
                logger.warning(
                    "Z2M bridge/devices carries NO last_seen fields — advanced.last_seen "
                    "appears DISABLED in configuration.yaml. The watchdog can then only see "
                    "devices via live messages; enable `advanced: {last_seen: epoch}` so "
                    "silent and never-paired sensors are detected reliably.")
        except Exception as e:
            logger.debug("Could not parse bridge/devices: %s", e)


def handle_availability(device_name, payload):
    """zigbee2mqtt/<device>/availability -> 'online'/'offline'."""
    state = payload.get('state') if isinstance(payload, dict) else payload
    if state in ('online', 'offline'):
        device_availability[device_name] = state
        if state == 'online':
            device_last_seen[device_name] = time.time()


def handle_sensor_data(write_api, device_name, payload):
    """Process a sensor reading from Zigbee2MQTT."""
    device_last_seen[device_name] = time.time()
    tags = {'device': device_name}

    # --- Temperature ---
    if 'temperature' in payload:
        temp = float(payload['temperature'])
        write_to_influx(write_api, 'temperature', tags.copy(), {'value': temp})
        logger.info("Temperature [%s]: %.1f°C", device_name, temp)

        breach_key = f"temp_{device_name}"
        if temp < TEMP_LOW or temp > TEMP_HIGH:
            if breach_key not in temp_breach_start:
                temp_breach_start[breach_key] = time.time()
            else:
                breach_duration = time.time() - temp_breach_start[breach_key]
                if breach_duration >= 900:
                    direction = "low" if temp < TEMP_LOW else "high"
                    threshold = f"<{TEMP_LOW}°C" if direction == "low" else f">{TEMP_HIGH}°C"
                    send_alert(
                        'CRITICAL',
                        'Temperature alert',
                        f"Sensor '{device_name}' has been reporting a temperature "
                        f"{'below' if direction == 'low' else 'above'} the acceptable range "
                        f"for {breach_duration/60:.0f} minutes.\n\n"
                        f"Current reading:  {temp}°C\n"
                        f"Threshold:        {threshold}\n\n"
                        f"Please check heating/cooling at the property."
                    )
        else:
            temp_breach_start.pop(breach_key, None)

    # --- Humidity (FIX 1: Tuya TS0201 x/10 scaling) ---
    if 'humidity' in payload:
        hum = float(payload['humidity'])
        if hum < 10.0:
            logger.warning("Humidity from '%s' looks scaled wrong (%.2f) — multiplying by 10. Raw: %s",
                           device_name, hum, payload['humidity'])
            hum = round(hum * 10, 1)
        write_to_influx(write_api, 'humidity', tags.copy(), {'value': hum})
        logger.info("Humidity [%s]: %.1f%%", device_name, hum)

    # --- Water leak (FIX 2: string "true"/"false"; FIX 5: re-assert last state) ---
    # P4: decide "is this a leak sensor" from what it EXPOSES, not its name.
    is_leak_sensor = (_classify_device(device_name, z2m_inventory.get(device_name, {})) == 'leak')

    if 'water_leak' in payload:
        leak_bool = bool(payload['water_leak'])
        water_leak_last_state[device_name] = leak_bool
        leak_str = "true" if leak_bool else "false"
        write_to_influx(write_api, 'water_leak', tags.copy(), {'detected': leak_str})
        logger.info("Water leak [%s]: %s", device_name, leak_str)

        if leak_bool:
            logger.critical("WATER LEAK DETECTED at %s!", device_name)
            send_alert(
                'CRITICAL', 'Water leak detected',
                f"A water leak has been detected by sensor '{device_name}'.\n\n"
                f"No automatic remediation is possible for leaks.\n"
                f"Immediate action required. Please check the property."
            )
    elif is_leak_sensor and ('linkquality' in payload or 'battery' in payload or 'voltage' in payload):
        last_state = water_leak_last_state.get(device_name, False)
        leak_str = "true" if last_state else "false"
        write_to_influx(write_api, 'water_leak', tags.copy(), {'detected': leak_str})
        logger.info("Water leak [%s]: check-in — asserting last known state (%s)",
                    device_name, leak_str)

    # --- Smart plug state ---
    if 'state' in payload:
        state = payload['state']
        write_to_influx(write_api, 'smart_plug', tags.copy(),
            {'state': state, 'power_on': state == 'ON'})
        logger.info("Smart plug [%s]: %s", device_name, state)
        if 'router' in device_name.lower():
            ROUTER_PLUG_TOPIC[0] = f"zigbee2mqtt/{device_name}"

    # --- Smart plug power consumption ---
    if 'power' in payload:
        write_to_influx(write_api, 'smart_plug_power', tags.copy(),
            {'watts': float(payload['power'])})

# =============================================================
# MQTT CALLBACKS
# =============================================================

def on_connect(client, userdata, flags, reason_code, properties=None):
    """FIX 3: Paho v2 reason_code is a ReasonCode object, not an int."""
    rc_str = str(reason_code)
    if rc_str == 'Success' or reason_code == 0:
        logger.info("Connected to MQTT broker at %s:%d", MQTT_HOST, MQTT_PORT)
        client.subscribe("zigbee2mqtt/+")
        client.subscribe("zigbee2mqtt/+/availability")
        client.subscribe("zigbee2mqtt/bridge/#")
        logger.info("Subscribed to zigbee2mqtt device, availability and bridge topics")
    else:
        logger.error("MQTT connection failed: %s", reason_code)


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    logger.warning("MQTT disconnected (code %s) — will reconnect", reason_code)


def on_message(client, userdata, msg):
    """Route every incoming MQTT message to the correct handler."""
    write_api = userdata['write_api']
    topic     = msg.topic

    try:
        payload = json.loads(msg.payload.decode('utf-8'))
    except (json.JSONDecodeError, UnicodeDecodeError):
        # Availability payloads may be bare strings like "online".
        raw = msg.payload.decode('utf-8', 'ignore').strip()
        if topic.endswith('/availability') and raw in ('online', 'offline'):
            handle_availability(topic.split('/')[1], raw)
        else:
            logger.debug("Non-JSON message on topic %s — skipping", topic)
        return

    # Bridge topics: inventory + availability source (Bug 5).
    if '/bridge/' in topic:
        handle_bridge_message(topic, payload)
        return

    if topic.endswith('/availability'):
        handle_availability(topic.split('/')[1], payload)
        return

    parts = topic.split('/')
    if len(parts) < 2:
        return
    device_name = parts[1]

    try:
        handle_sensor_data(write_api, device_name, payload)
    except Exception as e:
        logger.error("Error processing message from %s: %s", device_name, e)

# =============================================================
# SPEED TEST
# =============================================================

def speedtest_loop(write_api, transport_getter=None):
    """
    Background thread: measures throughput and writes it to InfluxDB TAGGED with
    the transport in effect, so 4G and broadband results never share a series.

    4G is metered. The WAN probe already proves the 4G link carries traffic, so
    by default we SKIP speed tests entirely while failed over (skip_on_4g). This
    is what quietly drained the trial SIM: ~80MB x ~48/day. If you do want 4G
    throughput numbers, set skip_on_4g: false — they then run at the slower
    interval_on_4g_seconds cadence.
    """
    delay     = CFG.get('speedtest.first_run_delay_seconds', 30)
    normal_iv = CFG.get('speedtest.interval_seconds', 1800)
    iv_4g     = CFG.get('speedtest.interval_on_4g_seconds', 21600)
    skip_4g   = CFG.get('speedtest.skip_on_4g', True)
    logger.info("Speed test thread started — first test in %d seconds", delay)
    time.sleep(delay)

    while True:
        transport = transport_getter() if transport_getter else 'primary'

        if transport == 'failover' and skip_4g:
            logger.info("On 4G (metered) — skipping speed test to preserve data allowance")
            time.sleep(normal_iv)   # re-check at normal cadence; resumes soon after failback
            continue

        try:
            logger.info("Running speed test (transport=%s) — about 20 seconds...", transport)
            result = subprocess.run(
                ['speedtest-cli', '--simple', '--secure'],
                capture_output=True, text=True, timeout=60
            )
            if result.returncode != 0:
                logger.warning("Speed test failed: %s", result.stderr.strip())
            else:
                download_mbps = None
                upload_mbps   = None
                for line in result.stdout.splitlines():
                    if line.startswith('Download:'):
                        download_mbps = round(float(line.split()[1]), 2)
                    elif line.startswith('Upload:'):
                        upload_mbps = round(float(line.split()[1]), 2)

                if download_mbps is not None and upload_mbps is not None:
                    # Tag with transport so fibre and 4G plot as separate series.
                    write_to_influx(write_api, 'speedtest', {'transport': transport},
                        {'download_mbps': download_mbps, 'upload_mbps': upload_mbps})
                    logger.info("Speed test complete (%s): download=%.1f Mbps  upload=%.1f Mbps",
                                transport, download_mbps, upload_mbps)
                else:
                    logger.warning("Speed test output unexpected: %s", result.stdout)

        except subprocess.TimeoutExpired:
            logger.warning("Speed test timed out after 60 seconds — skipping")
        except Exception as e:
            logger.error("Speed test error: %s", e)

        time.sleep(iv_4g if transport == 'failover' else normal_iv)

# =============================================================
# MAIN ENTRY POINT
# =============================================================

def main():
    logger.info("=" * 60)
    logger.info("Inntact Property Monitor starting (v3 — network reliability)")
    logger.info("Property: %s | Agency: %s", PROPERTY_SLUG, AGENCY_ID)
    logger.info("=" * 60)

    init_sqlite()

    # P11: refuse to run if the legacy inntact-failover.service is still enabled
    # (two brains fighting over the default route is the split-brain bug).
    assert_legacy_failover_disabled()

    influx_client = InfluxDBClient(
        url=INFLUX_HOST, token=INFLUX_TOKEN, org=INFLUX_ORG
    )
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)

    mqtt_client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"inntact-{PROPERTY_SLUG}"
    )
    if MQTT_HOST != 'localhost':
        mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    mqtt_client.user_data_set({'write_api': write_api})
    mqtt_client.on_connect    = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message    = on_message

    try:
        mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    except Exception as e:
        logger.error("Initial MQTT connection failed: %s — will retry", e)

    mqtt_client.loop_start()

    # Heartbeat
    threading.Thread(target=heartbeat_loop, args=(write_api,), daemon=True).start()
    logger.info("Heartbeat thread started")

    # Network monitor (owns failover/failback/router-reboot/events)
    netmon = NetworkMonitor(write_api, mqtt_getter=lambda: mqtt_client)
    threading.Thread(target=netmon.loop, daemon=True).start()
    logger.info("Network monitor thread started")

    # Speed test (transport-aware: skips 4G by default, tags every point)
    threading.Thread(target=speedtest_loop,
                     args=(write_api, lambda: netmon.transport), daemon=True).start()
    logger.info("Speed test thread started")

    # Sensor watchdog
    threading.Thread(target=sensor_watchdog_loop, daemon=True).start()
    logger.info("Sensor watchdog thread started")

    # Config sync (guarded — only if the module exposes the loop)
    if hasattr(config_sync, 'config_sync_loop'):
        threading.Thread(
            target=config_sync.config_sync_loop,
            args=(write_api, PROPERTY_SLUG, CONFIG_API_URL, CONFIG_API_TOKEN),
            daemon=True).start()
        logger.info("Config sync thread started")
    else:
        logger.warning("config_sync.config_sync_loop not found — config sync disabled")

    logger.info("Monitor running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutting down monitor...")
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        influx_client.close()
        logger.info("Monitor stopped cleanly")


if __name__ == '__main__':
    main()
