import logging
import os
import re
import shutil
import subprocess
import time
import requests
logger = logging.getLogger(__name__)
CONFIG_POLL_INTERVAL = 300
HOSTAPD_CONF = "/etc/hostapd/hostapd.conf"
HOSTAPD_BACKUP = "/etc/hostapd/hostapd.conf.inntact-bak"
APPLIED_VERSION_FILE = "/opt/inntact/.applied_config_version"
HTTP_TIMEOUT = 15
def _read_applied_version():
    try:
        with open(APPLIED_VERSION_FILE, "r") as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return -1
def _write_applied_version(version):
    try:
        with open(APPLIED_VERSION_FILE, "w") as f:
            f.write(str(version))
    except OSError as e:
        logger.warning(f"Could not persist applied config version: {e}")
def _fetch_remote_config(slug, api_url, token):
    url = f"{api_url.rstrip('/')}/api/config/{slug}"
    try:
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=HTTP_TIMEOUT,
        )
    except requests.RequestException as e:
        logger.warning(f"Config poll failed (network): {e}")
        return None
    if resp.status_code == 200:
        try:
            return resp.json()
        except ValueError:
            logger.warning("Config poll returned non-JSON response")
            return None
    elif resp.status_code in (401, 404):
        logger.error(f"Config poll rejected ({resp.status_code}) — check CONFIG_API_TOKEN and property slug")
        return None
    else:
        logger.warning(f"Config poll unexpected status {resp.status_code}")
        return None
def _build_hostapd_conf(current_text, ssid, password):
    lines = current_text.splitlines()
    out = []
    saw_ssid = saw_pass = False
    for line in lines:
        if re.match(r"^\s*ssid=", line):
            out.append(f"ssid={ssid}")
            saw_ssid = True
        elif re.match(r"^\s*wpa_passphrase=", line):
            out.append(f"wpa_passphrase={password}")
            saw_pass = True
        else:
            out.append(line)
    if not saw_ssid:
        out.append(f"ssid={ssid}")
    if not saw_pass:
        out.append(f"wpa_passphrase={password}")
    return "\n".join(out) + "\n"
def _validate_hostapd_conf(path):
    """
    Validate a hostapd config by parsing it directly.
    hostapd -t hangs on Pi 5 / hostapd v2.10 rather than exiting,
    so we check the file contents instead.
    """
    try:
        with open(path, "r") as f:
            text = f.read()
        ssid_match = re.search(r"^\s*ssid=(.+)$", text, re.MULTILINE)
        pass_match = re.search(r"^\s*wpa_passphrase=(.+)$", text, re.MULTILINE)
        if not ssid_match:
            logger.error("hostapd config validation failed: no ssid= line found")
            return False
        if not pass_match:
            logger.error("hostapd config validation failed: no wpa_passphrase= line found")
            return False
        passphrase = pass_match.group(1).strip()
        if len(passphrase) < 8:
            logger.error(f"hostapd config validation failed: passphrase too short ({len(passphrase)} chars)")
            return False
        return True
    except OSError as e:
        logger.error(f"Could not read hostapd config for validation: {e}")
        return False
def _hostapd_is_active():
    try:
        result = subprocess.run(
            ["systemctl", "is-active", "hostapd"],
            capture_output=True, text=True, timeout=10,
        )
        return result.stdout.strip() == "active"
    except subprocess.SubprocessError:
        return False
def _restart_hostapd():
    try:
        subprocess.run(["systemctl", "reset-failed", "hostapd"], timeout=10)
        subprocess.run(["systemctl", "restart", "hostapd"], timeout=30, check=True)
    except subprocess.SubprocessError as e:
        logger.error(f"hostapd restart command failed: {e}")
        return False
    time.sleep(4)
    return _hostapd_is_active()
def _report_status(write_api, slug, version, status, detail=""):
    if write_api is None:
        return
    try:
        from influxdb_client import Point
        import os
        bucket = os.getenv("INFLUX_BUCKET", "properties")
        org    = os.getenv("INFLUX_ORG", "inntact")
        point = (
            Point("config_sync")
            .tag("property", slug)
            .tag("property_slug", slug)
            .field("config_version", int(version))
            .field("status", status)
            .field("detail", detail)
        )
        write_api.write(bucket=bucket, org=org, record=point)
        logger.info(f"Config sync status reported: v{version} {status}")
    except Exception as e:
        logger.warning(f"Could not report config-sync status: {e}")
def _apply_wifi_change(slug, ssid, password, version, write_api):
    try:
        with open(HOSTAPD_CONF, "r") as f:
            current = f.read()
    except OSError as e:
        logger.error(f"Cannot read {HOSTAPD_CONF}: {e}")
        _report_status(write_api, slug, version, "validation_failed", "could not read current hostapd.conf")
        return False
    try:
        shutil.copy2(HOSTAPD_CONF, HOSTAPD_BACKUP)
    except OSError as e:
        logger.error(f"Cannot back up hostapd.conf: {e}")
        _report_status(write_api, slug, version, "validation_failed", "could not back up hostapd.conf")
        return False
    new_text = _build_hostapd_conf(current, ssid, password)
    tmp_path = HOSTAPD_CONF + ".inntact-new"
    try:
        with open(tmp_path, "w") as f:
            f.write(new_text)
    except OSError as e:
        logger.error(f"Cannot write temp hostapd.conf: {e}")
        _report_status(write_api, slug, version, "validation_failed", "could not write temp config")
        return False
    if not _validate_hostapd_conf(tmp_path):
        logger.error("New WiFi config failed validation — not applying")
        os.remove(tmp_path)
        _report_status(write_api, slug, version, "validation_failed", "hostapd -t rejected new config")
        return False
    try:
        os.replace(tmp_path, HOSTAPD_CONF)
    except OSError as e:
        logger.error(f"Cannot move new config into place: {e}")
        _report_status(write_api, slug, version, "validation_failed", "could not move config into place")
        return False
    if _restart_hostapd():
        logger.info(f"WiFi credentials updated to v{version} — AP is up")
        _write_applied_version(version)
        _report_status(write_api, slug, version, "applied", f"ssid={ssid}")
        return True
    logger.error("hostapd did not come up after change — rolling back")
    try:
        shutil.copy2(HOSTAPD_BACKUP, HOSTAPD_CONF)
        _restart_hostapd()
    except OSError as e:
        logger.critical(f"ROLLBACK FAILED — WiFi may be down: {e}")
        _report_status(write_api, slug, version, "rollback", "rollback copy failed — needs attention")
        return False
    _report_status(write_api, slug, version, "rollback", "new config brought AP down; restored previous")
    return False
def config_sync_loop(write_api, slug, api_url, token):
    if not api_url or not token:
        logger.warning("Config sync disabled — CONFIG_API_URL or CONFIG_API_TOKEN not set")
        return
    logger.info("Config sync thread started")
    while True:
        try:
            remote = _fetch_remote_config(slug, api_url, token)
            if remote is not None:
                remote_version = int(remote.get("config_version", 0))
                applied_version = _read_applied_version()
                if remote_version > applied_version:
                    logger.info(f"New config v{remote_version} available (applied: v{applied_version})")
                    wifi = remote.get("wifi", {})
                    ssid = wifi.get("ssid")
                    password = wifi.get("password")
                    if ssid and password and len(password) >= 8:
                        _apply_wifi_change(slug, ssid, password, remote_version, write_api)
                    else:
                        logger.error("Remote WiFi config invalid (missing ssid/password or password < 8 chars) — ignoring")
                        _report_status(write_api, slug, remote_version, "validation_failed", "remote ssid/password missing or too short")
        except Exception as e:
            logger.error(f"Config sync loop error: {e}")
        time.sleep(CONFIG_POLL_INTERVAL)
