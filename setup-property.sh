#!/bin/bash
# ==============================================
# Inntact — per-property provisioning
# Property: {PROPERTY_SLUG}
#
# Run ONCE on a Pi freshly flashed from the Inntact GOLDEN IMAGE, as root:
#     sudo bash setup-property.sh
#
# Prereq: scp this property's .env, configuration.yaml and monitor_config.yaml
# (from the Drive "Pi Setup" folder) into /home/pi/ first. This script copies
# them into place, applies this property's identity on top of the golden base
# (hostname, config, guest WiFi, fresh SSH host keys), and starts the services.
# The heavy install is already baked into the image.
# ==============================================
set -e

SLUG="{PROPERTY_SLUG}"
WIFI_SSID="{GUEST_WIFI_SSID}"
WIFI_PASS="{GUEST_WIFI_PASSWORD}"

# --- placeholder + WPA2 safety checks ---
for v in "$SLUG" "$WIFI_SSID" "$WIFI_PASS"; do
    if [[ "$v" == *"{"* || "$v" == *"}"* ]]; then
        echo "ERROR: a {PLACEHOLDER} was not substituted — regenerate this script from the skill."
        exit 1
    fi
done
if [ "${#WIFI_PASS}" -lt 8 ]; then
    echo "ERROR: guest WiFi password must be at least 8 characters (WPA2)."
    exit 1
fi

echo "=============================================="
echo " Inntact provisioning — $SLUG"
echo "=============================================="

# --- bring the property config into /opt/inntact/ (scp'd to /home/pi first) ---
for f in .env configuration.yaml monitor_config.yaml; do
    [ -f "/home/pi/$f" ] && cp "/home/pi/$f" "/opt/inntact/$f"
done
for f in .env configuration.yaml; do
    if [ ! -f "/opt/inntact/$f" ]; then
        echo "ERROR: /opt/inntact/$f not found. scp the Pi Setup files to /home/pi/ first, then re-run."
        exit 1
    fi
done
chmod 600 /opt/inntact/.env
if [ -f /opt/inntact/monitor_config.yaml ]; then
    echo "monitor_config.yaml present."
else
    echo "NOTE: monitor_config.yaml missing — the monitor will use built-in defaults."
fi

# --- [1/6] hostname ---
echo "[1/6] Setting hostname to $SLUG..."
hostnamectl set-hostname "$SLUG"
sed -i "s/127.0.1.1.*/127.0.1.1\t$SLUG/" /etc/hosts

# --- [2/6] Zigbee2MQTT config (GENERATE -> fresh, per-Pi Zigbee network) ---
echo "[2/6] Installing Zigbee2MQTT configuration..."
cp /opt/inntact/configuration.yaml /opt/zigbee2mqtt/data/configuration.yaml
chown root:root /opt/zigbee2mqtt/data/configuration.yaml
chmod 644 /opt/zigbee2mqtt/data/configuration.yaml

# --- [3/6] fresh SSH host keys (unique per Pi) ---
echo "[3/6] Regenerating SSH host keys..."
rm -f /etc/ssh/ssh_host_*
ssh-keygen -A
systemctl restart ssh

# --- [4/6] guest WiFi (hostapd) ---
echo "[4/6] Configuring guest WiFi ($WIFI_SSID)..."
cat > /etc/hostapd/hostapd.conf << EOF
interface=wlan0
driver=nl80211
ssid=${WIFI_SSID}
hw_mode=g
channel=6
ieee80211n=1
wmm_enabled=1
macaddr_acl=0
auth_algs=1
ignore_broadcast_ssid=0
wpa=2
wpa_passphrase=${WIFI_PASS}
wpa_key_mgmt=WPA-PSK
rsn_pairwise=CCMP
EOF
sed -i 's|#DAEMON_CONF=""|DAEMON_CONF="/etc/hostapd/hostapd.conf"|' /etc/default/hostapd

# --- [5/6] enable + start services ---
echo "[5/6] Enabling and starting services..."
systemctl enable zigbee2mqtt inntact-monitor hostapd
systemctl start mosquitto
sleep 2
systemctl start zigbee2mqtt
sleep 3
systemctl start inntact-monitor
systemctl start wlan0-up
sleep 4
systemctl reset-failed hostapd 2>/dev/null || true
systemctl start hostapd
systemctl start dnsmasq

# --- clean up the scp'd copies (the .env holds secrets) ---
rm -f /home/pi/.env /home/pi/configuration.yaml /home/pi/monitor_config.yaml

# --- [6/6] verify ---
echo "[6/6] Service states:"
for s in mosquitto zigbee2mqtt inntact-monitor hostapd dnsmasq wlan0-up; do
    printf "  %-16s %s\n" "$s" "$(systemctl is-active "$s")"
done

echo ""
echo "=============================================="
echo " $SLUG provisioned. Manual steps left:"
echo "=============================================="
echo " 1. Join Tailscale (remote access from anywhere):"
echo "      sudo tailscale up --ssh --hostname=inntact-$SLUG"
echo "    Open the printed URL to authenticate, then in the Tailscale"
echo "    admin console DISABLE KEY EXPIRY for this machine."
echo ""
echo " 2. Pair the sensors: open http://\$(hostname -I | awk '{print \$1}'):8099/"
echo "    in Safari (not Chrome) and follow sensor-pairing.md."
echo ""
echo " 3. Acceptance test: pull the Ethernet cable, confirm 4G failover keeps a"
echo "    guest online (~90s), plug back in, confirm clean failback and no alarm."
echo "=============================================="
