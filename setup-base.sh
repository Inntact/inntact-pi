#!/bin/bash
# ==============================================
# Inntact GOLDEN IMAGE — base install (identity-free)
#
# Run ONCE on a fresh Raspberry Pi OS Lite (64-bit) install, as root:
#     sudo bash setup-base.sh
#
# Installs everything that is IDENTICAL on every property: OS deps, Node,
# Zigbee2MQTT (compiled), Tailscale, the Python stack, the device code, the
# systemd services and update-pi.sh. It sets NO customer identity — no
# hostname, no .env, no WiFi password. That is applied per-Pi later by
# setup-property.sh. After this, generalise and capture the SD card as the
# reusable golden image.
# ==============================================
set -e

echo "=============================================="
echo " Inntact base image build (identity-free)"
echo "=============================================="

# ----------------------------------------------
# [1/12] System update
# ----------------------------------------------
echo "[1/12] Updating system packages..."
apt update && apt upgrade -y

# ----------------------------------------------
# [2/12] System dependencies (+ NodeSource Node 20)
# ----------------------------------------------
echo "[2/12] Installing system dependencies..."
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt install -y \
    python3 python3-pip python3-venv \
    git curl sqlite3 speedtest-cli \
    mosquitto mosquitto-clients \
    nodejs hostapd dnsmasq \
    iptables iptables-persistent netfilter-persistent

NODE_MAJOR=$(node -v | sed 's/^v//' | cut -d. -f1)
if [ "$NODE_MAJOR" -lt 20 ]; then
    echo "ERROR: Node.js v$NODE_MAJOR installed — Zigbee2MQTT needs v20+. Fix Node and re-run."
    exit 1
fi
echo "Node.js v$NODE_MAJOR — OK."

# ----------------------------------------------
# [3/12] Tailscale (installed only; each Pi authenticates in setup-property.sh)
# ----------------------------------------------
echo "[3/12] Installing Tailscale..."
curl -fsSL https://tailscale.com/install.sh | sh

# ----------------------------------------------
# [4/12] Directory structure
# ----------------------------------------------
echo "[4/12] Creating directories..."
mkdir -p /opt/inntact /var/log/inntact /var/log/zigbee2mqtt /etc/network/interfaces.d

# ----------------------------------------------
# [5/12] Python virtual environment + packages
# ----------------------------------------------
echo "[5/12] Setting up Python venv..."
python3 -m venv /opt/inntact/venv
/opt/inntact/venv/bin/pip install --upgrade pip
/opt/inntact/venv/bin/pip install \
    influxdb-client paho-mqtt python-dotenv pyyaml requests

# ----------------------------------------------
# [6/12] Device code from GitHub (baked; update-pi.sh refreshes later)
# ----------------------------------------------
echo "[6/12] Fetching monitor.py + config_sync.py from GitHub..."
REPO="https://raw.githubusercontent.com/Inntact/inntact-pi/refs/heads/main"
for f in monitor.py config_sync.py; do
    curl -fsSL "$REPO/$f" -o "/opt/inntact/$f.new"
    python3 -m py_compile "/opt/inntact/$f.new"
    mv "/opt/inntact/$f.new" "/opt/inntact/$f"
    echo "  $f installed."
done

# ----------------------------------------------
# [7/12] update-pi.sh (remote code updater)
# ----------------------------------------------
echo "[7/12] Installing update-pi.sh..."
cat > /usr/local/bin/update-pi.sh << 'EOF'
#!/bin/bash
# Inntact — pull latest monitor.py + config_sync.py from GitHub, compile-check,
# install, restart the monitor. Aborts before installing on a bad/failed pull.
set -e
REPO="https://raw.githubusercontent.com/Inntact/inntact-pi/refs/heads/main"
for f in monitor.py config_sync.py; do
    echo "Fetching $f ..."
    curl -fsSL "$REPO/$f?cb=$(date +%s)" -o "/opt/inntact/$f.new"
    python3 -m py_compile "/opt/inntact/$f.new"
    mv "/opt/inntact/$f.new" "/opt/inntact/$f"
    echo "  $f updated."
done
systemctl restart inntact-monitor
sleep 2
echo "inntact-monitor: $(systemctl is-active inntact-monitor)"
EOF
chmod +x /usr/local/bin/update-pi.sh

# ----------------------------------------------
# [8/12] inntact-monitor service
# Installed but NOT enabled — it needs the property .env first, so
# setup-property.sh enables and starts it. (Avoids a crash-loop in the image.)
# ----------------------------------------------
echo "[8/12] Installing inntact-monitor service (left disabled until provisioned)..."
cat > /etc/systemd/system/inntact-monitor.service << 'EOF'
[Unit]
Description=Inntact Property Monitor
After=network-online.target zigbee2mqtt.service
Wants=network-online.target
[Service]
Type=simple
User=root
WorkingDirectory=/opt/inntact
ExecStart=/opt/inntact/venv/bin/python3 /opt/inntact/monitor.py
Restart=always
RestartSec=15
StandardOutput=journal
StandardError=journal
[Install]
WantedBy=multi-user.target
EOF

# ----------------------------------------------
# [9/12] Zigbee2MQTT — install + compile (the slow part)
# Service installed but NOT enabled/started: it needs the property
# configuration.yaml first (which also makes it generate a fresh, per-Pi
# Zigbee network). setup-property.sh enables and starts it.
# ----------------------------------------------
echo "[9/12] Installing Zigbee2MQTT (this is the slow step — be patient)..."
rm -rf /opt/zigbee2mqtt
mkdir -p /opt/zigbee2mqtt
git clone --depth 1 https://github.com/Koenkk/zigbee2mqtt.git /opt/zigbee2mqtt
mkdir -p /opt/zigbee2mqtt/data
cd /opt/zigbee2mqtt
npm install -g pnpm@9
pnpm install
pnpm run prepack
cat > /etc/systemd/system/zigbee2mqtt.service << 'EOF'
[Unit]
Description=Zigbee2MQTT
After=network-online.target mosquitto.service
Wants=network-online.target
[Service]
Type=simple
ExecStart=/usr/bin/node /opt/zigbee2mqtt/index.js
WorkingDirectory=/opt/zigbee2mqtt
Restart=always
RestartSec=10
User=root
StandardOutput=journal
StandardError=journal
[Install]
WantedBy=multi-user.target
EOF

# ----------------------------------------------
# [10/12] Local Mosquitto broker (generic: localhost-only, anonymous)
# ----------------------------------------------
echo "[10/12] Configuring Mosquitto..."
cat > /etc/mosquitto/conf.d/inntact.conf << 'EOF'
listener 1883 localhost
allow_anonymous true
EOF
systemctl enable mosquitto

# ----------------------------------------------
# [11/12] Networking (generic): dnsmasq, wlan0 static IP, IP forwarding, NAT,
#         wlan0-up / rfkill-unblock / hostapd-wait services
# ----------------------------------------------
echo "[11/12] Configuring networking (generic)..."
mv /etc/dnsmasq.conf /etc/dnsmasq.conf.bak 2>/dev/null || true
cat > /etc/dnsmasq.conf << 'EOF'
interface=wlan0
dhcp-range=192.168.10.100,192.168.10.200,255.255.255.0,24h
dhcp-option=3,192.168.10.1
dhcp-option=6,8.8.8.8,8.8.4.4
EOF

cat > /etc/network/interfaces.d/wlan0 << 'EOF'
auto wlan0
iface wlan0 inet static
    address 192.168.10.1
    netmask 255.255.255.0
EOF

cat > /etc/sysctl.d/99-inntact.conf << 'EOF'
net.ipv4.ip_forward=1
EOF
sysctl -p /etc/sysctl.d/99-inntact.conf

# Base NAT for guest traffic via primary broadband (failover NAT is owned by monitor.py)
iptables -t nat -C POSTROUTING -o eth0 -j MASQUERADE 2>/dev/null || iptables -t nat -A POSTROUTING -o eth0 -j MASQUERADE
iptables -C FORWARD -i wlan0 -o eth0 -j ACCEPT 2>/dev/null || iptables -A FORWARD -i wlan0 -o eth0 -j ACCEPT
iptables -C FORWARD -i eth0 -o wlan0 -m state --state RELATED,ESTABLISHED -j ACCEPT 2>/dev/null || iptables -A FORWARD -i eth0 -o wlan0 -m state --state RELATED,ESTABLISHED -j ACCEPT
netfilter-persistent save

# Robust guest-interface bring-up. wlan0 is the guest-AP radio. The image can
# boot with WiFi soft-blocked (systemd-rfkill restores a saved 'blocked' state),
# which races the unblock. This helper unblocks and WAITS until it sticks, then
# brings the link up and assigns the IP idempotently. The service runs AFTER
# systemd-rfkill so the restored state can't re-block us.
cat > /usr/local/bin/wlan0-up.sh << 'EOF'
#!/bin/bash
set -u
for i in $(seq 1 15); do
    rfkill unblock wifi
    sleep 1
    rfkill list wifi | grep -q "Soft blocked: yes" || break
done
ip link set wlan0 up
ip addr replace 192.168.10.1/24 dev wlan0
EOF
chmod +x /usr/local/bin/wlan0-up.sh

cat > /etc/systemd/system/wlan0-up.service << 'EOF'
[Unit]
Description=Bring up wlan0 with static IP
After=sys-subsystem-net-devices-wlan0.device systemd-rfkill.service
Wants=sys-subsystem-net-devices-wlan0.device
Before=hostapd.service dnsmasq.service
[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/usr/local/bin/wlan0-up.sh
[Install]
WantedBy=multi-user.target
EOF

# Drop any saved 'blocked' rfkill state so systemd-rfkill can't restore it at boot.
rm -f /var/lib/systemd/rfkill/*

mkdir -p /etc/systemd/system/hostapd.service.d
cat > /etc/systemd/system/hostapd.service.d/override.conf << 'EOF'
[Unit]
After=wlan0-up.service
Wants=wlan0-up.service
EOF

cat > /etc/systemd/system/rfkill-unblock.service << 'EOF'
[Unit]
Description=Unblock WiFi radio
After=sys-subsystem-net-devices-wlan0.device
Wants=sys-subsystem-net-devices-wlan0.device
[Service]
Type=oneshot
ExecStart=/usr/sbin/rfkill unblock wifi
RemainAfterExit=yes
[Install]
WantedBy=multi-user.target
EOF

# Debian masks hostapd by default; unmask it (it's configured + enabled at provision).
systemctl unmask hostapd
systemctl daemon-reload

# Enable ONLY the generic services here. hostapd, zigbee2mqtt and inntact-monitor
# are enabled by setup-property.sh once their per-property config is in place.
# (Unmasking hostapd reverts it to its 'enabled' preset, so explicitly disable it.)
systemctl enable wlan0-up rfkill-unblock dnsmasq
systemctl disable hostapd 2>/dev/null || true

# ----------------------------------------------
# [12/12] Done
# ----------------------------------------------
echo "[12/12] Base install complete."
echo "=============================================="
echo " Base image ready — this Pi is NOT yet a property:"
echo "   - no hostname/identity, no .env, no guest WiFi password"
echo "   - inntact-monitor / zigbee2mqtt / hostapd installed but NOT enabled"
echo "   - Tailscale installed but NOT authenticated"
echo ""
echo " Next: run the generalise step, then capture the SD card as the image."
echo "=============================================="
