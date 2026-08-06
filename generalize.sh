#!/bin/bash
# ==============================================
# Inntact GOLDEN IMAGE — generalise
#
# Run on the finished base Pi, as root, as the LAST thing before you shut down
# to capture the SD card. It strips per-Pi and per-boot state so every Pi later
# flashed from the captured image is unique and clean:
#   - machine-id        (systemd regenerates a unique one on first boot)
#   - SSH host keys      (regenerated unique on first boot)
#   - Tailscale state    (each Pi authenticates fresh at provisioning)
#   - saved rfkill state (so WiFi isn't restored 'blocked')
#   - logs / leases / shell history
#
#     sudo bash generalize.sh
#     sudo shutdown -h now        # then pull the SD card and capture it
#
# Do NOT reboot after running this and before capturing — first boot would
# regenerate the very state we just cleared.
# ==============================================
set -u
echo "Generalising this Pi for golden-image capture..."

# Hold per-Pi state services
systemctl stop tailscaled 2>/dev/null || true

# 1. machine-id — blank so a unique one is generated on first boot
: > /etc/machine-id
rm -f /var/lib/dbus/machine-id
ln -sf /etc/machine-id /var/lib/dbus/machine-id

# 2. SSH host keys — removed so each Pi generates its own on first boot
rm -f /etc/ssh/ssh_host_*

# 3. Tailscale — each Pi joins fresh at provisioning
rm -f /var/lib/tailscale/tailscaled.state

# 4. WiFi radio — unblocked, and no saved 'blocked' state to restore
rfkill unblock wifi 2>/dev/null || true
rm -f /var/lib/systemd/rfkill/*

# 5. Logs / DHCP leases / history
journalctl --rotate 2>/dev/null || true
journalctl --vacuum-time=1s 2>/dev/null || true
rm -rf /var/log/journal/* 2>/dev/null || true
find /var/log -type f -exec truncate -s 0 {} \; 2>/dev/null || true
rm -f /var/lib/NetworkManager/*lease* /var/lib/dhcp/* 2>/dev/null || true
rm -f /root/.bash_history /home/pi/.bash_history 2>/dev/null || true

echo ""
echo "Generalised. Now capture the card:"
echo "  1. sudo shutdown -h now"
echo "  2. Pull the SD card, put it in your Mac"
echo "  3. Capture it (see the image-capture steps)"
echo "  Do NOT boot this Pi again before capturing."
