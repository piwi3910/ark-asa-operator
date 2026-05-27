#!/usr/bin/env bash
# hack/novanas-nfs-setup.sh — install + configure kernel NFS server on novanas, exporting /srv/k8s-nfs.
# Idempotent.

set -euo pipefail

: "${NOVANAS_HOST:=192.168.10.203}"
: "${NOVANAS_USER:=piwi}"
: "${NOVANAS_PASS:?NOVANAS_PASS must be set}"
: "${EXPORT_DIR:=/srv/k8s-nfs}"
: "${EXPORT_NETWORK:=192.168.10.0/24}"

# Password handling: the first line of the heredoc carries the sudo password,
# which the remote shell consumes via `read -r SUDO_PW` and forwards to
# `sudo -S` via stdin. Password never appears in argv on either host.
SSHPASS="$NOVANAS_PASS" sshpass -e ssh -o StrictHostKeyChecking=accept-new \
  "$NOVANAS_USER@$NOVANAS_HOST" \
  'read -r SUDO_PW; { printf "%s\n" "$SUDO_PW"; cat; } | sudo -S bash -s' <<REMOTE
$NOVANAS_PASS
set -euo pipefail
apt-get update -qq
apt-get install -yqq nfs-kernel-server
mkdir -p ${EXPORT_DIR}
chown nobody:nogroup ${EXPORT_DIR}
chmod 0777 ${EXPORT_DIR}
if ! grep -q "^${EXPORT_DIR}" /etc/exports 2>/dev/null; then
  echo "${EXPORT_DIR} ${EXPORT_NETWORK}(rw,sync,no_subtree_check,no_root_squash,insecure)" >> /etc/exports
fi
exportfs -ra
systemctl enable --now nfs-kernel-server
systemctl restart nfs-kernel-server
showmount -e localhost
REMOTE

echo "==> NFS server configured on $NOVANAS_HOST. Verifying from dev machine..."
if command -v showmount >/dev/null 2>&1; then
  showmount -e "$NOVANAS_HOST" || true
else
  echo "(showmount not installed locally; that's fine — csi-driver-nfs install will validate the export)"
fi
