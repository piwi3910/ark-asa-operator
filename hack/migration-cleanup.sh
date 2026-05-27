#!/usr/bin/env bash
# hack/migration-cleanup.sh — one-time removal of the AngellusMortis ark-operator from novanas.
# Idempotent: re-runs are safe.

set -euo pipefail

CTX="${KUBECTL_CONTEXT:-novanas}"
NS="${ARK_NAMESPACE:-ark-operator}"
NOVANAS_HOST="${NOVANAS_HOST:-192.168.10.203}"
NOVANAS_USER="${NOVANAS_USER:-piwi}"

run() { echo "+ $*"; "$@" || true; }

echo "==> Deleting ArkCluster (cascades pod + services + PVCs)"
run kubectl --context "$CTX" -n "$NS" delete arkcluster.mort.is piwis-place --ignore-not-found --wait=true

echo "==> Deleting AngellusMortis Deployment + ServiceAccount + RBAC"
run kubectl --context "$CTX" -n "$NS" delete deployment ark-operator --ignore-not-found
run kubectl --context "$CTX" -n "$NS" delete serviceaccount ark-operator --ignore-not-found
run kubectl --context "$CTX" -n "$NS" delete role ark-operator-role-namespaced --ignore-not-found
run kubectl --context "$CTX" -n "$NS" delete rolebinding ark-operator-rolebinding-namespaced --ignore-not-found

echo "==> Deleting cluster-scoped resources"
run kubectl --context "$CTX" delete clusterrole ark-operator-games-role-cluster --ignore-not-found
run kubectl --context "$CTX" delete clusterrolebinding ark-operator-games-rolebinding-cluster --ignore-not-found
run kubectl --context "$CTX" delete crd arkclusters.mort.is --ignore-not-found

echo "==> Verifying namespace is clean"
run kubectl --context "$CTX" -n "$NS" get all,pvc,configmap,secret

echo "==> Optional: cleanup operator-image scratch on novanas host"
if command -v sshpass >/dev/null && [ -n "${NOVANAS_PASS:-}" ]; then
  # Feed the sudo password to the remote `sudo -S` via stdin only. The
  # password is never embedded in the remote shell command string, so it
  # cannot appear in the remote process list (e.g. `ps auxf`). The remote
  # shell reads the first line of stdin into a variable, then pipes that
  # variable to `sudo -S` -- the password stays in process memory only.
  printf '%s\n' "$NOVANAS_PASS" | \
    SSHPASS="$NOVANAS_PASS" sshpass -e ssh -o StrictHostKeyChecking=accept-new \
      "$NOVANAS_USER@$NOVANAS_HOST" \
      'read -r SUDO_PW; printf "%s\n" "$SUDO_PW" | sudo -S sh -c "crictl rmi ghcr.io/angellusmortis/ark-server:v0.10.7-patched 2>/dev/null || true; crictl rmi ghcr.io/angellusmortis/ark-server:v0.10.7 2>/dev/null || true; crictl rmi ghcr.io/angellusmortis/ark-operator:v0.10.7 2>/dev/null || true; rm -rf /tmp/ark-patch"'
fi

echo "==> Done. kube-vip and namespace '$NS' preserved."
