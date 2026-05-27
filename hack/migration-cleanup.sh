#!/usr/bin/env bash
# hack/migration-cleanup.sh — one-time removal of the AngellusMortis ark-operator from novanas.
# Idempotent: re-runs are safe.

set -euo pipefail

CTX="${KUBECTL_CONTEXT:-novanas}"
NS="${ARK_NAMESPACE:-ark-operator}"

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
kubectl --context "$CTX" -n "$NS" get all,pvc,configmap,secret

echo "==> Optional: cleanup operator-image scratch on novanas host"
if command -v sshpass >/dev/null && [ -n "${NOVANAS_PASS:-}" ]; then
  SSHPASS="$NOVANAS_PASS" sshpass -e ssh -o StrictHostKeyChecking=accept-new piwi@192.168.10.203 \
    "echo '$NOVANAS_PASS' | sudo -S sh -c 'crictl rmi ghcr.io/angellusmortis/ark-server:v0.10.7-patched 2>/dev/null || true; crictl rmi ghcr.io/angellusmortis/ark-server:v0.10.7 2>/dev/null || true; crictl rmi ghcr.io/angellusmortis/ark-operator:v0.10.7 2>/dev/null || true; rm -rf /tmp/ark-patch'"
fi

echo "==> Done. kube-vip and namespace '$NS' preserved."
