#!/usr/bin/env bash
# hack/install-csi-driver-nfs.sh — install csi-driver-nfs into the novanas k3s cluster.
set -euo pipefail

CTX="${KUBECTL_CONTEXT:-novanas}"
CHART_VERSION="${CSI_NFS_VERSION:-v4.11.0}"

if ! command -v helm >/dev/null 2>&1; then
  echo "ERROR: helm not on PATH. Install Helm 3.x and re-run." >&2
  exit 1
fi

echo "==> Adding csi-driver-nfs Helm repo and updating"
helm repo add csi-driver-nfs https://raw.githubusercontent.com/kubernetes-csi/csi-driver-nfs/master/charts >/dev/null 2>&1 || true
helm repo update >/dev/null

echo "==> Installing/upgrading csi-driver-nfs $CHART_VERSION into kube-system"
helm --kube-context="$CTX" upgrade --install csi-driver-nfs csi-driver-nfs/csi-driver-nfs \
  --namespace kube-system --version "$CHART_VERSION" \
  --set controller.replicas=1 \
  --wait --timeout 5m

echo "==> Applying nfs-csi StorageClass"
kubectl --context "$CTX" apply -f hack/manifests/nfs-storageclass.yaml

echo "==> Verifying"
kubectl --context "$CTX" get storageclass nfs-csi
kubectl --context "$CTX" -n kube-system get pods -l app.kubernetes.io/name=csi-driver-nfs
