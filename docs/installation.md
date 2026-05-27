# Installation

## Prerequisites
1. Kubernetes 1.30 or newer.
2. A LoadBalancer controller. We've tested kube-vip on novanas with an IP pool of `192.168.10.210-219`.
3. An RWX StorageClass for the cluster-transfer PVC. We document `csi-driver-nfs`.
4. Egress to `*.steampowered.com` from pods.

## Setting up NFS + csi-driver-nfs on a single node

```bash
NOVANAS_PASS='your-ssh-password' ./hack/novanas-nfs-setup.sh
./hack/install-csi-driver-nfs.sh
```

## Install the operator (Helm)

```bash
helm install ark-asa-operator deploy/helm/ark-asa-operator/ \
  -n ark-operator --create-namespace
```

CRDs are installed and kept in sync via a `pre-install,pre-upgrade` Helm hook
that runs `kubectl apply --server-side` against the chart-bundled CRD. No
manual CRD step needed for upgrades.

## Install the operator (kustomize, dev path)

```bash
make install                                    # installs the CRD
make deploy IMG=ark-asa-operator:dev            # deploys the manager
```

## Apply an ArkCluster

```bash
# Create the secret first (server password, optional admin password override)
kubectl -n ark-operator create secret generic example-secrets \
  --from-literal=serverPassword=changeMe

# Apply the CR
kubectl apply -f docs/examples/single-map.yaml
kubectl -n ark-operator get arkcluster -w
```

## Watch the server come up

```bash
kubectl -n ark-operator get pod -l ark.watteel.com/cluster=example -w
kubectl -n ark-operator logs -f -l ark.watteel.com/cluster=example
```

ARK SA takes several minutes to boot on first run (steamcmd download +
Proton/Wine bootstrap + map load). Subsequent restarts are faster.

## Connect from the ARK client

Once `kubectl -n ark-operator get arkcluster <name>` shows `phase=Running`
and `ready=1/1`, search for the session in ARK's "Unofficial PC Sessions"
browser, enter the server password from your Secret, and join.
