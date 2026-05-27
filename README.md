# ARK ASA Operator

Kubernetes operator for [ARK: Survival Ascended](https://store.steampowered.com/app/2399830/) dedicated servers, using the community [`sknnr/ark-ascended-server`](https://github.com/jsknnr/ark-ascended-server) image.

Status: **alpha** — Phases 1–4 implemented (single + multi-map, blue/green, CurseForge mod polling). Phase 0 prereqs (NFS + csi-driver-nfs) need to exist in the target cluster.

## Quick start

Prereqs:
- Kubernetes 1.30+
- A LoadBalancer controller (kube-vip, MetalLB, etc.) with an IP pool
- An RWX-capable StorageClass (we ship docs for csi-driver-nfs)
- Pod egress to `*.steampowered.com`

Deploy:
```bash
# via kustomize (development)
make install
make deploy IMG=ghcr.io/piwi3910/ark-asa-operator:dev

# via Helm (production)
helm install ark-asa-operator deploy/helm/ark-asa-operator/ \
  -n ark-operator --create-namespace
```

Apply a sample:
```bash
kubectl apply -f docs/examples/single-map.yaml
kubectl -n ark-operator get arkcluster -w
```

## Architecture

`ArkCluster` is a namespaced CR (`ark.watteel.com/v1alpha1`) that produces:
- One server `Pod` per map, owned directly by the ArkCluster
- One `LoadBalancer` Service per map (UDP game + TCP RCON)
- Three PVCs per map (server-a, server-b, saves) plus one shared RWX cluster-transfer PVC across maps
- A Secret with an auto-generated admin password
- Two ConfigMaps per map (GameUserSettings.ini, Game.ini)

Updates use a blue/green roll: install on the inactive server PVC via a steamcmd `Job`, RCON-announce drain, wait `gracefulShutdown`, swap, recreate pod. The drain deadline lives in `status` so operator restart is safe.

Optional: with `spec.modAutoUpdate.enabled`, a separate controller polls CurseForge every `intervalMinutes` and triggers a roll when a tracked mod's latestFileID changes.

## Documentation
- [Examples](docs/examples/) — single-map, multi-map with cluster transfers, mods+CurseForge, piwi's place
- [Design spec](docs/superpowers/specs/2026-05-27-ark-asa-operator-design.md)
- [Implementation plan](docs/superpowers/plans/2026-05-27-ark-asa-operator-implementation.md)
- [Migration from AngellusMortis operator](docs/migration-from-angellusmortis.md)

## Phases shipped
- **Phase 0** — NFS + csi-driver-nfs ops scripts (hack/)
- **Phase 1** — Single-map MVP; piwi's place online and playable
- **Phase 2** — Blue/green per-map updates, RCON drain, auto-rollback, drain-deadline persistence
- **Phase 3** — Multi-map fan-out, OneAtATime rollout, orphan GC
- **Phase 4** — CurseForge mod-update polling

## License
Apache 2.0
