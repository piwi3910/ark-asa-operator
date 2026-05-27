# ARK ASA Operator — Design Spec

**Date:** 2026-05-27
**Status:** Draft (awaiting user approval)
**Scope:** Phases 1–4 (single design, phased delivery)

---

## 1. Overview

A Kubernetes operator that orchestrates [ARK: Survival Ascended](https://store.steampowered.com/app/2399830/) dedicated servers using the [`sknnr/ark-ascended-server`](https://github.com/jsknnr/ark-ascended-server) community image. Each `ArkCluster` custom resource maps to one or more ARK maps that share a cluster ID for in-game character/dino transfers. The operator handles per-map blue/green updates, shared cluster-transfer storage, mod tracking via CurseForge, and graceful RCON-driven drains.

The project replaces an unsuccessful deployment of `AngellusMortis/ark-operator` whose bundled `ark-server` image lacks Wine/Proton runtime dependencies and crashes during Steamworks initialization on every launch. This design takes the operator concepts from that project but uses a community-vetted ARK server image and fixes specific reliability bugs (drain-timer-in-memory, no auto-rollback, in-place pod resource patching).

**Target environment for Phase 1 rollout:** `novanas`, a single-node k3s cluster on amd64 (192.168.10.203), already running kube-vip.

**Repo:** `github.com/piwi3910/ark-asa-operator` (Apache 2.0).

## 2. Goals & non-goals

### Goals

- Run one or more ARK SA dedicated servers on Kubernetes via a single `ArkCluster` resource.
- Per-map blue/green install updates with zero-data-loss volume swap.
- Cross-map cluster transfers via a shared RWX volume.
- Optional CurseForge mod-update polling that triggers controlled restarts.
- RCON-driven graceful drains with operator-restart-safe deadlines.
- Auto-rollback when a freshly-updated pod fails to come up.
- Standard Kubernetes ergonomics: conditions, events, finalizers, leader election, Prometheus metrics, Helm chart, multi-arch operator image.

### Non-goals

- Running the ARK Windows binary on arm64 (impossible — only the operator is multi-arch).
- Custom ARK server image. We standardize on `sknnr/ark-ascended-server`; if upstream renames an env var or breaks the contract, a single image-contract test catches it.
- Player auth / web UI / RBAC for in-game admin (handled by RCON tooling externally).
- Backup/restore of saves (out of scope; standard Kubernetes PVC backup tools apply).
- OperatorHub / OLM packaging (possible Phase 5).
- BattlEye integration (BattlEye disabled by default; not maintained in this operator).

## 3. Phase decomposition

This spec covers all four phases as a single design. Implementation is phased so each phase is independently testable and shippable.

| Phase | Scope | Exit criterion |
|---|---|---|
| **1 — Foundation + single-map MVP** | Repo skeleton, CRD shaped for multi-map but reconciler validates exactly one map; LoadBalancer Service via kube-vip; per-map saves PVC; shared cluster PVC (created but unused); pod recreate on spec change; no graceful shutdown yet. | "piwi's place" online and playable. |
| **2 — Blue/green per-map updates** | A/B server-install PVCs; steamcmd init Job for the inactive volume; RCON graceful-shutdown window; pod swap orchestration; build-ID tracking in status; auto-rollback on CrashLoop. | Spec change triggers blue/green roll; rollback works in fault injection. |
| **3 — Multi-map fan-out** | Multi-map reconciliation; per-map port allocation from `gamePortStart`; rollout policies (OneAtATime / Parallel); cluster-transfer validation across maps. | A 2-map ArkCluster with cluster transfers verified end-to-end. |
| **4 — Mod management** | CurseForge polling controller; mod set → `-mods=` launch flag; mod-update-triggered restarts; per-map mod overrides. | Mod version bump on CurseForge causes a controlled restart of affected maps within `intervalMinutes`. |

Phase 1 deliberately includes the multi-map CRD shape and the shared cluster PVC so Phases 2–3 are purely additive — no CRD breaking changes between phases.

## 4. API surface

### 4.1 GroupVersionKind

- API group: `ark.watteel.com`
- Version: `v1alpha1`
- Kind: `ArkCluster`
- Scope: Namespaced
- Short names: `arkc`, `ark`

### 4.2 Spec

```yaml
apiVersion: ark.watteel.com/v1alpha1
kind: ArkCluster
metadata:
  name: piwis-place
  namespace: ark-operator
spec:
  image: ghcr.io/sknnr/ark-ascended-server:latest   # pluggable; default in CRD
  clusterID: piwis-place                            # ARK in-game cluster identifier

  maps:                                             # one or more
    - id: TheIsland_WP                              # ARK map slug
      mods: [927090]                                # optional, per-map; overrides global
      gameUserSettings: { configMapRef: { name: piwis-place-gus-island } }
      game:            { configMapRef: { name: piwis-place-game-island } }

  globalSettings:
    sessionNameFormat: "piwi's place - {map}"       # {map} substitutes friendly map name
    serverPassword:  { secretRef: { name: piwis-place-secrets, key: serverPassword } }
    adminPassword:   { secretRef: { name: piwis-place-secrets, key: adminPassword } }   # auto-gen if absent
    battleye: false
    allowedPlatforms: [ALL]                         # ALL | PC | XSX | PS5 | WINGDK
    maxPlayers: 70
    mods: []                                        # cluster-wide defaults
    extraOptions: ["ForceAllowCaveFlyers"]          # composed with '-'
    extraParams:  ["AdminLogging"]                  # composed with '?'
    gameUserSettings: { configMapRef: { name: piwis-place-gus-default } }
    game:            { configMapRef: { name: piwis-place-game-default } }

  storage:
    storageClass: ""                                # for RWO server/saves PVCs; "" = cluster default. Example value on novanas: openebs-zfspv
    clusterStorageClass: nfs-csi                    # for RWX cluster PVC (default: nfs-csi)
    serverPVCSize: 50Gi                             # per server-A and server-B per map
    savesPVCSize:  20Gi                             # per map
    clusterPVCSize: 5Gi                             # shared across maps
    persistOnDelete: false                          # if true, leave PVCs after ArkCluster delete

  service:
    type: LoadBalancer
    gamePortStart: 7777                             # ports increment per map index
    rconPortStart: 27020
    loadBalancerIPs: []                             # optional pinning, one per map index

  resources:                                        # per server pod
    requests: { cpu: 2, memory: 12Gi }
    limits:   { cpu: 6, memory: 28Gi }

  updateStrategy:
    type: BlueGreen                                 # BlueGreen | Recreate
    gracefulShutdown: 30m                           # RCON drain window
    rollout: OneAtATime                             # OneAtATime | Parallel (multi-map only)

  modAutoUpdate:
    enabled: true
    intervalMinutes: 60
    curseForgeAPIKeyRef: { name: piwis-place-secrets, key: curseforge-api-key }

  nodeSelector: {}
  tolerations: []
  podSecurityContext:
    runAsUser: 10000
    runAsGroup: 10000
    fsGroup: 10000
```

### 4.3 Status

```yaml
status:
  phase: Running                                    # Initializing | Running | Updating | Degraded | Failed
  conditions:                                       # standard k8s conditions
    - { type: Ready,           status: "True",  reason: AllMapsReady,    lastTransitionTime: ... }
    - { type: Progressing,     status: "False", reason: SteadyState,     lastTransitionTime: ... }
    - { type: Available,       status: "True",  reason: AtLeastOneMap,   lastTransitionTime: ... }
    - { type: ModsHealthy,     status: "True",  reason: LastPollOK,      lastTransitionTime: ... }
    - { type: StorageHealthy,  status: "True",  reason: AllPVCsBound,    lastTransitionTime: ... }
  maps:
    - id: TheIsland_WP
      phase: Running                                # Pending | Provisioning | InstallingActive | InstallingInactive | Running | DrainingActive | Swapping | Failed
      activeVolume: server-a                        # server-a | server-b
      activeBuildID: "23366068"
      pendingBuildID: ""
      address: "192.168.10.210:7777"
      rconAddress: "192.168.10.210:27020"
      sessionName: "piwi's place - The Island"
      lastSaveTime: "2026-05-27T16:30:00Z"
      pod: piwis-place-island-7c4b6
      drainDeadline: null                           # RFC3339 when in DrainingActive; null otherwise
      conditions:
        - { type: PVCsReady,         status: "True" }
        - { type: InstallSucceeded,  status: "True" }
        - { type: PodReady,          status: "True" }
        - { type: RCONReachable,     status: "True" }
        - { type: RollbackOccurred,  status: "False" }
        - { type: MapUpdateBlocked,  status: "False" }
  mods:
    lastCheckTime: "2026-05-27T20:00:00Z"
    nextCheckTime: "2026-05-27T21:00:00Z"
    lastError: ""
    tracked:
      - id: 927090
        slug: structures-plus
        installedVersion: "5.4.2"
        installedFileID: 4827541
        latestVersion: "5.5.0"
        latestFileID: 4912100
        lastChanged: "2026-05-27T18:00:00Z"
        affectedMaps: [island]
```

### 4.4 Key API design choices

- **Secrets, not inline passwords.** `serverPassword`, `adminPassword`, `curseForgeAPIKeyRef` are `SecretKeyRef`s. Operator auto-generates `adminPassword` if the referenced key is missing.
- **ConfigMap refs for INI overrides.** Both `gameUserSettings` and `game` (Game.ini) take ConfigMap refs (not inline strings). Keys inside the ConfigMap: `GameUserSettings.ini`, `Game.ini`. Lets users reuse INIs across maps and keeps the CR small.
- **Port allocation:** `gamePortStart` + map index. Map 0 → 7777, map 1 → 7778, etc. RCON same with `rconPortStart`. Reordering or removing a map shifts ports of remaining maps — documented.
- **Multiple LB IPs:** `loadBalancerIPs: [ip0, ip1, ...]` pins per-map. If omitted, kube-vip's pool allocates sequentially.
- **`{map}` substitution in `sessionNameFormat`:** so a multi-map cluster gets `"piwi's place - The Island"`, etc.
- **`storageClass` vs `clusterStorageClass`:** separate fields because RWO and RWX often use different drivers. The operator validates `clusterStorageClass` supports RWX and surfaces `ClusterStorageClassInvalid` if not.

## 5. Components

### 5.1 In the operator binary

| Controller | Owns | Watches | Responsibility |
|---|---|---|---|
| **ArkClusterController** | `ArkCluster` | ArkCluster + owned Pods, Services, PVCs, Jobs, ConfigMaps, Secrets | Primary reconciler: storage, networking, pod lifecycle, blue/green orchestration. |
| **ModUpdateController** | (no CRD; updates ArkCluster status + annotation) | ArkCluster | Periodic CurseForge poll, mod-change detection, triggers main reconcile via annotation. Only active when `spec.modAutoUpdate.enabled`. |

### 5.2 Workloads the operator creates

- **Volume-init Jobs** — one-shot Jobs that run `steamcmd +app_update 2430930 validate` against the inactive server PVC. Uses the same `sknnr/ark-ascended-server` image with overridden `command:`.
- **Server Pods** — one per map, owned directly by `ArkCluster` (not via Deployment/StatefulSet). Direct ownership chosen so the operator controls A/B volume swap precisely. AngellusMortis precedent.
- **Services** — one `LoadBalancer` Service per map with `game` (UDP) + `rcon` (TCP) ports.
- **ConfigMaps** — operator materializes `GameUserSettings.ini` and `Game.ini` from user-provided ConfigMap refs (or defaults) and mounts them via `subPath` into the saves directory.
- **Secrets** — auto-generated `adminPassword` Secret per ArkCluster if user didn't provide one. RCON config Secret mounted at `/etc/rcon/config`.

### 5.3 External dependencies (not deployed by the operator)

- **kube-vip** (or any LoadBalancer controller) with an IP pool. Operator doesn't manage it; assumed present.
- **csi-driver-nfs** with an `nfs-csi` StorageClass (or any RWX class).
- **CurseForge API** (HTTPS), only contacted from ModUpdateController, only when enabled.

## 6. Reconciliation model

### 6.1 ArkCluster-level state machine

```
Pending → Initializing → Running ⇄ Updating → Running
                                ↘ Degraded (transient failure)
                                ↘ Failed   (terminal failure)
```

`status.phase` is the cluster aggregate; `status.maps[i].phase` reflects each map independently.

### 6.2 Per-map state machine

```
Pending → Provisioning → InstallingActive → Running (active=server-a)
                       │                       │
                       └─ fail → Failed       update triggered
                                                │
                                                ▼
                                          InstallingInactive (server-b)
                                                │ ok
                                                ▼
                                          DrainingActive (RCON window; deadline persisted)
                                                │ pod gone
                                                ▼
                                          Swapping → Running (active=server-b)
                                                ↑
                                          (next update reuses server-a)
```

### 6.3 Per-cluster reconcile pseudocode

```
Reconcile(req):
  cluster ← Get(ArkCluster)
  if !cluster.exists or DeletionTimestamp set:
    runFinalizer(cluster)              // RCON SaveWorld + DoExit, then release resources
    return

  // 1. Cluster-level resources
  reconcileAdminPasswordSecret(cluster)
  reconcileClusterPVC(cluster)
  reconcileServerINIConfigMaps(cluster)

  // 2. Per-map fan-out
  for i, mapSpec in cluster.Spec.Maps:
    desired ← computeDesiredMap(cluster, mapSpec, i)
    current ← observeCurrentMap(cluster, mapSpec.id)
    reconcileMap(cluster, desired, current)

  // 3. Garbage-collect maps no longer in spec
  for orphan owned resources where map removed:
    delete(orphan) honoring drain semantics

  // 4. Aggregate status
  updateStatus(cluster)

  // 5. Time-based requeue
  return requeueAfter(perPhaseRequeue(cluster))
```

### 6.4 reconcileMap pseudocode

```
reconcileMap(cluster, desired, current):
  // PVCs (3 per map: server-a, server-b, saves)
  ensurePVC(server-a)
  ensurePVC(server-b)
  ensurePVC(saves)

  // Service
  ensureService(map, gamePort, rconPort, lbIP[i?])

  // First-run install: pick server-a
  if current.activeVolume == "":
    ensureInitJob(server-a)
    if !jobComplete: requeue
    setStatus(activeVolume=server-a)

  // Detect drift
  desiredHash ← hash(image, mods, ports, secrets-rev, ini-rev, ...)
  if current.podHash == desiredHash and current.pod healthy:
    return                  // steady state

  // Update path
  inactive ← otherOf(server-a, server-b)
  desiredBuildID ← steamcmdLatestBuildID(cached, refreshed every N min)

  // Step A: prepare inactive
  if status.pendingBuildID != desiredBuildID:
    ensureInitJob(inactive, validate=true)
    if !jobComplete: requeue
    setStatus(pendingBuildID=desiredBuildID)

  // Step B: drain active
  if updateStrategy.type == BlueGreen:
    if current.pod.Running:
      if status.drainDeadline == nil:
        rconAnnounceShutdown(gracefulShutdown)
        setStatus(drainDeadline=now+gracefulShutdown, phase=DrainingActive)
        return requeueAfter(min(gracefulShutdown, 60s))
      if now < status.drainDeadline:
        return requeueAfter(min(drainDeadline-now, 60s))
      // deadline reached
      rconSaveAndExit(current.pod)
      deletePod(current.pod)
      setStatus(phase=Swapping)
      return requeueAfter(5s)

  // Step C: swap
  setStatus(activeVolume=inactive, activeBuildID=pendingBuildID, pendingBuildID="", drainDeadline=nil)

  // Step D: create new pod with the new active volume
  createPod(map, activeVolume=inactive)
  setStatus(phase=Running)
```

### 6.5 Rollout across maps

- `rollout: OneAtATime` (default): only one map in `DrainingActive` or `Swapping` at a time. Others wait. Lets players in-game transfer characters to a still-up map before that map drains.
- `rollout: Parallel`: all affected maps drain in parallel. Faster but no in-game escape during the window.

### 6.6 Auto-rollback

When a freshly-swapped pod crash-loops (≥ N restarts in M seconds — default N=3, M=300):

1. Operator delete the failing pod.
2. Operator swaps `activeVolume` back to the previous value.
3. Operator recreates the pod with the old active volume.
4. Sets `status.maps[i].conditions[RollbackOccurred] = True` and emits an event.
5. Does NOT keep trying the failed update. User must change spec or clear the rollback condition manually to retry.

### 6.7 Drain-deadline persistence

`status.maps[i].drainDeadline` (RFC3339) is the *exclusive* source of truth for in-flight drain timers. The operator never holds drain state in memory. On every reconcile, the controller compares `now` to `drainDeadline` and either requeues until the deadline or proceeds. This is the explicit fix for the AngellusMortis bug where in-memory timers were reset by operator restart.

### 6.8 Finalization

- Finalizer: `ark.watteel.com/graceful-shutdown`.
- On delete:
  1. For each running map: RCON `SaveWorld` → `DoExit`, wait for pod termination up to `terminationGracePeriodSeconds`.
  2. Remove finalizer; GC cascades to PVCs/Services/etc. via owner refs.
- `spec.storage.persistOnDelete: true` ⇒ finalizer strips owner refs from PVCs before removing itself, so volumes survive deletion.
- Hard delete timeout: 10 min (operator flag `--graceful-delete-timeout`). After timeout, finalizer is removed unconditionally to prevent stuck deletions.

## 7. Storage

### 7.1 PVC topology

| Name template | Quantity | Access | Pod mount | Contains |
|---|---|---|---|---|
| `<cluster>-<map>-server-a` | one per map | RWO | `/home/steam/ark` (when active) | ARK install (~30 GB) |
| `<cluster>-<map>-server-b` | one per map | RWO | `/home/steam/ark` (when active) | ARK install (alternate) |
| `<cluster>-<map>-saves` | one per map | RWO | `/home/steam/ark/ShooterGame/Saved` | Saves, configs, logs, crashstacks |
| `<cluster>-cluster` | one per ArkCluster | RWX | `/srv/ark/cluster` | Cross-map character transfers |

Saves overlay the install dir via subPath mount so A↔B swap never affects player progress. Cluster PVC is mounted into every map's pod; ARK launched with `-ClusterDirOverride=/srv/ark/cluster -clusterid=<spec.clusterID>`.

### 7.2 StorageClass selection

- `spec.storage.storageClass` for RWO PVCs (server, saves).
- `spec.storage.clusterStorageClass` for RWX cluster PVC. Default `nfs-csi`.
- Operator validates the cluster StorageClass supports RWX (by reading its `allowedAccessModes` or by attempting an RWX PVC and inspecting events). Reports `ClusterStorageClassInvalid` condition on failure.

### 7.3 Reclaim & A/B reuse

- A/B reuse: after successful swap to `server-b`, `server-a` is kept (not wiped) for instant rollback. The next upgrade reuses `server-a` — storage bounded to 2× install size per map.
- `persistOnDelete: false` (default): finalizer cascades; PVCs deleted per StorageClass reclaim policy (typically `Delete`).
- `persistOnDelete: true`: finalizer strips owner refs; PVCs survive.

### 7.4 Phase 1 sizing on novanas

`2 × 50 GiB + 20 GiB + 5 GiB = 125 GiB` for one-map "piwi's place" — within novanas's 813 GiB free.

## 8. Networking

### 8.1 Service per map

```yaml
type: LoadBalancer
ports:
  - { name: game, port: 7777, targetPort: 7777, protocol: UDP }
  - { name: rcon, port: 27020, targetPort: 27020, protocol: TCP }
externalTrafficPolicy: Cluster      # default; override per ArkCluster via spec.service.externalTrafficPolicy
loadBalancerIP: <pinned or omit>
```

Default `externalTrafficPolicy: Cluster`. ARK SA authenticates players via Steam tickets, not source IPs (BattlEye disabled by default in this operator), so preserving client IPs is not a feature requirement. Cluster mode also works correctly on multi-node clusters where the LB-advertising node and the pod's node may differ. Opt back into `Local` per ArkCluster via `spec.service.externalTrafficPolicy: Local` if you want the cleaner topology on a single-node cluster.

### 8.2 Port allocation

Map index `i` → `gamePort = gamePortStart + i`, `rconPort = rconPortStart + i`. Reordering or removing a map shifts ports of remaining maps; the resulting drift is detected by the per-pod hash and triggers normal blue/green reconciliation (announce → drain → swap) for affected maps.

### 8.3 LB IP allocation

- If `spec.service.loadBalancerIPs[i]` set: pinned, operator validates IP is in kube-vip's pool (best-effort against the `kubevip` ConfigMap).
- Otherwise kube-vip allocates from the pool.
- Operator records the assigned IP in `status.maps[i].address`.

### 8.4 RCON exposure

RCON port is exposed via the LB. Admin password kept in a Secret; never on the command line. To restrict to in-cluster only later, add `spec.service.exposeRCON: false` (not in Phase 1).

### 8.5 Operator metrics & health

- `/metrics` on :8080 (Prometheus)
- `/healthz` on :8081 (manager + cache sync)
- `/readyz` on :8081 (last reconcile within 2× RequeueAfter)

## 9. Server Pod

### 9.1 Template (one per map)

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: piwis-place-island-<generation>
  labels:
    ark.watteel.com/cluster: piwis-place
    ark.watteel.com/map: island
    ark.watteel.com/role: server
    ark.watteel.com/active-volume: server-a
    ark.watteel.com/pod-template-hash: <hash>
  ownerReferences:
    - { apiVersion: ark.watteel.com/v1alpha1, kind: ArkCluster, name: piwis-place, controller: true, blockOwnerDeletion: true }
spec:
  restartPolicy: Always
  terminationGracePeriodSeconds: 1800
  securityContext: { runAsUser: 10000, runAsGroup: 10000, fsGroup: 10000 }
  containers:
    - name: ark
      image: <spec.image>
      env:
        SESSION_NAME, SERVER_MAP, GAME_PORT, RCON_PORT,
        SERVER_PASSWORD (Secret), SERVER_ADMIN_PASSWORD (Secret),
        EXTRA_SETTINGS, EXTRA_FLAGS, MODS
      ports:
        - { name: game, containerPort: 7777, protocol: UDP }
        - { name: rcon, containerPort: 27020, protocol: TCP }
      resources: <spec.resources>
      startupProbe: { tcpSocket: rcon, initialDelaySeconds: 60, periodSeconds: 10, failureThreshold: 60 }   # 10-min budget for cold start
      readinessProbe: { tcpSocket: rcon, periodSeconds: 15, timeoutSeconds: 10, failureThreshold: 4 }
      livenessProbe: { exec: rcon ListPlayers, periodSeconds: 30, timeoutSeconds: 10, failureThreshold: 10 }   # 5-min unresponsiveness budget
      lifecycle:
        preStop:
          exec: ["/bin/sh", "-c", "rcon SaveWorld; rcon DoExit"]   # bash /dev/tcp client
      volumeMounts:
        - { server, /home/steam/ark }
        - { saves, /home/steam/ark/ShooterGame/Saved }
        - { cluster-xfer, /srv/ark/cluster }
        - { gus-ini (subPath), /home/steam/ark/ShooterGame/Saved/Config/WindowsServer/GameUserSettings.ini, ro }
        - { game-ini (subPath), /home/steam/ark/ShooterGame/Saved/Config/WindowsServer/Game.ini, ro }
        - { rcon-config, /etc/rcon, ro }
  volumes:
    - server: PVC <cluster>-<map>-server-<a|b>   # whichever is currently active
    - saves: PVC <cluster>-<map>-saves
    - cluster-xfer: PVC <cluster>-cluster
    - gus-ini: ConfigMap
    - game-ini: ConfigMap
    - rcon-config: Secret
```

### 9.2 Key choices

- **Generous RCON-exec livenessProbe.** Calls `rcon -a 127.0.0.1:${RCON_PORT} -p "${SERVER_ADMIN_PASSWORD}" ListPlayers`. The `rcon` binary is already present in the `sknnr/ark-ascended-server` image (gorcon's `rcon-cli` at `/usr/local/bin/rcon`). 30-second period × 10 failure threshold = 5-minute budget before kubelet restarts the container, generous enough to absorb legitimate slow ticks (SaveWorld during heavy load, dense dino spawns). Gated by the startup probe, so a slow-loading server isn't killed while still booting. Liveness restart goes through the container's `preStop` (RCON SaveWorld + DoExit), so we get graceful behaviour for free. Auto-rollback on CrashLoop (R≥3 in 5 min) remains operator-driven and is layered on top.
- **`preStop` runs RCON SaveWorld + DoExit.** Two-layer drain: operator pre-delete RCON announce + container-level preStop. `terminationGracePeriodSeconds: 1800` covers the longest sane window.
- **RCON over plain TCP from a bash script in the image.** No external CLI dep; bash `/dev/tcp` writes the RCON protocol frames. Implementation under `internal/rcon` (see Section 13).
- **Admin password in a Secret mounted at `/etc/rcon/config`** so probe + preStop don't expand env vars on the command line.
- **Pod template hash label** drives drift detection.
- **Generation suffix** in pod name prevents stale-pod races during fast swap.

### 9.3 Init Job template

```yaml
apiVersion: batch/v1
kind: Job
metadata: { name: <cluster>-<map>-init-<volume>-<generation>, ownerRefs: ArkCluster }
spec:
  backoffLimit: 3
  ttlSecondsAfterFinished: 3600
  template:
    spec:
      restartPolicy: OnFailure
      securityContext: { runAsUser: 10000, runAsGroup: 10000, fsGroup: 10000 }
      containers:
        - name: install
          image: <same as server image>
          command: ["/bin/bash", "-c"]
          args:
            - |
              set -euo pipefail
              steamcmd \
                +@sSteamCmdForcePlatformType windows \
                +force_install_dir /home/steam/ark \
                +login anonymous \
                +app_update 2430930 validate \
                +quit
          volumeMounts:
            - { name: server, mountPath: /home/steam/ark }
      volumes:
        - { name: server, persistentVolumeClaim: { claimName: <cluster>-<map>-server-<inactive> } }
```

Init Jobs only mount the inactive volume; the running pod has the active volume. No RWO contention.

## 10. CurseForge mod-update controller

### 10.1 Flow

```
ModUpdateController                                 ArkClusterController
─────────────────                                   ─────────────────
  every intervalMinutes per cluster
        │
        ▼
  GetFiles(trackedModIDs, apiKey)
        │
        ▼
  diff vs status.mods.tracked
   ├── no change → patch status.mods.lastCheckTime
   └── change   → patch status.mods.tracked +
                  annotate(ark.watteel.com/mods-changed: <sha>)
                                                                  │
                                                                  ▼
                                                          reconcile triggered
                                                          (annotation ≠ last label)
                                                                  │
                                                                  ▼
                                                          determine affected maps,
                                                          enter Updating phase,
                                                          blue/green flow runs.
                                                                  │
                                                                  ▼ on swap success
                                                          label(ark.watteel.com/mods-applied: <sha>)
```

### 10.2 Status fields

```yaml
status.mods:
  lastCheckTime, nextCheckTime, lastError
  tracked:
    - id, slug, installedVersion, installedFileID,
      latestVersion, latestFileID, lastChanged, affectedMaps
```

### 10.3 Important note on `-mods=` semantics

ARK SA passes mod *IDs* (not file IDs) in `-mods=`; ARK itself resolves to the latest file. The operator's job is purely **deciding when to restart** to pick up new versions. `installedFileID` in status is informational.

### 10.4 Mod ↔ map affinity

- `spec.globalSettings.mods` applies to all maps not overriding `maps[i].mods`.
- `spec.maps[i].mods` replaces (does NOT merge with) globals for that map.
- Future: BobsMissions_WP (Club Ark) special-cased exclusion.

### 10.5 Failure modes

| Failure | Reaction |
|---|---|
| CurseForge 5xx | Backoff 3×, then `ModUpdateFailed`, retry at interval |
| 401/403 | `ModUpdateFailed` reason `APIKeyInvalid`; pause until Secret updates |
| Network blocked | Same as 5xx |
| Mod not found | Mark `NotFound`, omit from `-mods=`, warning event |
| All required mods unavailable for a map | `MapUpdateBlocked`, abort that map's update |

### 10.6 API key handling

API key only read inside ModUpdateController. Never logged. Missing Secret → `ModUpdateFailed` `APIKeyMissing` until Secret appears (Secret watch triggers re-reconcile).

## 11. Error handling & status

### 11.1 Cluster-level conditions

| Type | True | False |
|---|---|---|
| `Ready` | All maps Ready, all PVCs Bound, all Services have an IP, no errors | Anything broken |
| `Progressing` | Any map mid-update | Steady state |
| `Available` | ≥ 1 map serving players | Zero maps up |
| `ModsHealthy` | Last CurseForge poll OK | Last poll failed or any `MapUpdateBlocked` |
| `StorageHealthy` | All PVCs Bound, RWX class valid | Any PVC stuck or RWX invalid |

`Ready` and `Available` are decoupled. During a rolling update across maps, the cluster is `Ready=False, Progressing=True, Available=True`.

### 11.2 Per-map conditions

`PVCsReady, InstallSucceeded, PodReady, RCONReachable, RollbackOccurred, MapUpdateBlocked`.

### 11.3 Reason codes (controlled vocabulary)

`Initializing, Provisioning, InstallingActive, InstallingInactive, Running, Updating, Draining, Swapping, PVCPending, PVCFailed, StorageClassInvalid, ClusterStorageClassInvalid, InstallJobFailed, InstallJobTimeout, InstallSucceeded, RCONUnreachable, RCONTimeout, DrainAnnounceFailed, PodCrashLooping, RollbackPerformed, ModAPIKeyMissing, ModAPIKeyInvalid, ModAPIError, ModNotFound, LoadBalancerPending, LoadBalancerAssigned`.

Constants in code; greppable; one Event per transition.

### 11.4 Event budget

Emit on transitions and key milestones (init Job start/success/fail, blue/green swap, CurseForge poll failure dedup'd within 1h). Never per-reconcile.

### 11.5 Requeue strategy

| Map phase | Requeue |
|---|---|
| Running, no drift | 5 min |
| Provisioning / init running | 15s |
| DrainingActive | min(drainDeadline - now, 60s) |
| Swapping | 5s |
| Blocked on error | exponential backoff 30s → 10 min |

### 11.6 Metrics

- `ark_cluster_phase{cluster, phase}` (gauge 0/1)
- `ark_map_phase{cluster, map, phase}` (gauge 0/1)
- `ark_init_job_duration_seconds{cluster, map, volume}` (histogram)
- `ark_blue_green_swap_total{cluster, map, result}` (counter)
- `ark_mod_poll_total{cluster, result}` (counter)
- `ark_mod_poll_duration_seconds` (histogram)
- Plus standard controller-runtime metrics

## 12. Testing

### 12.1 Unit (`go test ./...`)

Pure functions: launch-command composition, port allocation, hash computation, condition transition table, status diff helpers. ≥ 80% coverage in pkg/ and internal/. Runs on every commit.

### 12.2 Controller tests (envtest)

`sigs.k8s.io/controller-runtime/pkg/envtest` — local kube-apiserver + etcd, no kubelet. Covers every reconciliation path including blue/green state machine, auto-rollback, finalizer, mod-update annotation flow. Ginkgo + Gomega.

### 12.3 End-to-end

- **Default e2e** in CI: `kind` cluster + MetalLB (kube-vip substitute) + csi-driver-nfs (in-pod NFS server) + Prometheus + a **fake-ark-server image** (`test/fake-ark-server/`) honoring the same env contract. No real ARK in CI.
- **Manual e2e** (`make e2e-real`): runs against any `KUBECONFIG` with the real `sknnr/ark-ascended-server` image. Gates releases.

### 12.4 Image-contract test

`test/image-contract/` runs the real `sknnr/ark-ascended-server` image with a minimal env, asserts launch command composed and `ShooterGame.log` produced. Single point of upstream dependency.

### 12.5 AngellusMortis regression test (named, permanent)

Start update on a 2-map cluster, wait until map[0] enters `DrainingActive` with `drainDeadline = now + 5min`. Kill operator pod. Assert: new operator picks up the same deadline (not a fresh 5 min), no second RCON announce, swap happens at the original deadline.

### 12.6 CI matrix

- Go: 1.23, 1.24
- K8s (envtest): 1.30, 1.32, 1.34
- Arch: linux/amd64 (primary) + linux/arm64 (smoke; no full e2e)

## 13. Repo layout

```
ark-asa-operator/
├── .github/workflows/      # ci, e2e, release, codeql
├── api/v1alpha1/           # CRD Go types
├── cmd/operator/main.go    # binary entrypoint
├── internal/
│   ├── controller/         # arkcluster + modupdate reconcilers
│   ├── ark/                # pure helpers (launchcmd, ports, modset)
│   ├── reconcile/          # pvc, service, pod, job, configmap, secret, status
│   ├── statemachine/       # explicit transition tables
│   ├── rcon/               # RCON client (plain TCP, no external deps)
│   ├── curseforge/         # client + fake server for tests
│   └── finalizer/
├── config/                 # kubebuilder-generated CRD/RBAC/manager manifests
├── deploy/helm/ark-asa-operator/   # Helm chart
├── docs/                   # README, installation, architecture, crd-reference, examples
├── hack/                   # boilerplate + tools
├── test/
│   ├── e2e/
│   ├── fake-ark-server/    # tiny fake image for CI
│   └── image-contract/     # validates real image still meets contract
├── PROJECT
├── Dockerfile
├── Makefile
├── go.mod / go.sum
├── LICENSE                 # Apache 2.0 (exists)
└── README.md
```

Makefile targets (subset): `generate, manifests, lint, test, test-unit, test-envtest, e2e, e2e-real, build, docker-build, docker-build-fake, helm-package, crd-ref-docs, install-tools`.

## 14. CI

| Workflow | Trigger | Job |
|---|---|---|
| `ci.yml` | PR, main | lint + unit + envtest matrix (Go 1.23/1.24 × K8s 1.30/1.32/1.34); Codecov upload |
| `e2e.yml` | PR, nightly | kind + MetalLB + csi-driver-nfs + Prometheus; fake-ark-server image; full e2e |
| `release.yml` | `v*` tag | multi-arch buildx (amd64, arm64) → ghcr.io; Helm package; GitHub Release with assets |
| `codeql.yml` | PR, weekly | CodeQL Go |

Branch protection on `main`: required checks (lint, test, envtest, e2e); 1 reviewer; no force push.

Versioning: semver. CRD `v1alpha1` for all of Phase 1–4. Per-release pre-tag convention `vX.Y.Z-pre.N`; clean `vX.Y.Z` only on explicit instruction (per user preference).

## 15. Deployment artifacts

### 15.1 Three install paths

1. **Plain manifests** — `kubectl apply -k config/default/` or single-file render attached to release.
2. **Helm chart** — `deploy/helm/ark-asa-operator/`; published as tarball + OCI to `ghcr.io/piwi3910/charts/ark-asa-operator`.
3. **OperatorHub/OLM** — out of scope for Phases 1–4.

### 15.2 Helm values (highlights)

```yaml
image:
  repository: ghcr.io/piwi3910/ark-asa-operator
  tag: ""                              # defaults to chart appVersion
installCRDs: true
namespace: ark-operator
serviceAccount: { create: true, name: "" }
rbac: { create: true }
resources:
  requests: { cpu: 100m, memory: 128Mi }
  limits:   { cpu: 500m, memory: 512Mi }
metrics:
  enabled: true
  serviceMonitor: { enabled: false, labels: {} }
webhook: { enabled: false }
leaderElection: { enabled: true }
logLevel: info
curseforgeBaseURL: ""                  # "" = real CurseForge; tests override
```

### 15.3 CRD upgrade policy

The chart includes a `pre-install,pre-upgrade` Helm hook (a one-shot Job using a stock `bitnami/kubectl` image with cluster-scoped CRD permissions) that applies the chart-bundled CRD via `kubectl apply --server-side --force-conflicts`. The CRD file is loaded by the chart via `.Files.Get` from `deploy/helm/ark-asa-operator/files/` (note: deliberately not in `crds/`, which Helm treats as install-only). With this, `helm upgrade` keeps the CRD in sync with the operator version automatically — no manual `kubectl apply` step required.

### 15.4 Multi-arch images

Operator: linux/amd64 + linux/arm64 via buildx. ARK server pods: amd64 only (limitation of ARK binary).

### 15.5 Kryton mirror

Per user preference: optional mirror to `harbor.kw.local/ark-asa-operator` on each `v*` tag. Controlled by workflow input; off by default unless user requests it on.

### 15.6 Documented install order

1. Verify K8s 1.30+, kube-vip (with IP pool), csi-driver-nfs (or any RWX class), egress to `*.steampowered.com`.
2. If no RWX storage: set up NFS server + csi-driver-nfs (separate ops doc).
3. `helm install ark-asa-operator oci://ghcr.io/piwi3910/charts/ark-asa-operator -n ark-operator --create-namespace`.
4. Create Secret with `serverPassword`, optional `adminPassword`, optional `curseforge-api-key`.
5. Apply an `ArkCluster` CR.
6. `kubectl get arkcluster -n ark-operator -w`.

## 16. Migration & cleanup from AngellusMortis

### 16.1 Resources to remove on novanas

| Resource | Action |
|---|---|
| `Deployment ark-operator/ark-operator` | Delete |
| `ArkCluster ark-operator/piwis-place` (CRD `arkclusters.mort.is`) | Delete |
| `CustomResourceDefinition arkclusters.mort.is` | Delete |
| ClusterRole/ClusterRoleBinding for AngellusMortis operator | Delete |
| Role/RoleBinding for AngellusMortis operator | Delete |
| `ServiceAccount ark-operator/ark-operator` | Delete |
| Owned Pod / Services / PVCs | Cascades from ArkCluster delete |
| `ghcr.io/angellusmortis/ark-server:v0.10.7*` in containerd | `crictl rmi` (optional) |
| `/tmp/ark-patch/` on novanas | `rm -rf` (optional) |
| `kube-vip-cloud-provider` + `kube-vip-ds` + `kubevip` ConfigMap | **Keep** |
| `Namespace ark-operator` | **Keep** — new operator deploys here |

### 16.2 Save preservation

Not needed. The piwis-place game was never actually played (Steamworks-init crash kept the server from accepting connections). Delete the saves PVC outright.

### 16.3 Cleanup sequence

```bash
kubectl --context novanas -n ark-operator delete arkcluster piwis-place
kubectl --context novanas -n ark-operator delete deployment ark-operator
kubectl --context novanas delete crd arkclusters.mort.is
kubectl --context novanas delete clusterrole ark-operator-games-role-cluster
kubectl --context novanas delete clusterrolebinding ark-operator-games-rolebinding-cluster
kubectl --context novanas -n ark-operator delete role ark-operator-role-namespaced
kubectl --context novanas -n ark-operator delete rolebinding ark-operator-rolebinding-namespaced
kubectl --context novanas -n ark-operator delete serviceaccount ark-operator
# Verify
kubectl --context novanas -n ark-operator get all,pvc,configmap,secret
# Optional host-side
ssh piwi@192.168.10.203 'sudo crictl rmi ghcr.io/angellusmortis/ark-server:v0.10.7-patched ghcr.io/angellusmortis/ark-server:v0.10.7 ghcr.io/angellusmortis/ark-operator:v0.10.7 || true'
ssh piwi@192.168.10.203 'rm -rf /tmp/ark-patch'
```

### 16.4 When this runs

Not automated. Manual one-time step performed as part of the Phase 1 rollout, immediately before installing the new operator's Helm chart. A short runbook (`docs/migration-from-angellusmortis.md`) is produced during implementation.

### 16.5 Rollback

If the new operator install fails badly, AngellusMortis manifests are preserved at `/tmp/ark-operator/` on the dev machine; reapply to restore the broken-but-known state. Limited rollback value since AngellusMortis can't run the game; primarily useful for capturing diagnostic state.

## 17. Open questions

- API group `ark.watteel.com` chosen by default; happy to switch to `ark.piwi3910.dev` (or similar) if you'd prefer not to use your email domain.
- Default `intervalMinutes` for CurseForge polling: 60. Reasonable starting point; revisit after Phase 4 ships.
- `terminationGracePeriodSeconds: 1800` matches the max sane drain window. Override per-spec if shorter drains are desired.
- Mirror to `harbor.kw.local`: off by default in release workflow; you can flip on at release time.
