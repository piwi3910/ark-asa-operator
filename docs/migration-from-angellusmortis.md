# Migrating from AngellusMortis/ark-operator

If you have the AngellusMortis ark-operator deployed and want to switch to this one:

## Why migrate
The AngellusMortis `ark-server` container image lacks the Wine/Proton runtime
dependencies that ARK SA's Steamworks SDK requires; servers crash on every
launch with a stack rooted in `lsteamclient.dll`. This operator standardizes on
the community `sknnr/ark-ascended-server` image, which works.

## Steps
1. Take a final save (if any) via RCON `SaveWorld` on each running pod.
2. Run the cleanup script:
   ```
   KUBECTL_CONTEXT=<your-context> NOVANAS_PASS=<sudo-pw> ./hack/migration-cleanup.sh
   ```
   This removes the AngellusMortis Deployment, ClusterRole/Binding, the CRD
   (`arkclusters.mort.is`), and the ArkCluster CR (which cascades pods +
   services + PVCs). kube-vip and the ark-operator namespace are preserved.
3. Set up NFS + csi-driver-nfs (one-time) per `docs/installation.md`.
4. Helm install this operator.
5. Apply a new `ArkCluster` CR using the new API group (`ark.watteel.com/v1alpha1`).

## Save preservation
The AngellusMortis saves PVC (`<cluster>-data`) won't be reused — schema and
mount layout differ. If you have meaningful save state, mount the old PVC
into a one-off pod and copy contents under `/srv/ark/data/...` to the new
operator's saves PVC, matching `/home/steam/ark/ShooterGame/Saved/`.
