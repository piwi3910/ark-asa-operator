// Copyright 2026 Pascal Watteel.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=arkc;ark,categories=ark
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Maps",type=integer,JSONPath=`.status.totalMaps`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyMaps`
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=`.metadata.creationTimestamp`
type ArkCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ArkClusterSpec   `json:"spec,omitempty"`
	Status ArkClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ArkClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ArkCluster `json:"items"`
}

// ArkClusterSpec is the desired state.
type ArkClusterSpec struct {
	// +kubebuilder:default="ghcr.io/sknnr/ark-ascended-server:latest"
	Image string `json:"image,omitempty"`

	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	ClusterID string `json:"clusterID"`

	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	Maps []MapSpec `json:"maps"`

	// +kubebuilder:default={}
	GlobalSettings GlobalSettings `json:"globalSettings,omitempty"`

	// +kubebuilder:default={}
	Storage StorageSpec `json:"storage,omitempty"`

	// +kubebuilder:default={}
	Service ServiceSpec `json:"service,omitempty"`

	// +kubebuilder:default={}
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// +kubebuilder:default={}
	UpdateStrategy UpdateStrategy `json:"updateStrategy,omitempty"`

	// +optional
	ModAutoUpdate *ModAutoUpdateSpec `json:"modAutoUpdate,omitempty"`

	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
	// +optional
	PodSecurityContext *corev1.PodSecurityContext `json:"podSecurityContext,omitempty"`
}

type MapSpec struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern="^[A-Za-z0-9_-]+$"
	ID string `json:"id"`

	// +optional
	Mods []int64 `json:"mods,omitempty"`

	// +optional
	GameUserSettings *ConfigMapRef `json:"gameUserSettings,omitempty"`
	// +optional
	Game *ConfigMapRef `json:"game,omitempty"`
}

type GlobalSettings struct {
	// +kubebuilder:default="{cluster} - {map}"
	SessionNameFormat string `json:"sessionNameFormat,omitempty"`

	// +optional
	ServerPassword *corev1.SecretKeySelector `json:"serverPassword,omitempty"`
	// +optional
	AdminPassword *corev1.SecretKeySelector `json:"adminPassword,omitempty"`

	// +kubebuilder:default=false
	BattlEye bool `json:"battleye,omitempty"`

	// +kubebuilder:default={ALL}
	AllowedPlatforms []string `json:"allowedPlatforms,omitempty"`

	// +kubebuilder:default=70
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=200
	MaxPlayers int32 `json:"maxPlayers,omitempty"`

	// +optional
	Mods []int64 `json:"mods,omitempty"`

	// +optional
	ExtraOptions []string `json:"extraOptions,omitempty"`
	// +optional
	ExtraParams []string `json:"extraParams,omitempty"`

	// +optional
	GameUserSettings *ConfigMapRef `json:"gameUserSettings,omitempty"`
	// +optional
	Game *ConfigMapRef `json:"game,omitempty"`
}

type ConfigMapRef struct {
	// +kubebuilder:validation:Required
	Name string `json:"name"`
}

type StorageSpec struct {
	// +optional
	StorageClass string `json:"storageClass,omitempty"`

	// +kubebuilder:default="nfs-csi"
	ClusterStorageClass string `json:"clusterStorageClass,omitempty"`

	// +kubebuilder:default="50Gi"
	ServerPVCSize string `json:"serverPVCSize,omitempty"`

	// +kubebuilder:default="20Gi"
	SavesPVCSize string `json:"savesPVCSize,omitempty"`

	// +kubebuilder:default="5Gi"
	ClusterPVCSize string `json:"clusterPVCSize,omitempty"`

	// +kubebuilder:default=false
	PersistOnDelete bool `json:"persistOnDelete,omitempty"`
}

type ServiceSpec struct {
	// +kubebuilder:default="LoadBalancer"
	// +kubebuilder:validation:Enum=LoadBalancer;NodePort;ClusterIP
	Type corev1.ServiceType `json:"type,omitempty"`

	// +kubebuilder:default=7777
	// +kubebuilder:validation:Minimum=1024
	// +kubebuilder:validation:Maximum=60000
	GamePortStart int32 `json:"gamePortStart,omitempty"`

	// +kubebuilder:default=27020
	// +kubebuilder:validation:Minimum=1024
	// +kubebuilder:validation:Maximum=60000
	RconPortStart int32 `json:"rconPortStart,omitempty"`

	// +optional
	LoadBalancerIPs []string `json:"loadBalancerIPs,omitempty"`

	// ExternalTrafficPolicy is the LoadBalancer/NodePort externalTrafficPolicy.
	// Defaults to Cluster — ARK SA authenticates players via Steam tickets,
	// not source IPs, so client IP preservation is not required and Cluster
	// works correctly on multi-node clusters where the LB-advertising node
	// and the pod's node may differ. Opt back into Local on a single-node
	// cluster if you want the cleaner topology.
	// +kubebuilder:default="Cluster"
	// +kubebuilder:validation:Enum=Cluster;Local
	ExternalTrafficPolicy corev1.ServiceExternalTrafficPolicy `json:"externalTrafficPolicy,omitempty"`
}

type UpdateStrategy struct {
	// +kubebuilder:default="BlueGreen"
	// +kubebuilder:validation:Enum=BlueGreen;Recreate
	Type UpdateStrategyType `json:"type,omitempty"`

	// +kubebuilder:default="30m"
	GracefulShutdown metav1.Duration `json:"gracefulShutdown,omitempty"`

	// +kubebuilder:default="OneAtATime"
	// +kubebuilder:validation:Enum=OneAtATime;Parallel
	Rollout RolloutPolicy `json:"rollout,omitempty"`
}

// +kubebuilder:validation:Enum=BlueGreen;Recreate
type UpdateStrategyType string

const (
	UpdateStrategyBlueGreen UpdateStrategyType = "BlueGreen"
	UpdateStrategyRecreate  UpdateStrategyType = "Recreate"
)

// +kubebuilder:validation:Enum=OneAtATime;Parallel
type RolloutPolicy string

const (
	RolloutOneAtATime RolloutPolicy = "OneAtATime"
	RolloutParallel   RolloutPolicy = "Parallel"
)

type ModAutoUpdateSpec struct {
	// +kubebuilder:default=true
	Enabled bool `json:"enabled,omitempty"`

	// +kubebuilder:default=60
	// +kubebuilder:validation:Minimum=5
	IntervalMinutes int32 `json:"intervalMinutes,omitempty"`

	// +kubebuilder:validation:Required
	CurseForgeAPIKeyRef corev1.SecretKeySelector `json:"curseForgeAPIKeyRef"`
}

// ArkClusterStatus is the observed state.
type ArkClusterStatus struct {
	// +optional
	Phase ClusterPhase `json:"phase,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// +optional
	Maps []MapStatus `json:"maps,omitempty"`
	// +optional
	Mods *ModStatus `json:"mods,omitempty"`
	// +optional
	TotalMaps int32 `json:"totalMaps,omitempty"`
	// +optional
	ReadyMaps int32 `json:"readyMaps,omitempty"`
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:validation:Enum=Pending;Initializing;Running;Updating;Degraded;Failed
type ClusterPhase string

const (
	ClusterPhasePending      ClusterPhase = "Pending"
	ClusterPhaseInitializing ClusterPhase = "Initializing"
	ClusterPhaseRunning      ClusterPhase = "Running"
	ClusterPhaseUpdating     ClusterPhase = "Updating"
	ClusterPhaseDegraded     ClusterPhase = "Degraded"
	ClusterPhaseFailed       ClusterPhase = "Failed"
)

type MapStatus struct {
	ID             string       `json:"id"`
	Phase          MapPhase     `json:"phase,omitempty"`
	ActiveVolume   string       `json:"activeVolume,omitempty"`
	ActiveBuildID  string       `json:"activeBuildID,omitempty"`
	PendingBuildID string       `json:"pendingBuildID,omitempty"`
	Address        string       `json:"address,omitempty"`
	RconAddress    string       `json:"rconAddress,omitempty"`
	SessionName    string       `json:"sessionName,omitempty"`
	LastSaveTime   *metav1.Time `json:"lastSaveTime,omitempty"`
	Pod            string       `json:"pod,omitempty"`
	// DrainDeadline is the source of truth for an in-flight RCON drain.
	// Persisted in status so operator restart never loses progress.
	DrainDeadline *metav1.Time       `json:"drainDeadline,omitempty"`
	Conditions    []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:validation:Enum=Pending;Provisioning;InstallingActive;InstallingInactive;Running;DrainingActive;Swapping;Failed
type MapPhase string

const (
	MapPhasePending            MapPhase = "Pending"
	MapPhaseProvisioning       MapPhase = "Provisioning"
	MapPhaseInstallingActive   MapPhase = "InstallingActive"
	MapPhaseInstallingInactive MapPhase = "InstallingInactive"
	MapPhaseRunning            MapPhase = "Running"
	MapPhaseDrainingActive     MapPhase = "DrainingActive"
	MapPhaseSwapping           MapPhase = "Swapping"
	MapPhaseFailed             MapPhase = "Failed"
)

type ModStatus struct {
	LastCheckTime *metav1.Time `json:"lastCheckTime,omitempty"`
	NextCheckTime *metav1.Time `json:"nextCheckTime,omitempty"`
	LastError     string       `json:"lastError,omitempty"`
	Tracked       []TrackedMod `json:"tracked,omitempty"`
}

type TrackedMod struct {
	ID               int64        `json:"id"`
	Slug             string       `json:"slug,omitempty"`
	InstalledVersion string       `json:"installedVersion,omitempty"`
	InstalledFileID  int64        `json:"installedFileID,omitempty"`
	LatestVersion    string       `json:"latestVersion,omitempty"`
	LatestFileID     int64        `json:"latestFileID,omitempty"`
	LastChanged      *metav1.Time `json:"lastChanged,omitempty"`
	AffectedMaps     []string     `json:"affectedMaps,omitempty"`
}

func init() {
	SchemeBuilder.Register(&ArkCluster{}, &ArkClusterList{})
}
