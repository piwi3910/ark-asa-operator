{{- define "ark-asa-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "ark-asa-operator.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "ark-asa-operator.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "ark-asa-operator.labels" -}}
app.kubernetes.io/name: {{ include "ark-asa-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "ark-asa-operator.serviceAccountName" -}}
{{ default (include "ark-asa-operator.fullname" .) .Values.serviceAccount.name }}
{{- end -}}
