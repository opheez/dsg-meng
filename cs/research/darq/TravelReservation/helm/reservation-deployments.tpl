{{- range .Values.services }}
apiVersion: apps/v1
kind: Deployment
metadata:
  labels:
    service: service{{ .num }}
  name: service{{ .num }}
spec:
  replicas: 1
  selector:
    matchLabels:
      service: service{{ .num }}
  strategy: {}
  template:
    metadata:
      labels:
        service: service{{ .num }}
    spec:
      priorityClassName: high-priority
      containers:
        - command:
            - "TravelReservation/TravelReservation -t service -w "workloads/{{ .Values.workload }}-service-{{ .num }}.csv" -n {{ .num }}"
          image: tianyuli96/faster:latest
          name: service{{ .num }}
          ports:
            - containerPort: 15721
          resources:
            requests:
              cpu: 2000m
            limits:
              cpu: 4000m
          envFrom:
            - configMapRef:
                name: env-config
      restartPolicy: Always
status: {}
---
{{- end }}