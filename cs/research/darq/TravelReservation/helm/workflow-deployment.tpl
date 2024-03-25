{{- range .Values.orchestrators }}
apiVersion: apps/v1
kind: Deployment
metadata:
  labels:
    service: orchestrator
  name: orchestrator{{ .num }}
spec:
  replicas: 1
  selector:
    matchLabels:
      service: orchestrator
  strategy: {}
  template:
    metadata:
      labels:
        service: orchestrator
    spec:
      priorityClassName: high-priority
      containers:
        - command:
            - "TravelReservation/TravelReservation -t orchestrator -n {{ .num }}"
          image: tianyuli96/hotelreservation:latest
          name: orchestrator{{ .num }}
          ports:
            - containerPort: {{ .Values.orchestrator-port }}
          resources:
            requests:
              cpu: 2000m
            limits:
              cpu: 4000m
      restartPolicy: Always
---
{{- end }}
