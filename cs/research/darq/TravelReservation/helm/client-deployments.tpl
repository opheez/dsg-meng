{{- range .Values.clients }}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: client{{ .num }}
spec:
  replicas: 1
  strategy: {}
  template:
    spec:
      priorityClassName: high-priority
      containers:
        - command:
            - "TravelReservation/TravelReservation -t client -w "workloads/{{ .Values.workload }}-client-{{ .num }}" -n {{ .num }}"
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