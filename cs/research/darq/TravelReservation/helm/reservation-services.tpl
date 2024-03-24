{{- range .Values.services }}
apiVersion: v1
kind: Service
metadata:
  labels:
    service: service{{ .num }}
  name: service{{ .num }}
spec:
  ports:
    - name: "15721"
      port: 15721
      targetPort: {{ .port }}
  selector:
    service: service{{ .num }}
---
{{- end }}