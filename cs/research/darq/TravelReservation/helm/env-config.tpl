apiVersion: v1
kind: ConfigMap
metadata:
  name: env-config
  namespace: default
data:
  AZURE_CONN_STRING: {{.Values.connString}}