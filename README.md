### Task 5 - Microservices with Consul (Kubernetes)
To start the system, you need to use minikube:
```
minikube start --driver=docker
```
To create pods for Hazelcast, Kafka and Postgres, I used [helm](https://helm.sh/uk/):
```
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add hazelcast https://hazelcast-charts.s3.amazonaws.com/
helm repo update
helm install counter-db bitnami/postgresql -f .\k8s\postgres-values.yaml
helm install hazelcast-node hazelcast/hazelcast -f .\k8s\hazelcast-values.yaml
```

