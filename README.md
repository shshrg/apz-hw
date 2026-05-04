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
helm install counter-db bitnami/postgresql -f ./k8s/postgres-values.yaml
helm install hazelcast-node hazelcast/hazelcast -f ./k8s/hazelcast-values.yaml
```
Next, create the microservices:
```
# postgres and hazelcast parameters
kubectl apply -f ./k8s/counter-db-secret.yaml -d ./k8s/hazelcast-config.yaml
# build images
docker build -t facade-service:latest ./facade
docker build -t counter-service:latest ./counter
docker build -t logging-service:latest ./logging
# deploy
kubectl apply -f ./k8s/facade-service-deployment.yaml -f ./k8s/facade-service-service.yaml -f ./k8s/facade-service.rbac
kubectl apply -f ./k8s/counter-service-deployment.yaml -f ./k8s/counter-service-service.yaml
kubectl apply -f ./k8s/logging-service-deployment.yaml -f ./k8s/logging-service-service.yaml
```
To send requests locally, you need to forward the port in facade-service:
```
kubectl apply -f ./k8s/counter-service-deployment.yaml -f ./k8s/counter-service-service.yaml
```
Now requests can be sent to ```http://localhost:8080```.
