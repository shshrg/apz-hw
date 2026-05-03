### Task 4 - Microservices with Message Queue
To launch the system, use docker compose. Run the following commands one by one:
```
docker compose up --build -d kafka hazelcast-node counter-db
docker compose up --build -d config-service
docker compose up --build -d facade-service counter-service logging-service
```
You can then test the system by sending POST or GET requests to facade-services. Some example requests are included in ```test.rest```.
Further explanations about the system can be found in my task report.