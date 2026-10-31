# Kafka Reliability Lab

Java 21 + Spring Boot + Apache Kafka lab for retries, duplicate event delivery, DLT routing, and hot-key partition skew.

## Prerequisites

Install JDK 21, Maven 3.9+, and Docker Desktop with Docker Compose. Verify:

    java -version
    mvn -version
    docker --version
    docker compose version

## 1. Start Kafka

From this directory:

    docker compose up -d
    docker compose ps

The local broker uses a single-node KRaft configuration and one replica per partition. It is for learning, not production HA.

If it does not become healthy:

    docker compose logs kafka

## 2. Run tests

    mvn clean test

The JUnit tests cover pure failure-policy/skew logic and database deduplication. They do not replace a broker-level integration test of the end-to-end retry/DLT flow.

## 3. Start the application

    mvn spring-boot:run

Wait for the startup message. Keep this terminal open. The app connects to localhost:9092 and uses an in-memory H2 database by default.

Check health in a second PowerShell terminal:

    Invoke-RestMethod -Uri 'http://localhost:8080/actuator/health'

## 4. Publish a normal event

    $body = @{
      customerId = 'C-101'
      productId = 'P-9'
      quantity = 2
      correlationId = 'req-101'
    } | ConvertTo-Json

    Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/api/orders' -ContentType 'application/json' -Body $body

Expected: HTTP 202 containing an event ID, order ID, topic, partition, and offset. This proves Kafka acknowledged the record, not that consumer work has finished.

## 5. Retry and recover

    $body = @{
      customerId = 'C-102'
      productId = 'P-9'
      quantity = 1
      failuresBeforeSuccess = 2
      correlationId = 'retry-demo'
    } | ConvertTo-Json

    Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/api/orders' -ContentType 'application/json' -Body $body

Expect two simulated failures and then success on the third listener delivery. Verify using application logs, not only the producer response.

## 6. Poison pill and DLT

    $body = @{
      customerId = 'C-103'
      productId = 'P-9'
      quantity = 1
      poisonPill = $true
      correlationId = 'poison-demo'
    } | ConvertTo-Json

    Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/api/orders' -ContentType 'application/json' -Body $body

The listener handler retries twice after the initial delivery, then routes the exhausted event to orders.events.DLT. Watch for SIMULATED_FAILURE and DLT_RECEIVED. A DLT requires operational ownership and a replay/remediation process.

## 7. Duplicate delivery

    Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/api/demo/duplicates?count=5'
    Start-Sleep -Seconds 2
    Invoke-RestMethod -Uri 'http://localhost:8080/api/demo/stats'

Five records share one event ID. The H2 table has a primary key on event_id, so only one insert should succeed and later copies should log DUPLICATE_IGNORED. Restart the app first if you need a clean in-memory count.

## 8. Partition skew

    Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/api/demo/partition-skew?events=1000&hotPercent=95'

The planner emits 950 events with one shared key and 50 distinct-key events. Kafka sends records with the same key to one partition. Inspect actual partition counts with Kafka tooling; the response does not guess the hot partition number.

## 9. Key source files

- Application: src/main/java/com/gyanesh/kafkalab/KafkaReliabilityLabApplication.java
- Topic definitions: src/main/java/com/gyanesh/kafkalab/config/KafkaTopicConfig.java
- Retry/DLT handler: src/main/java/com/gyanesh/kafkalab/config/KafkaListenerConfig.java
- Producer: src/main/java/com/gyanesh/kafkalab/producer/OrderEventProducer.java
- Consumer: src/main/java/com/gyanesh/kafkalab/consumer/OrderEventConsumer.java
- DB idempotency: src/main/java/com/gyanesh/kafkalab/service/EventProcessingService.java
- Failure injection: src/main/java/com/gyanesh/kafkalab/core/DeliveryFailurePolicy.java
- Skew generator: src/main/java/com/gyanesh/kafkalab/core/PartitionSkewPlanner.java

## 10. Stop

Stop Spring Boot with Ctrl+C, then:

    docker compose down

H2 data is lost when the application process stops.

## Limitations

- No transactional outbox implementation is included; see Module 6 for the dual-write problem.
- One broker and replication factor one cannot prove broker failover or replicated durability.
- The unique event ID protects the demo database insert, not arbitrary external side effects.
- Do not claim integration testing passed unless you ran the app with a live broker and captured results.

See docs/FAILURE_EXERCISES.md and docs/TEST_RUN_LOG.md for exercises and a test evidence template.
