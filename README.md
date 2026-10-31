# Kafka Projects and Interview Learning

Hands-on Apache Kafka learning focused on Java, Spring Boot, reliability engineering, and backend interviews.

## Start here

Read the modules in order:

1. [Module 1 — Kafka fundamentals](docs/modules/module-01-fundamentals.md)
2. [Module 2 — Ordering and delivery guarantees](docs/modules/module-02-ordering-delivery-guarantees.md)
3. [Module 3 — Consumer reliability](docs/modules/module-03-consumer-reliability.md)
4. [Module 4 — Producer performance](docs/modules/module-04-producer-performance.md)
5. [Module 5 — Scaling, backpressure, monitoring](docs/modules/module-05-scaling-backpressure-monitoring.md)
6. [Module 6 — REST vs event-driven architecture](docs/modules/module-06-rest-vs-kafka.md)
7. [Module 7 — Hands-on reliability lab](docs/modules/module-07-hands-on-lab.md)

Then follow the [Kafka Reliability Lab run guide](projects/kafka-reliability-lab/README.md).

## Quick start (Windows PowerShell)

Prerequisites: JDK 21, Maven 3.9+, and Docker Desktop with Docker Compose.

    cd projects/kafka-reliability-lab
    java -version
    mvn -version
    docker compose version
    docker compose up -d
    docker compose ps
    mvn clean test
    mvn spring-boot:run

Keep the application terminal open. In a second PowerShell terminal, publish a test event:

    $body = @{
      customerId = 'C-101'
      productId = 'P-9'
      quantity = 2
      correlationId = 'req-101'
    } | ConvertTo-Json
    Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/api/orders' -ContentType 'application/json' -Body $body

For duplicate delivery, run:

    Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/api/demo/duplicates?count=5'
    Start-Sleep -Seconds 2
    Invoke-RestMethod -Uri 'http://localhost:8080/api/demo/stats'

For permanent failure, send an order request with poisonPill set to true. After two retries, the record should be routed to the DLT. For partition skew, call:

    Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/api/demo/partition-skew?events=1000&hotPercent=95'

Stop the lab with Ctrl+C in the app terminal, then run docker compose down from the project directory.

## What is in the repository?

- Seven module-wise notes with concepts, interview checkpoints, and exercises.
- Java 21 / Spring Boot producer-consumer application.
- Local single-node Kafka broker via Docker Compose.
- Event-ID deduplication using a database uniqueness constraint.
- Bounded retries and a dead-letter topic (DLT).
- Failure injection and a shared-hot-key partition-skew workload.
- JUnit tests for failure policy, idempotency, and skew planning.

## Important limitations

A Kafka producer acknowledgement confirms the broker write according to its configured guarantees; it does not confirm consumer business completion. The producer idempotence setting is not a replacement for consumer-side idempotency. The H2 ledger makes this demo's database insert replay-safe; external effects such as payment or email require their own idempotency and reconciliation. The Docker setup has one broker and replication factor one, so it is not a high-availability demonstration. The lab does not implement the transactional outbox.

See the [project README](projects/kafka-reliability-lab/README.md), [failure exercises](projects/kafka-reliability-lab/docs/FAILURE_EXERCISES.md), and [test run log template](projects/kafka-reliability-lab/docs/TEST_RUN_LOG.md).
