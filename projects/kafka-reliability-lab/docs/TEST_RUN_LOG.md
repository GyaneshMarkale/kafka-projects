# Test Run Log

Only mark checks as passed after you run them and capture actual output.

## Environment

- Date/time:
- OS:
- Java version:
- Maven version:
- Docker version:
- Kafka image:

## Results

| Check | Status before local execution | Evidence to capture |
|---|---|---|
| mvn clean test | Not run in authoring environment | Surefire summary |
| docker compose up -d | Not run in authoring environment | Broker health |
| Spring Boot startup | Not run in authoring environment | Startup log |
| Transient retry scenario | Not run in authoring environment | Attempt numbers and successful process log |
| Poison message / DLT | Not run in authoring environment | DLT_RECEIVED log |
| Duplicate scenario | Not run in authoring environment | Unique count and duplicate logs |
| Partition skew | Not run in authoring environment | Actual per-partition counts |

Unit tests for pure functions and the database service do not prove end-to-end Kafka listener, retry, offset, or DLT behavior. Keep these validation levels separate.
