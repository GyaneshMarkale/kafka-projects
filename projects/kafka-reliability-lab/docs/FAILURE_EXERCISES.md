# Failure Exercises

Run one scenario at a time, inspect application logs, and explain the failure window.

## 1. Transient failure and recovery

Publish an order with failuresBeforeSuccess set to 2. Observe two failed deliveries followed by a successful third delivery. Explain producer acknowledgement versus consumer completion.

## 2. Poison message and DLT

Publish an order with poisonPill set to true. Observe bounded attempts and DLT_RECEIVED. List what an operational DLT process needs: alert, owner, payload and error inspection, replay policy, and audit trail.

## 3. Duplicate event

Call the duplicate endpoint with count=5. Confirm all copies share one event ID and the H2 ledger contains one unique row for that ID. Explain why the unique event ID check and producer idempotence are different mechanisms.

## 4. DB commit before offset commit

Draw the sequence: consumer receives record; database insert commits; process dies before offset commit; record is delivered again. Explain why the unique constraint makes the repeated insert a no-op. State why an external payment provider still needs an idempotency key.

## 5. Partition skew

Publish 1,000 records with 95% on one key. Inspect real partition distribution and lag. Explain why adding consumers cannot split one partition assignment in the same group.

## 6. Design extension: outbox

Add an outbox table and relay after drawing the design. Order data and outbox intent must commit in one DB transaction. The relay can publish more than once, so consumers must remain idempotent.
