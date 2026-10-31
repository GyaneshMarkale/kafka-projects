package com.gyanesh.kafkalab.api;

import com.gyanesh.kafkalab.core.PartitionSkewPlanner;
import com.gyanesh.kafkalab.events.CreateOrderRequest;
import com.gyanesh.kafkalab.events.DemoReceipt;
import com.gyanesh.kafkalab.events.OrderEvent;
import com.gyanesh.kafkalab.events.ProcessingSummary;
import com.gyanesh.kafkalab.events.PublishReceipt;
import com.gyanesh.kafkalab.producer.OrderEventProducer;
import com.gyanesh.kafkalab.service.EventProcessingService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.IntStream;

@RestController
@RequestMapping("/api")
public class OrderController {
    private final OrderEventProducer producer;
    private final EventProcessingService processingService;

    public OrderController(OrderEventProducer producer, EventProcessingService processingService) {
        this.producer = producer;
        this.processingService = processingService;
    }

    @PostMapping("/orders")
    public CompletionStage<ResponseEntity<PublishReceipt>> createOrder(
            @Valid @RequestBody CreateOrderRequest request) {
        String orderId = "ORD-" + UUID.randomUUID();
        String eventId = UUID.randomUUID().toString();
        String correlationId = request.correlationId() == null || request.correlationId().isBlank()
                ? UUID.randomUUID().toString() : request.correlationId();
        OrderEvent event = new OrderEvent(
                eventId, orderId, request.customerId(), request.productId(), request.quantity(),
                Instant.now().toString(),
                request.failuresBeforeSuccess() == null ? 0 : request.failuresBeforeSuccess(),
                Boolean.TRUE.equals(request.poisonPill()), correlationId);

        return producer.publish(orderId, event).thenApply(result -> ResponseEntity.accepted().body(
                new PublishReceipt(eventId, orderId, result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(), result.getRecordMetadata().offset(),
                        "PUBLISHED_TO_KAFKA",
                        "Kafka acknowledged the record; this does not mean consumer processing has completed.")));
    }

    @PostMapping("/demo/duplicates")
    public CompletableFuture<ResponseEntity<DemoReceipt>> publishDuplicates(
            @RequestParam(defaultValue = "2") int count) {
        if (count < 2 || count > 100) {
            throw new IllegalArgumentException("count must be between 2 and 100");
        }
        String duplicateId = "DUP-" + UUID.randomUUID();
        OrderEvent duplicateEvent = new OrderEvent(
                duplicateId, "ORD-DUP-DEMO", "customer-demo", "product-demo", 1,
                Instant.now().toString(), 0, false, "corr-duplicate-demo");
        List<CompletableFuture<?>> sends = IntStream.range(0, count)
                .mapToObj(i -> producer.publish(duplicateEvent.orderId(), duplicateEvent))
                .map(future -> (CompletableFuture<?>) future)
                .toList();
        return CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new))
                .thenApply(ignored -> ResponseEntity.accepted().body(new DemoReceipt(
                        "duplicate-event", count, duplicateId, 0, 0,
                        "Copies share one eventId. Expect one DB insert and duplicate-ignore logs for the rest.")));
    }

    @PostMapping("/demo/partition-skew")
    public CompletableFuture<ResponseEntity<DemoReceipt>> publishPartitionSkew(
            @RequestParam(defaultValue = "100") int events,
            @RequestParam(defaultValue = "90") int hotPercent) {
        PartitionSkewPlanner.Plan plan = PartitionSkewPlanner.plan(events, hotPercent);
        List<CompletableFuture<?>> sends = plan.events().stream()
                .map(planned -> {
                    OrderEvent event = new OrderEvent(
                            planned.eventId(), planned.partitionKey(), "customer-skew-demo", "product-skew-demo", 1,
                            Instant.now().toString(), 0, false, "corr-skew-demo");
                    return producer.publish(planned.partitionKey(), event);
                })
                .map(future -> (CompletableFuture<?>) future)
                .toList();
        return CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new))
                .thenApply(ignored -> ResponseEntity.accepted().body(new DemoReceipt(
                        "partition-skew", events, null, plan.hotKeyCount(), plan.coldKeyCount(),
                        "Hot records share one key and therefore map to one partition. Inspect partition counts with Kafka tooling; this response does not guess the partition number.")));
    }

    @GetMapping("/demo/stats")
    public ProcessingSummary stats() {
        return new ProcessingSummary(processingService.countUniqueProcessedEvents(),
                "Distinct event IDs inserted into the H2 ledger; not an end-to-end business completion count.");
    }
}
