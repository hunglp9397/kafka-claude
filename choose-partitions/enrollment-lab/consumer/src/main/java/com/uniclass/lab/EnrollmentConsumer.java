package com.uniclass.lab;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class EnrollmentConsumer {

    private final MetricsService metrics;

    @Value("${lab.simulated-processing-ms:80}")
    private long simulatedProcessingMs;

    @KafkaListener(
        topics       = "enrollment-event",
        groupId      = "enrollment-processor",
        concurrency  = "${lab.concurrency:3}"
    )
    public void consume(EnrollmentEvent event, Acknowledgment ack) {
        metrics.recordStart();
        long start = System.currentTimeMillis();

        try {
            // Giả lập business logic: validate + ghi DB + push CRM
            // Thay bằng enrollmentService.process(event) nếu muốn đo thật
            simulateProcessing();

            ack.acknowledge();

            long processingMs = System.currentTimeMillis() - start;
            metrics.recordProcessingLatency(processingMs);

            // End-to-end: từ lúc producer gửi đến lúc consumer xử lý xong
            if (event.getProducedAt() > 0) {
                long e2eMs = System.currentTimeMillis() - event.getProducedAt();
                metrics.recordEndToEndLatency(e2eMs);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Consumer bị interrupt", e);
        }
    }

    /**
     * Giả lập processing time với jitter ±20% để giống thực tế.
     * Thay thế bằng logic thật của Uniclass để đo chính xác.
     */
    private void simulateProcessing() throws InterruptedException {
        long jitter = (long)(simulatedProcessingMs * 0.2 * (Math.random() * 2 - 1));
        Thread.sleep(Math.max(1, simulatedProcessingMs + jitter));
    }
}
