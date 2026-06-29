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
public class OrderConsumer {

    private final MetricsService metrics;

    @Value("${lab.normal-processing-ms:50}")
    private long normalProcessingMs;

    @Value("${lab.poison-processing-ms:20000}")
    private long poisonProcessingMs;

    @KafkaListener(
        topics      = "order-event",
        groupId     = "order-processor",
        concurrency = "${lab.concurrency:3}"
    )
    public void consume(OrderEvent event, Acknowledgment ack) throws InterruptedException {
        long start = System.currentTimeMillis();

        if (event.isPoison()) {
            log.warn("☣ POISON orderId={} — downstream call giả treo {}ms (có thể vượt max.poll.interval.ms)",
                event.getOrderId(), poisonProcessingMs);
            metrics.recordPoison();
            // Mô phỏng DB deadlock / API timeout / GC pause dài — block ngay trong poll loop
            Thread.sleep(poisonProcessingMs);
        } else {
            Thread.sleep(normalProcessingMs);
        }

        ack.acknowledge();
        metrics.recordProcessingLatency(System.currentTimeMillis() - start);
    }
}
