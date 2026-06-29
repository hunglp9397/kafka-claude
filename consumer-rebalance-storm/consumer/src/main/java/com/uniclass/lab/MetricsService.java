package com.uniclass.lab;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
public class MetricsService {

    private final List<Long> processingLatencies = new CopyOnWriteArrayList<>();
    private final AtomicLong totalProcessed   = new AtomicLong(0);
    private final AtomicLong poisonCount      = new AtomicLong(0);
    private final AtomicLong rebalanceCount   = new AtomicLong(0);
    private final AtomicLong startTime        = new AtomicLong(0);
    private final AtomicLong lastRebalanceAt  = new AtomicLong(0);
    private volatile int assignedPartitions   = 0;

    public void recordStart() {
        startTime.compareAndSet(0, System.currentTimeMillis());
    }

    public void recordProcessingLatency(long latencyMs) {
        recordStart();
        processingLatencies.add(latencyMs);
        totalProcessed.incrementAndGet();
    }

    public void recordPoison() {
        poisonCount.incrementAndGet();
    }

    /** Gọi khi coordinator revoke partition khỏi consumer này — dấu hiệu rebalance đang xảy ra */
    public void recordRebalanceRevoked(Collection<TopicPartition> partitions) {
        long count = rebalanceCount.incrementAndGet();
        lastRebalanceAt.set(System.currentTimeMillis());
        log.warn("⚠ REBALANCE #{} — revoke {} partition(s): {}", count, partitions.size(), partitions);
    }

    public void recordRebalanceAssigned(Collection<TopicPartition> partitions) {
        assignedPartitions = partitions.size();
        log.info("Partition được gán lại: {} ({} partition)", partitions, partitions.size());
    }

    /** Log tự động mỗi 10 giây */
    @Scheduled(fixedDelay = 10_000)
    public void logStats() {
        if (totalProcessed.get() == 0) return;
        Map<String, Object> stats = getStats();
        log.info("===== LAB METRICS =====");
        log.info("Tổng đã xử lý     : {} message", stats.get("totalProcessed"));
        log.info("Throughput        : {} msg/s", stats.get("throughputPerSecond"));
        log.info("Poison đã xử lý   : {}", stats.get("poisonMessagesProcessed"));
        log.info("Số lần rebalance  : {}", stats.get("rebalanceCount"));
        log.info("Partition đang giữ: {}", stats.get("assignedPartitions"));
        log.info("Processing P95    : {}ms", stats.get("processingP95"));
        log.info("=======================");
    }

    /** Expose ra REST endpoint để monitor.py đọc */
    public Map<String, Object> getStats() {
        List<Long> pSorted = sorted(processingLatencies);

        long count   = totalProcessed.get();
        long elapsed = startTime.get() == 0 ? 1
            : (System.currentTimeMillis() - startTime.get()) / 1000;
        double tps = elapsed > 0 ? Math.round((double) count / elapsed * 10) / 10.0 : 0;

        long lastRebalance = lastRebalanceAt.get();
        long secondsSinceLastRebalance = lastRebalance == 0 ? -1
            : (System.currentTimeMillis() - lastRebalance) / 1000;

        return Map.of(
            "totalProcessed",           count,
            "elapsedSeconds",           elapsed,
            "throughputPerSecond",      tps,
            "processingP50",            percentile(pSorted, 50),
            "processingP95",            percentile(pSorted, 95),
            "processingP99",            percentile(pSorted, 99),
            "poisonMessagesProcessed",  poisonCount.get(),
            "rebalanceCount",           rebalanceCount.get(),
            "assignedPartitions",       assignedPartitions,
            "secondsSinceLastRebalance", secondsSinceLastRebalance
        );
    }

    private List<Long> sorted(List<Long> input) {
        List<Long> copy = new ArrayList<>(input);
        Collections.sort(copy);
        return copy;
    }

    private long percentile(List<Long> sorted, int pct) {
        if (sorted.isEmpty()) return 0;
        int idx = (int) Math.ceil(sorted.size() * pct / 100.0) - 1;
        return sorted.get(Math.max(0, Math.min(idx, sorted.size() - 1)));
    }
}
