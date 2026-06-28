package com.uniclass.lab;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
@EnableScheduling
public class MetricsService {

    private final List<Long> processingLatencies = new CopyOnWriteArrayList<>();
    private final List<Long> endToEndLatencies   = new CopyOnWriteArrayList<>();
    private final AtomicLong totalProcessed = new AtomicLong(0);
    private final AtomicLong startTime      = new AtomicLong(0);

    public void recordStart() {
        startTime.compareAndSet(0, System.currentTimeMillis());
    }

    /** Ghi latency xử lý nội bộ (trong consumer handler) */
    public void recordProcessingLatency(long latencyMs) {
        processingLatencies.add(latencyMs);
        totalProcessed.incrementAndGet();
    }

    /** Ghi latency end-to-end (từ lúc producer gửi đến khi consumer xử lý xong) */
    public void recordEndToEndLatency(long latencyMs) {
        endToEndLatencies.add(latencyMs);
    }

    /** Log tự động mỗi 10 giây */
    @Scheduled(fixedDelay = 10_000)
    public void logStats() {
        if (processingLatencies.isEmpty()) return;
        Map<String, Object> stats = getStats();
        log.info("===== LAB METRICS =====");
        log.info("Tổng đã xử lý  : {} message", stats.get("totalProcessed"));
        log.info("Thời gian chạy : {}s",         stats.get("elapsedSeconds"));
        log.info("Throughput     : {} msg/s",     stats.get("throughputPerSecond"));
        log.info("--- Processing latency (bên trong consumer) ---");
        log.info("  P50 : {}ms", stats.get("processingP50"));
        log.info("  P95 : {}ms  ← dùng cái này để tính partition", stats.get("processingP95"));
        log.info("  P99 : {}ms", stats.get("processingP99"));
        log.info("--- End-to-end latency (producer → consumer xong) ---");
        log.info("  P50 : {}ms", stats.get("e2eP50"));
        log.info("  P95 : {}ms", stats.get("e2eP95"));
        log.info("=======================");
    }

    /** Expose ra REST endpoint để analyzer.py đọc */
    public Map<String, Object> getStats() {
        List<Long> pSorted  = sorted(processingLatencies);
        List<Long> e2eSorted = sorted(endToEndLatencies);

        long count   = totalProcessed.get();
        long elapsed = startTime.get() == 0 ? 1
            : (System.currentTimeMillis() - startTime.get()) / 1000;
        double tps = elapsed > 0 ? Math.round((double) count / elapsed * 10) / 10.0 : 0;

        return Map.of(
            "totalProcessed",      count,
            "elapsedSeconds",      elapsed,
            "throughputPerSecond", tps,
            "processingP50",       percentile(pSorted, 50),
            "processingP95",       percentile(pSorted, 95),
            "processingP99",       percentile(pSorted, 99),
            "e2eP50",              percentile(e2eSorted, 50),
            "e2eP95",              percentile(e2eSorted, 95),
            "e2eP99",              percentile(e2eSorted, 99)
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
