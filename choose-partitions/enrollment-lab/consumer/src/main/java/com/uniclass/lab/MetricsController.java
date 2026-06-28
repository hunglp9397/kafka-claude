package com.uniclass.lab;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/lab")
@RequiredArgsConstructor
public class MetricsController {

    private final MetricsService metricsService;

    /** Analyzer đọc endpoint này để tính số partition */
    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        return metricsService.getStats();
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "UP");
    }
}
