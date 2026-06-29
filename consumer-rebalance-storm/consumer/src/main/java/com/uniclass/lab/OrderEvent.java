package com.uniclass.lab;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderEvent {
    private String orderId;
    private String customerId;
    private long producedAt;
    private boolean poison;   // true = giả lập downstream call bị treo (DB deadlock, API timeout, GC pause...)
}
