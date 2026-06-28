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
public class EnrollmentEvent {
    private String studentId;
    private String provinceId;
    private String campaignId;
    private long producedAt;   // epoch ms — tính end-to-end latency
    private String phase;      // warmup | spike | taper
}
