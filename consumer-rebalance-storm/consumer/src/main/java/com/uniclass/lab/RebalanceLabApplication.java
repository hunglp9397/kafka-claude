package com.uniclass.lab;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class RebalanceLabApplication {
    public static void main(String[] args) {
        SpringApplication.run(RebalanceLabApplication.class, args);
    }
}
