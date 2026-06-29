package com.uniclass.lab;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.Collection;

/**
 * Đăng ký rebalance listener để đo số lần group bị rebalance —
 * autoconfigured factory của Spring Boot không cho hook listener này,
 * nên phải tự khai báo factory bean cùng tên để override.
 */
@Configuration
@RequiredArgsConstructor
public class KafkaConsumerConfig {

    private final ConsumerFactory<Object, Object> consumerFactory;
    private final MetricsService metrics;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        factory.getContainerProperties().setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {
            @Override
            public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                if (!partitions.isEmpty()) {
                    metrics.recordRebalanceRevoked(partitions);
                }
            }

            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                metrics.recordRebalanceAssigned(partitions);
            }
        });

        return factory;
    }
}
