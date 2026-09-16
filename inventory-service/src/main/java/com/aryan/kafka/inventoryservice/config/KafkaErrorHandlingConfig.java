package com.aryan.kafka.inventoryservice.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorHandlingConfig {

    @Bean
    public CommonErrorHandler kafkaErrorHandler(
            KafkaTemplate<String, Object> kafkaTemplate) {

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate,
                        (record, exception) ->
                                new TopicPartition(
                                        record.topic() + "-dlt",
                                        record.partition()
                                )
                );
        recoverer.setFailIfSendResultIsError(true);

        FixedBackOff fixedBackOff =
                new FixedBackOff(2000L, 2L);

        DefaultErrorHandler errorHandler =
                new DefaultErrorHandler(recoverer, fixedBackOff);

        errorHandler.setCommitRecovered(true);

        return errorHandler;
    }
}