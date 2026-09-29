package com.marcosfurquim.iotcase.config;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.FixedBackOff;
import org.apache.kafka.clients.admin.NewTopic;

@Configuration
public class KafkaConfiguration {
    private static final Logger log = LoggerFactory.getLogger(KafkaConfiguration.class);

    @Bean
    NewTopics caseTopics(CaseProperties properties) {
        return new NewTopics(
                new NewTopic(properties.topics().events(), properties.partitions(), (short) 1),
                new NewTopic(properties.topics().deadLetter(), properties.partitions(), (short) 1)
        );
    }

    @Bean
    DefaultErrorHandler caseErrorHandler(KafkaTemplate<Object, Object> template,
                                         CaseProperties properties,
                                         MeterRegistry metrics) {
        DeadLetterPublishingRecoverer publisher = new DeadLetterPublishingRecoverer(
                template,
                (record, exception) -> new TopicPartition(properties.topics().deadLetter(), record.partition())
        );
        publisher.setFailIfSendResultIsError(true);
        DefaultErrorHandler handler = new DefaultErrorHandler((record, exception) -> {
            publisher.accept(record, exception);
            metrics.counter("case.events.dead_lettered").increment();
            log.warn("event_dead_lettered partition={} offset={} reason={}",
                    record.partition(), record.offset(), exception.getClass().getSimpleName());
        }, new FixedBackOff(500L, 2L));
        handler.addNotRetryableExceptions(IllegalArgumentException.class, DeserializationException.class);
        return handler;
    }
}
