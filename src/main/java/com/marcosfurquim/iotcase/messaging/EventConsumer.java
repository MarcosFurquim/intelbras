package com.marcosfurquim.iotcase.messaging;

import com.marcosfurquim.iotcase.model.EventEnvelope;
import com.marcosfurquim.iotcase.processing.EventProcessor;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class EventConsumer {
    private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);
    private final EventProcessor processor;
    private final MeterRegistry metrics;

    public EventConsumer(EventProcessor processor, MeterRegistry metrics) {
        this.processor = processor;
        this.metrics = metrics;
    }

    @KafkaListener(topics = "${case.topics.events}")
    public void consume(ConsumerRecord<String, EventEnvelope> record) {
        EventEnvelope event = record.value();
        boolean inserted = processor.process(event);
        metrics.counter(inserted ? "case.events.processed" : "case.events.duplicate",
                "type", event.messageType()).increment();
        log.info("event_consumed id={} type={} device={} partition={} offset={} outcome={}",
                event.eventId(), event.messageType(), event.deviceId(), record.partition(),
                record.offset(), inserted ? "processed" : "duplicate");
    }
}
