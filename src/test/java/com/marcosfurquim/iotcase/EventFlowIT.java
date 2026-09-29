package com.marcosfurquim.iotcase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.marcosfurquim.iotcase.importer.CsvEventReader;
import com.marcosfurquim.iotcase.importer.CsvImportService;
import com.marcosfurquim.iotcase.model.EventEnvelope;
import com.marcosfurquim.iotcase.processing.ProcessedEventRepository;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class EventFlowIT {
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.2");

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine")
            .withDatabaseName("case_events")
            .withUsername("case_user")
            .withPassword("case_password");

    @DynamicPropertySource
    static void configure(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("case.csv-path", () -> "src/test/resources/synthetic-events.csv");
    }

    @Autowired CsvImportService importer;
    @Autowired ProcessedEventRepository repository;
    @Autowired KafkaTemplate<String, EventEnvelope> producer;
    @Autowired ObjectMapper mapper;
    @Autowired MeterRegistry metrics;

    @Test
    void importsDeduplicatesAndSendsPoisonEventToDlt() throws Exception {
        CsvEventReader.ReadResult first = importer.importConfiguredCsv();
        assertThat(first.accepted()).isEqualTo(3);
        assertThat(first.rejected()).isZero();
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(totalProcessed()).isEqualTo(3));

        importer.importConfiguredCsv();
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(duplicateCount()).isEqualTo(3));
        assertThat(totalProcessed()).isEqualTo(3);

        EventEnvelope poison = event("unknown", "poison-id");
        EventEnvelope next = event("online", "next-id");
        producer.send("iot.events", "TEST-DEVICE-3", poison).get(10, TimeUnit.SECONDS);
        producer.send("iot.events", "TEST-DEVICE-3", next).get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(totalProcessed()).isEqualTo(4));
        assertThat(readDltRecord()).contains("poison-id");
    }

    private long totalProcessed() {
        return repository.countByType().values().stream().mapToLong(Long::longValue).sum();
    }

    private long duplicateCount() {
        return Math.round(metrics.get("case.events.duplicate").counters().stream()
                .mapToDouble(counter -> counter.count()).sum());
    }

    private EventEnvelope event(String type, String id) throws Exception {
        return new EventEnvelope(1, id, 10, "TEST-DEVICE-3", type, Instant.now(),
                mapper.readTree("{\"msgType\":\"" + type + "\",\"did\":\"TEST-DEVICE-3\"}"));
    }

    private String readDltRecord() {
        Properties settings = new Properties();
        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-check-" + UUID.randomUUID());
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(settings)) {
            consumer.subscribe(java.util.List.of("iot.events.dlt"));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                var records = consumer.poll(Duration.ofMillis(500));
                for (var record : records) return record.value();
            }
        }
        throw new AssertionError("Evento inválido não chegou à DLT");
    }
}
