package com.marcosfurquim.iotcase.importer;

import com.marcosfurquim.iotcase.config.CaseProperties;
import com.marcosfurquim.iotcase.model.EventEnvelope;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class CsvImportService {
    private static final Logger log = LoggerFactory.getLogger(CsvImportService.class);
    private final CsvEventReader reader;
    private final KafkaTemplate<String, EventEnvelope> producer;
    private final CaseProperties properties;
    private final MeterRegistry metrics;

    public CsvImportService(CsvEventReader reader, KafkaTemplate<String, EventEnvelope> producer,
                            CaseProperties properties, MeterRegistry metrics) {
        this.reader = reader;
        this.producer = producer;
        this.properties = properties;
        this.metrics = metrics;
    }

    public CsvEventReader.ReadResult importConfiguredCsv() throws IOException {
        Path path = Path.of(properties.csvPath());
        if (!Files.isRegularFile(path)) {
            throw new IOException("CSV não encontrado em " + path.toAbsolutePath());
        }
        CsvEventReader.ReadResult result = reader.read(path, this::publish, rejected -> {
            metrics.counter("case.events.rejected").increment();
            log.warn("csv_row_rejected record={} reason={}", rejected.csvRecordNumber(), rejected.reason());
        });
        log.info("csv_import_finished published={} rejected={}", result.accepted(), result.rejected());
        return result;
    }

    private void publish(EventEnvelope event) {
        try {
            producer.send(properties.topics().events(), event.deviceId(), event)
                    .get(15, TimeUnit.SECONDS);
            metrics.counter("case.events.published", "type", event.messageType()).increment();
            log.info("event_published id={} type={} device={}",
                    event.eventId(), event.messageType(), event.deviceId());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Importação interrompida", exception);
        } catch (ExecutionException | TimeoutException exception) {
            throw new IllegalStateException("Falha ao publicar evento no Kafka", exception);
        }
    }
}
