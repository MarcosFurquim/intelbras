package com.marcosfurquim.iotcase.importer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marcosfurquim.iotcase.model.EventEnvelope;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HexFormat;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Component;

@Component
public class CsvEventReader {
    private static final Set<String> REQUIRED_COLUMNS = Set.of(
            "event_number", "timestamp_log", "msgType", "did", "body_completo");
    private final ObjectMapper mapper;

    public CsvEventReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public ReadResult read(Path path, Consumer<EventEnvelope> onEvent,
                           Consumer<RejectedRow> onRejected) throws IOException {
        int accepted = 0;
        int rejected = 0;
        try (Reader input = Files.newBufferedReader(path, StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.builder()
                     .setHeader()
                     .setSkipHeaderRecord(true)
                     .setIgnoreEmptyLines(true)
                     .get()
                     .parse(input)) {
            if (!parser.getHeaderMap().keySet().containsAll(REQUIRED_COLUMNS)) {
                throw new IllegalArgumentException("CSV sem as colunas obrigatórias: " + REQUIRED_COLUMNS);
            }
            for (CSVRecord row : parser) {
                try {
                    onEvent.accept(toEnvelope(row));
                    accepted++;
                } catch (InvalidEventException exception) {
                    rejected++;
                    onRejected.accept(new RejectedRow(row.getRecordNumber(), exception.getMessage()));
                }
            }
        }
        return new ReadResult(accepted, rejected);
    }

    EventEnvelope toEnvelope(CSVRecord row) {
        try {
            int sourceRow = Integer.parseInt(row.get("event_number"));
            String raw = required(row.get("body_completo"), "body_completo");
            JsonNode payload = mapper.readTree(raw);
            if (payload == null || !payload.isObject()) {
                throw new InvalidEventException("body_completo deve ser um objeto JSON");
            }
            String type = required(row.get("msgType"), "msgType");
            if (!type.equals(payload.path("msgType").asText())) {
                throw new InvalidEventException("msgType difere entre CSV e body_completo");
            }
            String deviceId = firstNonBlank(row.get("did"), payload.path("did").asText(null),
                    payload.path("deviceId").asText(null));
            if (deviceId == null) {
                throw new InvalidEventException("evento sem identificador de dispositivo");
            }
            String csvDevice = row.get("did");
            String bodyDevice = firstNonBlank(payload.path("did").asText(null),
                    payload.path("deviceId").asText(null));
            if (csvDevice != null && !csvDevice.isBlank() && bodyDevice != null
                    && !csvDevice.equals(bodyDevice)) {
                throw new InvalidEventException("did difere entre CSV e body_completo");
            }
            Instant loggedAt = OffsetDateTime.parse(required(row.get("timestamp_log"), "timestamp_log"))
                    .toInstant();
            byte[] normalized = mapper.writeValueAsBytes(payload);
            String eventId = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(normalized));
            return new EventEnvelope(1, eventId, sourceRow, deviceId, type, loggedAt, payload);
        } catch (InvalidEventException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new InvalidEventException("linha inválida: " + exception.getClass().getSimpleName());
        }
    }

    private static String required(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new InvalidEventException(name + " vazio");
        }
        return value;
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) return value;
        }
        return null;
    }

    public record ReadResult(int accepted, int rejected) {}
    public record RejectedRow(long csvRecordNumber, String reason) {}

    public static class InvalidEventException extends IllegalArgumentException {
        public InvalidEventException(String message) {
            super(message);
        }
    }
}
