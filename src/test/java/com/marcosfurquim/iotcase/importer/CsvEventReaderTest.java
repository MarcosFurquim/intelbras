package com.marcosfurquim.iotcase.importer;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.marcosfurquim.iotcase.model.EventEnvelope;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

class CsvEventReaderTest {
    private final CsvEventReader reader = new CsvEventReader(new ObjectMapper());

    @TempDir Path tempDir;

    @Test
    void readsDifferentPayloadShapesAndStableIds() throws Exception {
        Path path = Path.of("src/test/resources/synthetic-events.csv");
        List<EventEnvelope> first = new ArrayList<>();
        List<CsvEventReader.RejectedRow> rejected = new ArrayList<>();
        CsvEventReader.ReadResult result = reader.read(path, first::add, rejected::add);

        assertThat(result.accepted()).isEqualTo(3);
        assertThat(result.rejected()).isZero();
        assertThat(rejected).isEmpty();
        assertThat(first).extracting(EventEnvelope::messageType)
                .containsExactly("iotProperty", "online", "informationTransfer");
        assertThat(first.get(2).deviceId()).isEqualTo("TEST-DEVICE-2");

        List<EventEnvelope> second = new ArrayList<>();
        reader.read(path, second::add, ignored -> {});
        assertThat(second).extracting(EventEnvelope::eventId)
                .containsExactlyElementsOf(first.stream().map(EventEnvelope::eventId).toList());
    }

    @Test
    void realDatasetHasHundredValidEventsWhenAvailable() throws Exception {
        Path path = Path.of("input/case_backend_senior_eventos_sanitizados.csv");
        org.junit.jupiter.api.Assumptions.assumeTrue(java.nio.file.Files.exists(path));
        List<EventEnvelope> events = new ArrayList<>();
        CsvEventReader.ReadResult result = reader.read(path, events::add, ignored -> {});

        assertThat(result.accepted()).isEqualTo(100);
        assertThat(result.rejected()).isZero();
        assertThat(events.stream().map(EventEnvelope::messageType).distinct()).hasSize(11);
        assertThat(events.stream().map(EventEnvelope::eventId).distinct()).hasSize(100);
    }

    @Test
    void rejectsBadRowAndStillReadsFollowingEvent() throws Exception {
        List<String> lines = Files.readAllLines(Path.of("src/test/resources/synthetic-events.csv"));
        Path path = tempDir.resolve("broken.csv");
        Files.writeString(path, String.join("\n", lines.get(0), lines.get(1),
                "9,2026-09-25T08:00:00+00:00,online,TEST-DEVICE-1,,,,,,,,{},not-json",
                lines.get(2)) + "\n");
        List<EventEnvelope> accepted = new ArrayList<>();
        List<CsvEventReader.RejectedRow> rejected = new ArrayList<>();

        CsvEventReader.ReadResult result = reader.read(path, accepted::add, rejected::add);

        assertThat(result.accepted()).isEqualTo(2);
        assertThat(result.rejected()).isEqualTo(1);
        assertThat(rejected.get(0).csvRecordNumber()).isEqualTo(2);
        assertThat(accepted).extracting(EventEnvelope::sourceRow).containsExactly(1, 2);
    }
}
