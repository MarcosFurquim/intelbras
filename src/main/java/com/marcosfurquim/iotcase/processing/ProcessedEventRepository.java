package com.marcosfurquim.iotcase.processing;

import com.marcosfurquim.iotcase.model.EventEnvelope;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ProcessedEventRepository {
    private final JdbcTemplate jdbc;

    public ProcessedEventRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public boolean insertIfAbsent(EventEnvelope event, Classification classification) {
        int inserted = jdbc.update("""
                INSERT INTO processed_events
                    (event_id, source_row, device_id, message_type, category, detail)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (event_id) DO NOTHING
                """, event.eventId(), event.sourceRow(), event.deviceId(), event.messageType(),
                classification.category(), classification.detail());
        return inserted == 1;
    }

    public Map<String, Long> countByType() {
        Map<String, Long> counts = new LinkedHashMap<>();
        for (Map<String, Object> row : jdbc.queryForList(
                "SELECT message_type, COUNT(*) AS total FROM processed_events GROUP BY message_type ORDER BY message_type")) {
            counts.put((String) row.get("message_type"), ((Number) row.get("total")).longValue());
        }
        return counts;
    }
}
