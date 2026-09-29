package com.marcosfurquim.iotcase.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

public record EventEnvelope(
        int schemaVersion,
        String eventId,
        int sourceRow,
        String deviceId,
        String messageType,
        Instant loggedAt,
        JsonNode payload
) {}
