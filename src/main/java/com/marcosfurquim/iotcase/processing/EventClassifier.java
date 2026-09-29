package com.marcosfurquim.iotcase.processing;

import com.fasterxml.jackson.databind.JsonNode;
import com.marcosfurquim.iotcase.model.EventEnvelope;
import org.springframework.stereotype.Component;

@Component
public class EventClassifier {
    public Classification classify(EventEnvelope event) {
        if (event.schemaVersion() != 1 || event.payload() == null || !event.payload().isObject()) {
            throw new IllegalArgumentException("Envelope inválido ou versão desconhecida");
        }
        JsonNode body = event.payload();
        if (!event.messageType().equals(body.path("msgType").asText())) {
            throw new IllegalArgumentException("Tipo do envelope difere do payload");
        }
        return switch (event.messageType()) {
            case "iotProperty" -> property(body);
            case "online", "offline" -> new Classification("device_status", event.messageType());
            case "iotEvent" -> iotEvent(body);
            case "videoMotion", "mobileDetect", "human", "smartVideoMotion", "openCamera" ->
                    new Classification("detection", "kind=" + event.messageType());
            case "informationTransfer" -> informationTransfer(body);
            case "upgradeFail" -> new Classification("upgrade", "status=" + safe(body.path("upgradeStatus")));
            default -> throw new IllegalArgumentException("Tipo de evento não suportado");
        };
    }

    private Classification property(JsonNode body) {
        JsonNode properties = body.path("content").path("properties");
        if (!properties.isObject() || properties.isEmpty()) {
            throw new IllegalArgumentException("iotProperty sem propriedades");
        }
        return new Classification("property", "property_count=" + properties.size());
    }

    private Classification iotEvent(JsonNode body) {
        String code = body.path("content").path("event").asText();
        if (code.isBlank()) {
            throw new IllegalArgumentException("iotEvent sem código");
        }
        return new Classification("iot_event", "event_code=" + code);
    }

    private Classification informationTransfer(JsonNode body) {
        if (body.path("deviceId").asText().isBlank()) {
            throw new IllegalArgumentException("informationTransfer sem deviceId");
        }
        return new Classification("transfer", "channel=" + safe(body.path("channelId")));
    }

    private String safe(JsonNode node) {
        String value = node.asText("");
        return value.length() > 40 ? value.substring(0, 40) : value;
    }
}
