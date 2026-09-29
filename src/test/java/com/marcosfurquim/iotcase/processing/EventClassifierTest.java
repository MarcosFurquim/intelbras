package com.marcosfurquim.iotcase.processing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.marcosfurquim.iotcase.model.EventEnvelope;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class EventClassifierTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final EventClassifier classifier = new EventClassifier();

    @Test
    void classifiesAllEventFamiliesInDataset() throws Exception {
        assertThat(classify("iotProperty", "{\"content\":{\"properties\":{\"battery\":90}}}").category())
                .isEqualTo("property");
        assertThat(classify("online", "{}").category()).isEqualTo("device_status");
        assertThat(classify("offline", "{}").category()).isEqualTo("device_status");
        assertThat(classify("iotEvent", "{\"content\":{\"event\":\"32000\"}}").category())
                .isEqualTo("iot_event");
        for (String type : new String[] {"videoMotion", "mobileDetect", "human",
                "smartVideoMotion", "openCamera"}) {
            assertThat(classify(type, "{}").category()).isEqualTo("detection");
        }
        assertThat(classify("informationTransfer", "{\"deviceId\":\"TEST-2\"}").category())
                .isEqualTo("transfer");
        assertThat(classify("upgradeFail", "{\"upgradeStatus\":2}").category())
                .isEqualTo("upgrade");
    }

    @Test
    void rejectsUnknownTypeAndMalformedPropertyEvent() {
        assertThatThrownBy(() -> classify("unknown", "{}"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> classify("iotProperty", "{\"content\":{}}"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private Classification classify(String type, String extraFields) throws Exception {
        String body = mapper.writeValueAsString(mapper.readTree(extraFields));
        var node = (com.fasterxml.jackson.databind.node.ObjectNode) mapper.readTree(body);
        node.put("msgType", type);
        return classifier.classify(new EventEnvelope(1, "test-id", 1, "TEST-1", type,
                Instant.parse("2026-09-25T08:00:00Z"), node));
    }
}
