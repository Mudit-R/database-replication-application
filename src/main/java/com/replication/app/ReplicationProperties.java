package com.replication.app;

import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Holds the merged replication configuration loaded by MappingLoader at startup.
 *
 * MappingLoader scans the configured mappings directory, reads all *.yml files,
 * and populates this bean with the combined set of targetSchemas and the envelope
 * definition. No longer bound via @ConfigurationProperties — data is injected
 * programmatically so that schemas can come from any number of external files.
 */
@Component("replicationProperties")
public class ReplicationProperties {

    private Envelope envelope;
    private List<Map<String, Object>> targetSchemas;

    public Envelope getEnvelope() {
        return envelope;
    }

    public void setEnvelope(Envelope envelope) {
        this.envelope = envelope;
    }

    public List<Map<String, Object>> getTargetSchemas() {
        return targetSchemas;
    }

    public void setTargetSchemas(List<Map<String, Object>> targetSchemas) {
        this.targetSchemas = targetSchemas;
    }

    /**
     * Dynamically extracts distinct, non-empty filter-topic values from all loaded schemas.
     * Used by @KafkaListener in ReplicationConsumer to subscribe to all required topics
     * without any manual configuration.
     */
    public List<String> getTopics() {
        if (targetSchemas == null || targetSchemas.isEmpty()) {
            return Collections.emptyList();
        }
        return targetSchemas.stream()
                .map(schema -> (String) schema.get("filter-topic"))
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(t -> !t.isEmpty())
                .distinct()
                .collect(Collectors.toList());
    }

    public static class Envelope {
        private String operationPath;

        public String getOperationPath() {
            return operationPath;
        }

        public void setOperationPath(String operationPath) {
            this.operationPath = operationPath;
        }
    }
}
