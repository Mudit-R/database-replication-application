package com.replication.app;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Binds the "app.replication.*" section of mapping.yml (imported by application.yml).
 *
 * targetSchemas is a list so that one incoming Kafka message can be applied
 * to multiple RDBMS tables. Each entry in the list is an independent
 * target-schema block with its own table-name, userkey-path, and column
 * mappings.
 *
 * It also dynamically resolves the list of Kafka topics to listen to
 * by extracting all unique "filter-topic" declarations from target-schemas.
 */
@Component("replicationProperties")
@ConfigurationProperties(prefix = "app.replication")
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
     * Dynamically extracts distinct, non-empty filter-topic values from target-schemas.
     * Used by @KafkaListener in ReplicationConsumer to automatically subscribe to all schema topics.
     */
    public List<String> getTopics() {
        if (targetSchemas == null) {
            return List.of();
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
