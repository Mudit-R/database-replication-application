package com.replication.app;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Binds the "app.replication.*" section of application.yml.
 *
 * targetSchemas is a list so that one incoming Kafka message can be applied
 * to multiple RDBMS tables. Each entry in the list is an independent
 * target-schema block with its own table-name, userkey-path, and column
 * mappings. The consumer iterates the list and executes SQL for each one.
 */
@Component
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
