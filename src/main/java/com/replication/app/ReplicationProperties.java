package com.replication.app;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Binds the "app.replication.*" section of application.yml.
 *
 * targetSchema is kept as a raw Map because it mixes one plain value
 * ("table-name") with several nested {path, type} column definitions.
 * The consumer pulls "table-name" out separately and treats every
 * other entry as a column mapping.
 */
@Component
@ConfigurationProperties(prefix = "app.replication")
public class ReplicationProperties {

    private Envelope envelope;
    private Map<String, Object> targetSchema;

    public Envelope getEnvelope() {
        return envelope;
    }

    public void setEnvelope(Envelope envelope) {
        this.envelope = envelope;
    }

    public Map<String, Object> getTargetSchema() {
        return targetSchema;
    }

    public void setTargetSchema(Map<String, Object> targetSchema) {
        this.targetSchema = targetSchema;
    }

    public static class Envelope {
        private String setPath;
        private String userkeyPath;
        private String operationPath;

        public String getSetPath() {
            return setPath;
        }

        public void setSetPath(String setPath) {
            this.setPath = setPath;
        }

        public String getUserkeyPath() {
            return userkeyPath;
        }

        public void setUserkeyPath(String userkeyPath) {
            this.userkeyPath = userkeyPath;
        }

        public String getOperationPath() {
            return operationPath;
        }

        public void setOperationPath(String operationPath) {
            this.operationPath = operationPath;
        }
    }
}
