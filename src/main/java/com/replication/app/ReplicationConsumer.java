package com.replication.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Consumes change-event messages from multiple Kafka topics and replicates them into
 * one or more RDBMS tables described by app.replication.target-schemas.
 *
 * It filters schemas dynamically based on the source Kafka topic and/or internal message properties.
 */
@Component
public class ReplicationConsumer {

    private static final Logger log = LogManager.getLogger(ReplicationConsumer.class);

    private final ReplicationProperties properties;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ReplicationConsumer(ReplicationProperties properties, JdbcTemplate jdbcTemplate) {
        this.properties = properties;
        this.jdbcTemplate = jdbcTemplate;
    }

    @KafkaListener(topics = "#{'${app.kafka.topics}'.split(',')}", groupId = "${app.kafka.group-id}", concurrency = "${app.kafka.concurrency}")
    public void onMessage(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Received message on topic '{}': {}", topic, message);
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> json = objectMapper.readValue(message, Map.class);

            String operationPath = properties.getEnvelope().getOperationPath();

            for (Map<String, Object> schema : properties.getTargetSchemas()) {
                String tableName = (String) schema.get("table-name");

                if (!schemaApplies(json, schema, topic)) {
                    log.info("Schema '{}' skipped — filter did not match or required path was missing", tableName);
                    continue;
                }

                String userKeyPath = (String) schema.get("userkey-path");

                // buildAll handles both single-row and array-expansion schemas
                List<SqlBuilder.Result> results =
                        SqlBuilder.buildAll(json, operationPath, userKeyPath, schema);

                if (results.isEmpty()) {
                    log.info("Schema '{}' produced 0 rows (source-array was empty)", tableName);
                }

                for (SqlBuilder.Result result : results) {
                    executeResult(result);
                }
            }

        } catch (Exception e) {
            log.error("Failed to process message: {}", message, e);
        }
    }

    private void executeResult(SqlBuilder.Result result) {
        if (result.insertSql != null) {
            int rowsUpdated = jdbcTemplate.update(result.sql, result.parameters.toArray());
            log.info("UPDATE affected {} row(s) on table '{}'", rowsUpdated, result.tableName);
            if (rowsUpdated == 0) {
                jdbcTemplate.update(result.insertSql, result.insertParameters.toArray());
                log.info("No existing row — INSERT executed for key '{}'", result.primaryKeyColumn);
            }
        } else {
            jdbcTemplate.update(result.sql, result.parameters.toArray());
            log.info("DELETE executed on table '{}'", result.tableName);
        }
    }

    private boolean schemaApplies(Map<String, Object> json, Map<String, Object> schema, String topic) {
        String filterTopic = (String) schema.get("filter-topic");
        if (filterTopic != null && !filterTopic.equalsIgnoreCase(topic)) {
            return false;
        }
        
        String filterPath  = (String) schema.get("filter-path");
        String filterValue = (String) schema.get("filter-value");
        if (filterPath != null && filterValue != null) {
            Object actual = SqlBuilder.resolvePath(json, filterPath);
            if (actual == null || !filterValue.equalsIgnoreCase(String.valueOf(actual))) {
                return false;
            }
        }

        // Format Recognition layer: validate presence of required paths
        @SuppressWarnings("unchecked")
        List<String> requiredPaths = (List<String>) schema.get("required-paths");
        if (requiredPaths != null) {
            for (String path : requiredPaths) {
                if (SqlBuilder.resolvePath(json, path) == null) {
                    log.info("Schema '{}' skipped — required path '{}' is missing or null", schema.get("table-name"), path);
                    return false;
                }
            }
        }

        return true;
    }
}
