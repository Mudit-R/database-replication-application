package com.replication.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Consumes NoSQL change-event messages from Kafka and replicates them into
 * one or more RDBMS tables described by app.replication.target-schemas.
 *
 * Each entry in target-schemas is processed independently against the same
 * incoming message. This means a single Kafka message can fan out and write
 * to multiple tables in one step.
 *
 * All the actual mapping/SQL-building logic lives in SqlBuilder so it can
 * be reused by the standalone demo tool (see com.replication.demo.DemoRunner).
 * This class's only job is: read the message, hand it to SqlBuilder for each
 * schema, run the result against the database.
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

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${app.kafka.group-id}", concurrency = "${app.kafka.concurrency}")
    public void onMessage(String message) {
        log.info("Received message: {}", message);
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> json = objectMapper.readValue(message, Map.class);

            String operationPath = properties.getEnvelope().getOperationPath();

            // Fan out: apply the same message to every configured target schema.
            for (Map<String, Object> schema : properties.getTargetSchemas()) {
                String tableName = (String) schema.get("table-name");
                String userKeyPath = (String) schema.get("userkey-path");

                log.info("Processing schema for table '{}'", tableName);

                SqlBuilder.Result result = SqlBuilder.build(json, operationPath, userKeyPath, schema);
                executeResult(result);
            }

        } catch (Exception e) {
            log.error("Failed to process message: {}", message, e);
        }
    }

    private void executeResult(SqlBuilder.Result result) {
        if (result.insertSql != null) {
            // Upsert: try UPDATE first
            int rowsUpdated = jdbcTemplate.update(result.sql, result.parameters.toArray());
            log.info("UPDATE affected {} row(s) on table '{}'", rowsUpdated, result.tableName);
            if (rowsUpdated == 0) {
                // Row didn't exist yet, INSERT it
                jdbcTemplate.update(result.insertSql, result.insertParameters.toArray());
                log.info("No existing row found — INSERT executed for key '{}'", result.primaryKeyColumn);
            }
        } else {
            // DELETE
            jdbcTemplate.update(result.sql, result.parameters.toArray());
            log.info("DELETE executed on table '{}'", result.tableName);
        }
    }
}
