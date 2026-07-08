package com.replication.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Consumes NoSQL change-event messages from Kafka and replicates them into
 * the existing MySQL table described by app.replication.target-schema.
 *
 * All the actual mapping/SQL-building logic lives in SqlBuilder so it can
 * be reused by the standalone demo tool (see com.replication.demo.DemoRunner).
 * This class's only job is: read the message, hand it to SqlBuilder, run
 * the result against the database.
 */
@Component
public class ReplicationConsumer {

    private final ReplicationProperties properties;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ReplicationConsumer(ReplicationProperties properties, JdbcTemplate jdbcTemplate) {
        this.properties = properties;
        this.jdbcTemplate = jdbcTemplate;
    }

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${app.kafka.group-id}", concurrency = "${app.kafka.concurrency}")
    public void onMessage(String message) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> json = objectMapper.readValue(message, Map.class);

            SqlBuilder.Result result = SqlBuilder.build(
                    json,
                    properties.getEnvelope().getOperationPath(),
                    properties.getEnvelope().getUserkeyPath(),
                    properties.getTargetSchema());

            jdbcTemplate.update(result.sql, result.parameters.toArray());
        } catch (Exception e) {
            System.err.println("Failed to process message: " + message);
            e.printStackTrace();
        }
    }
}
