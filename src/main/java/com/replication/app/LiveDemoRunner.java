package com.replication.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Live demo tool — runs INSIDE the Spring Boot context.
 *
 * Activated only when the "live-demo" profile is active:
 *   mvn spring-boot:run -Dspring-boot.run.profiles=live-demo
 *
 * What it does:
 *   1. Truncates all 4 target tables (clean slate).
 *   2. Loads every sample JSON from resources/samples/.
 *   3. Infers the correct Kafka topic for each message by matching it
 *      against the filter-path / filter-value in each schema entry.
 *   4. Calls ReplicationConsumer.onMessage() — the exact same code path
 *      the real Kafka listener uses — so real SQL runs against real MySQL.
 *   5. Queries each table and prints the rows that were written.
 */
@Component
@Profile("live-demo")
public class LiveDemoRunner implements ApplicationRunner {

    private static final Logger log = LogManager.getLogger(LiveDemoRunner.class);

    /** Fallback topic used when no schema filter matches a message. */
    private static final String DEFAULT_TOPIC = "nosql-replication";

    /** Tables to truncate and then display, in a logical order. */
    private static final List<String> TABLES = Arrays.asList(
            "db_invoices", "db_audit_log", "orders", "order_items");

    private final ReplicationConsumer consumer;
    private final ReplicationProperties properties;
    private final JdbcTemplate jdbcTemplate;
    private final ConfigurableApplicationContext context;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public LiveDemoRunner(ReplicationConsumer consumer,
                          ReplicationProperties properties,
                          JdbcTemplate jdbcTemplate,
                          ConfigurableApplicationContext context) {
        this.consumer     = consumer;
        this.properties   = properties;
        this.jdbcTemplate = jdbcTemplate;
        this.context      = context;
    }

    // -------------------------------------------------------------------------
    // ApplicationRunner entry point
    // -------------------------------------------------------------------------

    @Override
    public void run(ApplicationArguments args) throws Exception {

        printBanner("LIVE DEMO — replication-engine");

        // ── Step 1: Clean slate ───────────────────────────────────────────────
        System.out.println("Step 1: Truncating tables...");
        for (String table : TABLES) {
            jdbcTemplate.execute("TRUNCATE TABLE " + table);
            System.out.println("  OK  " + table + " truncated");
        }

        // ── Step 2 + 3 + 4: Load samples, infer topic, fire through consumer ──
        System.out.println();
        System.out.println("Step 2: Loading and processing sample messages...");
        System.out.println();

        Resource[] resources = new PathMatchingResourcePatternResolver()
                .getResources("classpath:samples/*.json");

        // Sort by filename so they run in a deterministic order
        Arrays.sort(resources, (a, b) -> a.getFilename().compareTo(b.getFilename()));

        int sent = 0;
        for (Resource resource : resources) {
            String filename = resource.getFilename();
            String json = org.springframework.util.StreamUtils.copyToString(resource.getInputStream(), StandardCharsets.UTF_8);

            String topic = inferTopic(json);

            System.out.printf("  [%02d] %-35s -> topic: %s%n",
                    ++sent, filename, topic);

            try {
                consumer.onMessage(json, topic);
            } catch (Exception e) {
                System.out.println("       ERROR: " + e.getMessage());
                log.error("Failed to process sample {}: {}", filename, e.getMessage(), e);
            }
        }

        System.out.println();
        System.out.printf("  %d message(s) processed.%n", sent);

        // ── Step 5: Query and print results ───────────────────────────────────
        System.out.println();
        System.out.println("Step 3: Querying database results...");

        for (String table : TABLES) {
            printTable(table);
        }

        printBanner("DEMO COMPLETE");
        context.close();
    }

    // -------------------------------------------------------------------------
    // Topic inference
    // -------------------------------------------------------------------------

    /**
     * Walks all schema entries from application.yml.
     * The first schema whose filter-path / filter-value matches this message
     * determines the topic. Falls back to DEFAULT_TOPIC if no schema matches.
     */
    @SuppressWarnings("unchecked")
    private String inferTopic(String jsonText) {
        try {
            Map<String, Object> json = objectMapper.readValue(jsonText, Map.class);

            for (Map<String, Object> schema : properties.getTargetSchemas()) {
                String filterTopic = (String) schema.get("filter-topic");
                String filterPath  = (String) schema.get("filter-path");
                String filterValue = (String) schema.get("filter-value");

                if (filterTopic == null) continue;

                // Schema has a content filter — check if this message satisfies it
                if (filterPath != null && filterValue != null) {
                    Object actual = SqlBuilder.resolvePath(json, filterPath);
                    if (actual != null && filterValue.equalsIgnoreCase(String.valueOf(actual))) {
                        return filterTopic;
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Could not parse message for topic inference, using default topic: {}", e.getMessage());
        }

        return DEFAULT_TOPIC;
    }

    // -------------------------------------------------------------------------
    // Formatting helpers
    // -------------------------------------------------------------------------

    private void printBanner(String title) {
        String line = "============================================================";
        System.out.println();
        System.out.println(line);
        System.out.printf("  %s%n", title);
        System.out.println(line);
        System.out.println();
    }

    /**
     * Queries the given table and prints all rows as a plain ASCII grid.
     * Column widths adapt to the widest value in each column, capped at 40 chars
     * so wide JSON blobs do not break the layout.
     */
    private void printTable(String tableName) {
        System.out.println("----------------------------------------------------------");
        System.out.println("  Table: " + tableName);

        List<Map<String, Object>> rows = jdbcTemplate.queryForList("SELECT * FROM " + tableName);

        if (rows.isEmpty()) {
            System.out.println("  (no rows)");
            System.out.println();
            return;
        }

        List<String> columns = new java.util.ArrayList<>(rows.get(0).keySet());

        // Calculate column widths
        int[] widths = new int[columns.size()];
        for (int c = 0; c < columns.size(); c++) {
            widths[c] = columns.get(c).length();
        }
        for (Map<String, Object> row : rows) {
            for (int c = 0; c < columns.size(); c++) {
                Object val = row.get(columns.get(c));
                int len = val == null ? 4 : String.valueOf(val).length();
                if (len > widths[c]) widths[c] = len;
            }
        }
        // Cap at 40 chars
        for (int c = 0; c < widths.length; c++) {
            widths[c] = Math.min(widths[c], 40);
        }

        String separator = buildSeparator(widths);
        System.out.println(separator);
        System.out.println(buildRow(columns.toArray(new String[0]), widths));
        System.out.println(separator);

        for (Map<String, Object> row : rows) {
            String[] values = columns.stream()
                    .map(col -> {
                        Object v = row.get(col);
                        return v == null ? "NULL" : String.valueOf(v);
                    })
                    .toArray(String[]::new);
            System.out.println(buildRow(values, widths));
        }

        System.out.println(separator);
        System.out.printf("  %d row(s)%n", rows.size());
        System.out.println();
    }

    private String buildSeparator(int[] widths) {
        StringBuilder sb = new StringBuilder("  +");
        for (int w : widths) {
            for (int i = 0; i < w + 2; i++) sb.append("-");
            sb.append("+");
        }
        return sb.toString();
    }

    private String buildRow(String[] values, int[] widths) {
        StringBuilder sb = new StringBuilder("  |");
        for (int c = 0; c < values.length; c++) {
            String cell = values[c];
            if (cell.length() > widths[c]) {
                cell = cell.substring(0, widths[c] - 3) + "...";
            }
            sb.append(String.format(" %-" + widths[c] + "s |", cell));
        }
        return sb.toString();
    }
}
