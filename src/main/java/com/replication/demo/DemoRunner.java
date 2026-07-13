package com.replication.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.replication.app.SqlBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

/**
 * Standalone demo tool - NOT part of the Kafka/MySQL app.
 *
 * Reads application.yml and one JSON message, prints the SQL that WOULD be run
 * for every matching schema (including array-expanded schemas).
 *
 * Run with the bundled example:
 *   mvn compile exec:java
 *
 * Run with your own message file:
 *   mvn compile exec:java -Dexec.args="path/to/your-message.json"
 */
public class DemoRunner {

    private static final Logger log = LogManager.getLogger(DemoRunner.class);

    public static void main(String[] args) throws Exception {
        Map<String, Object> yaml = loadYaml();

        @SuppressWarnings("unchecked")
        Map<String, Object> app = (Map<String, Object>) yaml.get("app");
        @SuppressWarnings("unchecked")
        Map<String, Object> replication = (Map<String, Object>) app.get("replication");
        @SuppressWarnings("unchecked")
        Map<String, Object> envelope = (Map<String, Object>) replication.get("envelope");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> targetSchemas =
                (List<Map<String, Object>>) replication.get("target-schemas");

        String operationPath = (String) envelope.get("operation-path");

        String jsonText = loadJsonMessage(args);
        ObjectMapper objectMapper = new ObjectMapper();
        @SuppressWarnings("unchecked")
        Map<String, Object> json = objectMapper.readValue(jsonText, Map.class);

        System.out.println("=================================================");
        System.out.println("Input message:");
        System.out.println(jsonText.trim());
        System.out.println("=================================================");

        for (int i = 0; i < targetSchemas.size(); i++) {
            Map<String, Object> schema = targetSchemas.get(i);
            String tableName = (String) schema.get("table-name");

            System.out.println();
            System.out.println("--- Schema " + (i + 1) + ": " + tableName + " ---");

            if (!schemaApplies(json, schema)) {
                String fp = (String) schema.get("filter-path");
                String fv = (String) schema.get("filter-value");
                System.out.println("[SKIPPED] filter-path='" + fp + "' filter-value='" + fv
                        + "' did not match this message");
                continue;
            }

            String userKeyPath = (String) schema.get("userkey-path");

            try {
                List<SqlBuilder.Result> results =
                        SqlBuilder.buildAll(json, operationPath, userKeyPath, schema);

                if (results.isEmpty()) {
                    System.out.println("[SKIPPED] source-array was missing or empty");
                    continue;
                }

                for (int r = 0; r < results.size(); r++) {
                    SqlBuilder.Result result = results.get(r);
                    if (results.size() > 1) {
                        System.out.println();
                        System.out.println("  Row " + (r + 1) + " of " + results.size() + ":");
                    }
                    printResult(result);
                }

            } catch (Exception e) {
                log.error("Could not build SQL for schema '{}': {}", tableName, e.getMessage());
                System.out.println("ERROR: " + e.getMessage());
            }
        }

        System.out.println();
        System.out.println("=================================================");
    }

    private static void printResult(SqlBuilder.Result result) {
        System.out.println("  Table:           " + result.tableName);
        System.out.println("  Operation:       " + result.operation);
        System.out.println("  Primary key col: " + result.primaryKeyColumn);

        if (result.insertSql != null) {
            System.out.println();
            System.out.println("  Step 1 - UPDATE (runs first):");
            System.out.println("  " + SqlBuilder.preview(result.sql, result.parameters));
            System.out.println();
            System.out.println("  Step 2 - INSERT (runs only if Step 1 updated 0 rows):");
            System.out.println("  " + SqlBuilder.preview(result.insertSql, result.insertParameters));
        } else {
            System.out.println();
            System.out.println("  Generated SQL:");
            System.out.println("  " + SqlBuilder.preview(result.sql, result.parameters));
        }
    }

    private static boolean schemaApplies(Map<String, Object> json, Map<String, Object> schema) {
        String filterPath  = (String) schema.get("filter-path");
        String filterValue = (String) schema.get("filter-value");
        if (filterPath == null || filterValue == null) {
            return true;
        }
        Object actual = SqlBuilder.resolvePath(json, filterPath);
        return actual != null && filterValue.equalsIgnoreCase(String.valueOf(actual));
    }

    private static Map<String, Object> loadYaml() throws Exception {
        try (InputStream in = DemoRunner.class.getClassLoader().getResourceAsStream("application.yml")) {
            if (in == null) throw new IllegalStateException("Could not find application.yml");
            return new Yaml().load(in);
        }
    }

    private static String loadJsonMessage(String[] args) throws Exception {
        if (args.length > 0) {
            return new String(Files.readAllBytes(new File(args[0]).toPath()));
        }
        try (InputStream in = DemoRunner.class.getClassLoader().getResourceAsStream("sample-message.json")) {
            if (in == null) throw new IllegalStateException("Could not find sample-message.json");
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            byte[] chunk = new byte[4096];
            int n;
            while ((n = in.read(chunk)) != -1) buffer.write(chunk, 0, n);
            return buffer.toString();
        }
    }
}
