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
 * It reads application.yml (for the mapping config) and one JSON message,
 * then prints the SQL that WOULD be run for each target schema. It never
 * connects to Kafka or MySQL, so you can use it to sanity-check your config
 * on your laptop before wiring up any real infrastructure.
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
        List<Map<String, Object>> targetSchemas = (List<Map<String, Object>>) replication.get("target-schemas");

        String operationPath = (String) envelope.get("operation-path");

        String jsonText = loadJsonMessage(args);

        ObjectMapper objectMapper = new ObjectMapper();
        @SuppressWarnings("unchecked")
        Map<String, Object> json = objectMapper.readValue(jsonText, Map.class);

        System.out.println("=================================================");
        System.out.println("Input message:");
        System.out.println(jsonText.trim());
        System.out.println("=================================================");

        // Fan out: print SQL for every configured target schema.
        for (int i = 0; i < targetSchemas.size(); i++) {
            Map<String, Object> schema = targetSchemas.get(i);
            String tableName = (String) schema.get("table-name");
            String userKeyPath = (String) schema.get("userkey-path");

            System.out.println();
            System.out.println("--- Schema " + (i + 1) + ": " + tableName + " ---");

            try {
                log.info("Building SQL for schema '{}' ...", tableName);
                SqlBuilder.Result result = SqlBuilder.build(json, operationPath, userKeyPath, schema);

                log.info("SQL built - operation={}, table={}, pk={}",
                        result.operation, result.tableName, result.primaryKeyColumn);

                System.out.println("Table:            " + result.tableName);
                System.out.println("Operation:        " + result.operation);
                System.out.println("Primary key col:  " + result.primaryKeyColumn);

                if (result.insertSql != null) {
                    // Upsert: show both statements
                    System.out.println();
                    System.out.println("Step 1 - UPDATE (runs first):");
                    System.out.println(SqlBuilder.preview(result.sql, result.parameters));
                    System.out.println();
                    System.out.println("Step 2 - INSERT (runs only if Step 1 updated 0 rows):");
                    System.out.println(SqlBuilder.preview(result.insertSql, result.insertParameters));
                    System.out.println();
                    System.out.println("Parameterized UPDATE: " + result.sql);
                    System.out.println("Parameters:           " + result.parameters);
                    System.out.println();
                    System.out.println("Parameterized INSERT: " + result.insertSql);
                    System.out.println("Parameters:           " + result.insertParameters);
                } else {
                    // Delete: single statement
                    System.out.println();
                    System.out.println("Generated SQL (readable):");
                    System.out.println(SqlBuilder.preview(result.sql, result.parameters));
                    System.out.println();
                    System.out.println("Parameterized form: " + result.sql);
                    System.out.println("Parameters:         " + result.parameters);
                }
            } catch (Exception e) {
                log.error("Could not build SQL for schema '{}': {}", tableName, e.getMessage());
                System.out.println("Could not build SQL for schema '" + tableName + "': " + e.getMessage());
            }
        }

        System.out.println();
        System.out.println("=================================================");
    }

    private static Map<String, Object> loadYaml() throws Exception {
        try (InputStream in = DemoRunner.class.getClassLoader().getResourceAsStream("application.yml")) {
            if (in == null) {
                throw new IllegalStateException("Could not find application.yml on the classpath.");
            }
            return new Yaml().load(in);
        }
    }

    private static String loadJsonMessage(String[] args) throws Exception {
        if (args.length > 0) {
            return new String(Files.readAllBytes(new File(args[0]).toPath()));
        }
        try (InputStream in = DemoRunner.class.getClassLoader().getResourceAsStream("sample-message.json")) {
            if (in == null) {
                throw new IllegalStateException("Could not find sample-message.json on the classpath.");
            }
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            byte[] chunk = new byte[4096];
            int n;
            while ((n = in.read(chunk)) != -1) {
                buffer.write(chunk, 0, n);
            }
            return buffer.toString();
        }
    }
}
