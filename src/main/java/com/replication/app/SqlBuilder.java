package com.replication.app;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * Turns one incoming JSON message into a ready-to-run SQL statement, based
 * on the target-schema mapping from application.yml.
 *
 * This class is plain Java on purpose - no Spring, no JDBC, no Kafka - so
 * it can be reused by both the live Kafka consumer (ReplicationConsumer)
 * and the standalone demo tool (DemoRunner).
 */
public final class SqlBuilder {

    private static final String INSERT_OPERATION = "insert";
    private static final String UPDATE_OPERATION = "update";
    private static final String DELETE_OPERATION = "delete";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SqlBuilder() {
    }

    /** Everything you'd want to know about the statement that was generated. */
    public static class Result {
        public final String tableName;
        public final String operation;
        public final String primaryKeyColumn;
        public final String sql;
        public final List<Object> parameters;

        public Result(String tableName, String operation, String primaryKeyColumn,
                      String sql, List<Object> parameters) {
            this.tableName = tableName;
            this.operation = operation;
            this.primaryKeyColumn = primaryKeyColumn;
            this.sql = sql;
            this.parameters = parameters;
        }
    }

    public static Result build(Map<String, Object> json,
                                String operationPath,
                                String userKeyPath,
                                Map<String, Object> targetSchema) {

        String tableName = (String) targetSchema.get("table-name");

        Object rawOperation = resolvePath(json, operationPath);
        if (rawOperation == null) {
            throw new IllegalArgumentException("No value found at operation-path: " + operationPath);
        }
        String operation = String.valueOf(rawOperation);

        LinkedHashMap<String, Object> columnValues = new LinkedHashMap<>();
        String primaryKeyColumn = null;

        for (Map.Entry<String, Object> entry : targetSchema.entrySet()) {
            String columnName = entry.getKey();
            if (columnName.equals("table-name")) {
                continue;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> columnConfig = (Map<String, Object>) entry.getValue();
            String path = (String) columnConfig.get("path");
            Object value = flatten(resolvePath(json, path));
            columnValues.put(columnName, value);

            if (path.equals(userKeyPath)) {
                primaryKeyColumn = columnName;
            }
        }

        if (primaryKeyColumn == null) {
            throw new IllegalStateException("No target-schema column maps to userkey-path: " + userKeyPath);
        }

        if (DELETE_OPERATION.equalsIgnoreCase(operation)) {
            String sql = "DELETE FROM " + tableName + " WHERE " + primaryKeyColumn + " = ?";
            List<Object> parameters = Collections.singletonList(columnValues.get(primaryKeyColumn));
            return new Result(tableName, "DELETE", primaryKeyColumn, sql, parameters);
        }

        if (INSERT_OPERATION.equalsIgnoreCase(operation) || UPDATE_OPERATION.equalsIgnoreCase(operation)) {
            String columns = String.join(", ", columnValues.keySet());
            String placeholders = String.join(", ", Collections.nCopies(columnValues.size(), "?"));

            StringBuilder updateClause = new StringBuilder();
            for (String column : columnValues.keySet()) {
                if (column.equals(primaryKeyColumn)) {
                    continue;
                }
                if (updateClause.length() > 0) {
                    updateClause.append(", ");
                }
                updateClause.append(column).append(" = VALUES(").append(column).append(")");
            }

            String sql = "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + placeholders + ")"
                    + " ON DUPLICATE KEY UPDATE " + updateClause;
            List<Object> parameters = new ArrayList<>(columnValues.values());
            return new Result(tableName, "UPSERT (insert/update)", primaryKeyColumn, sql, parameters);
        }

        throw new IllegalArgumentException("Unknown operation '" + operation + "' found at path: " + operationPath);
    }

    /** Walks a dot-separated path (e.g. "header.uuid") through nested maps. Missing fields return null. */
    @SuppressWarnings("unchecked")
    public static Object resolvePath(Map<String, Object> data, String path) {
        String[] parts = path.split("\\.");
        Object current = data;
        for (String part : parts) {
            if (!(current instanceof Map)) {
                return null;
            }
            current = ((Map<String, Object>) current).get(part);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    /**
     * If the value is a nested object (Map) or array (List), serializes it to a
     * JSON string so it can be stored in a plain TEXT/JSON database column.
     * Simple values (String, Number, Boolean, null) are returned as-is.
     */
    static Object flatten(Object value) {
        if (value instanceof Map || value instanceof List) {
            try {
                return MAPPER.writeValueAsString(value);
            } catch (JsonProcessingException e) {
                // Fall back to toString() if serialization somehow fails
                return value.toString();
            }
        }
        return value;
    }

    /**
     * Builds a human-readable version of the SQL with literal values substituted in
     * place of the ? placeholders. This is for display only - never execute this
     * string directly, use the parameterized `sql` + `parameters` instead.
     */
    public static String preview(String sql, List<Object> parameters) {
        String result = sql;
        for (Object param : parameters) {
            String literal;
            if (param == null) {
                literal = "NULL";
            } else if (param instanceof Number || param instanceof Boolean) {
                literal = String.valueOf(param);
            } else {
                literal = "'" + String.valueOf(param).replace("'", "''") + "'";
            }
            result = result.replaceFirst("\\?", Matcher.quoteReplacement(literal));
        }
        return result;
    }
}
