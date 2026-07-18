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
 * Turns one incoming JSON message into one or more ready-to-run SQL statements,
 * based on the target-schema mapping from application.yml.
 *
 * Two entry points:
 *   build()    - single result, for schemas with no source-array (unchanged behaviour)
 *   buildAll() - list of results; iterates source-array if present, otherwise
 *                delegates to build() and returns a single-element list.
 */
public final class SqlBuilder {

    private static final String INSERT_OPERATION = "insert";
    private static final String UPDATE_OPERATION = "update";
    private static final String DELETE_OPERATION = "delete";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SqlBuilder() {
    }

    /** Everything you'd want to know about one generated SQL statement. */
    public static class Result {
        public final String tableName;
        public final String operation;
        public final String primaryKeyColumn;
        public final String sql;                    // UPDATE for upsert, DELETE for delete
        public final List<Object> parameters;       // params for sql
        public final String insertSql;              // INSERT for upsert, null for delete
        public final List<Object> insertParameters; // params for insertSql, null for delete

        // Used for DELETE
        public Result(String tableName, String operation, String primaryKeyColumn,
                      String sql, List<Object> parameters) {
            this(tableName, operation, primaryKeyColumn, sql, parameters, null, null);
        }

        // Used for UPSERT
        public Result(String tableName, String operation, String primaryKeyColumn,
                      String sql, List<Object> parameters,
                      String insertSql, List<Object> insertParameters) {
            this.tableName = tableName;
            this.operation = operation;
            this.primaryKeyColumn = primaryKeyColumn;
            this.sql = sql;
            this.parameters = parameters;
            this.insertSql = insertSql;
            this.insertParameters = insertParameters;
        }
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Returns one SQL result for a schema that has no source-array.
     * Kept for backwards compatibility — delegates to buildAll() internally.
     */
    public static Result build(Map<String, Object> json,
                               String operationPath,
                               String userKeyPath,
                               Map<String, Object> targetSchema) {
        List<Result> all = buildAll(json, operationPath, userKeyPath, targetSchema);
        if (all.isEmpty()) {
            throw new IllegalStateException("buildAll returned empty list for non-array schema");
        }
        return all.get(0);
    }

    /**
     * Main entry point.
     *
     * If the schema has no source-array: returns a single-element list (one row).
     * If the schema has source-array:    iterates every element of that array and
     *   returns one Result per element. Column paths are resolved against each
     *   element first; if missing, they fall back to the top-level message.
     *   Columns with pk-source: "generated" get an auto key of <parent_uuid>-<index>.
     */
    public static List<Result> buildAll(Map<String, Object> json,
                                        String operationPath,
                                        String userKeyPath,
                                        Map<String, Object> targetSchema) {

        String sourceArrayPath = (String) targetSchema.get("source-array");

        if (sourceArrayPath == null) {
            // No array — build exactly one result from the top-level message
            return Collections.singletonList(buildSingle(json, json, -1, operationPath, userKeyPath, targetSchema));
        }

        // Array mode — iterate each element
        Object arrayObj = resolvePath(json, sourceArrayPath);
        if (!(arrayObj instanceof List)) {
            // Array missing or empty — skip silently
            return Collections.emptyList();
        }

        @SuppressWarnings("unchecked")
        List<Object> array = (List<Object>) arrayObj;

        List<Result> results = new ArrayList<>();
        for (int i = 0; i < array.size(); i++) {
            Object elem = array.get(i);
            @SuppressWarnings("unchecked")
            Map<String, Object> element = (elem instanceof Map)
                    ? (Map<String, Object>) elem
                    : Collections.emptyMap();
            results.add(buildSingle(json, element, i, operationPath, userKeyPath, targetSchema));
        }
        return results;
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Builds one SQL Result.
     *
     * @param topLevel  the full incoming JSON message (used for operation, fallback paths)
     * @param context   the object to resolve column paths against first
     *                  (equals topLevel for non-array schemas; equals the array element
     *                   for array schemas)
     * @param index     position of this element in the source array (-1 if not array mode)
     */
    @SuppressWarnings("unchecked")
    private static Result buildSingle(Map<String, Object> topLevel,
                                      Map<String, Object> context,
                                      int index,
                                      String operationPath,
                                      String userKeyPath,
                                      Map<String, Object> targetSchema) {

        String tableName = (String) targetSchema.get("table-name");

        Object rawOperation = resolvePath(topLevel, operationPath);
        if (rawOperation == null) {
            throw new IllegalArgumentException("No value found at operation-path: " + operationPath);
        }
        String operation = String.valueOf(rawOperation);

        // Parent UUID — used for generated keys
        String parentUuid = String.valueOf(resolvePath(topLevel, userKeyPath));

        LinkedHashMap<String, Object> columnValues = new LinkedHashMap<>();
        String primaryKeyColumn = null;

        for (Map.Entry<String, Object> entry : targetSchema.entrySet()) {
            String columnName = entry.getKey();
            if (columnName.startsWith("filter-") || "table-name".equals(columnName)
                    || "userkey-path".equals(columnName) || "source-array".equals(columnName)
                    || "required-paths".equals(columnName)) {
                continue;
            }
            if (!(entry.getValue() instanceof Map)) {
                continue;
            }

            Map<String, Object> columnConfig = (Map<String, Object>) entry.getValue();
            String path = (String) columnConfig.get("path");
            Object value;

            if ("generated".equals(columnConfig.get("pk-source"))) {
                // Auto-generate composite key: <parent_uuid>-<array_index>
                value = parentUuid + "-" + index;
                primaryKeyColumn = columnName;
            } else {
                // Resolve from context (array element) first, then fall back to top-level
                value = resolvePath(context, path);
                if (value == null) {
                    value = resolvePath(topLevel, path);
                }
                value = flatten(value);

                if (userKeyPath != null && userKeyPath.equals(path) && primaryKeyColumn == null) {
                    primaryKeyColumn = columnName;
                }
            }

            columnValues.put(columnName, value);
        }

        if (primaryKeyColumn == null) {
            throw new IllegalStateException(
                    "No target-schema column maps to userkey-path: " + userKeyPath);
        }

        if (DELETE_OPERATION.equalsIgnoreCase(operation)) {
            String sql = "DELETE FROM " + tableName + " WHERE " + primaryKeyColumn + " = ?";
            List<Object> parameters = Collections.singletonList(columnValues.get(primaryKeyColumn));
            return new Result(tableName, "DELETE", primaryKeyColumn, sql, parameters);
        }

        if (INSERT_OPERATION.equalsIgnoreCase(operation) || UPDATE_OPERATION.equalsIgnoreCase(operation)) {

            // Standard UPDATE — works on all RDBMS
            StringBuilder setClause = new StringBuilder();
            List<Object> updateParams = new ArrayList<>();
            for (Map.Entry<String, Object> e : columnValues.entrySet()) {
                if (e.getKey().equals(primaryKeyColumn)) continue;
                if (setClause.length() > 0) setClause.append(", ");
                setClause.append(e.getKey()).append(" = ?");
                updateParams.add(e.getValue());
            }
            updateParams.add(columnValues.get(primaryKeyColumn));
            String updateSql = "UPDATE " + tableName + " SET " + setClause
                    + " WHERE " + primaryKeyColumn + " = ?";

            // Standard INSERT — works on all RDBMS
            String columns = String.join(", ", columnValues.keySet());
            String placeholders = String.join(", ", Collections.nCopies(columnValues.size(), "?"));
            String insertSql = "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + placeholders + ")";
            List<Object> insertParams = new ArrayList<>(columnValues.values());

            return new Result(tableName, "UPSERT (insert/update)", primaryKeyColumn,
                    updateSql, updateParams, insertSql, insertParams);
        }

        throw new IllegalArgumentException(
                "Unknown operation '" + operation + "' found at path: " + operationPath);
    }

    // -------------------------------------------------------------------------
    // Utility methods (public so DemoRunner can reuse them)
    // -------------------------------------------------------------------------

    /** Walks a dot-separated path (e.g. "header.uuid") through nested maps. Missing fields return null. */
    @SuppressWarnings("unchecked")
    public static Object resolvePath(Map<String, Object> data, String path) {
        if (data == null || path == null) return null;
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
                return value.toString();
            }
        }
        return value;
    }

    /**
     * Builds a human-readable version of the SQL with literal values substituted in
     * place of the ? placeholders. For display only — never execute this string directly.
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
