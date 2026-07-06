# Replication Engine — Complete Line-by-Line Guide

> This document explains every single line of every file in the project,
> written for someone who does not know Java.
> The order follows a natural presentation flow: what problem we solve →
> how to configure it → how to run the demo → how the code works inside.

---

## Table of Contents

1. [What This Project Does (Big Picture)](#1-what-this-project-does-big-picture)
2. [File Structure](#2-file-structure)
3. [The Input: sample-message.json](#3-the-input-sample-messagejson)
4. [The Config File: application.yml](#4-the-config-file-applicationyml)
5. [The Build File: pom.xml](#5-the-build-file-pomxml)
6. [App Code: ReplicationEngineApplication.java](#6-app-code-replicationengineapplicationjava)
7. [App Code: ReplicationProperties.java](#7-app-code-replicationpropertiesjava)
8. [App Code: SqlBuilder.java](#8-app-code-sqlbuilderjava)
9. [App Code: ReplicationConsumer.java](#9-app-code-replicationconsumerjava)
10. [Demo Code: DemoRunner.java](#10-demo-code-demorunnerjava)
11. [How to Run the Demo](#11-how-to-run-the-demo)

---

## 1. What This Project Does (Big Picture)

### The Problem

Many companies store data in two types of databases simultaneously:

- A **NoSQL database** (e.g. MongoDB, DynamoDB, Cassandra) — fast, flexible,
  handles huge amounts of data quickly. Data is stored as JSON-like documents
  with no fixed structure.
- A **Relational database / RDBMS** (e.g. MySQL) — structured, uses rows and
  columns, great for reporting and querying with SQL.

When a record changes in the NoSQL database (someone creates an invoice, updates
a vendor name, or deletes a record), that change needs to be reflected in MySQL
too. This process is called **database replication**.

### What This App Does

This app is a **database replication engine**. It:

1. **Listens** to a Kafka topic (a message queue). Whenever something changes in
   the NoSQL database, a JSON message describing that change is published to Kafka.
2. **Reads** that JSON message and figures out what changed.
3. **Generates** the correct SQL statement (INSERT, UPDATE, or DELETE) to apply
   the same change to the MySQL table.
4. **Executes** that SQL against MySQL.

### The Mapping Problem

The NoSQL JSON message and the MySQL table do not use the same field names.
For example, the JSON field `"vendor"` maps to the MySQL column `vendor_name`.
The field `"For_order"` maps to `order_for`. This mapping is defined in the
config file (`application.yml`) so you can change it without touching any code.

### The Demo Tool

Because Kafka and MySQL require real infrastructure to run, a standalone
**demo tool** is included. You give it a JSON message and it prints the SQL
that would be generated — no Kafka, no MySQL required.

---

## 2. File Structure

```
replication-engine/
|
+-- pom.xml                          <- Build instructions (like a package.json)
|
+-- src/main/resources/
|   +-- application.yml              <- All configuration (the only file you edit day-to-day)
|   +-- sample-message.json         <- Example Kafka message used by the demo
|
+-- src/main/java/com/replication/
    |
    +-- app/                         <- The real application (Spring Boot + Kafka + MySQL)
    |   +-- ReplicationEngineApplication.java   <- Entry point (start here)
    |   +-- ReplicationProperties.java          <- Reads config from application.yml
    |   +-- SqlBuilder.java                     <- Core logic: JSON -> SQL
    |   +-- ReplicationConsumer.java            <- Listens to Kafka, runs SQL
    |
    +-- demo/                        <- The demo tool (no Kafka/MySQL needed)
        +-- DemoRunner.java          <- Run this to preview SQL from a JSON message
```

**Why two folders under `java/`?**
`app/` is the real production application. `demo/` is a testing/presentation tool
that lets you try the logic without any infrastructure.

---

## 3. The Input: sample-message.json

This file is what a real Kafka message from the NoSQL database would look like.
It is used by the demo tool as the example input.

```json
{
  "header": {
    "db": "dbsdaf",
    "tableName": "invoicek",
    "uuid": "INV-8869",
    "action": "delete"
  },
  "invoice_amount": 12000.50,
  "vendor": "f LLC",
  "paid": true,
  "child": {
    "for_order": "orderid1"
  },
  "For_order": "parent_orderid1",
  "random_garbage_field": "ignore me"
}
```

**Line-by-line explanation:**

| Field | What it means |
|---|---|
| `"header": { ... }` | A nested section inside the JSON. The NoSQL database puts metadata here. |
| `"db": "dbsdaf"` | The name of the source NoSQL database. Not used by this app currently. |
| `"tableName": "invoicek"` | The source table/collection name. Not used by this app currently (noted in application.yml). |
| `"uuid": "INV-8869"` | The unique ID of this record. This becomes the primary key in MySQL. |
| `"action": "delete"` | What happened — `insert`, `update`, or `delete`. This tells the app which SQL to generate. |
| `"invoice_amount": 12000.50` | A data field — maps to the `invoiceamount` MySQL column. |
| `"vendor": "f LLC"` | A data field — maps to the `vendor_name` MySQL column. |
| `"paid": true` | A boolean field — maps to the `paid_paise` MySQL column. |
| `"child": { "for_order": "orderid1" }` | A nested object. The field inside it is accessed with dot-notation: `child.for_order`. Maps to `column1`. |
| `"For_order": "parent_orderid1"` | A top-level field — maps to `order_for` MySQL column. |
| `"random_garbage_field": "ignore me"` | A field that does NOT exist in the mapping config. The app silently ignores it. |

**Key point:** The JSON structure can be messy and inconsistent — it can have extra
fields, nested objects, missing fields. The app handles all of this gracefully.

---

## 4. The Config File: application.yml

This is the most important file for day-to-day use. **You never need to change
Java code to add a new column or change a field mapping — you only edit this file.**

```yaml
spring:
  application:
    name: replication-engine

  datasource:
    url: jdbc:mysql://localhost:3306/replication_db
    username: root
    password: password
    driver-class-name: com.mysql.cj.jdbc.Driver

  kafka:
    bootstrap-servers: localhost:9092

app:
  kafka:
    topic: nosql-replication
    group-id: replication-consumer-group
    concurrency: 3

  replication:
    envelope:
      set-path: "header.tableName"   #not being used for the sql generation currently
      userkey-path: "header.uuid"
      operation-path: "header.action"

    target-schema:
      table-name: "db_invoices"
      invoice_id:
        path: "header.uuid"
        type: "VARCHAR(255)"
      invoiceamount:
        path: "invoice_amount"
        type: "DECIMAL(20,6)"
      vendor_name:
        path: "vendor"
        type: "VARCHAR(255)"
      paid_paise:
        path: "paid"
        type: "BOOLEAN"
      order_for:
        path: "For_order"
        type: "VARCHAR(255)"
      column1:
        path: "child.for_order"
        type: "VARCHAR(255)"
```

**Line-by-line explanation:**

#### Spring section (infrastructure settings)

| Line | What it means |
|---|---|
| `spring:` | Start of configuration for the Spring Boot framework (the framework running this app). |
| `application: name: replication-engine` | The name of this app. Used in logs and monitoring. |
| `datasource: url: jdbc:mysql://localhost:3306/replication_db` | The address of the MySQL database. `localhost` = same machine, `3306` = MySQL's default port, `replication_db` = database name. Change this for production. |
| `username: root` | MySQL login username. Change for production. |
| `password: password` | MySQL login password. Change for production. |
| `driver-class-name: com.mysql.cj.jdbc.Driver` | Tells Java which library to use to talk to MySQL. This line never changes for MySQL. |
| `kafka: bootstrap-servers: localhost:9092` | The address of the Kafka server. `9092` is Kafka's default port. |

#### App / Kafka section

| Line | What it means |
|---|---|
| `topic: nosql-replication` | The name of the Kafka topic (channel) this app listens to. Messages from the NoSQL database arrive here. |
| `group-id: replication-consumer-group` | A Kafka concept — identifies this app as part of a group of consumers. Kafka uses this to track which messages have been processed. |
| `concurrency: 3` | How many messages to process at the same time in parallel (3 threads). Increase for higher throughput. |

#### Envelope section (tells the app where to find key metadata inside the JSON)

| Line | What it means |
|---|---|
| `set-path: "header.tableName"` | The path in the JSON that holds the source table name. Currently not used in SQL generation (noted in comment). |
| `userkey-path: "header.uuid"` | The path in the JSON that holds the unique record ID. The app uses this as the primary key in MySQL. In the example, this resolves to `"INV-8869"`. |
| `operation-path: "header.action"` | The path in the JSON that says what operation happened. The app reads this to decide INSERT, UPDATE, or DELETE. |

**What is dot-notation (e.g. `"header.uuid"`)?**
It means "go into the `header` section, then get the `uuid` field". For `"child.for_order"`,
it means "go into the `child` section, then get the `for_order` field". You can go as
many levels deep as you need.

#### Target-schema section (the column mapping)

This is the heart of the configuration. It defines the MySQL table and maps
each MySQL column to a field path in the JSON.

| Line | What it means |
|---|---|
| `table-name: "db_invoices"` | The name of the MySQL table to write to. |
| `invoice_id:` | This is a MySQL column name. |
| `  path: "header.uuid"` | To get the value for `invoice_id`, look at `header.uuid` in the JSON. |
| `  type: "VARCHAR(255)"` | The MySQL data type. Informational — used for documentation, not enforced by the code. |
| `invoiceamount:` | MySQL column. Gets its value from `invoice_amount` in the JSON. |
| `vendor_name:` | MySQL column. Gets its value from `vendor` in the JSON (different name!). |
| `paid_paise:` | MySQL column. Gets its value from `paid` in the JSON. |
| `order_for:` | MySQL column. Gets its value from `For_order` in the JSON. |
| `column1:` | MySQL column. Gets its value from `child.for_order` — a nested field. |

**To add a new column:** Simply add a new entry here with the column name,
its `path` in the JSON, and its `type`. No Java code changes needed.

---

## 5. The Build File: pom.xml

`pom.xml` is Maven's build file. Think of it like `package.json` in Node.js — it
defines the project name, dependencies (external libraries), and how to build/run.

**Line-by-line explanation:**

| Section / Line | What it means |
|---|---|
| `<modelVersion>4.0.0</modelVersion>` | The version of Maven's file format. Always `4.0.0`. Never changes. |
| `<parent> spring-boot-starter-parent 3.3.5 </parent>` | This app is a child of Spring Boot's standard project template. This one block gives us sensible defaults for dozens of settings so we don't have to configure them manually. `3.3.5` is the Spring Boot version. |
| `<groupId>com.replication</groupId>` | The company/team namespace for this project. Like a domain name in reverse. |
| `<artifactId>replication-engine</artifactId>` | The name of this specific project. |
| `<version>1.0.0</version>` | The version of this app. |
| `<packaging>jar</packaging>` | Build output is a `.jar` file (a single executable Java archive). |
| `<java.version>22</java.version>` | This project requires Java 22 or compatible. |
| `spring-boot-starter` | The core Spring Boot framework — handles startup, config loading, logging. |
| `spring-boot-starter-jdbc` | Gives the app a ready-to-use `JdbcTemplate` to run SQL against MySQL. |
| `spring-kafka` | Allows the app to connect to Kafka and listen for messages. |
| `jackson-databind` | The library that parses JSON strings into Java objects (Maps). |
| `snakeyaml` | The library that reads YAML files. Used by the demo tool to read `application.yml` directly. |
| `mysql-connector-j` | The JDBC driver — the bridge between Java and MySQL. `scope=runtime` means it is only needed when running, not compiling. |
| `spring-boot-maven-plugin` + `mainClass` | When you do `mvn package`, this creates a single executable JAR. The `mainClass` tells Maven which class has the `main()` method to start the real app. |
| `exec-maven-plugin` + `mainClass` | Allows `mvn compile exec:java` to run the demo tool. The `mainClass` here points to `DemoRunner`, not the real app. |

---

## 6. App Code: ReplicationEngineApplication.java

This is the **entry point** of the real Spring Boot application — the first file
that runs when you start the app for production use (with Kafka and MySQL).

```java
package com.replication.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ReplicationEngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReplicationEngineApplication.class, args);
    }
}
```

| Line | What it means |
|---|---|
| `package com.replication.app;` | Declares this file belongs to the `app` group. Java uses this like a folder path to organise classes. |
| `import org.springframework.boot.SpringApplication;` | Brings in Spring Boot's launcher class so we can use it below. |
| `import ...SpringBootApplication;` | Brings in the `@SpringBootApplication` annotation. |
| `@SpringBootApplication` | A single annotation that does three things at once: (1) marks this as a Spring Boot app, (2) enables automatic configuration — Spring figures out what you need from your dependencies, (3) tells Spring to scan this package and find all components automatically. |
| `public class ReplicationEngineApplication {` | Defines the class. `public` means anyone can access it. |
| `public static void main(String[] args) {` | The standard Java entry point. The JVM calls this method first when the program starts. `String[] args` are any command-line arguments passed in. |
| `SpringApplication.run(ReplicationEngineApplication.class, args);` | Starts the entire Spring Boot application. This one line: reads `application.yml`, connects to Kafka, connects to MySQL, starts listening for messages. Everything is automatic from here. |

**Why is this file so small?** Spring Boot's philosophy is "convention over
configuration" — it reads your dependencies and config, and sets everything up
automatically. You only write code for what is unique to your app.

---

## 7. App Code: ReplicationProperties.java

This file's job is to **read the `app.replication.*` section of `application.yml`**
and make it available to the rest of the Java code as typed objects you can
call methods on.

```java
package com.replication.app;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import java.util.Map;

@Component
@ConfigurationProperties(prefix = "app.replication")
public class ReplicationProperties {

    private Envelope envelope;
    private Map<String, Object> targetSchema;

    public Envelope getEnvelope() { return envelope; }
    public void setEnvelope(Envelope envelope) { this.envelope = envelope; }

    public Map<String, Object> getTargetSchema() { return targetSchema; }
    public void setTargetSchema(Map<String, Object> targetSchema) { this.targetSchema = targetSchema; }

    public static class Envelope {
        private String setPath;
        private String userkeyPath;
        private String operationPath;

        public String getSetPath() { return setPath; }
        public void setSetPath(String setPath) { this.setPath = setPath; }

        public String getUserkeyPath() { return userkeyPath; }
        public void setUserkeyPath(String userkeyPath) { this.userkeyPath = userkeyPath; }

        public String getOperationPath() { return operationPath; }
        public void setOperationPath(String operationPath) { this.operationPath = operationPath; }
    }
}
```

| Line(s) | What it means |
|---|---|
| `@Component` | Tells Spring "manage this object for me". Spring creates one instance of this class at startup and makes it available to any other class that needs it. |
| `@ConfigurationProperties(prefix = "app.replication")` | Tells Spring to read the `app.replication` section from `application.yml` and automatically fill in this class's fields. For example, `app.replication.envelope.operation-path` becomes the `operationPath` field. |
| `private Envelope envelope;` | A field that holds the `envelope:` subsection from the config (the three path settings). |
| `private Map<String, Object> targetSchema;` | A field that holds the entire `target-schema:` section as a key-value map (column name to column config). |
| `public Envelope getEnvelope()` | A **getter** — allows other classes to read the `envelope` value. Java convention: every private field has a `getX()` and `setX()` method. |
| `public void setEnvelope(...)` | A **setter** — Spring Boot calls this at startup to fill in the value it read from the YAML file. |
| `public static class Envelope {` | A class defined inside another class. Represents the nested `envelope:` block in YAML. `static` here means it does not need an instance of the outer class to exist. |
| `private String setPath;` | Holds `set-path` from YAML. Note: YAML uses `kebab-case` (hyphens), Java uses `camelCase` — Spring converts automatically. Currently unused in SQL generation. |
| `private String userkeyPath;` | Holds `userkey-path` — the JSON path to the primary key value. |
| `private String operationPath;` | Holds `operation-path` — the JSON path to `insert`/`update`/`delete`. |
| All the `getX()` / `setX()` methods | Standard Java getters and setters. Spring needs `setX()` to fill in values at startup. Other classes need `getX()` to read them at runtime. No logic, just data access. |

---

## 8. App Code: SqlBuilder.java

This is the **core logic file** — the most important class in the project.
It takes a JSON message and produces a SQL statement. It has no Spring, no Kafka,
no MySQL — it is pure Java logic, which is why the demo tool can also use it.

### Imports

```java
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
```

| Import | What it means |
|---|---|
| `ArrayList` | A resizable list. Used to build the list of SQL parameter values. |
| `Collections` | Utility methods. Used for `singletonList` (a one-item list) and `nCopies` (repeat a value N times). |
| `LinkedHashMap` | A key-value map that **remembers insertion order**. Critical here — SQL columns must appear in the same order every time. |
| `List` | The general interface for lists. |
| `Map` | The general interface for key-value maps. Used to represent both the JSON and the config. |
| `Matcher` | Used in the `preview()` method to safely substitute values into the SQL string. |

### Class declaration

```java
public final class SqlBuilder {
    private static final String INSERT_OPERATION = "insert";
    private static final String UPDATE_OPERATION = "update";
    private static final String DELETE_OPERATION = "delete";

    private SqlBuilder() { }
```

| Line | What it means |
|---|---|
| `public final class SqlBuilder` | `final` means no other class can extend this one. It is a utility class — not meant to be subclassed. |
| `private static final String INSERT_OPERATION = "insert";` | A constant. `static` = belongs to the class, not any instance. `final` = cannot be changed. Storing strings as constants avoids typos — if you mistype the constant name the compiler catches it. |
| `private SqlBuilder() { }` | A private constructor — prevents anyone from creating an instance with `new SqlBuilder()`. All its methods are `static`, so you never need an instance. |

### The Result class

```java
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
```

| Line | What it means |
|---|---|
| `public static class Result` | A simple container class that bundles together everything the SQL build process produces. Returning one `Result` is cleaner than returning 5 separate values. |
| `public final String tableName;` | The MySQL table name (e.g. `"db_invoices"`). `final` = once set, cannot change. |
| `public final String operation;` | A human-readable label: `"DELETE"` or `"UPSERT (insert/update)"`. |
| `public final String primaryKeyColumn;` | The MySQL column name that acts as the primary key (e.g. `"invoice_id"`). |
| `public final String sql;` | The generated SQL string with `?` placeholders (safe, parameterized form). |
| `public final List<Object> parameters;` | The actual values to substitute into the `?` placeholders, in order. |
| `public Result(...)` | The constructor — the only way to create a `Result`. All five fields must be provided. |
| `this.tableName = tableName;` | `this` refers to the object being created. This copies the passed-in value into the object's field. |

### The build() method

```java
    public static Result build(Map<String, Object> json,
                                String operationPath,
                                String userKeyPath,
                                Map<String, Object> targetSchema) {
```

| Parameter | What it means |
|---|---|
| `Map<String, Object> json` | The parsed JSON message. Keys are strings (field names), values can be anything (strings, numbers, booleans, nested Maps). |
| `String operationPath` | The dot-path to find the operation (e.g. `"header.action"`). |
| `String userKeyPath` | The dot-path to find the primary key value (e.g. `"header.uuid"`). |
| `Map<String, Object> targetSchema` | The column mapping from `application.yml`. |

```java
        String tableName = (String) targetSchema.get("table-name");

        Object rawOperation = resolvePath(json, operationPath);
        if (rawOperation == null) {
            throw new IllegalArgumentException("No value found at operation-path: " + operationPath);
        }
        String operation = String.valueOf(rawOperation);
```

| Line | What it means |
|---|---|
| `(String) targetSchema.get("table-name")` | Reads the MySQL table name from config. `(String)` is a cast — we tell Java "trust me, this value is a String". |
| `resolvePath(json, operationPath)` | Looks up the operation field in the JSON (navigates to e.g. `header.action` and gets `"delete"`). |
| `if (rawOperation == null) { throw ... }` | If the field does not exist in the JSON, crash with a helpful error. We cannot proceed without knowing the operation. |
| `String.valueOf(rawOperation)` | Converts the raw value (which Java tracks as a generic `Object`) into a `String`. `String.valueOf()` safely handles any type. |

```java
        LinkedHashMap<String, Object> columnValues = new LinkedHashMap<>();
        String primaryKeyColumn = null;

        for (Map.Entry<String, Object> entry : targetSchema.entrySet()) {
            String columnName = entry.getKey();
            if (columnName.equals("table-name")) {
                continue;
            }

            Map<String, Object> columnConfig = (Map<String, Object>) entry.getValue();
            String path = (String) columnConfig.get("path");
            Object value = resolvePath(json, path);
            columnValues.put(columnName, value);

            if (path.equals(userKeyPath)) {
                primaryKeyColumn = columnName;
            }
        }
```

| Line | What it means |
|---|---|
| `LinkedHashMap<String, Object> columnValues` | Creates an ordered map to store column-name-to-value pairs. Order is preserved — important for SQL correctness. |
| `String primaryKeyColumn = null;` | We have not found the primary key column yet. Will be filled in during the loop. |
| `for (Map.Entry<String, Object> entry : targetSchema.entrySet())` | Loops over every entry in the `target-schema` config. Each `entry` is one column definition. |
| `String columnName = entry.getKey()` | Gets the MySQL column name (e.g. `"invoice_id"`, `"vendor_name"`). |
| `if (columnName.equals("table-name")) { continue; }` | Skips the `table-name` entry — it is a special config key, not a column. `continue` skips to the next loop iteration. |
| `Map<String, Object> columnConfig` | The value for each column entry is itself a Map containing `path` and `type`. |
| `String path = (String) columnConfig.get("path")` | Gets the dot-path for this column (e.g. `"child.for_order"`). |
| `Object value = resolvePath(json, path)` | Navigates into the JSON using that path. If the field is missing, returns `null`. |
| `columnValues.put(columnName, value)` | Stores the column-to-value pair. If value is `null` (field was missing), it stores `null` — which becomes `NULL` in SQL. |
| `if (path.equals(userKeyPath)) { primaryKeyColumn = columnName; }` | If this column's path matches the primary key path in config, remember its column name. Needed later for `WHERE` clauses. |

```java
        if (primaryKeyColumn == null) {
            throw new IllegalStateException("No target-schema column maps to userkey-path: " + userKeyPath);
        }
```

| Line | What it means |
|---|---|
| If no column's path matched `userkey-path` | This is a config error. Crash with a clear message telling the user to fix `application.yml`. |

### DELETE branch

```java
        if (DELETE_OPERATION.equalsIgnoreCase(operation)) {
            String sql = "DELETE FROM " + tableName + " WHERE " + primaryKeyColumn + " = ?";
            List<Object> parameters = Collections.singletonList(columnValues.get(primaryKeyColumn));
            return new Result(tableName, "DELETE", primaryKeyColumn, sql, parameters);
        }
```

| Line | What it means |
|---|---|
| `equalsIgnoreCase(operation)` | Checks if the action is `"delete"`. Case-insensitive, so `"Delete"` or `"DELETE"` also work. |
| `"DELETE FROM " + tableName + " WHERE " + primaryKeyColumn + " = ?"` | Builds the DELETE SQL. The `?` is a safe placeholder. Example result: `DELETE FROM db_invoices WHERE invoice_id = ?` |
| `Collections.singletonList(...)` | Creates a one-item list with just the primary key value. DELETE only needs this one value for the `WHERE` clause. |
| `return new Result(...)` | Returns immediately. The INSERT/UPDATE logic below is not reached for DELETE. |

### INSERT/UPDATE branch (Upsert)

```java
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

        throw new IllegalArgumentException("Unknown operation '" + operation + "'...");
    }
```

| Line | What it means |
|---|---|
| `INSERT_OPERATION... \|\| UPDATE_OPERATION...` | Handles both `"insert"` and `"update"` the same way — as an upsert. `\|\|` means "or". |
| `String.join(", ", columnValues.keySet())` | Creates comma-separated column names: `"invoice_id, invoiceamount, vendor_name, ..."` |
| `Collections.nCopies(columnValues.size(), "?")` | Creates a list of `?` strings, one per column. Joined: `"?, ?, ?, ?, ?, ?"` |
| `StringBuilder updateClause` | A mutable string builder. Efficient for building SQL in a loop. |
| `if (column.equals(primaryKeyColumn)) { continue; }` | Never put the primary key in the UPDATE part — we do not want to overwrite it. |
| `if (updateClause.length() > 0) { append(", ") }` | Adds a comma separator after the first column. |
| `column + " = VALUES(" + column + ")"` | Adds one update like: `invoiceamount = VALUES(invoiceamount)`. MySQL's `VALUES(column)` refers to the value we tried to insert. |
| `ON DUPLICATE KEY UPDATE` | MySQL feature: "try to INSERT; if a row with this primary key already exists, UPDATE the other columns instead." Handles both insert and update in one statement. |
| `new ArrayList<>(columnValues.values())` | Collects all column values in order to fill the `?` placeholders. |
| Final `throw` | If the action is anything other than `insert`, `update`, or `delete` — crash with a clear error. |

### The resolvePath() method

```java
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
```

| Line | What it means |
|---|---|
| `path.split("\\.")` | Splits `"child.for_order"` into `["child", "for_order"]`. The `\\.` is a regex for a literal dot (just `.` in regex means "any character"). |
| `Object current = data` | Start at the top of the JSON map. |
| `for (String part : parts)` | Walk through each key in the path one step at a time. |
| `if (!(current instanceof Map)) { return null; }` | If we have reached something that is not a Map (e.g. a string or number), we cannot go deeper. Return `null` — field not found. |
| `current = ((Map) current).get(part)` | Move one level deeper into the map using the current key. |
| `if (current == null) { return null; }` | If the key does not exist at this level, return `null` — field is missing from this message. |
| `return current` | We walked the full path and found the value. Return it. |

### The preview() method

```java
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
```

| Line | What it means |
|---|---|
| `preview(String sql, List<Object> parameters)` | Creates a **human-readable** version of the SQL with real values instead of `?`. Only for display — never executed directly. |
| `for (Object param : parameters)` | Loop over each value in order, substituting one `?` at a time. |
| `if (param == null) { literal = "NULL"; }` | Missing fields become SQL `NULL`. |
| `param instanceof Number \|\| param instanceof Boolean` | Numbers and booleans go in without quotes: `12000.5`, `true`. |
| `literal = "'" + ... + "'"` | Strings get wrapped in single quotes: `'Google LLC'`. |
| `.replace("'", "''")` | If the string contains a single quote (e.g. `O'Brien`), escape it as `''` per SQL standard — prevents SQL injection in the display string. |
| `result.replaceFirst("\\?", Matcher.quoteReplacement(literal))` | Replaces the next `?` in the SQL with the literal. `Matcher.quoteReplacement` prevents special regex characters in the value from causing issues. |

---

## 9. App Code: ReplicationConsumer.java

This is the **glue** between Kafka (input) and MySQL (output). It uses `SqlBuilder`
for all the logic and just handles the "receive message -> run SQL" pipeline.

```java
package com.replication.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
```

| Import | What it means |
|---|---|
| `ObjectMapper` | Jackson's JSON parser. Converts a raw JSON string into a Java Map. |
| `JdbcTemplate` | Spring's database helper. Runs SQL statements against MySQL safely. |
| `KafkaListener` | The annotation that makes a method automatically called when a Kafka message arrives. |
| `Component` | Marks this class so Spring manages it (creates it, injects its dependencies). |

```java
@Component
public class ReplicationConsumer {

    private final ReplicationProperties properties;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ReplicationConsumer(ReplicationProperties properties, JdbcTemplate jdbcTemplate) {
        this.properties = properties;
        this.jdbcTemplate = jdbcTemplate;
    }
```

| Line | What it means |
|---|---|
| `@Component` | Tells Spring to create one instance of this class and manage it. |
| `private final ReplicationProperties properties;` | Holds the config values (paths, column mappings). `final` = cannot be reassigned after construction. |
| `private final JdbcTemplate jdbcTemplate;` | The MySQL executor. Spring creates and configures this from the datasource settings in `application.yml`. |
| `private final ObjectMapper objectMapper = new ObjectMapper();` | The JSON parser, created directly because it is a simple stateless tool. |
| `public ReplicationConsumer(ReplicationProperties properties, JdbcTemplate jdbcTemplate)` | The constructor. Spring calls this to create the consumer, **automatically passing in** the `properties` and `jdbcTemplate` objects it already manages. This is called **Dependency Injection**. |
| `this.properties = properties;` | Stores the injected config object for later use. |
| `this.jdbcTemplate = jdbcTemplate;` | Stores the injected database executor for later use. |

```java
    @KafkaListener(
            topics = "${app.kafka.topic}",
            groupId = "${app.kafka.group-id}",
            concurrency = "${app.kafka.concurrency}"
    )
    public void onMessage(String message) {
```

| Line | What it means |
|---|---|
| `@KafkaListener(...)` | Marks this method as a Kafka message handler. Spring Kafka automatically calls `onMessage()` every time a new message arrives on the configured topic. |
| `topics = "${app.kafka.topic}"` | `${...}` is a placeholder — Spring fills in the value from `application.yml`. In this case `nosql-replication`. |
| `groupId = "${app.kafka.group-id}"` | Which consumer group this app belongs to. Kafka tracks progress per group. |
| `concurrency = "${app.kafka.concurrency}"` | How many threads to run in parallel. Value is `3` from config. |
| `public void onMessage(String message)` | This method receives each Kafka message as a raw string. `void` means it does not return anything. |

```java
        try {
            Map<String, Object> json = objectMapper.readValue(message, Map.class);

            SqlBuilder.Result result = SqlBuilder.build(
                    json,
                    properties.getEnvelope().getOperationPath(),
                    properties.getEnvelope().getUserkeyPath(),
                    properties.getTargetSchema()
            );

            jdbcTemplate.update(result.sql, result.parameters.toArray());
        } catch (Exception e) {
            System.err.println("Failed to process message: " + message);
            e.printStackTrace();
        }
```

| Line | What it means |
|---|---|
| `try { ... } catch (Exception e) { ... }` | A safety net. If anything goes wrong (bad JSON, database error), the `catch` block runs instead of crashing the whole app. |
| `objectMapper.readValue(message, Map.class)` | Parses the JSON string into a Java Map. |
| `SqlBuilder.build(json, ...)` | Calls the core logic with the parsed message and the config values. Returns a `Result` with the SQL and parameters. |
| `properties.getEnvelope().getOperationPath()` | Reads `operation-path` from config (`"header.action"`). |
| `properties.getEnvelope().getUserkeyPath()` | Reads `userkey-path` from config (`"header.uuid"`). |
| `properties.getTargetSchema()` | Reads the entire column mapping from config. |
| `jdbcTemplate.update(result.sql, result.parameters.toArray())` | Executes the SQL against MySQL. The parameters fill in the `?` placeholders safely, preventing SQL injection. |
| `System.err.println(...)` | Prints the error and the original message to the error log. The app keeps running — one bad message does not stop everything. |
| `e.printStackTrace()` | Prints the full technical error trace for debugging. |

---

## 10. Demo Code: DemoRunner.java

This is a **standalone tool** — it has no Kafka, no MySQL, no Spring Boot.
You run it to preview what SQL would be generated from a JSON message.

```java
package com.replication.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.replication.app.SqlBuilder;
import org.yaml.snakeyaml.Yaml;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Map;
```

| Import | What it means |
|---|---|
| `package com.replication.demo` | This is in the `demo` group, separate from the `app` group. |
| `import SqlBuilder` | Imports the core logic from the `app` group — the one shared piece. |
| `import Yaml` | SnakeYAML library to read `application.yml` manually (no Spring to do it automatically here). |
| `ByteArrayOutputStream, File, InputStream, Files` | Standard Java file/stream tools for reading files from disk or the classpath. |

```java
public class DemoRunner {

    public static void main(String[] args) throws Exception {
        Map<String, Object> yaml = loadYaml();

        Map<String, Object> app = (Map<String, Object>) yaml.get("app");
        Map<String, Object> replication = (Map<String, Object>) app.get("replication");
        Map<String, Object> envelope = (Map<String, Object>) replication.get("envelope");
        Map<String, Object> targetSchema = (Map<String, Object>) replication.get("target-schema");

        String operationPath = (String) envelope.get("operation-path");
        String userKeyPath = (String) envelope.get("userkey-path");
```

| Line | What it means |
|---|---|
| `public static void main(String[] args) throws Exception` | Java entry point for the demo. `throws Exception` means errors are allowed to bubble up and crash — acceptable for a demo tool. |
| `loadYaml()` | Calls the helper method below to read `application.yml` into a Map. |
| `yaml.get("app")` | Navigates into the YAML Map to get the `app:` section. |
| `app.get("replication")` | Goes one level deeper to get the `replication:` section. |
| `replication.get("envelope")` | Gets the `envelope:` subsection. |
| `replication.get("target-schema")` | Gets the column mapping. |
| `envelope.get("operation-path")` | Reads `"header.action"` from config. |
| `envelope.get("userkey-path")` | Reads `"header.uuid"` from config. |

```java
        String jsonText = loadJsonMessage(args);

        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> json = objectMapper.readValue(jsonText, Map.class);

        System.out.println("=================================================");
        System.out.println("Input message:");
        System.out.println(jsonText.trim());
        System.out.println("=================================================");
```

| Line | What it means |
|---|---|
| `loadJsonMessage(args)` | Reads the JSON message from a file argument if provided, otherwise uses the bundled `sample-message.json`. |
| `objectMapper.readValue(jsonText, Map.class)` | Parses the JSON string into a Java Map for processing. |
| `System.out.println(...)` | Prints text to the terminal. This is the demo's only output — there is no GUI. |
| `jsonText.trim()` | Removes any leading/trailing whitespace from the JSON text before printing. |

```java
        try {
            SqlBuilder.Result result = SqlBuilder.build(json, operationPath, userKeyPath, targetSchema);

            System.out.println("Table:            " + result.tableName);
            System.out.println("Operation:        " + result.operation);
            System.out.println("Primary key col:  " + result.primaryKeyColumn);

            System.out.println("Generated SQL (readable, values filled in):");
            System.out.println(SqlBuilder.preview(result.sql, result.parameters));

            System.out.println("Parameterized form (what actually runs against MySQL):");
            System.out.println(result.sql);
            System.out.println("Parameter values, in order: " + result.parameters);
        } catch (Exception e) {
            System.out.println("Could not build SQL from this message: " + e.getMessage());
        }
        System.out.println("=================================================");
```

| Line | What it means |
|---|---|
| `SqlBuilder.build(json, operationPath, userKeyPath, targetSchema)` | Same call as in the real app — calls the shared core logic. |
| `result.tableName`, `result.operation`, `result.primaryKeyColumn` | Prints metadata: which table, what operation, which column is the primary key. |
| `SqlBuilder.preview(result.sql, result.parameters)` | Prints the **readable** SQL with values filled in — the one you show in a demo. |
| `result.sql` | Prints the **parameterized** SQL with `?` placeholders — what runs in production (safe from SQL injection). |
| `result.parameters` | Prints the list of values that fill those `?` placeholders. |
| `catch (Exception e)` | If the message is malformed or config is wrong, print a friendly error instead of a Java stack trace. |

```java
    private static Map<String, Object> loadYaml() throws Exception {
        try (InputStream in = DemoRunner.class.getClassLoader().getResourceAsStream("application.yml")) {
            if (in == null) {
                throw new IllegalStateException("Could not find application.yml on the classpath.");
            }
            return new Yaml().load(in);
        }
    }
```

| Line | What it means |
|---|---|
| `private static Map<String, Object> loadYaml()` | A helper method only used within this class. |
| `try (InputStream in = ...)` | A "try-with-resources" block. Opens the file and guarantees it is closed when done, even if an error occurs. |
| `DemoRunner.class.getClassLoader().getResourceAsStream("application.yml")` | Finds `application.yml` inside the compiled output (the classpath). This is how Java reads bundled resource files — not by filesystem path. |
| `if (in == null) { throw ... }` | If the file was not found (e.g. project not compiled yet), crash with a clear message. |
| `new Yaml().load(in)` | Parses the YAML file into a nested Java Map. |

```java
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
```

| Line | What it means |
|---|---|
| `if (args.length > 0)` | If a file path was passed on the command line (e.g. `-Dexec.args="my-message.json"`), use that file. |
| `Files.readAllBytes(new File(args[0]).toPath())` | Reads the entire file at the given path into a byte array, then converts to a String. |
| `getResourceAsStream("sample-message.json")` | If no file was provided, fall back to the bundled `sample-message.json`. |
| `ByteArrayOutputStream buffer` | A buffer to accumulate bytes as we read them. |
| `byte[] chunk = new byte[4096]` | A 4KB temporary holding area. We read the file in chunks of 4096 bytes at a time. |
| `while ((n = in.read(chunk)) != -1)` | Keep reading chunks until end of file. `-1` means end-of-file. `n` = how many bytes were read in this chunk. |
| `buffer.write(chunk, 0, n)` | Write each chunk into the buffer. We use `n` not `4096` because the last chunk may be shorter. |
| `return buffer.toString()` | Converts all accumulated bytes into a String and returns it. |

---

## 11. How to Run the Demo

### One-time setup in your terminal

```powershell
cd "C:\Users\mohit\OneDrive\Documents\Mudit FIles\P3 auto\replication-engine"

$env:JAVA_HOME = "C:\gaming\code"
$env:PATH = "C:\Users\mohit\maven\apache-maven-3.9.6\bin;C:\gaming\code\bin;" + $env:PATH
```

### Run the demo

```powershell
mvn compile exec:java
```

### What happens step by step

1. Maven compiles all `.java` files into bytecode (`.class` files) in the `target/` folder
2. Maven runs `DemoRunner.main()`
3. `DemoRunner` reads `application.yml` for the column mapping config (using SnakeYAML)
4. `DemoRunner` reads `sample-message.json` as the input message
5. `SqlBuilder.build()` processes the message and generates the SQL
6. The result is printed to the terminal between the `===` separator lines

### To test different scenarios

Edit `sample-message.json` and change `"action"` to one of:
- `"insert"` — generates an `INSERT ... ON DUPLICATE KEY UPDATE ...`
- `"update"` — generates the same upsert as insert
- `"delete"` — generates `DELETE FROM ... WHERE ...`

Then run `mvn compile exec:java` again.

### To add a new column mapping (no Java needed)

Edit `application.yml` under `target-schema:`:
```yaml
your_mysql_column_name:
  path: "the.json.path.to.the.value"
  type: "VARCHAR(255)"
```
Run `mvn compile exec:java` — the new column appears in the generated SQL automatically.
