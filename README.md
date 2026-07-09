# database-replication-application

A Spring Boot service that listens to a Kafka topic and mirrors change events into any JDBC-compatible relational database (MySQL, PostgreSQL, SQL Server, Oracle, etc.).

## What it does

Whenever a record changes in a NoSQL source, a JSON message is published to Kafka. This app picks up that message, maps the fields to the target table schema using configuration in `application.yml`, and runs the appropriate SQL — an upsert for inserts/updates, or a delete.

## Project structure

```
src/main/java/com/replication/app/
  ReplicationEngineApplication.java   - entry point
  ReplicationProperties.java          - binds app.replication.* config
  ReplicationConsumer.java            - Kafka listener, calls SqlBuilder, runs the SQL
  SqlBuilder.java                     - builds the SQL from a JSON message + schema config

src/main/java/com/replication/demo/
  DemoRunner.java                     - standalone tool to test SQL generation locally

src/main/resources/
  application.yml                     - Kafka, datasource and field-mapping config
  sample-message.json                 - example input for the demo
  log4j2.xml                          - logging config (console + rolling file)
```

## Source files

**`ReplicationEngineApplication.java`**  
The entry point. The single `@SpringBootApplication` annotation triggers Spring Boot's auto-configuration — it reads `application.yml`, sets up the Kafka consumer threads, JDBC connection pool, and wires all the components together automatically.

**`ReplicationProperties.java`**  
Binds the `app.replication.*` section of `application.yml` into a typed Java object. Holds the field mapping config (which JSON path maps to which database column) and envelope config (where to find the operation type and primary key in the message).

**`SqlBuilder.java`**  
Pure Java — no Spring, no database. Takes a parsed JSON message and the field-mapping config, resolves each field path, and produces ready-to-run SQL statements. Handles insert/update as a two-step upsert (standard `UPDATE` then `INSERT` if nothing was updated) and delete as a `DELETE WHERE`. Uses only standard SQL so it works with any RDBMS. Also has a `preview()` method that fills in the `?` placeholders for display purposes.

**`ReplicationConsumer.java`**  
The Kafka listener. Spring Kafka calls `onMessage()` for every message on the configured topic. It parses the JSON string, passes it to `SqlBuilder`, then runs the result against the database using `JdbcTemplate`. For upserts it runs `UPDATE` first and only runs `INSERT` if no rows were affected. If a message fails for any reason it is logged and skipped so it doesn't block the rest of the queue.

**`DemoRunner.java`**  
A standalone tool that runs without Kafka or a database. It reads `application.yml` for the mapping config and `sample-message.json` for the input, then prints the SQL that would be generated. Useful for sanity-checking your config before wiring up any real infrastructure.

## Sample input / output

**Input** (`sample-message.json`):
```json
{
  "header": {
    "uuid": "INV-8869",
    "action": "insert"
  },
  "invoice_amount": 12000.50,
  "vendor": "f LLC"
}
```

**Output** (console):
```
Table:            db_invoices
Operation:        UPSERT (insert/update)
Primary key col:  invoice_id

Step 1 - UPDATE (runs first):
UPDATE db_invoices SET invoiceamount = 12000.5, vendor_name = 'f LLC' WHERE invoice_id = 'INV-8869'

Step 2 - INSERT (runs only if Step 1 updated 0 rows):
INSERT INTO db_invoices (invoice_id, invoiceamount, vendor_name) VALUES ('INV-8869', 12000.5, 'f LLC')
```

The app reads `header.uuid` as the primary key and `header.action` to determine the operation. Fields not present in the mapping config are ignored entirely. Nested objects and arrays are automatically serialized to JSON strings before being stored.

## Running the demo (no Kafka or database needed)

**Double-click** `run-demo.bat` in the project root, or from a terminal:

```cmd
.\run-demo.bat
```

To test with your own message file:

```cmd
.\run-demo.bat path\to\your-message.json
```

## Running the full app

**1. Add your JDBC driver to `pom.xml`**

The app works with any JDBC-compatible database. Add the driver for your target database, for example:

```xml
<!-- PostgreSQL -->
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <scope>runtime</scope>
</dependency>

<!-- SQL Server -->
<dependency>
    <groupId>com.microsoft.sqlserver</groupId>
    <artifactId>mssql-jdbc</artifactId>
    <scope>runtime</scope>
</dependency>

<!-- MySQL -->
<dependency>
    <groupId>com.mysql</groupId>
    <artifactId>mysql-connector-j</artifactId>
    <scope>runtime</scope>
</dependency>
```

**2. Update `application.yml`** with your Kafka bootstrap server, datasource URL, and topic name:

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/mydb   # any JDBC URL
    username: myuser
    password: mypassword
    driver-class-name: org.postgresql.Driver
```

**3. Run:**

```bash
mvn clean package
java -jar target/replication-engine-1.0.0.jar
```

The target table must already exist with a primary key on whichever column is configured as `userkey-path`.

## Configuration

All field mappings live in `application.yml` under `app.replication.target-schema`. Each entry maps a database column name to a dot-separated path in the incoming JSON.

```yaml
app:
  replication:
    envelope:
      operation-path: header.action      # insert / update / delete
      userkey-path:   header.uuid        # used as the primary key
    target-schema:
      table-name: db_invoices
      invoice_id:
        path: header.uuid
      invoiceamount:
        path: invoice_amount
```

## Logging

Logs are written to both the console and a rolling daily file under `logs/replication-engine.log`. Log configuration is in `src/main/resources/log4j2.xml`.
