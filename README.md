# database-replication-application

A Spring Boot service that listens to a Kafka topic and mirrors change events into a MySQL table.

## What it does

Whenever a record changes in a NoSQL source, a JSON message is published to Kafka. This app picks up that message, maps the fields to the target MySQL schema using configuration in `application.yml`, and runs the appropriate SQL — an upsert for inserts/updates, or a delete.

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
```

## Source files

**`ReplicationEngineApplication.java`**  
The entry point. The single `@SpringBootApplication` annotation triggers Spring Boot's auto-configuration — it reads `application.yml`, sets up the Kafka consumer threads, JDBC connection pool, and wires all the components together automatically.

**`ReplicationProperties.java`**  
Binds the `app.replication.*` section of `application.yml` into a typed Java object. Holds the field mapping config (which JSON path maps to which MySQL column) and envelope config (where to find the operation type and primary key in the message).

**`SqlBuilder.java`**  
Pure Java — no Spring, no database. Takes a parsed JSON message and the field-mapping config, resolves each field path, and produces a ready-to-run SQL statement. Handles insert/update as a MySQL upsert (`INSERT ... ON DUPLICATE KEY UPDATE`) and delete as a `DELETE WHERE`. Also has a `preview()` method that fills in the `?` placeholders for display purposes.

**`ReplicationConsumer.java`**  
The Kafka listener. Spring Kafka calls `onMessage()` for every message on the configured topic. It parses the JSON string, passes it to `SqlBuilder`, then runs the result against MySQL using `JdbcTemplate`. If a message fails for any reason it is logged and skipped so it doesn't block the rest of the queue.

**`DemoRunner.java`**  
A standalone tool that runs without Kafka or MySQL. It reads `application.yml` for the mapping config and `sample-message.json` for the input, then prints the SQL that would be generated. Useful for sanity-checking your config before wiring up any real infrastructure.

## Sample input / output

**Input** (`sample-message.json`):
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

**Output** (console):
```
Table:            db_invoices
Operation:        DELETE
Primary key col:  invoice_id

Generated SQL (readable, values filled in):
DELETE FROM db_invoices WHERE invoice_id = 'INV-8869'

Parameterized form (what actually runs against MySQL):
DELETE FROM db_invoices WHERE invoice_id = ?
Parameter values, in order: [INV-8869]
```

The app reads `header.uuid` as the primary key and `header.action` to determine the operation. Fields not present in the mapping config (like `random_garbage_field`) are ignored entirely.

## Running the demo (no Kafka or MySQL needed)

**Double-click** `run-demo.bat` in the project root, or from a terminal:

```cmd
.\run-demo.bat
```

To test with your own message file:

```bash
mvn compile exec:java -Dexec.args="path/to/your-message.json"
```

## Running the full app

Update `application.yml` with your Kafka bootstrap server, MySQL datasource, and topic name, then:

```bash
mvn clean package
java -jar target/replication-engine-1.0.0.jar
```

The target MySQL table must already exist with a primary key on whichever column is configured as `userkey-path`.

## Configuration

All field mappings live in `application.yml` under `app.replication.target-schema`. Each entry maps a MySQL column name to a dot-separated path in the incoming JSON.

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
