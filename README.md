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

## Running the demo (no Kafka or MySQL needed)

The demo reads `sample-message.json` and prints the SQL that would be generated.

```bash
mvn compile exec:java
```

To test with your own message:

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
