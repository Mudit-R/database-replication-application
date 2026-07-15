# database-replication-application

A Spring Boot service that listens to multiple Kafka topics simultaneously and mirrors change events into any JDBC-compatible relational database (MySQL, PostgreSQL, SQL Server, Oracle, etc.).

## What it does

Whenever a record changes in a NoSQL source, a JSON message is published to a Kafka topic. This app picks up that message, validates its format, routes it to the correct target table(s) based on config, and runs the appropriate SQL — an upsert for inserts/updates, or a delete.

A single incoming message can:
- Write to **multiple RDBMS tables at once** (fan-out)
- **Iterate an array** inside the message and produce one row per element (e.g. order items)
- Be **routed or rejected** based on which Kafka topic it came from and whether it matches the expected field structure

## Project structure

```
src/main/java/com/replication/app/
  ReplicationEngineApplication.java   - entry point
  ReplicationProperties.java          - binds app.replication.* config
  ReplicationConsumer.java            - Kafka listener, routes/filters, calls SqlBuilder, runs SQL
  SqlBuilder.java                     - builds SQL from a JSON message + schema config

src/main/java/com/replication/demo/
  DemoRunner.java                     - standalone tool to test SQL generation locally

src/main/resources/
  application.yml                     - Kafka topics, datasource, and field-mapping config
  sample-message.json                 - example input for the demo
  log4j2.xml                          - logging config (console + rolling file)
  samples/                            - additional sample messages for testing
```

## Source files

**`ReplicationEngineApplication.java`**  
The entry point. The single `@SpringBootApplication` annotation triggers Spring Boot's auto-configuration — it reads `application.yml`, sets up the Kafka consumer threads, JDBC connection pool, and wires all the components together automatically.

**`ReplicationProperties.java`**  
Binds the `app.replication.*` section of `application.yml` into a typed Java object. Holds the field mapping config and envelope config.

**`SqlBuilder.java`**  
Pure Java — no Spring, no database. Takes a parsed JSON message and the field-mapping config, resolves each field path, and produces ready-to-run SQL. Supports array iteration via `source-array` to produce multiple rows from a single message. Also has a `preview()` method that fills in the `?` placeholders for display purposes.

**`ReplicationConsumer.java`**  
The Kafka listener. Subscribes to all configured topics simultaneously. For each incoming message it reads the source topic name, validates the message format against `required-paths`, matches it against `filter-topic` and `filter-path`/`filter-value`, and only then executes SQL. Failed or non-matching messages are safely skipped without blocking the queue.

**`DemoRunner.java`**  
A standalone tool that runs without Kafka or a database. Reads `application.yml` and a JSON file, then prints the SQL that would be generated. Useful for sanity-checking config before wiring up real infrastructure.

---

## Feature 1: Complex Queries — Multi-Table Update with Array Iteration

A single incoming message can fan out and write to multiple RDBMS tables simultaneously. When a schema has `source-array` set, the engine iterates that array and writes **one row per element**.

### Example — Order message with 3 items

**Input** (sent to Kafka topic `nosql-orders`):
```json
{
  "header": { "uuid": "ORD-001", "action": "insert", "type": "order" },
  "customer_name": "John Doe",
  "total_amount": 1024.00,
  "total_items": 3,
  "status": "confirmed",
  "items": [
    { "sku": "LAPTOP-01",  "qty": 1, "price": 999.00 },
    { "sku": "MOUSE-01",   "qty": 2, "price": 12.50  },
    { "sku": "USB-HUB-01", "qty": 1, "price": 24.99  }
  ]
}
```

**Output — SQL executed across 3 tables:**
```sql
-- Table 1: orders (1 row — top-level message fields)
INSERT INTO orders (order_id, customer_name, total_amount, status)
  VALUES ('ORD-001', 'John Doe', 1024.0, 'confirmed')

-- Table 2: order_items (3 rows — one per element in items[])
INSERT INTO order_items (item_id, order_id, sku, qty, price)
  VALUES ('ORD-001-0', 'ORD-001', 'LAPTOP-01', 1, 999.0)

INSERT INTO order_items (item_id, order_id, sku, qty, price)
  VALUES ('ORD-001-1', 'ORD-001', 'MOUSE-01', 2, 12.5)

INSERT INTO order_items (item_id, order_id, sku, qty, price)
  VALUES ('ORD-001-2', 'ORD-001', 'USB-HUB-01', 1, 24.99)
target-schemas:

  # Schema 1: orders -- one row per message, no array iteration
  - table-name: "orders"
    userkey-path: "header.uuid"
    filter-topic: "nosql-orders"
    filter-path: "header.type"
    filter-value: "order"
    required-paths:
      - "header.uuid"
      - "customer_name"
      - "total_amount"
    order_id:
      path: "header.uuid"
    customer_name:
      path: "customer_name"
    total_amount:
      path: "total_amount"
    status:
      path: "status"

  # Schema 2: order_items -- source-array iterates items[], one row per element
  - table-name: "order_items"
    userkey-path: "header.uuid"
    filter-topic: "nosql-orders"
    filter-path: "header.type"
    filter-value: "order"
    source-array: "items"           # iterate this array -- one row per element
    required-paths:
      - "header.uuid"
      - "items"
    item_id:
      path: "header.uuid"
      pk-source: "generated"        # auto-generates ORD-001-0, ORD-001-1, ORD-001-2
    order_id:
      path: "header.uuid"           # not on the element -- falls back to top-level message
    sku:
      path: "sku"                   # resolved from each array element
    qty:
      path: "qty"
    price:
      path: "price"
```

**Target table schemas (what the RDBMS tables look like):**

```sql
-- Table 1: orders
CREATE TABLE orders (
    order_id      VARCHAR(255) NOT NULL,
    customer_name VARCHAR(255),
    total_amount  DECIMAL(20,6),
    status        VARCHAR(50),
    PRIMARY KEY (order_id)
);

-- Table 2: order_items -- one row per element in items[]
CREATE TABLE order_items (
    item_id   VARCHAR(255) NOT NULL,
    order_id  VARCHAR(255),
    sku       VARCHAR(255),
    qty       INT,
    price     DECIMAL(20,6),
    PRIMARY KEY (item_id)
);
```

**Result: 1 message → 4 SQL statements → 2 tables → 4 rows written**

The invoice schemas (`db_invoices`, `db_audit_log`) are skipped entirely because their `filter-topic` is `nosql-replication`, not `nosql-orders`.

---

## Feature 1: Topic Routing and Format Filtering

The engine validates every incoming message against three layers before writing anything:

1. **Topic filter** — which Kafka topic did this message come from?
2. **Value filter** — does a specific field in the message match an expected value?
3. **Format recognition** — does the message contain all the required structural fields?

### Example A — Correct invoice message on the correct topic

**Input** (sent to Kafka topic `nosql-replication`):
```json
{
  "header": { "uuid": "INV-001", "action": "insert", "type": "invoice" },
  "invoice_amount": 5000.00,
  "vendor": "Acme Corp",
  "paid": true
}
```

**Routing result:**
```
Schema: db_invoices   — topic matches ✓, type=invoice ✓, required fields present ✓  → WRITTEN
Schema: db_audit_log  — topic matches ✓, type=invoice ✓, required fields present ✓  → WRITTEN
Schema: orders        — topic is nosql-orders, message came from nosql-replication   → SKIPPED
Schema: order_items   — topic is nosql-orders, message came from nosql-replication   → SKIPPED
```

### Example B — Invoice message with a missing required field

**Input** (sent to Kafka topic `nosql-replication`):
```json
{
  "header": { "uuid": "INV-002", "action": "insert", "type": "invoice" },
  "vendor": "Broken Corp"
}
```
`invoice_amount` is missing.

**Routing result:**
```
Schema: db_invoices   — topic ✓, type=invoice ✓, but "invoice_amount" is MISSING    → SKIPPED
Schema: db_audit_log  — topic ✓, type=invoice ✓, but "invoice_amount" is MISSING    → SKIPPED

Result: Nothing written to any table. Message safely rejected due to bad format.
```

### Example C — Wrong message format on wrong topic

**Input** (an order-format message accidentally sent to `nosql-replication`):
```json
{
  "header": { "uuid": "ORD-999", "action": "insert", "type": "order" },
  "customer_name": "Wrong Channel",
  "total_amount": 500.00
}
```

**Routing result:**
```
Schema: db_invoices   — topic ✓ but header.type is "order", expected "invoice"       → SKIPPED
Schema: db_audit_log  — topic ✓ but header.type is "order", expected "invoice"       → SKIPPED
Schema: orders        — topic is nosql-orders, came from nosql-replication            → SKIPPED
Schema: order_items   — topic is nosql-orders, came from nosql-replication            → SKIPPED

Result: Nothing written. Wrong format on wrong topic is completely ignored.
```

---

## Running the demo (no Kafka or database needed)

**Double-click** `run-demo.bat` in the project root, or from a terminal:

```cmd
.\run-demo.bat
```

To test with a specific sample message:

```cmd
.\run-demo.bat src\main\resources\samples\21-order-with-items.json
```

---

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

**2. Update `application.yml`** with your Kafka bootstrap server, datasource URL, and topic names:

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/mydb
    username: myuser
    password: mypassword
    driver-class-name: org.postgresql.Driver

app:
  kafka:
    topics: "nosql-replication,nosql-orders"
```

**3. Run:**

```bash
mvn clean package
java -jar target/replication-engine-1.0.0.jar
```

---

## Configuration

All field mappings live in `application.yml` under `app.replication.target-schemas`. Each schema entry can have:

| Property | Required | Description |
| :--- | :--- | :--- |
| `table-name` | Yes | Target RDBMS table |
| `userkey-path` | Yes | JSON path to the primary key value |
| `filter-topic` | No | Only process this schema if the message came from this topic |
| `filter-path` | No | JSON path to check for a specific value |
| `filter-value` | No | Expected value at filter-path |
| `required-paths` | No | List of JSON paths that must be present — format validation |
| `source-array` | No | JSON path to an array — produces one row per element |
| `pk-source: generated` | No | On a column in an array schema — auto-generates `<uuid>-<index>` |

```yaml
app:
  kafka:
    topics: "nosql-replication,nosql-orders"
  replication:
    envelope:
      operation-path: "header.action"
    target-schemas:
      - table-name: "db_invoices"
        userkey-path: "header.uuid"
        filter-topic: "nosql-replication"
        filter-path: "header.type"
        filter-value: "invoice"
        required-paths:
          - "header.uuid"
          - "invoice_amount"
          - "vendor"
        invoice_id:
          path: "header.uuid"
        invoiceamount:
          path: "invoice_amount"
```

## Logging

Logs are written to both the console and a rolling daily file under `logs/replication-engine.log`. Log configuration is in `src/main/resources/log4j2.xml`.
