# Replication Engine — Demo Guide & Verification Tables

This directory contains two  demonstration tools for testing and verifying the Replication Engine:

1. **Dry-Run Demo** (`DemoRunner`): Generates and previews SQL statements in memory without requiring a database or Kafka.
2. **Live Spring Boot Demo** (`LiveDemoRunner`): Runs inside the actual Spring Boot context, executes real JDBC SQL statements against a example MySQL Database (which I created with docker), and displays formatted database tables.

---

## Sample Messages Location

All test JSON payloads used by the demonstration tools are stored in the repository at:
- [src/main/resources/samples/](src/main/resources/samples/) - Directory containing all 21 test JSON payloads (invoice scenarios, order scenarios with array nested items, deletes, updates, unicode characters, and edge cases).
- [src/main/resources/sample-message.json](src/main/resources/sample-message.json) - Default single invoice message payload for simple dry-run testing.

---

## 1. Dry-Run Demo (`DemoRunner`)

The Dry-Run Demo tests the engine's SQL generation logic against `application.yml` mappings without touching any database or Kafka broker.

### How to Run

- **Windows Batch File:** Double-click `run-demo.bat` or run:
  ```cmd
  .\run-demo.bat
  ```
- **Custom JSON Message:**
  ```cmd
  .\run-demo.bat src\main\resources\samples\21-order-with-items.json
  ```
- **Maven Command:**
  ```bash
  mvn compile exec:java
  ```

---

## 2. Live Spring Boot Demo (`LiveDemoRunner`)

The Live Demo executes inside the real Spring Boot application environment. On startup, it:
1. Truncates all 4 RDBMS target tables (`db_invoices`, `db_audit_log`, `orders`, `order_items`).
2. Loads all 21 sample JSON payloads from `src/main/resources/samples/`.
3. Dynamically infers topic routing for each payload.
4. Passes messages directly through `ReplicationConsumer.onMessage()` — executing live SQL upserts/deletes against the Target database.
5. Queries MySQL using `JdbcTemplate` and displays ASCII tables of all rows written.

### Prerequisites

Ensure Docker Desktop is running and start the MySQL container: (example target database that I used, but the code can run for any rdbms)
```bash
docker-compose up -d
```

### How to Run

- **Windows Batch File:** Double-click `run-live-demo.bat` or run:
  ```cmd
  .\run-live-demo.bat
  ```
- **Maven Command:**
  ```bash
  mvn spring-boot:run -Dspring-boot.run.profiles=live-demo
  ```

---

## Live Demo Output Tables (MySQL)

Below are the complete contents of all 4 database tables after executing the Live Demo across all 21 sample messages.

### Table 1: `db_invoices` (16 rows)

| invoice_id | invoiceamount | vendor_name | paid_paise | order_for | column1 | addr_city | addr_state | addr_district |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **INV-002** | 1800.00 | Acme Corp | `true` | ORD-100 | ORD-100-A | *NULL* | *NULL* | *NULL* |
| **INV-003** | 9999.99 | Nested Corp | `true` | ORD-200 | ORD-200-A | London | *NULL* | *NULL* |
| **INV-004** | 3200.00 | Array Vendor | `false` | ORD-300 | ORD-300-A | *NULL* | *NULL* | *NULL* |
| **INV-005** | 7500.00 | Complex Co | `true` | ORD-400 | ORD-400-A | Mumbai | *NULL* | *NULL* |
| **INV-006** | 100.00 | O'Reilly & Sons / "Tech" LLC | `false` | ORD-IT'S-COMPLEX/001 | sub-order#1 | *NULL* | *NULL* | *NULL* |
| **INV-008** | 99999999.99 | Big Numbers Ltd | `true` | ORD-LARGE | SUB-LARGE | *NULL* | *NULL* | *NULL* |
| **INV-009** | 0.00 | Zero Invoice | `false` | ORD-ZERO | SUB-ZERO | *NULL* | *NULL* | *NULL* |
| **INV-011** | 4500.75 | Update Vendor | `true` | ORD-UPD | SUB-UPD | *NULL* | *NULL* | *NULL* |
| **INV-012** | 2200.00 | Uppercase Action | `false` | ORD-UPPER | SUB-UPPER | *NULL* | *NULL* | *NULL* |
| **INV-013** | 8800.00 | Unicode Vendor | `true` | ORD-?-500 | SUB-UNICODE | *NULL* | *NULL* | *NULL* |
| **INV-014** | 5500.00 | Full Complex Vendor | `true` | ORD-FULL | SUB-FULL | Noida | UP | *NULL* |
| **INV-ADDR-001** | 3500.00 | Alpha Retailers | `true` | ORD-N1 | SUB-N1 | Mumbai | Maharashtra | Mumbai City |
| **INV-ADDR-002** | 7200.50 | Beta Distributors | `false` | ORD-N2 | SUB-N2 | Bengaluru | Karnataka | Bengaluru Urban |
| **INV-COMPLEX-STR** | 5000.00 | Test Vendor | `true` | `{"abc":"val_abc"}` | `{"nested":"stringified"}` | *NULL* | *NULL* | *NULL* |
| **INV-ENT-001** | 95000.00 | Tata Consultancy Services | `true` | ORD-E-001 | SUB-E-001 | Mumbai | Maharashtra | Mumbai City |
| **INV-SMB-001** | 1200.00 | Local Traders Co | `false` | ORD-S-001 | SUB-S-001 | Delhi | Delhi NCR | Central Delhi |

---

### Table 2: `db_audit_log` (17 rows)

| log_id | action | amount | created_by |
| :--- | :--- | :--- | :--- |
| **INV-002** | update | 1800.00 | Acme Corp |
| **INV-003** | insert | 9999.99 | Nested Corp |
| **INV-004** | insert | 3200.00 | Array Vendor |
| **INV-005** | insert | 7500.00 | Complex Co |
| **INV-006** | insert | 100.00 | O'Reilly & Sons / "Tech" LLC |
| **INV-007** | insert | 0.01 | *NULL* |
| **INV-008** | insert | 99999999.99 | Big Numbers Ltd |
| **INV-009** | insert | 0.00 | Zero Invoice |
| **INV-011** | update | 4500.75 | Update Vendor |
| **INV-012** | INSERT | 2200.00 | Uppercase Action |
| **INV-013** | insert | 8800.00 | Unicode Vendor |
| **INV-014** | insert | 5500.00 | Full Complex Vendor |
| **INV-ADDR-001** | insert | 3500.00 | Alpha Retailers |
| **INV-ADDR-002** | insert | 7200.50 | Beta Distributors |
| **INV-COMPLEX-STR** | insert | 5000.00 | Test Vendor |
| **INV-ENT-001** | insert | 95000.00 | Tata Consultancy Services |
| **INV-SMB-001** | insert | 1200.00 | Local Traders Co |

---

### Table 3: `orders` (1 row)

| order_id | customer_name | total_amount | status |
| :--- | :--- | :--- | :--- |
| **ORD-001** | John Doe | 1024.00 | confirmed |

---

### Table 4: `order_items` (3 rows — array expanded from single order message)

| item_id | order_id | sku | qty | price |
| :--- | :--- | :--- | :--- | :--- |
| **ORD-001-0** | ORD-001 | LAPTOP-01 | 1 | 999.00 |
| **ORD-001-1** | ORD-001 | MOUSE-01 | 2 | 12.50 |
| **ORD-001-2** | ORD-001 | USB-HUB-01 | 1 | 24.99 |
