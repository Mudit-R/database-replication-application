Subject: Task 2 - Schema Normalization: Sample Input and Output

Hi,

Below is a clean walkthrough of how a single incoming order message splits
across three relational tables simultaneously. This is the e-commerce example
we discussed.

---

HOW IT WORKS

The config (application.yml) defines a list of target-schemas. Each schema
maps to one RDBMS table. When a message arrives on Kafka, the engine checks
every schema in the list:

  - filter-path / filter-value  Controls which schemas apply to which messages.
                                An order message only triggers order schemas.
                                An invoice message only triggers invoice schemas.

  - source-array                If a schema has this field, the engine iterates
                                that array in the message and writes one row per
                                element instead of one row for the whole message.

  - pk-source: "generated"      Auto-generates a composite primary key for child
                                rows: <parent_id>-<array_index>

---

INPUT MESSAGE (sent to Kafka topic: nosql-replication)

{
  "header": {
    "uuid": "ORD-001",
    "action": "insert",
    "type": "order"
  },
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

---

CONFIG (relevant section from application.yml)

    target-schemas:

      # Schema 3: orders — one row per message (no source-array)
      - table-name: "orders"
        userkey-path: "header.uuid"
        filter-path: "header.type"
        filter-value: "order"
        order_id:
          path: "header.uuid"
        customer_name:
          path: "customer_name"
        total_amount:
          path: "total_amount"
        status:
          path: "status"

      # Schema 4: order_items — one row per element in items[]
      - table-name: "order_items"
        userkey-path: "header.uuid"
        filter-path: "header.type"
        filter-value: "order"
        source-array: "items"           <-- iterates the items array
        item_id:
          path: "header.uuid"
          pk-source: "generated"        <-- auto-generates ORD-001-0, ORD-001-1 etc.
        order_id:
          path: "header.uuid"           <-- top-level fallback
        sku:
          path: "sku"                   <-- resolved per array element
        qty:
          path: "qty"
        price:
          path: "price"

      # Schema 5: order_audit — one row per message (no source-array)
      - table-name: "order_audit"
        userkey-path: "header.uuid"
        filter-path: "header.type"
        filter-value: "order"
        log_id:
          path: "header.uuid"
        action:
          path: "header.action"
        written_by:
          path: "customer_name"
        item_count:
          path: "total_items"

---

OUTPUT — SQL EXECUTED

The engine generates and runs the following SQL statements from that one message:

-- Table 1: orders (1 row)

UPDATE orders
  SET customer_name = 'John Doe', total_amount = 1024.0, status = 'confirmed'
  WHERE order_id = 'ORD-001';

INSERT INTO orders (order_id, customer_name, total_amount, status)
  VALUES ('ORD-001', 'John Doe', 1024.0, 'confirmed');
  -- INSERT only runs if UPDATE affected 0 rows (new record)


-- Table 2: order_items (3 rows — one per element in items[])

UPDATE order_items
  SET order_id = 'ORD-001', sku = 'LAPTOP-01', qty = 1, price = 999.0
  WHERE item_id = 'ORD-001-0';

INSERT INTO order_items (item_id, order_id, sku, qty, price)
  VALUES ('ORD-001-0', 'ORD-001', 'LAPTOP-01', 1, 999.0);

---

UPDATE order_items
  SET order_id = 'ORD-001', sku = 'MOUSE-01', qty = 2, price = 12.5
  WHERE item_id = 'ORD-001-1';

INSERT INTO order_items (item_id, order_id, sku, qty, price)
  VALUES ('ORD-001-1', 'ORD-001', 'MOUSE-01', 2, 12.5);

---

UPDATE order_items
  SET order_id = 'ORD-001', sku = 'USB-HUB-01', qty = 1, price = 24.99
  WHERE item_id = 'ORD-001-2';

INSERT INTO order_items (item_id, order_id, sku, qty, price)
  VALUES ('ORD-001-2', 'ORD-001', 'USB-HUB-01', 1, 24.99);


-- Table 3: order_audit (1 row)

UPDATE order_audit
  SET action = 'insert', written_by = 'John Doe', item_count = 3
  WHERE log_id = 'ORD-001';

INSERT INTO order_audit (log_id, action, written_by, item_count)
  VALUES ('ORD-001', 'insert', 'John Doe', 3);

---

OUTPUT — FINAL DATABASE STATE

SELECT * FROM orders;

  order_id | customer_name | total_amount | status
  ORD-001  | John Doe      | 1024.000000  | confirmed


SELECT * FROM order_items;

  item_id     | order_id | sku        | qty | price
  ORD-001-0   | ORD-001  | LAPTOP-01  | 1   | 999.000000
  ORD-001-1   | ORD-001  | MOUSE-01   | 2   | 12.500000
  ORD-001-2   | ORD-001  | USB-HUB-01 | 1   | 24.990000


SELECT * FROM order_audit;

  log_id  | action | written_by | item_count
  ORD-001 | insert | John Doe   | 3


NOTE: The invoice schemas (db_invoices, db_audit_log) were skipped entirely
because the message had header.type = "order", not "invoice".

---

SUMMARY

  1 Kafka message  -->  5 SQL statements  -->  3 tables updated  -->  5 total rows

  Table         Rows Written  Reason
  orders        1             One row per message (no source-array)
  order_items   3             source-array: "items" — one row per item in the array
  order_audit   1             One row per message (no source-array)
  db_invoices   0             SKIPPED — header.type did not match filter-value: invoice
  db_audit_log  0             SKIPPED — header.type did not match filter-value: invoice

Thanks
