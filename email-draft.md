Subject: Task 2 Config Design - Schema Normalization & Multi-Table Splitting (for Monday review)

Hi,

Below is the proposed configuration plan for Task 2 before I start any coding.
Let me know if the approach looks right and we can go through it on Monday.

---

SUMMARY

The replication engine currently maps one incoming message to one row in one table.
Task 2 requires it to split a single complex message (like an e-commerce order) across
multiple normalized RDBMS tables at the same time.

---

THE E-COMMERCE EXAMPLE

Input message (arrives on the Kafka topic):

{
  "header": { "uuid": "ORD-001", "action": "insert" },
  "customer_name": "John Doe",
  "total_amount": 1024.00,
  "status": "confirmed",
  "items": [
    { "sku": "LAPTOP-01", "qty": 1, "price": 999.00 },
    { "sku": "MOUSE-01",  "qty": 2, "price": 12.50  }
  ]
}

This single message must produce rows in three tables:

Table 1: orders (one row per message)
  order_id | customer_name | total_amount | status
  ORD-001  | John Doe      | 1024.00      | confirmed

Table 2: order_items (one row per element in the items array)
  item_id      | order_id | sku        | qty | price
  ORD-001-0    | ORD-001  | LAPTOP-01  | 1   | 999.00
  ORD-001-1    | ORD-001  | MOUSE-01   | 2   | 12.50

Table 3: order_audit (lightweight log, one row per message)
  log_id  | action | written_by
  ORD-001 | insert | John Doe

---

PROPOSED CONFIG CHANGE

The current config already supports writing to multiple tables using a
target-schemas list. The only new concept needed is one optional field
per schema called source-array.

  - If source-array is not set: schema works as today (one row per message)
  - If source-array is set: the engine iterates each element of that array
    and writes one row per element. Column paths are checked against the
    array element first, then fall back to the top-level message.

Example (application.yml):

    target-schemas:

      - table-name: "orders"
        userkey-path: "header.uuid"
        order_id:
          path: "header.uuid"
        customer_name:
          path: "customer_name"
        total_amount:
          path: "total_amount"
        status:
          path: "status"

      - table-name: "order_items"
        userkey-path: "header.uuid"
        source-array: "items"           # new field — iterate this array
        item_id:
          path: "header.uuid"
          pk-source: "generated"        # auto-generates ORD-001-0, ORD-001-1
        order_id:
          path: "header.uuid"           # falls back to top-level message
        sku:
          path: "sku"                   # resolved per array element
        qty:
          path: "qty"
        price:
          path: "price"

      - table-name: "order_audit"
        userkey-path: "header.uuid"
        log_id:
          path: "header.uuid"
        action:
          path: "header.action"
        written_by:
          path: "customer_name"

---

WHAT CHANGES IN THE CODE (no code written yet)

  - application.yml      Add source-array and pk-source fields to relevant schemas
  - SqlBuilder.java      Add buildAll() that returns one Result per array element
                         (if no source-array, returns a single-element list as before)
  - ReplicationConsumer  Call buildAll() and execute each result
  - DemoRunner           Same change for the preview tool
  - mysql-init.sql       Add orders, order_items, order_audit table definitions

Total new code is approximately 30-40 lines. Everything else is a loop swap.
Existing invoice mapping is not affected.

---

OPEN QUESTIONS FOR MONDAY

1. Should the item primary key always be <parent_uuid>-<index> (e.g. ORD-001-0),
   or should the format be configurable?

2. If the items array is empty, should the engine skip silently or raise an error?

3. For a DELETE on the parent message, should the engine also delete the child rows
   in order_items, or leave that to the database foreign key constraints?

Let me know your thoughts and I will start coding once we have aligned on Monday.

Thanks
