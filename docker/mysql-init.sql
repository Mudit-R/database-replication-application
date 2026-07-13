-- Auto-run on MySQL container startup

-- Invoice tables
CREATE TABLE IF NOT EXISTS db_invoices (
    invoice_id    VARCHAR(255) NOT NULL,
    invoiceamount DECIMAL(20,6),
    vendor_name   VARCHAR(255),
    paid_paise    BOOLEAN,
    order_for     VARCHAR(255),
    column1       VARCHAR(255),
    child_data    TEXT,
    addr_city     VARCHAR(255),
    addr_state    VARCHAR(255),
    addr_district VARCHAR(255),
    PRIMARY KEY (invoice_id)
);

CREATE TABLE IF NOT EXISTS db_audit_log (
    log_id      VARCHAR(255) NOT NULL,
    action      VARCHAR(50),
    amount      DECIMAL(20,6),
    created_by  VARCHAR(255),
    PRIMARY KEY (log_id)
);

-- Order tables (e-commerce normalization example)
-- A single incoming order message splits into all three of these tables.

CREATE TABLE IF NOT EXISTS orders (
    order_id      VARCHAR(255) NOT NULL,
    customer_name VARCHAR(255),
    total_amount  DECIMAL(20,6),
    status        VARCHAR(50),
    PRIMARY KEY (order_id)
);

-- One row per item in the incoming message's items array.
-- item_id is auto-generated as <order_id>-<index> (e.g. ORD-001-0).
CREATE TABLE IF NOT EXISTS order_items (
    item_id   VARCHAR(255) NOT NULL,
    order_id  VARCHAR(255),
    sku       VARCHAR(255),
    qty       INT,
    price     DECIMAL(20,6),
    PRIMARY KEY (item_id)
);

CREATE TABLE IF NOT EXISTS order_audit (
    log_id     VARCHAR(255) NOT NULL,
    action     VARCHAR(50),
    written_by VARCHAR(255),
    item_count INT,
    PRIMARY KEY (log_id)
);
