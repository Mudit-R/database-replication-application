-- Auto-run on MySQL container startup

-- Table 1: Main invoice records
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

-- Table 2: Audit log - populated from the same Kafka message as db_invoices.
-- Demonstrates multi-table fan-out from a single incoming event.
CREATE TABLE IF NOT EXISTS db_audit_log (
    log_id      VARCHAR(255) NOT NULL,
    action      VARCHAR(50),
    amount      DECIMAL(20,6),
    created_by  VARCHAR(255),
    PRIMARY KEY (log_id)
);
