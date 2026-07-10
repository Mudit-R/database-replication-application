-- Auto-run on MySQL container startup
CREATE TABLE IF NOT EXISTS db_invoices (
    invoice_id    VARCHAR(255) NOT NULL,
    invoiceamount DECIMAL(20,6),
    vendor_name   VARCHAR(255),
    paid_paise    BOOLEAN,
    order_for     VARCHAR(255),
    column1       VARCHAR(255),
    child_data    TEXT,
    PRIMARY KEY (invoice_id)
);
