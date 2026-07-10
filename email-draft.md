Subject: Verification results with sample JSON and SQL logs

Hey,

I've set up the local Kafka queue and MySQL database containers to test the replication app.

Everything works correctly. Below are the details for all 18 test cases, listing the input JSON followed by the executed SQL queries.

--- 

Case 1: Simple Delete

Input JSON message:
```json
{
  "header": {
    "uuid": "INV-001",
    "action": "delete"
  }
}
```

Queries executed:
DELETE FROM db_invoices WHERE invoice_id = 'INV-001'

--- 

Case 2: Simple Insert

Input JSON message:
```json
{
  "header": { "uuid": "INV-002", "action": "insert" },
  "invoice_amount": 1500.00,
  "vendor": "Acme Corp",
  "paid": false,
  "For_order": "ORD-100",
  "child": { "for_order": "ORD-100-A" }
}
```

Queries executed:
UPDATE Query:
UPDATE db_invoices SET invoiceamount = 1500.0, vendor_name = 'Acme Corp', paid_paise = false, order_for = 'ORD-100', column1 = 'ORD-100-A', addr_city = NULL, addr_state = NULL, addr_district = NULL WHERE invoice_id = 'INV-002'

INSERT Query:
INSERT INTO db_invoices (invoice_id, invoiceamount, vendor_name, paid_paise, order_for, column1, addr_city, addr_state, addr_district) VALUES ('INV-002', 1500.0, 'Acme Corp', false, 'ORD-100', 'ORD-100-A', NULL, NULL, NULL)

--- 

Case 3: Simple Update

Input JSON message:
```json
{
  "header": { "uuid": "INV-002", "action": "update" },
  "invoice_amount": 1800.00,
  "vendor": "Acme Corp",
  "paid": true,
  "For_order": "ORD-100",
  "child": { "for_order": "ORD-100-A" }
}
```

Queries executed:
UPDATE Query:
UPDATE db_invoices SET invoiceamount = 1800.0, vendor_name = 'Acme Corp', paid_paise = true, order_for = 'ORD-100', column1 = 'ORD-100-A', addr_city = NULL, addr_state = NULL, addr_district = NULL WHERE invoice_id = 'INV-002'

INSERT Query:
INSERT INTO db_invoices (invoice_id, invoiceamount, vendor_name, paid_paise, order_for, column1, addr_city, addr_state, addr_district) VALUES ('INV-002', 1800.0, 'Acme Corp', true, 'ORD-100', 'ORD-100-A', NULL, NULL, NULL)

--- 

Case 4: Nested Object

Input JSON message:
```json
{
  "header": { "uuid": "INV-003", "action": "insert" },
  "invoice_amount": 9999.99,
  "vendor": "Nested Corp",
  "paid": true,
  "For_order": "ORD-200",
  "child": {
    "for_order": "ORD-200-A",
    "address": {
      "street": "42 Baker Street",
      "city": "London",
      "country": "UK"
    }
  }
}
```

Queries executed:
UPDATE Query:
UPDATE db_invoices SET invoiceamount = 9999.99, vendor_name = 'Nested Corp', paid_paise = true, order_for = 'ORD-200', column1 = 'ORD-200-A', addr_city = 'London', addr_state = NULL, addr_district = NULL WHERE invoice_id = 'INV-003'

INSERT Query:
INSERT INTO db_invoices (invoice_id, invoiceamount, vendor_name, paid_paise, order_for, column1, addr_city, addr_state, addr_district) VALUES ('INV-003', 9999.99, 'Nested Corp', true, 'ORD-200', 'ORD-200-A', 'London', NULL, NULL)

--- 

Case 5: Array In Child

Input JSON message:
```json
{
  "header": { "uuid": "INV-004", "action": "insert" },
  "invoice_amount": 3200.00,
  "vendor": "Array Vendor",
  "paid": false,
  "For_order": "ORD-300",
  "child": {
    "for_order": "ORD-300-A",
    "tags": ["urgent", "international", "bulk"]
  }
}
```

Queries executed:
UPDATE Query:
UPDATE db_invoices SET invoiceamount = 3200.0, vendor_name = 'Array Vendor', paid_paise = false, order_for = 'ORD-300', column1 = 'ORD-300-A', addr_city = NULL, addr_state = NULL, addr_district = NULL WHERE invoice_id = 'INV-004'

INSERT Query:
INSERT INTO db_invoices (invoice_id, invoiceamount, vendor_name, paid_paise, order_for, column1, addr_city, addr_state, addr_district) VALUES ('INV-004', 3200.0, 'Array Vendor', false, 'ORD-300', 'ORD-300-A', NULL, NULL, NULL)

--- 

Case 6: Complex Child

Input JSON message:
```json
{
  "header": { "uuid": "INV-005", "action": "insert" },
  "invoice_amount": 7500.00,
  "vendor": "Complex Co",
  "paid": true,
  "For_order": "ORD-400",
  "child": {
    "for_order": "ORD-400-A",
    "tags": ["express", "fragile"],
    "address": { "city": "Mumbai", "pin": 400001 },
    "contacts": [
      { "name": "Raj", "phone": "9999999999" },
      { "name": "Priya", "phone": "8888888888" }
    ]
  }
}
```

Queries executed:
UPDATE Query:
UPDATE db_invoices SET invoiceamount = 7500.0, vendor_name = 'Complex Co', paid_paise = true, order_for = 'ORD-400', column1 = 'ORD-400-A', addr_city = 'Mumbai', addr_state = NULL, addr_district = NULL WHERE invoice_id = 'INV-005'

INSERT Query:
INSERT INTO db_invoices (invoice_id, invoiceamount, vendor_name, paid_paise, order_for, column1, addr_city, addr_state, addr_district) VALUES ('INV-005', 7500.0, 'Complex Co', true, 'ORD-400', 'ORD-400-A', 'Mumbai', NULL, NULL)

--- 

Case 7: Special Characters

Input JSON message:
```json
{
  "header": { "uuid": "INV-006", "action": "insert" },
  "invoice_amount": 100.00,
  "vendor": "O'Reilly & Sons / \"Tech\" LLC",
  "paid": false,
  "For_order": "ORD-IT'S-COMPLEX/001",
  "child": { "for_order": "sub-order#1" }
}
```

Queries executed:
UPDATE Query:
UPDATE db_invoices SET invoiceamount = 100.0, vendor_name = 'O''Reilly & Sons / "Tech" LLC', paid_paise = false, order_for = 'ORD-IT''S-COMPLEX/001', column1 = 'sub-order#1', addr_city = NULL, addr_state = NULL, addr_district = NULL WHERE invoice_id = 'INV-006'

INSERT Query:
INSERT INTO db_invoices (invoice_id, invoiceamount, vendor_name, paid_paise, order_for, column1, addr_city, addr_state, addr_district) VALUES ('INV-006', 100.0, 'O''Reilly & Sons / "Tech" LLC', false, 'ORD-IT''S-COMPLEX/001', 'sub-order#1', NULL, NULL, NULL)

--- 

Case 8: Missing Fields

Input JSON message:
```json
{
  "header": { "uuid": "INV-007", "action": "insert" },
  "invoice_amount": 0.01
}
```

Queries executed:
UPDATE Query:
UPDATE db_invoices SET invoiceamount = 0.01, vendor_name = NULL, paid_paise = NULL, order_for = NULL, column1 = NULL, addr_city = NULL, addr_state = NULL, addr_district = NULL WHERE invoice_id = 'INV-007'

INSERT Query:
INSERT INTO db_invoices (invoice_id, invoiceamount, vendor_name, paid_paise, order_for, column1, addr_city, addr_state, addr_district) VALUES ('INV-007', 0.01, NULL, NULL, NULL, NULL, NULL, NULL, NULL)

--- 

Case 9: Large Decimal

Input JSON message:
```json
{
  "header": { "uuid": "INV-008", "action": "insert" },
  "invoice_amount": 99999999.999999,
  "vendor": "Big Numbers Ltd",
  "paid": true,
  "For_order": "ORD-LARGE",
  "child": { "for_order": "SUB-LARGE" }
}
```

Queries executed:
UPDATE Query:
UPDATE db_invoices SET invoiceamount = 9.9999999999999E7, vendor_name = 'Big Numbers Ltd', paid_paise = true, order_for = 'ORD-LARGE', column1 = 'SUB-LARGE', addr_city = NULL, addr_state = NULL, addr_district = NULL WHERE invoice_id = 'INV-008'

INSERT Query:
INSERT INTO db_invoices (invoice_id, invoiceamount, vendor_name, paid_paise, order_for, column1, addr_city, addr_state, addr_district) VALUES ('INV-008', 9.9999999999999E7, 'Big Numbers Ltd', true, 'ORD-LARGE', 'SUB-LARGE', NULL, NULL, NULL)

--- 

Case 10: Zero Amount

Input JSON message:
```json
{
  "header": { "uuid": "INV-009", "action": "insert" },
  "invoice_amount": 0,
  "vendor": "Zero Invoice",
  "paid": false,
  "For_order": "ORD-ZERO",
  "child": { "for_order": "SUB-ZERO" }
}
```

Queries executed:
UPDATE Query:
UPDATE db_invoices SET invoiceamount = 0, vendor_name = 'Zero Invoice', paid_paise = false, order_for = 'ORD-ZERO', column1 = 'SUB-ZERO', addr_city = NULL, addr_state = NULL, addr_district = NULL WHERE invoice_id = 'INV-009'

INSERT Query:
INSERT INTO db_invoices (invoice_id, invoiceamount, vendor_name, paid_paise, order_for, column1, addr_city, addr_state, addr_district) VALUES ('INV-009', 0, 'Zero Invoice', false, 'ORD-ZERO', 'SUB-ZERO', NULL, NULL, NULL)

--- 

Case 11: Delete Extra Fields

Input JSON message:
```json
{
  "header": { "uuid": "INV-010", "action": "delete" },
  "invoice_amount": 5000,
  "vendor": "Delete With Extra Fields",
  "random1": "ignored",
  "random2": 12345,
  "random3": { "key": "value" }
}
```

Queries executed:
DELETE FROM db_invoices WHERE invoice_id = 'INV-010'

--- 

Case 12: Update With Nested

Input JSON message:
```json
{
  "header": { "uuid": "INV-011", "action": "update" },
  "invoice_amount": 4500.75,
  "vendor": "Update Vendor",
  "paid": true,
  "For_order": "ORD-UPD",
  "child": {
    "for_order": "SUB-UPD",
    "metadata": { "source": "mongodb", "version": 3 }
  }
}
```

Queries executed:
UPDATE Query:
UPDATE db_invoices SET invoiceamount = 4500.75, vendor_name = 'Update Vendor', paid_paise = true, order_for = 'ORD-UPD', column1 = 'SUB-UPD', addr_city = NULL, addr_state = NULL, addr_district = NULL WHERE invoice_id = 'INV-011'

INSERT Query:
INSERT INTO db_invoices (invoice_id, invoiceamount, vendor_name, paid_paise, order_for, column1, addr_city, addr_state, addr_district) VALUES ('INV-011', 4500.75, 'Update Vendor', true, 'ORD-UPD', 'SUB-UPD', NULL, NULL, NULL)

--- 

Case 13: Uppercase Action

Input JSON message:
```json
{
  "header": { "uuid": "INV-012", "action": "INSERT" },
  "invoice_amount": 2200.00,
  "vendor": "Uppercase Action",
  "paid": false,
  "For_order": "ORD-UPPER",
  "child": { "for_order": "SUB-UPPER" }
}
```

Queries executed:
UPDATE Query:
UPDATE db_invoices SET invoiceamount = 2200.0, vendor_name = 'Uppercase Action', paid_paise = false, order_for = 'ORD-UPPER', column1 = 'SUB-UPPER', addr_city = NULL, addr_state = NULL, addr_district = NULL WHERE invoice_id = 'INV-012'

INSERT Query:
INSERT INTO db_invoices (invoice_id, invoiceamount, vendor_name, paid_paise, order_for, column1, addr_city, addr_state, addr_district) VALUES ('INV-012', 2200.0, 'Uppercase Action', false, 'ORD-UPPER', 'SUB-UPPER', NULL, NULL, NULL)

--- 

Case 14: Unicode Values

Input JSON message:
```json
{
  "header": { "uuid": "INV-013", "action": "insert" },
  "invoice_amount": 8800.00,
  "vendor": "Unicode Vendor \u20b9\u00e9\u4e2d\u6587",
  "paid": true,
  "For_order": "ORD-\u20b9-500",
  "child": { "for_order": "SUB-UNICODE" }
}
```

Queries executed:
UPDATE Query:
UPDATE db_invoices SET invoiceamount = 8800.0, vendor_name = 'Unicode Vendor ?é??', paid_paise = true, order_for = 'ORD-?-500', column1 = 'SUB-UNICODE', addr_city = NULL, addr_state = NULL, addr_district = NULL WHERE invoice_id = 'INV-013'

INSERT Query:
INSERT INTO db_invoices (invoice_id, invoiceamount, vendor_name, paid_paise, order_for, column1, addr_city, addr_state, addr_district) VALUES ('INV-013', 8800.0, 'Unicode Vendor ?é??', true, 'ORD-?-500', 'SUB-UNICODE', NULL, NULL, NULL)

--- 

Case 15: Full Complex

Input JSON message:
```json
{
  "header": { "uuid": "INV-014", "action": "insert" },
  "invoice_amount": 5500.00,
  "vendor": "Full Complex Vendor",
  "paid": true,
  "For_order": "ORD-FULL",
  "child": {
    "for_order": "SUB-FULL",
    "tags": ["priority-1", "bulk", "pre-paid"],
    "address": {
      "street": "Plot 7, Sector 21",
      "city": "Noida",
      "state": "UP",
      "pin": 201301
    },
    "contacts": [ 
      { "name": "Alice", "role": "billing" },
      { "name": "Bob", "role": "shipping" }
    ],
    "notes": "Handle with care: O'Brien's order"
  },
  "extra_field_1": "ignored",
  "extra_field_2": 9999,
  "extra_field_3": { "junk": true }
}
```

Queries executed:
UPDATE Query:
UPDATE db_invoices SET invoiceamount = 5500.0, vendor_name = 'Full Complex Vendor', paid_paise = true, order_for = 'ORD-FULL', column1 = 'SUB-FULL', addr_city = 'Noida', addr_state = 'UP', addr_district = NULL WHERE invoice_id = 'INV-014'

INSERT Query:
INSERT INTO db_invoices (invoice_id, invoiceamount, vendor_name, paid_paise, order_for, column1, addr_city, addr_state, addr_district) VALUES ('INV-014', 5500.0, 'Full Complex Vendor', true, 'ORD-FULL', 'SUB-FULL', 'Noida', 'UP', NULL)

--- 

Case 16: Stringified Json

Input JSON message:
```json
{
  "header": { "uuid": "INV-COMPLEX-STR", "action": "insert" },
  "invoice_amount": 5000,
  "vendor": "Test Vendor",
  "paid": true,
  "For_order": "{\"abc\":\"val_abc\"}",
  "child": { "for_order": "{\"nested\":\"stringified\",\"num\":42}" }
}
```

Queries executed:
UPDATE Query:
UPDATE db_invoices SET invoiceamount = 5000, vendor_name = 'Test Vendor', paid_paise = true, order_for = '{"abc":"val_abc"}', column1 = '{"nested":"stringified","num":42}', addr_city = NULL, addr_state = NULL, addr_district = NULL WHERE invoice_id = 'INV-COMPLEX-STR'

INSERT Query:
INSERT INTO db_invoices (invoice_id, invoiceamount, vendor_name, paid_paise, order_for, column1, addr_city, addr_state, addr_district) VALUES ('INV-COMPLEX-STR', 5000, 'Test Vendor', true, '{"abc":"val_abc"}', '{"nested":"stringified","num":42}', NULL, NULL, NULL)

--- 

Case 17: Nested Address Ex1

Input JSON message:
```json
{
  "header": { "uuid": "INV-ADDR-001", "action": "insert" },
  "invoice_amount": 3500.00,
  "vendor": "Alpha Retailers",
  "paid": true,
  "For_order": "ORD-N1",
  "child": {
    "for_order": "SUB-N1",
    "address": {
      "city": "Mumbai",
      "state": "Maharashtra",
      "district": "Mumbai City"
    }
  }
}
```

Queries executed:
UPDATE Query:
UPDATE db_invoices SET invoiceamount = 3500.0, vendor_name = 'Alpha Retailers', paid_paise = true, order_for = 'ORD-N1', column1 = 'SUB-N1', addr_city = 'Mumbai', addr_state = 'Maharashtra', addr_district = 'Mumbai City' WHERE invoice_id = 'INV-ADDR-001'

INSERT Query:
INSERT INTO db_invoices (invoice_id, invoiceamount, vendor_name, paid_paise, order_for, column1, addr_city, addr_state, addr_district) VALUES ('INV-ADDR-001', 3500.0, 'Alpha Retailers', true, 'ORD-N1', 'SUB-N1', 'Mumbai', 'Maharashtra', 'Mumbai City')

--- 

Case 18: Nested Address Ex2

Input JSON message:
```json
{
  "header": { "uuid": "INV-ADDR-002", "action": "insert" },
  "invoice_amount": 7200.50,
  "vendor": "Beta Distributors",
  "paid": false,
  "For_order": "ORD-N2",
  "child": {
    "for_order": "SUB-N2",
    "address": {
      "city": "Bengaluru",
      "state": "Karnataka",
      "district": "Bengaluru Urban"
    }
  }
}
```

Queries executed:
UPDATE Query:
UPDATE db_invoices SET invoiceamount = 7200.5, vendor_name = 'Beta Distributors', paid_paise = false, order_for = 'ORD-N2', column1 = 'SUB-N2', addr_city = 'Bengaluru', addr_state = 'Karnataka', addr_district = 'Bengaluru Urban' WHERE invoice_id = 'INV-ADDR-002'

INSERT Query:
INSERT INTO db_invoices (invoice_id, invoiceamount, vendor_name, paid_paise, order_for, column1, addr_city, addr_state, addr_district) VALUES ('INV-ADDR-002', 7200.5, 'Beta Distributors', false, 'ORD-N2', 'SUB-N2', 'Bengaluru', 'Karnataka', 'Bengaluru Urban')

---

Let me know if we need to adjust any schema configuration.

Thanks
