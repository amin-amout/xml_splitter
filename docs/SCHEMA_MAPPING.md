# XML to Relational Schema Mapping Examples

This document provides concrete examples of how XML structures are transformed into relational tables.

---

## Example 1: E-commerce Orders XML

### Source XML Structure
```xml
<?xml version="1.0" encoding="UTF-8"?>
<Orders>
  <Order OrderID="12345" OrderDate="2025-11-15">
    <Customer CustomerID="C001" Name="John Doe" Email="john@example.com"/>
    <ShippingAddress>
      <Street>123 Main St</Street>
      <City>Springfield</City>
      <State>IL</State>
      <ZipCode>62701</ZipCode>
    </ShippingAddress>
    <OrderItems>
      <Item ItemID="A100" Quantity="2" UnitPrice="29.99">
        <ProductName>Widget</ProductName>
        <Category>Electronics</Category>
      </Item>
      <Item ItemID="B200" Quantity="1" UnitPrice="49.99">
        <ProductName>Gadget</ProductName>
        <Category>Electronics</Category>
      </Item>
    </OrderItems>
    <Status>Shipped</Status>
  </Order>
</Orders>
```

### Relational Tables Output

#### Table 1: orders (Parent Table)
```
+----------+------------+-------------+-------------------+------------------+
| order_id | order_date | customer_id | customer_name     | customer_email   |
+----------+------------+-------------+-------------------+------------------+
| 12345    | 2025-11-15 | C001        | John Doe          | john@example.com |
+----------+------------+-------------+-------------------+------------------+

| shipping_street | shipping_city | shipping_state | shipping_zipcode | status  | record_id | ingestion_timestamp     |
|-----------------|---------------|----------------|------------------|---------|-----------|-------------------------|
| 123 Main St     | Springfield   | IL             | 62701            | Shipped | 1         | 2025-11-22 10:30:00.000 |
```

#### Table 2: order_items (Child Table)
```
+-----------+----------+---------+----------+-------------+--------------+------------------+---------+
| child_id  | order_id | item_id | quantity | unit_price  | product_name | category         | parent_id |
+-----------+----------+---------+----------+-------------+--------------+------------------+---------+
| 1         | 12345    | A100    | 2        | 29.99       | Widget       | Electronics      | 1         |
| 2         | 12345    | B200    | 1        | 49.99       | Gadget       | Electronics      | 1         |
+-----------+----------+---------+----------+-------------+--------------+------------------+---------+
```

### Spark Code Mapping

```scala
// Main orders table
val ordersTable = df.select(
  col("_OrderID").as("order_id"),
  col("_OrderDate").as("order_date"),
  col("Customer._CustomerID").as("customer_id"),
  col("Customer._Name").as("customer_name"),
  col("Customer._Email").as("customer_email"),
  col("ShippingAddress.Street").as("shipping_street"),
  col("ShippingAddress.City").as("shipping_city"),
  col("ShippingAddress.State").as("shipping_state"),
  col("ShippingAddress.ZipCode").as("shipping_zipcode"),
  col("Status").as("status"),
  col("record_id"),
  col("ingestion_timestamp")
)

// Order items child table
val orderItemsTable = df.select(
  col("record_id").as("parent_id"),
  col("_OrderID").as("order_id"),
  explode_outer(col("OrderItems.Item")).as("item")
).select(
  monotonically_increasing_id().as("child_id"),
  col("parent_id"),
  col("order_id"),
  col("item._ItemID").as("item_id"),
  col("item._Quantity").as("quantity"),
  col("item._UnitPrice").as("unit_price"),
  col("item.ProductName").as("product_name"),
  col("item.Category").as("category")
)
```

---

## Example 2: Banking Transactions XML

### Source XML Structure
```xml
<?xml version="1.0" encoding="UTF-8"?>
<Transactions>
  <Transaction TransactionID="T9999" Timestamp="2025-11-22T08:15:30Z">
    <Account AccountNumber="123456789" AccountType="Checking"/>
    <Amount Currency="USD">1250.00</Amount>
    <TransactionType>DEBIT</TransactionType>
    <Merchant>
      <Name>Acme Store</Name>
      <MerchantID>M5678</MerchantID>
      <Category>Retail</Category>
    </Merchant>
    <Tags>
      <Tag>online</Tag>
      <Tag>recurring</Tag>
    </Tags>
    <OptionalField/>  <!-- Empty/missing field -->
  </Transaction>
</Transactions>
```

### Relational Tables Output

#### Table 1: transactions
```
+----------------+-------------------------+----------------+--------------+--------+----------+
| transaction_id | timestamp               | account_number | account_type | amount | currency |
+----------------+-------------------------+----------------+--------------+--------+----------+
| T9999          | 2025-11-22T08:15:30.000 | 123456789      | Checking     | 1250.00| USD      |
+----------------+-------------------------+----------------+--------------+--------+----------+

| transaction_type | merchant_name | merchant_id | merchant_category | optional_field | record_id |
|------------------|---------------|-------------|-------------------|----------------|-----------|
| DEBIT            | Acme Store    | M5678       | Retail            | NULL           | 1         |
```

#### Table 2: transaction_tags (Many-to-Many)
```
+-----------+----------------+---------------+-----------+
| child_id  | transaction_id | tag_value     | parent_id |
+-----------+----------------+---------------+-----------+
| 1         | T9999          | online        | 1         |
| 2         | T9999          | recurring     | 1         |
+-----------+----------------+---------------+-----------+
```

### Handling Special Cases

```scala
// Handle empty/missing fields with null
val transactionsTable = df.select(
  col("_TransactionID").as("transaction_id"),
  col("_Timestamp").cast("timestamp").as("timestamp"),
  col("Account._AccountNumber").as("account_number"),
  col("Account._AccountType").as("account_type"),
  col("Amount._VALUE").cast("decimal(18,2)").as("amount"),
  col("Amount._Currency").as("currency"),
  col("TransactionType").as("transaction_type"),
  col("Merchant.Name").as("merchant_name"),
  col("Merchant.MerchantID").as("merchant_id"),
  col("Merchant.Category").as("merchant_category"),
  coalesce(col("OptionalField"), lit(null)).as("optional_field"),
  col("record_id")
)

// Handle array of primitive values (tags)
val tagsTable = df
  .filter(col("Tags").isNotNull)
  .select(
    col("record_id").as("parent_id"),
    col("_TransactionID").as("transaction_id"),
    explode_outer(col("Tags.Tag")).as("tag_value")
  )
  .withColumn("child_id", monotonically_increasing_id())
```

---

## Example 3: Healthcare Patient Records

### Source XML Structure
```xml
<?xml version="1.0" encoding="UTF-8"?>
<Patients>
  <Patient PatientID="P12345" DateOfBirth="1985-03-15">
    <PersonalInfo>
      <FirstName>Jane</FirstName>
      <LastName>Smith</LastName>
      <Gender>F</Gender>
    </PersonalInfo>
    <Visits>
      <Visit VisitID="V001" VisitDate="2025-10-01">
        <Diagnosis Code="J20.9">Acute bronchitis</Diagnosis>
        <Prescriptions>
          <Prescription DrugName="Amoxicillin" Dosage="500mg" Frequency="3x daily"/>
          <Prescription DrugName="Cough Syrup" Dosage="10ml" Frequency="as needed"/>
        </Prescriptions>
      </Visit>
      <Visit VisitID="V002" VisitDate="2025-11-10">
        <Diagnosis Code="Z00.00">General checkup</Diagnosis>
        <Prescriptions/>  <!-- No prescriptions -->
      </Visit>
    </Visits>
  </Patient>
</Patients>
```

### Relational Tables Output

#### Table 1: patients
```
+------------+---------------+------------+-----------+--------+-----------+
| patient_id | date_of_birth | first_name | last_name | gender | record_id |
+------------+---------------+------------+-----------+--------+-----------+
| P12345     | 1985-03-15    | Jane       | Smith     | F      | 1         |
+------------+---------------+------------+-----------+--------+-----------+
```

#### Table 2: visits (1:N with patients)
```
+-----------+------------+----------+------------+----------------+----------------------+-----------+
| child_id  | patient_id | visit_id | visit_date | diagnosis_code | diagnosis_text       | parent_id |
+-----------+------------+----------+------------+----------------+----------------------+-----------+
| 1         | P12345     | V001     | 2025-10-01 | J20.9          | Acute bronchitis     | 1         |
| 2         | P12345     | V002     | 2025-11-10 | Z00.00         | General checkup      | 1         |
+-----------+------------+----------+------------+----------------+----------------------+-----------+
```

#### Table 3: prescriptions (1:N with visits)
```
+-----------+----------+---------------+---------+-----------+-----------+
| child_id  | visit_id | drug_name     | dosage  | frequency | parent_id |
+-----------+----------+---------------+---------+-----------+-----------+
| 1         | V001     | Amoxicillin   | 500mg   | 3x daily  | 1         |
| 2         | V001     | Cough Syrup   | 10ml    | as needed | 1         |
+-----------+----------+---------------+---------+-----------+-----------+
```

### Multi-Level Nesting Spark Code

```scala
// Patients table
val patientsTable = df.select(
  col("_PatientID").as("patient_id"),
  col("_DateOfBirth").cast("date").as("date_of_birth"),
  col("PersonalInfo.FirstName").as("first_name"),
  col("PersonalInfo.LastName").as("last_name"),
  col("PersonalInfo.Gender").as("gender"),
  col("record_id")
)

// Visits table (explode first level)
val visitsTable = df.select(
  col("record_id").as("parent_id"),
  col("_PatientID").as("patient_id"),
  explode_outer(col("Visits.Visit")).as("visit")
).select(
  monotonically_increasing_id().as("child_id"),
  col("parent_id"),
  col("patient_id"),
  col("visit._VisitID").as("visit_id"),
  col("visit._VisitDate").cast("date").as("visit_date"),
  col("visit.Diagnosis._Code").as("diagnosis_code"),
  col("visit.Diagnosis._VALUE").as("diagnosis_text")
)

// Prescriptions table (explode second level)
val prescriptionsTable = df.select(
  col("_PatientID").as("patient_id"),
  explode_outer(col("Visits.Visit")).as("visit")
).select(
  col("visit._VisitID").as("visit_id"),
  explode_outer(col("visit.Prescriptions.Prescription")).as("rx")
).select(
  monotonically_increasing_id().as("child_id"),
  col("visit_id"),
  col("rx._DrugName").as("drug_name"),
  col("rx._Dosage").as("dosage"),
  col("rx._Frequency").as("frequency")
)
```

---

## Common Patterns Summary

### 1. XML Attributes → Columns
XML attributes (e.g., `OrderID="12345"`) are prefixed with underscore by spark-xml:
```scala
col("_OrderID").as("order_id")
```

### 2. Nested Elements → Dot Notation
```scala
col("Customer.Name")  // <Customer><Name>...</Name></Customer>
```

### 3. Element with Attributes + Value → Two Columns
```xml
<Amount Currency="USD">1250.00</Amount>
```
Maps to:
```scala
col("Amount._Currency").as("currency")
col("Amount._VALUE").as("amount")
```

### 4. Repeated Elements → Child Table with explode
```scala
explode_outer(col("OrderItems.Item"))  // Handles empty arrays
```

### 5. Empty/Missing Elements → NULL
```scala
coalesce(col("OptionalField"), lit(null))
when(col("Field").isNull, lit("DEFAULT")).otherwise(col("Field"))
```

### 6. Multi-Level Nesting → Multiple explode Operations
```scala
// First level
explode_outer(col("Visits.Visit")).as("visit")
// Second level
explode_outer(col("visit.Prescriptions.Prescription"))
```

### 7. Surrogate Keys
```scala
monotonically_increasing_id().as("id")
```

### 8. Foreign Keys
Parent's `record_id` becomes child's `parent_id`

---

## Type Casting Examples

```scala
// Dates
col("_OrderDate").cast("date")
col("_Timestamp").cast("timestamp")

// Decimals
col("Amount._VALUE").cast("decimal(18,2)")

// Integers
col("_Quantity").cast("int")

// Booleans
when(col("IsActive") === "true", lit(true)).otherwise(lit(false))
```

---

## Python/PySpark Equivalents

```python
from pyspark.sql.functions import col, explode_outer, monotonically_increasing_id, coalesce, lit

# Same patterns work in PySpark
orders_table = df.select(
    col("_OrderID").alias("order_id"),
    col("_OrderDate").alias("order_date"),
    col("Customer._CustomerID").alias("customer_id"),
    col("record_id")
)

# Explode nested arrays
items_table = df.select(
    col("record_id").alias("parent_id"),
    explode_outer(col("OrderItems.Item")).alias("item")
).select(
    monotonically_increasing_id().alias("child_id"),
    col("parent_id"),
    col("item.*")
)
```

---

## Validation Queries

After creating tables, validate with:

```sql
-- Check row counts
SELECT COUNT(*) FROM main_table;
SELECT COUNT(*) FROM child_table;

-- Verify foreign keys
SELECT COUNT(*) 
FROM child_table c
LEFT JOIN main_table m ON c.parent_id = m.record_id
WHERE m.record_id IS NULL;

-- Check for nulls in critical fields
SELECT COUNT(*) FROM main_table WHERE customer_id IS NULL;

-- Verify data integrity
SELECT parent_id, COUNT(*) as item_count
FROM child_table
GROUP BY parent_id
ORDER BY item_count DESC
LIMIT 10;
```
