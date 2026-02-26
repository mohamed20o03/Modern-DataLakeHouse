# Auto-Partitioning Algorithm: Concepts & Decision Logic

A comprehensive guide to understanding HOW the system automatically picks the best partition column.

---

## Table of Contents

1. [What is Partitioning?](#what-is-partitioning)
2. [The Problem: Why Auto-Detection Matters](#the-problem-why-auto-detection-matters)
3. [The Auto-Partitioning Algorithm](#the-auto-partitioning-algorithm)
4. [Decision Tree: How to Pick the Best Column](#decision-tree-how-to-pick-the-best-column)
5. [Column Type Detection](#column-type-detection)
6. [Heuristics for Selection](#heuristics-for-selection)
7. [Examples: Algorithm in Action](#examples-algorithm-in-action)
8. [Edge Cases & Fallbacks](#edge-cases--fallbacks)

---

## What is Partitioning?

### The Concept

**Partitioning** is organizing data into logical groups (folders) based on column values.

#### Without Partitioning

```
Table: sales
Storage location: s3://warehouse/sales/
  ├── data-file-1.parquet  (rows 1-100: Jan, Feb, Mar data all mixed)
  └── data-file-2.parquet  (rows 101-200: Apr, May, Jun data all mixed)

When you query: "Show me all February sales"
→ Spark must read BOTH files completely
→ Then filter in memory to find February rows
```

#### With Partitioning

```
Table: sales (PARTITIONED BY month)
Storage location: s3://warehouse/sales/
  ├── month=202601/
  │   └── data-file-1.parquet  (only January rows: 100 rows)
  ├── month=202602/
  │   └── data-file-2.parquet  (only February rows: 80 rows)
  └── month=202603/
      └── data-file-3.parquet  (only March rows: 90 rows)

When you query: "Show me all February sales"
→ Spark reads Iceberg's manifest (metadata file)
→ Manifest says: "February data is in folder month=202602"
→ Spark reads ONLY that folder
→ Much faster!
```

### Why Partitioning Helps

**Without partitioning:** Read 200 rows, filter to 80 matches
**With partitioning:** Read 80 rows directly, all match

**Speedup:** If you have 365 daily folders (1 year of data), and query for 1 month:

- Without: Read 365 folders = 365 × file_open_overhead
- With: Read 1 folder = 1 × file_open_overhead
- **Speedup: 365×**

---

## The Problem: Why Auto-Detection Matters

### Your Challenge

Your system ingests data from **many different sources** with **different schemas**:

```
Table 1: sales
  Columns: id, amount, customer_id, event_date, region

Table 2: users
  Columns: user_id, name, signup_date, country

Table 3: products
  Columns: product_id, name, category, price, supplier_id

Table 4: logs
  Columns: log_id, message, severity, timestamp

Table 5: clicks
  Columns: session_id, page_url, click_time, user_agent
```

### Manual Approach (❌ Doesn't Scale)

If you had to manually decide partitioning for each table:

```
For sales: "Partition by event_date"
For users: "Partition by signup_date"
For products: "Partition by category"
For logs: "Partition by timestamp"
For clicks: "Partition by click_time"
```

**Problems:**

- ❌ Requires human decision for EVERY table
- ❌ What if 100 new tables arrive tomorrow?
- ❌ Different teams have different schemas
- ❌ Errors are easy (human forgets, picks wrong column)

### Solution: Auto-Detection ✅

**One algorithm that works for all tables:**

```
New CSV arrives
    ↓
System analyzes schema
    ↓
Algorithm picks best partition column automatically
    ↓
Data partitioned optimally without human intervention
```

---

## The Auto-Partitioning Algorithm

### High-Level Pseudocode

```
ALGORITHM: Auto-Partition-Detector(table_schema)

INPUT: Schema of incoming CSV (column names + data types)
OUTPUT: Best partition strategy (column name, partition type, or NONE)

STEP 1: Analyze column types
    FOR EACH column in schema:
        Determine data type: DATE, TIMESTAMP, STRING, INT, DOUBLE, etc.
    END FOR

STEP 2: Look for TEMPORAL columns
    IF any column is DATE or TIMESTAMP type:
        PRIORITY CHECK (in order):
        1. Column named "date"?
        2. Column named "event_date"?
        3. Column named "created_date"?
        4. Column named "transaction_date"?
        5. Column named "timestamp"?
        6. Column named "created_at"?
        7. Column named "updated_at"?

        IF ANY matched:
            RETURN: Partition strategy = TEMPORAL
            RETURN: Partition column = matched column name
            RETURN: Partition expression = month(matched column)
        ELSE:
            Continue to scan for any DATE/TIMESTAMP column
            IF found:
                RETURN: Partition strategy = TEMPORAL
                RETURN: Partition column = that column name
                RETURN: Partition expression = month(matched column)

STEP 3: Look for CATEGORICAL columns
    (Only if no TEMPORAL column was found)

    FOR EACH STRING-type column in schema:
        Calculate: cardinality = (distinct values) / (total rows)

        IF cardinality < 0.1 AND distinct values < 1000:
            PRIORITY CHECK (in order):
            1. Column named "category"?
            2. Column named "region"?
            3. Column named "country"?
            4. Column named "product_type"?
            5. Column named "tenant_id"?

            IF ANY matched:
                RETURN: Partition strategy = CATEGORICAL
                RETURN: Partition column = matched column name
                RETURN: Partition expression = column name (no transformation)
    END FOR

    IF no priority column matched:
        Scan all STRING columns in order
        IF any has cardinality < 0.1 AND distinct < 1000:
            RETURN: Partition strategy = CATEGORICAL
            RETURN: Partition column = that column name
            RETURN: Partition expression = column name

STEP 4: No suitable partition found
    RETURN: Partition strategy = NONE
    RETURN: Partition column = NULL
    RETURN: Reason = "No temporal or categorical columns detected"

END ALGORITHM
```

---

## Decision Tree: How to Pick the Best Column

### Visual Decision Tree

```
┌─────────────────────────────────────┐
│ Analyze incoming CSV schema         │
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│ Question: Does table have DATE or   │
│           TIMESTAMP columns?        │
└────────────┬────────────────────────┘
             │
      ┌──────┴──────┐
      │             │
     YES           NO
      │             │
      ▼             ▼
   ┌─────────┐  ┌─────────────────────────────────┐
   │ TEMPORAL │  │ Question: Does table have low-  │
   │ FOUND ✓  │  │ cardinality STRING columns?     │
   └─────────┘  │ (distinct < 1000 AND            │
                │ cardinality ratio < 0.1)        │
                └────────────┬────────────────────┘
                             │
                      ┌──────┴──────┐
                      │             │
                     YES           NO
                      │             │
                      ▼             ▼
                 ┌──────────┐  ┌──────────┐
                 │CATEGORICAL│  │  NONE   │
                 │FOUND ✓    │  │ FOUND   │
                 └──────────┘  └──────────┘
```

### Which Takes Priority?

**TEMPORAL partitioning is ALWAYS preferred over CATEGORICAL**

Why? Because:

- **Temporal data** changes continuously (time always moves forward)
- **Queries almost always filter by date** (last 7 days, month-to-date, etc.)
- **Temporal partitions naturally age** (old partitions are archived/deleted)

Example:

```
Table has BOTH:
  - event_date (TEMPORAL column) ✓
  - category (CATEGORICAL column) ✓

Decision: Use TEMPORAL (event_date)
Reason: Temporal is more universally useful
```

---

## Column Type Detection

### Step 1: Identify Data Types

When a CSV is loaded, Spark infers data types:

```
CSV Header:
  id,name,event_date,category,price,created_at

Spark inference:
  id          → INTEGER (numbers like 1, 2, 3)
  name        → STRING (text like "Alice", "Bob")
  event_date  → DATE (dates like 2026-02-15)  ← TEMPORAL!
  category    → STRING (text like "Electronics")  ← Could be CATEGORICAL
  price       → DOUBLE (decimals like 99.99)
  created_at  → TIMESTAMP (dates with time)  ← TEMPORAL!
```

### Step 2: Filter by Type

The algorithm **only considers**:

- **DATE columns** → Check for temporal partition
- **TIMESTAMP columns** → Check for temporal partition
- **STRING columns** → Check for categorical partition

All other types (INT, DOUBLE, BOOLEAN) are **ignored**.

Why ignore others?

- **INT (user_id, product_id):** Usually unique or near-unique → poor partitioning
- **DOUBLE (price, amount):** Continuous values → creates too many partitions
- **BOOLEAN:** Only 2 values (true/false) → terrible partitioning

---

## Heuristics for Selection

### Heuristic 1: Priority Naming Convention

The algorithm has a **priority list of known good column names**.

#### For Temporal

```
Priority order (checked in this order):
  1. date              ← Most common name
  2. event_date        ← Data warehouse standard
  3. created_date      ← Creation timestamp
  4. transaction_date  ← Financial data standard
  5. timestamp         ← Generic name
  6. created_at        ← ORM standard (Rails, etc.)
  7. updated_at        ← ORM standard
  8. event_timestamp   ← Event streaming standard
```

Example:

```
Table schema: id, amount, event_date, created_date
             ↑                 ↑            ↑
           ignore            MATCH first!
                            (event_date is priority #2)

Decision: Partition by event_date
(Skip created_date because event_date came first)
```

#### For Categorical

```
Priority order:
  1. category         ← E-commerce standard
  2. region           ← Geographic standard
  3. country          ← Geographic standard
  4. product_type     ← Product data standard
  5. tenant_id        ← Multi-tenant standard
  6. user_type        ← User classification
  7. status           ← State machine
  8. source           ← Data source tracking
  9. environment      ← Deployment (prod/staging/dev)
```

Example:

```
Table schema: product_id, name, category, region, country
                                  ↑        ↑        ↑
                                MATCH!   (skip)   (skip)

Decision: Partition by category
(category is priority #1)
```

### Heuristic 2: Data Type Matching

Even if column name doesn't match priority list, the algorithm checks data type.

Example:

```
Table 1: user_created, name, email
                ↑
           Not in priority list, but...

Is "user_created" a DATE/TIMESTAMP? YES ✓
→ Algorithm: "This looks temporal, partition by month(user_created)"

Table 2: product_id, name, store_location
                                   ↑
           Not in priority list, but...

Is "store_location" a STRING? YES
Does it have low cardinality? (10 stores, 100,000 products)
Cardinality = 10/100,000 = 0.0001 = 0.01% < 10% ✓
Distinct values = 10 < 1000 ✓
→ Algorithm: "This is categorical, partition by store_location"
```

### Heuristic 3: Cardinality Analysis (Only for Categorical)

**Cardinality** = How many distinct values exist as percentage of total rows

```
Definition:
  Cardinality = (number of distinct values) / (total rows) × 100%

Examples:

Example 1: Product Categories
  Total rows: 100,000 products
  Distinct categories: 15
  Cardinality = 15 / 100,000 = 0.015% ✓ GOOD for partitioning

Example 2: User IDs
  Total rows: 100,000 rows
  Distinct users: 95,000
  Cardinality = 95,000 / 100,000 = 95% ✗ BAD for partitioning
  (Almost every row is unique, partition too fine-grained)

Example 3: Severity Levels
  Total rows: 1,000,000 logs
  Distinct severities: 5 (DEBUG, INFO, WARN, ERROR, FATAL)
  Cardinality = 5 / 1,000,000 = 0.0005% ✓ GOOD for partitioning

Example 4: Email Addresses
  Total rows: 100,000 users
  Distinct emails: 99,500
  Cardinality = 99,500 / 100,000 = 99.5% ✗ BAD for partitioning
  (Almost every user has unique email)
```

### Cardinality Thresholds

```
Algorithm rule for CATEGORICAL partitioning:

Cardinality MUST BE:
  ✓ Less than 10% (< 0.1)
  ✓ AND fewer than 1000 distinct values

Why these thresholds?

If cardinality > 10%:
  → Too many partitions
  → Each partition too small
  → File I/O overhead outweighs benefits
  → Example: 1M rows, 200K distinct values = too many partitions

If distinct values > 1000:
  → Creates 1000+ folders in storage
  → Hard to manage
  → Network overhead (opening 1000 files)
  → MinIO metadata overhead
  Example: 1M user IDs → 1M partitions = terrible!
```

---

## Examples: Algorithm in Action

### Example 1: E-commerce Sales Table

**Incoming CSV:**

```
product_id,customer_id,order_amount,order_date,region
123,456,99.99,2026-02-15,US
124,457,149.99,2026-02-15,EU
...
```

**Algorithm Execution:**

```
Step 1: Analyze columns
  product_id      → INT type (ignore)
  customer_id     → INT type (ignore)
  order_amount    → DOUBLE type (ignore)
  order_date      → DATE type ← TEMPORAL!
  region          → STRING type ← Could be CATEGORICAL

Step 2: Look for TEMPORAL columns
  Check priority list:
    1. "date"? No
    2. "event_date"? No
    3. "created_date"? No
    4. "transaction_date"? No
    5. "timestamp"? No
    6. "created_at"? No
    7. "updated_at"? No
    8. "event_timestamp"? No

  Scan all DATE/TIMESTAMP columns:
    Found: "order_date" is DATE type ✓

  DECISION: TEMPORAL partition on "order_date"
            Expression: month(order_date)

  ✓ STOP HERE (temporal found, don't check categorical)

Result: PARTITION BY month(order_date)
```

---

### Example 2: Multi-Tenant SaaS Platform

**Incoming CSV:**

```
tenant_id,user_id,action,timestamp,resource_name
acme-corp,1001,create,2026-02-15T10:30:00Z,project-x
acme-corp,1002,read,2026-02-15T10:31:00Z,project-x
globex,2001,delete,2026-02-15T10:32:00Z,document-y
...
```

**Algorithm Execution:**

```
Step 1: Analyze columns
  tenant_id       → STRING type (possibly categorical)
  user_id         → INT type (ignore)
  action          → STRING type (possibly categorical)
  timestamp       → TIMESTAMP type ← TEMPORAL!
  resource_name   → STRING type (possibly categorical)

Step 2: Look for TEMPORAL columns
  Check priority list:
    1-5. None match
    7. "timestamp"? YES ✓

  DECISION: TEMPORAL partition on "timestamp"
            Expression: month(timestamp)

  ✓ STOP (temporal found)

Result: PARTITION BY month(timestamp)
        (tenant_id is ignored even though it's there)
```

---

### Example 3: Product Catalog (No Temporal Data)

**Incoming CSV:**

```
sku,name,category,supplier,color,price
SKU-001,Laptop-A,Electronics,Supplier-X,Silver,1200.00
SKU-002,Mouse-B,Electronics,Supplier-Y,Black,25.00
SKU-003,Shirt-C,Clothing,Supplier-Z,Blue,45.00
...
Total rows: 100,000
```

**Algorithm Execution:**

```
Step 1: Analyze columns
  sku         → STRING type (ignore - too many distinct values)
  name        → STRING type (ignore)
  category    → STRING type ← Could be CATEGORICAL
  supplier    → STRING type ← Could be CATEGORICAL
  color       → STRING type ← Could be CATEGORICAL
  price       → DOUBLE type (ignore)

Step 2: Look for TEMPORAL columns
  DATE columns? NO
  TIMESTAMP columns? NO

  → No temporal partition found, continue

Step 3: Look for CATEGORICAL columns
  Check priority list for "category":
    1. "category"? YES! ✓

  BUT first check cardinality:
    Distinct values: 8 (Electronics, Clothing, Sports, etc.)
    Total rows: 100,000
    Cardinality = 8 / 100,000 = 0.008% ✓ Less than 10%
    Distinct values = 8 < 1000 ✓

  DECISION: CATEGORICAL partition on "category"
            Expression: category (no transformation)

Result: PARTITION BY category
```

---

### Example 4: User Behavior (Multiple Candidates)

**Incoming CSV:**

```
session_id,user_id,event_type,event_timestamp,country,utm_source
sess-1001,1,click,2026-02-15T10:00:00Z,US,google
sess-1002,2,view,2026-02-15T10:01:00Z,CA,direct
sess-1003,3,purchase,2026-02-15T10:02:00Z,UK,facebook
...
Total rows: 50,000,000
```

**Algorithm Execution:**

```
Step 1: Analyze columns
  session_id          → STRING (ignore)
  user_id             → INT (ignore)
  event_type          → STRING (possibly categorical: 12 types)
  event_timestamp     → TIMESTAMP ← TEMPORAL!
  country             → STRING (possibly categorical: 195 countries)
  utm_source          → STRING (possibly categorical: 50 sources)

Step 2: Look for TEMPORAL columns
  "timestamp"? No
  "created_at"? No
  "event_timestamp"? YES! ✓

  DECISION: TEMPORAL partition on "event_timestamp"
            Expression: month(event_timestamp)

  ✓ STOP (temporal found, ignore categorical)

Result: PARTITION BY month(event_timestamp)
        (country and utm_source are ignored because temporal was found first)
```

---

### Example 5: Flat CSV with No Good Partition

**Incoming CSV:**

```
transaction_id,payer_id,payee_id,amount,reference_number
TX-001,A123,B456,50.00,ABC123
TX-002,C789,D101,75.50,DEF456
TX-003,E112,F131,100.25,GHI789
...
```

**Algorithm Execution:**

```
Step 1: Analyze columns
  transaction_id   → STRING (mostly unique)
  payer_id         → STRING (high cardinality)
  payee_id         → STRING (high cardinality)
  amount           → DOUBLE (continuous, ignore)
  reference        → STRING (mostly unique)

Step 2: Look for TEMPORAL columns
  Any DATE/TIMESTAMP? NO

  → No temporal partition found, continue

Step 3: Look for CATEGORICAL columns
  transaction_id:
    Distinct: ~50,000 out of 50,000 rows
    Cardinality: 100% ✗ Too high

  payer_id:
    Distinct: ~48,000 out of 50,000 rows
    Cardinality: 96% ✗ Too high

  payee_id:
    Distinct: ~48,500 out of 50,000 rows
    Cardinality: 97% ✗ Too high

  reference_number:
    Distinct: ~50,000 out of 50,000 rows
    Cardinality: 100% ✗ Too high

  No STRING column with cardinality < 10%

Step 4: No suitable partition found
  DECISION: NO PARTITIONING
            Reason: "No temporal or categorical columns detected"

Result: STORE WITHOUT PARTITIONING
        (Data still benefits from Iceberg ACID, schema evolution)
```

---

## Edge Cases & Fallbacks

### Edge Case 1: Column Exists But Has Wrong Type

```
CSV has column named "date" but it's STRING type (text, not actual dates)
  Column name: "date" ← matches priority #1
  Data type: STRING ← not DATE or TIMESTAMP

Decision: SKIP this column (name matched but type wrong)
          Continue to next temporal column or categorical
```

### Edge Case 2: Multiple Columns of Same Type

```
CSV has:
  - created_date (DATE type)
  - updated_date (DATE type)
  - deleted_date (DATE type)

Priority check:
  1. "date"? No
  2. "event_date"? No
  3. "created_date"? YES ✓

Decision: Use "created_date" (matched priority #3)
          IGNORE updated_date and deleted_date
```

### Edge Case 3: Very Small Table

```
CSV with 10 rows:
  id, name, created_date

Analysis:
  created_date is TEMPORAL ✓

BUT: With only 10 rows, partitioning doesn't help
  Iceberg would create 10 separate partitions
  Overhead outweighs benefits

Current implementation: Still partitions (simple approach)
Future optimization: Skip partitioning if row count < threshold
```

### Edge Case 4: Categorical Column with NULL Values

```
CSV has column "region" with some NULL values:
  region values: US, EU, APAC, NULL, NULL, US
  Distinct values: 4 (including NULL)

Cardinality calculation: 4 / total rows
Treats NULL as a distinct value

Decision: Still valid if cardinality < 10%
Note: Iceberg handles NULL partitions automatically
```

### Edge Case 5: Decimal/String Dates

```
CSV has column named "event_date" but stored as STRING "2026-02-15"
(Not typed as DATE in Parquet)

Current implementation:
  Spark schema inference might treat it as STRING type
  → Algorithm skips it (STRING type, not DATE type)
  → Looks for CATEGORICAL instead

Best practice: Ensure date columns are properly typed when reading CSV
  spark.read()
    .option("inferSchema", "true")
    .csv(file)
```

---

## Summary: The Algorithm Logic

### Flowchart

```
┌──────────────────────────────────┐
│ 1. Read CSV & Infer Schema       │
│    (Get all column names & types)│
└────────────┬─────────────────────┘
             │
┌────────────▼─────────────────────┐
│ 2. Filter by Type                │
│    Keep only: DATE, TIMESTAMP,   │
│    STRING columns                │
└────────────┬─────────────────────┘
             │
┌────────────▼─────────────────────┐
│ 3. Priority Check (TEMPORAL)     │
│    ├─ "date"?                    │
│    ├─ "event_date"?              │
│    ├─ "created_date"?            │
│    ├─ "timestamp"?               │
│    └─ ... (check all DATE cols)  │
└────────────┬─────────────────────┘
             │
      ┌──────┴──────┐
      │             │
    FOUND         NOT FOUND
      │             │
      ▼             ▼
   RETURN      ┌────────────────────┐
   TEMPORAL    │ 4. Priority Check  │
              │ (CATEGORICAL)       │
              │ ├─ "category"?      │
              │ ├─ "region"?        │
              │ ├─ "country"?       │
              │ └─ ... (check all   │
              │    STRING cols)     │
              └────────────┬────────┘
                           │
                    ┌──────┴──────┐
                    │             │
                  FOUND        NOT FOUND
                    │             │
                    ▼             ▼
              Check cardinality   RETURN NONE
              (< 10% & < 1000)
                    │
              ┌─────┴────┐
              │          │
            VALID      INVALID
              │          │
              ▼          ▼
           RETURN      Continue to
           CATEGORICAL next column
```

### Decision Summary

```
1️⃣ TEMPORAL PARTITION (Priority: HIGHEST)
   ✅ Happens if: Any DATE or TIMESTAMP column exists
   ✅ Partition expression: month(column_name)
   ✅ Performance: 50-100× speedup on temporal queries

2️⃣ CATEGORICAL PARTITION (Priority: MEDIUM)
   ✅ Happens if: No temporal columns, but low-cardinality STRING exists
   ✅ Conditions: < 10% cardinality AND < 1000 distinct values
   ✅ Partition expression: column_name (no transformation)
   ✅ Performance: 5-40× speedup on categorical queries

3️⃣ NO PARTITION (Priority: FALLBACK)
   ✅ Happens if: Neither temporal nor categorical columns found
   ✅ Data still stored in Iceberg (benefits from ACID, schema evolution)
   ✅ Performance: No partitioning benefit, but still optimized
```

---

## Why This Algorithm?

### Universality

Works for **ANY** CSV schema without configuration:

```
Sales → Partitions by date
Users → Partitions by signup date
Products → Partitions by category
Logs → Partitions by timestamp
Analytics → Partitions by month
```

### Robustness

Handles edge cases gracefully:

- Missing temporal columns → Falls back to categorical
- Missing categorical columns → No partitioning (still works)
- Multiple temporal columns → Uses priority order
- Unusual column names → Scans all columns by type

### Performance Guaranteed

Always picks the **most universally applicable** partition:

- Temporal > Categorical (temporal is more common in queries)
- Priority names > Generic names (industry standards more likely to help)
- Low cardinality > High cardinality (smaller partition count = better)

### Zero Configuration

No need to tell the system:

- What columns exist
- What data types they are
- Which is best for partitioning
- How many partitions to create

**All automatic! 🎯**
