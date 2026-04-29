## Context

The staging layer is where raw data gets cleaned and standardised — **once**, at the lowest layer. Every downstream model builds on staging, so any fix you make here automatically propagates everywhere.

You will build all three staging models for Jaffle Shop:

- `stg_customers` — review and tidy the CTE pattern (column renaming)
- `stg_orders` — standardise order information (column renaming)
- `stg_payments` — apply the CTE pattern with a **numeric transformation** (cents to dollars)

The payments model introduces something new. The raw data stores payment amounts as integers in **cents** — 1000 means $10.00. This is standard practice in finance and e-commerce databases: integers avoid the floating-point rounding errors that accumulate when storing `10.0000000000001` millions of times. Your staging model converts those cents to dollars once at the source, so every downstream model inherits clean dollar values automatically.

## Objective

Build all three staging models and document them in `schema.yml`:

- One staging model per source table
- CTE pattern throughout: `source` CTE + `renamed` CTE + final `SELECT`
- Column renaming for consistency
- Numeric type casting for the payments model

## What is Staging?

Raw data arrives exactly as the source system produced it — inconsistent column names, amounts stored as integers in cents, `id` columns that tell you nothing about what they identify. You cannot build reliable analytics on that directly.

The staging layer is where you clean that up, **once**, in a single place.

**A staging model does exactly one job per source table:**

- `id` → `customer_id`
- `user_id` (in orders) → `customer_id` (consistent FK naming)
- `amount = 1000` (cents) → `amount = 10.00` (dollars)
- One row in, one row out — no joins, no aggregation

**Staging models should:**

- Rename columns for consistency (`id` → `customer_id`)
- Cast types properly (`DECIMAL`, `INTEGER`, `DATE`)
- Apply minimal cleaning (trim whitespace, lowercase where needed)
- Be 1:1 with source tables — no joining, no aggregating
- Materialise as views (lightweight and always up to date)

**Staging models should NOT:**

- Join tables — that is the intermediate layer's job
- Aggregate data — that is the mart layer's job
- Apply business logic — staging is purely structural cleanup
- Filter rows extensively — keep it as close to raw as possible

## Prerequisites

- Completed the previous challenge with a `jaffle_shop_dbt/` directory containing:
  - `dbt_project.yml` configured for jaffle_shop_dbt
  - `models/schema.yml` with source definitions (3 tables)
  - `dev.duckdb` with raw data loaded

## 0. Copy Your Work from Previous Challenge

**Important:** Each challenge has its own directory. Copy your dbt project files from the previous challenge.

```bash
cp -rP ../../../{{ local_path_to("03-Data-Transformation/09-Data-Layers-And-Intro-DBT/03-Configure-And-Document-Sources") }}/jaffle_shop_dbt .

# Verify the symlink copied correctly
ls -l jaffle_shop_dbt/dev.duckdb
# Should show: dev.duckdb -> ../../../dbt-shared/dev.duckdb
```

Then commit so you have a clean starting point for this challenge:

```bash
git add jaffle_shop_dbt
git commit -m "Copied setup from previous challenge"
git push origin master
```

<details>
<summary markdown="span">**💡 Why does the database carry over automatically?**</summary>

The `dev.duckdb` file in your project is a symlink pointing to `../../../dbt-shared/dev.duckdb`. When you `cp -rP` the project, the symlink is copied as-is. Because all challenge directories sit at the same depth under their unit folder, the relative path still resolves to the same shared database — no recreation needed. DBeaver stays connected without any path changes.

**Tip:** If you made mistakes in the previous challenge, this is your chance to fix them before continuing!

</details>

## 1. Review and Tidy stg_customers

### 1.1. Open Your Existing Model

Open the staging model you carried over from the previous challenge:

```bash
code jaffle_shop_dbt/models/staging/stg_customers.sql
```

Confirm it already matches all of these requirements before moving on:

1. **Uses CTE pattern** with a `source` CTE and a `renamed` CTE
2. **References the source** using `{{ source('jaffle_shop', 'customers') }}`
3. **Column transformations:**
   - Renames `id` to `customer_id`
   - Keeps `first_name` and `last_name` as-is

If you completed the previous challenge correctly, all three should already be in place. If anything is missing, fix it now before continuing.

<details>
<summary markdown="span">**💡 Hint: CTE structure template**</summary>

```sql
WITH source AS (
    SELECT * FROM {{ source('???', '???') }}
),

renamed AS (
    SELECT
        ??? AS customer_id,  -- Which column should be renamed?
        ???,                 -- Keep first_name
        ???                  -- Keep last_name
    FROM source
)

SELECT * FROM renamed
```

Fill in the `???` based on the requirements above.

</details>

<details>
<summary markdown="span">**🔍 Need more help? Check the source columns**</summary>

**🗄️ In DBeaver**, check what columns exist in the raw table:

```sql
SELECT * FROM raw.raw_customers LIMIT 5;
```

You will see: `id`, `first_name`, `last_name`

Your staging model should transform these to: `customer_id`, `first_name`, `last_name`

</details>

### 1.2. Test Your Model

**Compile** (check SQL without running):

```bash
cd jaffle_shop_dbt
dbt compile --select stg_customers
```

**Run** (execute and create view):

**🗄️ Note:** If DBeaver is connected, disconnect it first (right-click connection → Disconnect) to avoid database locks.

```bash
dbt run --select stg_customers
```

<details>
<summary markdown="span">**Expected output**</summary>

```plaintext
Running with dbt=1.7.x
Completed successfully

Done. PASS=1 WARN=0 ERROR=0 SKIP=0 TOTAL=1
```

</details>

**Verify your column transformations worked:**

**🗄️ In DBeaver**, connect to `jaffle_shop_shared` and run:

```sql
SELECT * FROM main_staging.stg_customers LIMIT 10;
```

**Verify you see these columns:**

- `customer_id` (NOT `id`)
- `first_name`
- `last_name`

### 🧪 Checkpoint 1: Push First Staging Model

**📍 In your terminal**, navigate to the challenge directory:

```bash
cd ..
```

Run the checkpoint 1 tests:

```bash
pytest tests/test_customers.py -v
```

**If tests pass**, commit and push:

```bash
git add jaffle_shop_dbt/models/staging/stg_customers.sql
git commit -m "Review and tidy stg_customers staging model"
git push origin master
```

`stg_customers` is clean and consistent. The same CTE pattern — `source` → `renamed` → `SELECT *` — is what you'll apply to orders and payments too.

---

## 2. Create stg_orders

### 2.1. Write the Model

**Your Challenge:** Transform the raw orders data following the same CTE pattern.

**Requirements:**

1. **File location:** `models/staging/stg_orders.sql`
2. **Use the CTE pattern** (`WITH source`, `renamed`)
3. **Reference:** `{{ source('jaffle_shop', 'orders') }}`
4. **Column transformations:**
   - Rename `id` to `order_id`
   - Rename `user_id` to `customer_id` (standardise FK naming)
   - Keep `order_date` as-is
   - Keep `status` as-is

```bash
code jaffle_shop_dbt/models/staging/stg_orders.sql
```

<details>
<summary markdown="span">**💡 Hint: Column mapping**</summary>

**Source columns → Staging columns:**

- `id` → `order_id`
- `user_id` → `customer_id`
- `order_date` → `order_date` (no change)
- `status` → `status` (no change)

Why rename `user_id` to `customer_id`? It creates consistent naming across all models — every foreign key to customers is called `customer_id`.

</details>

<details>
<summary markdown="span">**🔍 Verify source columns**</summary>

**🗄️ In DBeaver:**

```sql
SELECT * FROM raw.raw_orders LIMIT 5;
```

</details>

### 2.2. Run and Verify

**🗄️ Note:** If DBeaver is connected, disconnect it first (right-click connection → Disconnect) to avoid database locks.

```bash
cd jaffle_shop_dbt
dbt run --select stg_orders
```

<details>
<summary markdown="span">**Expected output**</summary>

```plaintext
Done. PASS=1 WARN=0 ERROR=0 SKIP=0 TOTAL=1
```

</details>

**🗄️ In DBeaver**, verify column names:

```sql
SELECT * FROM main_staging.stg_orders LIMIT 5;
-- Should show: order_id, customer_id, order_date, status
```

**❌ Common mistakes:**

- Did you rename `id` → `order_id`?
- Did you rename `user_id` → `customer_id`?

### 🧪 Checkpoint 2: Push stg_orders

**📍 In your terminal**, navigate to the challenge directory:

```bash
cd ..
```

Run the checkpoint 2 tests:

```bash
pytest tests/test_orders.py -v
```

**If tests pass**, commit and push:

```bash
git add jaffle_shop_dbt/models/staging/stg_orders.sql
git commit -m "Add stg_orders staging model"
git push origin master
```

Two staging models down, one to go. `stg_payments` introduces something new: the raw data stores amounts as integers in cents, and staging is exactly the right place to fix that — once, at the lowest layer.

---

## 3. Create stg_payments — Type Casting

### 3.1. Understand the Raw Data

Before writing any SQL, look at the raw data so you know exactly what you are transforming.

**🗄️ In DBeaver**, run:

```sql
SELECT
    id,
    order_id,
    payment_method,
    amount                  AS amount_cents,
    amount / 100.0          AS amount_dollars
FROM raw.raw_payments
LIMIT 10;
```

You should see amounts like `1000`, `2000` (cents) and their dollar equivalents `10.00`, `20.00`.

### 3.2. Write stg_payments.sql

**📍 In your terminal** (inside `jaffle_shop_dbt/`):

```bash
code models/staging/stg_payments.sql
```

**Requirements:**

1. File location: `models/staging/stg_payments.sql`
2. Use the CTE pattern (`WITH source AS (...)`, `renamed AS (...)`)
3. Reference: `{{ source('jaffle_shop', 'payments') }}`
4. Column transformations:
   - `id` → `payment_id` (rename)
   - `order_id` → `order_id` (no change)
   - `payment_method` → `payment_method` (no change)
   - `amount` → `amount` (divide by `100.0` to convert cents to dollars)

<details>
<summary markdown="span">**💡 Hint: Basic structure**</summary>

```sql
WITH source AS (
    SELECT * FROM {{ source('jaffle_shop', 'payments') }}
),

renamed AS (
    SELECT
        id              AS payment_id,
        order_id,
        payment_method,
        amount / 100.0  AS amount
    FROM source
)

SELECT * FROM renamed
```

</details>

<details>
<summary markdown="span">**💡 Hint: Why 100.0 and not 100?**</summary>

In SQL, dividing two integers returns an integer:

```sql
1000 / 100   -- Returns 10 (integer, no decimal)
1000 / 100.0 -- Returns 10.0 (decimal, correct)
```

If you divide by `100`, amounts like $1.50 (150 cents) would become `1` — truncation loses the 50 cents entirely. Always divide by `100.0`.

</details>

<details>
<summary markdown="span">**💡 Hint: Explicit type casting**</summary>

For precise decimal control, cast the result explicitly:

```sql
CAST(amount / 100.0 AS DECIMAL(10,2)) AS amount
```

`DECIMAL(10,2)` means: up to 10 digits total, 2 after the decimal point. Good practice for money columns but not required for tests to pass.

</details>

<details>
<summary markdown="span">**🔧 Troubleshooting: Type errors**</summary>

If DuckDB raises a type error on the division, check the raw column type:

```sql
SELECT typeof(amount) FROM raw.raw_payments LIMIT 1;
```

If it returns `INTEGER`, the division `amount / 100.0` should work as-is. If you get unexpected NULLs, use `TRY_CAST`:

```sql
TRY_CAST(amount / 100.0 AS DECIMAL(10,2)) AS amount
```

</details>

### 3.3. Run and Verify

**🗄️ Note:** If DBeaver is connected, disconnect it first (right-click connection → Disconnect) to avoid database locks.

```bash
cd jaffle_shop_dbt
dbt run --select stg_payments
```

<details>
<summary markdown="span">**Expected output**</summary>

```plaintext
Done. PASS=1 WARN=0 ERROR=0 SKIP=0 TOTAL=1
```

</details>

**🗄️ In DBeaver**, verify:

```sql
SELECT * FROM main_staging.stg_payments LIMIT 10;
```

Check that:

- Column names match: `payment_id`, `order_id`, `payment_method`, `amount`
- `amount` values are dollars (e.g. `10.00`), not cents (e.g. `1000`)

**Run the full staging layer together to confirm nothing broke:**

**🗄️ Note:** If DBeaver is connected, disconnect it first (right-click connection → Disconnect) to avoid database locks.

```bash
dbt run --select staging
```

**💡 `--select staging` runs every model inside the `staging/` folder at once** — no need to list model names individually.

<details>
<summary markdown="span">**Expected output**</summary>

```plaintext
Done. PASS=3 WARN=0 ERROR=0 SKIP=0 TOTAL=3
```

</details>

### 🧪 Checkpoint 3: Push stg_payments

**📍 In your terminal**, navigate to the challenge directory:

```bash
cd ..
```

Run the checkpoint 3 tests:

```bash
pytest tests/test_payments.py -v
```

**If tests pass**, commit and push:

```bash
git add jaffle_shop_dbt/models/staging/stg_payments.sql
git commit -m "Add stg_payments with cents-to-dollars conversion"
git push origin master
```

All three staging models are built. The last step is to document them in `schema.yml` so dbt can track lineage and you have a reference for every column's meaning.

---

## 4. Document All Three Models in schema.yml

Open `models/schema.yml` (or `models/staging/schema.yml` if that is where yours lives) and add a `models:` section documenting all three staging models — one entry per model, with a description for the model itself and for each column.

For each model, document:

- What the model represents (one sentence)
- Every output column and what it contains — pay special attention to any renamed or transformed columns (e.g. `amount` in `stg_payments`)

<details>
<summary markdown="span">**💡 Hint: schema.yml structure**</summary>

The `models:` section sits at the same indentation level as `sources:` in your existing file. Each model entry looks like:

```yaml
models:
  - name: stg_customers
    description: "..."
    columns:
      - name: customer_id
        description: "..."
      - name: first_name
        description: "..."
```

Repeat this block for `stg_orders` and `stg_payments`. Check your three SQL files to confirm the exact column names you used — don't guess.

Not sure what columns your models produced? **🗄️ In DBeaver**, connect to `jaffle_shop_shared` and run:

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'main_staging'
  AND table_name = 'stg_customers';
```

Repeat for `stg_orders` and `stg_payments` by changing the `table_name` value. This returns just column names and types — exactly what you need for `schema.yml`.

<details>
<summary markdown="span">**💡 Shorthand: DESCRIBE also works in recent DuckDB versions**</summary>

In DuckDB 0.8 and later, you can also run:

```sql
DESCRIBE main_staging.stg_customers;
```

We use the `information_schema` query above because it works across all DuckDB versions and is standard SQL — so it will feel familiar if you work with other databases too.

</details>

</details>

Save the file, then verify the YAML is valid:

```bash
cd jaffle_shop_dbt
dbt parse
```

### 🧪 Checkpoint 4: Push Documentation

**📍 In your terminal**, navigate to the challenge directory:

```bash
cd ..
```

Run the checkpoint 4 tests:

```bash
pytest tests/test_documentation.py -v
```

**Optional: Run all tests together:**

```bash
make
```

**Expected results (all tests):**

- All tests passed

**If all tests pass**, commit and push:

```bash
git add jaffle_shop_dbt/models/
git commit -m "Add schema.yml documentation for all staging models"
git push origin master
```

---

The staging layer is complete and documented. In the next challenge you'll build on top of it — joining `stg_orders` and `stg_payments` together in the intermediate layer.

## 🎉 Challenge Complete

The staging layer is complete. Three models, one pattern, one source of truth for raw data cleaning.

**Key takeaways:**

- `{{ source() }}` only in staging — this is the only layer that touches raw tables directly
- Rename aggressively here — `id` → `customer_id` so every downstream model inherits clean, unambiguous names
- One staging model per source table, and transform once at the lowest layer — cast types, fix integer division (`100.0` not `100`), convert units here
