# Understanding the Data Modeling

This tutorial explains the data modeling approach used in the ecommerce example. We’ll cover the business context,
schema design and relationships, incremental loading strategy, data quality validations, transformations rationale,
realistic queries the model supports, the assets to create (and their dependencies), and how Bruin compares to
alternatives.

## Business context and goals

Domains covered:

- Customers
- Products and Product Variants
- Orders and Order Items

Key analytics goals:

- Revenue and orders over time, with daily granularity
- Product and variant performance by category
- Variant profitability (selling price vs. manufacturing cost)
- Customer segmentation by age and geography
- KPI set: AOV, units per order, top SKUs, repeat purchase rate (extensible)

## Schema and relationships

We model a normalized core with conformed dimensions and fact tables. Primary/foreign keys and cardinalities:

- customers(id) -> orders(customer_id) [1:N]
- orders(id) -> order_items(order_id) [1:N]
- products(id) -> product_variants(product_id) [1:N]
- product_variants(id) -> order_items(variant_id) [1:N]

Source DDL (simplified excerpt) reflects these relationships and includes basic constraints:

<details>

```sql
-- customers
CREATE TABLE customers
(
    id         BIGSERIAL PRIMARY KEY,
    full_name  TEXT        NOT NULL,
    email      TEXT        NOT NULL UNIQUE CHECK (position('@' in email) > 1 AND position('.' in email) > 1),
    country    TEXT        NOT NULL,
    city       TEXT        NOT NULL,
    age        INTEGER     NOT NULL CHECK (age >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- products
CREATE TABLE products
(
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT        NOT NULL,
    category   TEXT        NOT NULL,
    sku        TEXT        NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- product_variants
CREATE TABLE product_variants
(
    id                  BIGSERIAL PRIMARY KEY,
    product_id          BIGINT         NOT NULL REFERENCES products (id) ON DELETE CASCADE,
    variant_sku         TEXT           NOT NULL UNIQUE,
    color               TEXT,
    size                TEXT,
    manufacturing_price NUMERIC(10, 2) NOT NULL CHECK (manufacturing_price >= 0),
    selling_price       NUMERIC(10, 2) NOT NULL CHECK (selling_price >= 0),
    stock_quantity      INTEGER        NOT NULL DEFAULT 0 CHECK (stock_quantity >= 0),
    is_active           BOOLEAN        NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- orders
CREATE TABLE orders
(
    id           BIGSERIAL PRIMARY KEY,
    customer_id  BIGINT         NOT NULL REFERENCES customers (id),
    order_date   TIMESTAMPTZ    NOT NULL DEFAULT now(),
    status       TEXT           NOT NULL CHECK (status IN ('pending', 'paid', 'cancelled', 'shipped')),
    total_amount NUMERIC(12, 2) NOT NULL DEFAULT 0 CHECK (total_amount >= 0),
    created_at   TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- order_items
CREATE TABLE order_items
(
    id          BIGSERIAL PRIMARY KEY,
    order_id    BIGINT         NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    variant_id  BIGINT         NOT NULL REFERENCES product_variants (id),
    quantity    INTEGER        NOT NULL CHECK (quantity > 0),
    unit_price  NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0),
    total_price NUMERIC(12, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    created_at  TIMESTAMPTZ    NOT NULL DEFAULT now()
);
```

</details>

## Layers and assets

We use a three-layer approach in this tutorial, which is: `raw → staging → mart`.

- **Raw (landing):** 
  - exact source tables from **Postgres Source**; 
  - minimal coercions; 
  - preserves created_at and updated_at.
- **Staging (standardize + validate):**
  - conform types;
  - apply baseline checks;
  - light cleansing;
  - keep keys and timestamps.
- **Mart (analytics-ready):** 
  - dimensional tables/views to support analytics and BI queries;
  - derived metrics;
  - derived tables/views to support analytics and BI queries.

Key assets in this repository:

- Staging:
    - ecommerce/assets/staging/stg.customers.asset.sql
    - ecommerce/assets/staging/stg.products.sql
    - ecommerce/assets/staging/stg.product_variants.sql
    - ecommerce/assets/staging/stg.orders.sql
    - ecommerce/assets/staging/stg.order_items.sql
- Mart:
    - ecommerce/assets/mart/mart.sales_daily.sql
    - ecommerce/assets/mart/mart.product_performance.sql
    - ecommerce/assets/mart/mart.variant_profitability.sql
    - ecommerce/assets/mart/mart.customers_by_age.asset.sql
    - ecommerce/assets/mart/mart.customers_by_county.asset.sql

## Incremental loading strategy

Timestamps used:

- Dimensions: updated_at (fallback to created_at when updated_at missing) in customers, products, product_variants.
- Orders: updated_at for upserts; order_date for time-series.
- Order Items: created_at typically append-only; can reprocess recent N days for late-arriving corrections.

Approach:

- Maintain per-asset watermark on updated_at/created_at.
- When building staging, select rows where timestamp > last_watermark.
- Idempotency via merge/upsert on primary key with last-updated-wins.
- For late-arriving changes, reprocess recent time window (e.g., 3–7 days) or rely on updated_at-driven merges.

## Data quality validations

We embed checks directly in @bruin metadata within staging assets. Examples:

- Orders (ecommerce/assets/staging/stg.orders.sql) ensures:
    - order_id not null and unique
    - status in accepted_values: ['pending','paid','cancelled','shipped']
    - total_amount ≥ 0 (clamped to 0 if negative)

- Order Items (ecommerce/assets/staging/stg.order_items.sql) ensures:
    - quantity ≥ 1
    - unit_price ≥ 0, total_price ≥ 0
    - created_at not null

- Products & Variants ensure uniqueness of sku/variant_sku and non-negative prices.
- Customers: email format validated (also present in DDL) and age ≥ 0.

This combination provides early detection of anomalies and prevents bad records from cascading into marts.

## Transformations rationale

- Normalize entities (customers, products, variants) to reduce duplication and enable reuse across facts.
- Standardize and validate in staging to create a trustworthy base for analytics.
- Clamp obviously-invalid negative amounts to 0 in staging to keep marts stable while surfacing issues through
  checks/logs.
- Compute derived metrics (e.g., total_price in order_items, profit in variant analysis) once and reuse downstream.
- Preserve timestamps for incremental processing and potential point-in-time expansions later.

## Realistic ecommerce queries

This model supports a wide range of analyses:

1) Daily sales time series

- Based on mart.sales_daily: date, orders_count, units_sold, revenue.

2) Product/variant performance by category

- Using mart.product_performance to see sales and units by product category and variant.

3) Variant profitability

- Using mart.variant_profitability to compute profit = selling_price − manufacturing_price times units sold.

4) Customer segmentation by age and county

- Using mart.customers_by_age and mart.customers_by_county for geographic and demographic cuts.

5) Average Order Value (AOV) and units per order

- From mart.sales_daily or by aggregating stg.orders and stg.order_items.

6) Top SKUs and repeat purchase rate (extensible)

- Rank variants by revenue/units; add cohort logic later to measure repeat rate.

Example query snippets:

```sql
-- Daily revenue and orders
SELECT order_date::date AS d, COUNT(DISTINCT order_id) AS orders, SUM(total_amount) AS revenue
FROM stg.orders
GROUP BY 1
ORDER BY 1;
```

```sql
-- Product performance by category
SELECT p.category, SUM(oi.quantity) AS units, SUM(oi.total_price) AS revenue
FROM stg.order_items oi
         JOIN stg.product_variants v ON v.variant_id = oi.variant_id
         JOIN stg.products p ON p.product_id = v.product_id
GROUP BY 1
ORDER BY revenue DESC;
```

```sql
-- Variant profitability
SELECT v.variant_id,
       SUM(oi.quantity)                                                            AS units,
       SUM(oi.total_price)                                                         AS revenue,
       SUM(oi.quantity) * ANY_VALUE(v.manufacturing_price)                         AS cost,
       SUM(oi.total_price) - (SUM(oi.quantity) * ANY_VALUE(v.manufacturing_price)) AS profit
FROM stg.order_items oi
         JOIN stg.product_variants v ON v.variant_id = oi.variant_id
GROUP BY 1
ORDER BY profit DESC;
```

## Assets and dependencies

We structure dependencies to build from source truth to analytics-ready marts:

- Raw → Staging → Mart

Examples from the repo:

- stg.orders depends on raw.orders, raw.customers
- stg.order_items depends on raw.order_items
- mart.sales_daily depends on stg.orders and stg.order_items
- mart.product_performance depends on stg.products, stg.product_variants, stg.order_items
- mart.variant_profitability depends on stg.product_variants, stg.order_items
- mart.customers_by_age depends on stg.customers
- mart.customers_by_county depends on stg.customers

Dependency sketch:

```
raw.*
  ├─ stg.customers
  ├─ stg.products
  ├─ stg.product_variants
  ├─ stg.orders
  └─ stg.order_items
        ├─ mart.sales_daily
        ├─ mart.product_performance
        ├─ mart.variant_profitability
        ├─ mart.customers_by_age
        └─ mart.customers_by_county
```

## Bruin vs alternatives (value proposition)

Bruin’s strengths in this workflow:

- Single-file asset declaration: SQL + metadata via @bruin blocks (dependencies, checks, materialization) in one place.
- Built-in data quality: not-null, unique, range, accepted_values, and custom validations inline with the asset.
- Explicit lineage: depends lists make orchestration and build order clear and auditable.
- Local-first: works great with DuckDB for rapid iteration; simple logs and runs history.

Comparison:

- dbt: excellent for SQL transformations, macros, tests. Bruin offers a more compact, batteries-included approach for
  smaller teams or quick starts; can coexist if desired.
- Airflow + SQL scripts: strong orchestration, but more boilerplate for data quality and metadata; Bruin reduces
  overhead by bundling checks and lineage.
- Ad-hoc notebooks: great for exploration but weak on reproducibility and lineage; Bruin formalizes assets and
  dependencies without heavy setup.

When to choose Bruin:

- You want fast local iteration with clear asset definitions, dependencies, and validations.
- You prefer minimal scaffolding to get from raw to marts.

## Where to look in the repo

- Staging checks examples:
    - ecommerce/assets/staging/stg.orders.sql (accepted_values on status; range and not_null checks)
    - ecommerce/assets/staging/stg.order_items.sql (range checks for quantity and prices)
- Mart examples:
    - ecommerce/assets/mart/mart.sales_daily.sql
    - ecommerce/assets/mart/mart.product_performance.sql
    - ecommerce/assets/mart/mart.variant_profitability.sql
    - ecommerce/assets/mart/mart.customers_by_age.asset.sql
    - ecommerce/assets/mart/mart.customers_by_county.asset.sql

## Summary

This model provides a clean normalized core with clear PK/FK relationships, timestamps for incremental loading, embedded
data quality checks, and marts that answer realistic ecommerce questions. By using Bruin’s declarative assets and
checks, you get faster iteration, reliable builds, and easy-to-understand dependencies compared to heavier alternatives.