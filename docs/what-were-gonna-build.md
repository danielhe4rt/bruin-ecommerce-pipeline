# Concepts

This repository contains a ecommerce demo featuring a tutorial on how to get started with Bruin that you can run
locally.

## 1. Prerequisites

Before we start, make sure your local environment has everything you need to build and run the Bruin pipeline.

### 1.1 Requirements

You’ll need the following tools installed:

* **Bruin CLI** — installed and configured (used to run and validate assets)
* **Python 3.9+** — required for data generation scripts and optional Python-based assets
* **uv** *(recommended)* — a fast Python package manager to create and manage virtual environments
* **PostgreSQL** — available locally or via Docker (used as the operational source / OLTP system)
* **DuckDB** — installed locally or via Docker (used as the analytical storage / OLAP system)

### 1.2 Optional (but helpful)

* **VS Code Bruin extension** — for syntax highlighting, autocompletion, and inline validation
* **Docker Compose** — to spin up a ready-to-use PostgreSQL service without manual setup

### 1.3 Installing Bruin CLI

Use curl to install Bruin on macOS, Linux or Windows:

```bash
curl -LsSf https://getbruin.com/install/cli | sh
```

Or you can also use wget to install Bruin:

```bash
wget -qO- https://getbruin.com/install/cli | sh
```

### 1.4 Setting up DuckDB

DuckDB is a lightweight, in-process analytical database — think of it as SQLite for analytics.
It runs entirely locally (no server needed) and is optimized for analytical queries, aggregations, and data
transformations — making it perfect for local development, data validation, and experimentation.

In this tutorial, DuckDB will serve as our analytical storage layer — the destination where we’ll:

- Ingest raw data extracted from PostgreSQL
- Clean and validate data in staging
- Create analytics-ready marts with aggregated and trustworthy datasets
- Run validation checks and ad-hoc SQL queries interactively with bruin query

In short:

> PostgreSQL = operational source (OLTP) <br>
> DuckDB = analytical store (OLAP)

To install it in your local machine, you can:

```bash
curl https://install.duckdb.org | sh
```

You can learn more at [DuckDB documentation](https://duckdb.org/docs/installation/python.html).

## 2. Understanding the Data Model

For this tutorial, we’ll use a hands-on example: building a pipeline that moves data from a PostgreSQL source into a
DuckDB destination, while applying the following core data engineering concepts:

- Attend to the business context
- Build a realistic Data Modeling, schema design and relationships
- Understand the incremental loading strategy and when/where to use
- Implement data quality validations
- Implement transformations rationale

### 2.1 The Business Context

In e-commerce — and most analytics-driven businesses — data pipelines are typically organized into **layers**, each
serving a specific purpose. This layered structure keeps data **reliable, interpretable, and adaptable** as the business
scales.

The ultimate goal is to make data analysis effortless and focused on **answering real business questions** across key
domains, such as:

| Domain                            | Analysis Focus                                                                        |
|-----------------------------------|---------------------------------------------------------------------------------------|
| **Customers**                     | Segmentation by age, geography, and behavior patterns                                 |
| **Products and Product Variants** | Performance by category, profitability, and pricing insights                          |
| **Orders and Order Items**        | Daily revenue, order volume, and purchasing trends                                    |
| **KPI layer**                     | Metrics like **AOV**, **units per order**, **top SKUs**, and **repeat purchase rate** |

By structuring data this way, teams can move from raw information to actionable insights with clarity and confidence.

### 2.2 The Data Modeling Approach

Before we start building, it’s important to understand the data relationships that form the backbone of most e-commerce
systems. These relationships define how information flows across entities — from customers placing orders to
products being sold and analyzed.

Here’s the simplified schema we’ll use throughout this tutorial:

| From                 | To                           | Relationship Type |
|----------------------|------------------------------|-------------------|
| customers(id)        | orders(customer_id)          | 1:N (One-to-Many) |
| orders(id)           | order_items(order_id)        | 1:N (One-to-Many) |
| products(id)         | product_variants(product_id) | 1:N (One-to-Many) |
| product_variants(id) | order_items(variant_id)      | 1:N (One-to-Many) |

<details>

<summary>Click to visualize the Mermaid Graph</summary>

```mermaid

graph TD
;
    customers(customers) --> orders(orders)
    orders(orders) --> order_items(order_items)
    products(products) --> product_variants(product_variants)
    product_variants(product_variants) --> order_items(order_items)
```

</details>


This model mirrors a real-world e-commerce workflow:

- A customer makes one or more orders.
- Each order contains one or more items.
- Each item points to a specific product variant (like a red T-shirt, size M).
- Each variant belongs to a broader product (e.g., “Classic T-Shirt”).

These foreign key relationships allow us to join datasets efficiently and enable downstream analytics like:

| Domain    | Materialized Table                | Depends On                                                              |
|:----------|-----------------------------------|-------------------------------------------------------------------------|
| Customers | `mart.customers_by_age`           | `stg.customers`                                                         |
| Customers | `mart.customers_by_country`       | `stg.customers`                                                         |
| Products  | `mart.revenue_by_product`         | `raw.order_items`, `raw.product_variants`, `raw.products`               |
| Products  | `mart.revenue_by_category`        | `raw.order_items`, `raw.product_variants`, `raw.products`               |
| Products  | `mart.revenue_by_product_variant` | `stg.product_variants`, `stg.order_items`, `stg.orders`, `stg.products` |
| Sales     | `mart.sales_daily`                | `stg.order_items`, `stg.orders`                                         |

By establishing these links early, every layer of the pipeline — raw, staging, and mart — can remain consistent and
traceable across transformations.

## 3. Setting up the Source Database (Docker + PostgresSQL)

### 3.1 Running the Database Instance

First, you must spin up a local PostgreSQL instance in seconds using Docker — perfect for running your Bruin pipelines
or
development scripts.

```bash
docker run -d \
  --name bruin-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=postgres \
  -p 5432:5432 \
  postgres:18-alpine
```

This starts a container named postgres running on port 5432, with:

* **User**: postgres
* **Password**: postgres
* **Database**: ecommerce

Now, check if you can connect to the database using the following command:

```bash
docker exec -it bruin-postgres psql -U postgres
```

The output should look like this:

```text
psql (16.10 (Debian 16.10-1.pgdg13+1))
Type "help" for help.

postgres=#
```

### 3.2 Creating the Database Tables

Now you must create the schema that will be used in the tutorial. Click in **details** below, copy the SQL and paste in
the terminal.

<details>

```sql
CREATE TABLE IF NOT EXISTS customers
(
    id
    BIGSERIAL
    PRIMARY
    KEY,
    full_name
    TEXT
    NOT
    NULL,
    email
    TEXT
    NOT
    NULL
    UNIQUE,
    country
    TEXT
    NOT
    NULL,
    age
    INTEGER
    NOT
    NULL
    CHECK
(
    age
    >=
    0
),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now
(
),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now
(
)
    );


CREATE TABLE IF NOT EXISTS products
(
    id
    BIGSERIAL
    PRIMARY
    KEY,
    name
    TEXT
    NOT
    NULL,
    category
    TEXT
    NOT
    NULL,
    sku
    TEXT
    NOT
    NULL
    UNIQUE,
    created_at
    TIMESTAMPTZ
    NOT
    NULL
    DEFAULT
    now
(
),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now
(
)
    );


CREATE TABLE IF NOT EXISTS product_variants
(
    id
    BIGSERIAL
    PRIMARY
    KEY,
    product_id
    BIGINT
    NOT
    NULL
    REFERENCES
    products
(
    id
) ON DELETE CASCADE,
    variant_sku TEXT NOT NULL UNIQUE,
    color TEXT,
    size TEXT,
    manufacturing_price NUMERIC
(
    10,
    2
) NOT NULL,
    selling_price NUMERIC
(
    10,
    2
) NOT NULL,
    stock_quantity INTEGER NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now
(
),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now
(
)
    );


CREATE TABLE IF NOT EXISTS orders
(
    id
    BIGSERIAL
    PRIMARY
    KEY,
    customer_id
    BIGINT
    NOT
    NULL
    REFERENCES
    customers
(
    id
),
    order_date TIMESTAMPTZ NOT NULL DEFAULT now
(
),
    status TEXT NOT NULL,
    total_amount NUMERIC
(
    12,
    2
),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now
(
),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now
(
)
    );


CREATE TABLE IF NOT EXISTS order_items
(
    id
    BIGSERIAL
    PRIMARY
    KEY,
    order_id
    BIGINT
    NOT
    NULL
    REFERENCES
    orders
(
    id
) ON DELETE CASCADE,
    variant_id BIGINT NOT NULL REFERENCES product_variants
(
    id
),
    quantity INTEGER NOT NULL,
    unit_price NUMERIC
(
    10,
    2
) NOT NULL,
    total_price NUMERIC
(
    12,
    2
) GENERATED ALWAYS AS
(
    quantity
    *
    unit_price
) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now
(
)
    );
```

</details>

After copying and pasting the SQL, press **Enter** to execute the query. The output should look like this:

```diff
... pasted command
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE

+ postgres=# \d 
                   List of relations
 Schema |          Name           |   Type   |  Owner
--------+-------------------------+----------+----------
 public | customers               | table    | postgres
 public | customers_id_seq        | sequence | postgres
 public | orders                  | table    | postgres
 public | orders_id_seq           | sequence | postgres
 public | product_variants        | table    | postgres
 public | product_variants_id_seq | sequence | postgres
 public | products                | table    | postgres
 public | products_id_seq         | sequence | postgres
(8 rows)
```

> You can also run \d or \d+ to list all tables in the database.

After that, you can close it by typing **\q** and pressing **Enter**. Now, we're ready to populate the database with
some test data.

## 4. Populating the Source Test Data

Since we're using Python and `uv`, we can use the `uv` package to create a virtual environment and install the
dependencies.

### 4.1 Init Project with `uv`

Let's create a new directory for our project and initialize a new virtual environment and enter it:

```bash
uv init ecommerce-bruin --bare --vcs git
# Initialized project `ecommerce-bruin ` at `~/ecommerce-bruin`

cd ecommerce-bruin
```

Now, let's add the needed dependencies to our project:

```bash
uv add faker psycopg
```

The output must be something like this:

```diff
Using CPython 3.13.3
Creating virtual environment at: .venv
Installed 3 packages in 27ms
+ faker==37.11.0
+ psycopg==3.2.10
+ tzdata==2025.2
```

### 4.2 Creating the Data Generator Script

Now, let's create a new file called `generate_data.py` and add the following code:

<details>

<summary>Click to expand the code snippet</summary>

```python
#!/usr/bin/env python3
"""
Generate realistic e-commerce demo data for PostgreSQL with time window and chaos injection.

Populates: customers, products, product_variants, orders, order_items
Idempotent & incremental: uses UPSERTs with conflict resolution.
Usage:
    uv run generate_data.py --dsn postgresql://user:pass@localhost:5432/ecommerce \
      --starting-at 2024-01-01 --ending-at 2024-06-30 --chaos-percent 5
"""

import argparse
import random
from datetime import datetime, timedelta, timezone
from faker import Faker
import psycopg

fake = Faker()
random.seed(42)  # deterministic output


def parse_args():
    now = datetime.now(timezone.utc)
    beginning_of_the_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_the_day = now.replace(hour=23, minute=59, second=59, microsecond=999999)

    p = argparse.ArgumentParser(description="Generate demo data for e-commerce tables.")
    p.add_argument("--dsn", required=True, help="PostgreSQL DSN")
    p.add_argument("--customers", type=int, default=100)
    p.add_argument("--products", type=int, default=50)
    p.add_argument("--orders", type=int, default=500)
    p.add_argument("--max-items-per-order", type=int, default=5)
    p.add_argument("--chaos-percent", type=float, default=0.0, help="Percentage of product variants with invalid sizes")
    p.add_argument("--starting-at", type=str,
                   default=beginning_of_the_day, help="ISO start date (e.g., 2024-01-01)")
    p.add_argument("--ending-at", type=str,
                   default=end_of_the_day,
                   help="ISO end date (e.g., 2024-06-30)")
    return p.parse_args()


def parse_date(date_str):
    if isinstance(date_str, str):
        return datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
    return date_str


def random_date(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))


def generate_customers(n):
    for _ in range(n):
        yield (
            fake.name(),
            fake.unique.email(),
            fake.country(),
            random.randint(18, 70),
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
        )


def generate_products(n):
    categories = ["t-shirts", "hoodies", "shoes", "accessories", "jackets"]
    for _ in range(n):
        cat = random.choice(categories)
        yield (
            f"{fake.word().capitalize()} {cat}",
            cat,
            fake.unique.bothify("SKU-####-??").upper(),
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
        )


def generate_product_variants(product_rows, chaos_percent):
    for pid, category in product_rows:
        for _ in range(random.randint(1, 4)):
            variant_sku = fake.unique.bothify("VAR-####-??").upper()
            color = random.choice(["Red", "Blue", "Black", "White", "Green", None])

            # Normal logic
            if category == "shoes":
                size = str(random.choice([36, 38, 40, 42, 44]))
            else:
                size = random.choice(["S", "M", "L", "XL"])

            # Controlled chaos
            if chaos_percent > 0 and random.random() < (chaos_percent / 100):
                if category == "shoes":
                    size = random.choice(["S", "M", "L"])  # wrong for shoes
                else:
                    size = str(random.choice([37, 41, 45]))  # wrong for non-shoes

            manuf = round(random.uniform(10, 80), 2)
            sell = round(manuf * random.uniform(1.2, 2.0), 2)
            yield (
                pid,
                variant_sku,
                color,
                size,
                manuf,
                sell,
                random.randint(0, 200),
                True,
                datetime.now(timezone.utc),
                datetime.now(timezone.utc),
            )


def generate_orders(customers, n, start, end):
    statuses = ["pending", "paid", "cancelled", "shipped"]
    for _ in range(n):
        yield (
            random.choice(customers),
            random_date(start, end),
            random.choices(statuses, weights=[0.2, 0.5, 0.1, 0.2])[0],
            0.0,
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
        )


def generate_order_items(order_ids, variant_ids, max_items):
    for oid in order_ids:
        for vid in random.sample(variant_ids, random.randint(1, max_items)):
            qty = random.randint(1, 3)
            yield (
                oid,
                vid,
                qty,
                round(random.uniform(20, 200), 2),
                datetime.now(timezone.utc),
            )


def upsert(conn, sql, data):
    with conn.cursor() as cur:
        cur.executemany(sql, data)
    conn.commit()


def main():
    args = parse_args()
    conn = psycopg.connect(args.dsn)

    start = parse_date(args.starting_at)
    end = parse_date(args.ending_at)
    print(
        f"Generating data between: \n Starting at: {start.strftime('%Y-%m-%d %H:%M:%S')} \n Ending at: {end.strftime('%Y-%m-%d %H:%M:%S')}")

    print(f"Inserting/updating {args.customers} customers...")
    upsert(
        conn,
        """
        INSERT INTO customers (full_name, email, country, age, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (email) DO
        UPDATE
            SET full_name = EXCLUDED.full_name,
            country = EXCLUDED.country,
            age = EXCLUDED.age,
            updated_at = EXCLUDED.updated_at;
        """,
        generate_customers(args.customers),
    )

    print(f"Inserting/updating {args.products} products...")
    upsert(
        conn,
        """
        INSERT INTO products (name, category, sku, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s) ON CONFLICT (sku) DO
        UPDATE
            SET name = EXCLUDED.name,
            category = EXCLUDED.category,
            updated_at = EXCLUDED.updated_at;
        """,
        generate_products(args.products),
    )

    product_rows = list(conn.execute("SELECT id, category FROM products"))
    print(f"Generating product variants (chaos: {args.chaos_percent}%)...")
    upsert(
        conn,
        """
        INSERT INTO product_variants
        (product_id, variant_sku, color, size, manufacturing_price, selling_price, stock_quantity, is_active,
         created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (variant_sku) DO
        UPDATE
            SET color = EXCLUDED.color,
            size = EXCLUDED.size,
            manufacturing_price = EXCLUDED.manufacturing_price,
            selling_price = EXCLUDED.selling_price,
            stock_quantity = EXCLUDED.stock_quantity,
            updated_at = EXCLUDED.updated_at;
        """,
        generate_product_variants(product_rows, args.chaos_percent),
    )

    customer_ids = [r[0] for r in conn.execute("SELECT id FROM customers")]
    print(f"Inserting/updating {args.orders} orders...")
    upsert(
        conn,
        """
        INSERT INTO orders (customer_id, order_date, status, total_amount, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO
        UPDATE
            SET status = EXCLUDED.status,
            total_amount = EXCLUDED.total_amount,
            updated_at = EXCLUDED.updated_at;
        """,
        generate_orders(customer_ids, args.orders, start, end),
    )

    order_ids = [r[0] for r in conn.execute("SELECT id FROM orders")]
    variant_ids = [r[0] for r in conn.execute("SELECT id FROM product_variants")]
    print("Inserting/updating order items...")
    upsert(
        conn,
        """
        INSERT INTO order_items (order_id, variant_id, quantity, unit_price, created_at)
        VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING;
        """,
        generate_order_items(order_ids, variant_ids, args.max_items_per_order),
    )

    print("✅ Data generation complete. Ready for incremental ingestion tests.")
    conn.close()


if __name__ == "__main__":
    main()
```

</details>


After that, let's run the script and see if everything works as expected:

```bash
uv run generate_data.py --dsn postgresql://postgres:postgres@localhost:5432/postgres 
```

The output should look like this:

```text
Generating data between: 
 Starting at: 2025-10-10 00:00:00 
 Ending at: 2025-10-15 23:59:59
Inserting/updating 100 customers...
Inserting/updating 50 products...
Generating product variants (chaos: 0.0%)...
Inserting/updating 500 orders...
Inserting/updating order items...
✅ Data generation complete. Ready for incremental ingestion tests.
```

Cool! Now we're ready to start our Bruin Pipeline!

<details>

<summary>Click to expand the Script Argument List</summary>

| Argument                | Type             | Default                    | Description                                                                                      |
|-------------------------|------------------|----------------------------|--------------------------------------------------------------------------------------------------|
| `--dsn`                 | `str`            | **(required)**             | PostgreSQL connection string (DSN). Example: `postgresql://user:pass@localhost:5432/ecommerce`   |
| `--customers`           | `int`            | `100`                      | Number of customers to generate.                                                                 |
| `--products`            | `int`            | `50`                       | Number of products to generate.                                                                  |
| `--orders`              | `int`            | `500`                      | Number of orders to generate.                                                                    |
| `--max-items-per-order` | `int`            | `5`                        | Maximum number of items per order. Each order will have 1–N items.                               |
| `--chaos-percent`       | `float`          | `0.0`                      | Percentage of product variants that should have **invalid sizes** (used for validation testing). |
| `--starting-at`         | `str (ISO date)` | **Yesterday at 00:00 UTC** | Beginning of the generated data window. Example: `2024-01-01`.                                   |
| `--ending-at`           | `str (ISO date)` | **Today at 23:59 UTC**     | End of the generated data window. Example: `2024-06-30`.                                         |

</details>

> **Note:** You can also run the script with the `--help` flag to see the available arguments.

## 5. Creating the Bruin Pipeline

So far, we have ingested `generate_data.py` content into our `PostgreSQL` database, which will act as a source. Now,
let's start our Bruin Pipeline and understand the base concepts. For that, you must run the command below:

```bash
bruin init default orders-performance
```

> Make sure that you're inside the `ecommerce-bruin` directory!

After that, you will have the following directory structure:

```diff
 ./eccomerce-bruin
 ├── generate_data.py
 ├── orders-performance
 │         ├── assets
-│         │         ├── example.sql
-│         │         └── pythonsample
-│         │             ├── country_list.py
-│         │             └── requirements.txt
 │         ├── pipeline.yml
 │         └── README.md
 └── pyproject.toml
```

First, you must delete the `example.sql` file and the `pythonsample` directory.

To build this project, we're going to rebuild the structure so it can fit our needs. The pipeline consists of three main
layers:

| Layer   | Purpose                                   | Tables                                                                                 |
|---------|-------------------------------------------|----------------------------------------------------------------------------------------------|
| Raw     | Exact copies of source tables             | raw.customers, raw.products, raw.product_variants, raw.orders, raw.order_items               |
| Staging | Cleaned, typed, validated                 | stg.customers, stg.products, stg.product_variants, stg.orders, stg.order_items               |
| Mart    | Analytics-ready aggregations/segmentation | mart.sales_daily, mart.product_performance, mart.variant_profitability, mart.customers_by_age |


So, the first step is to create the `assets` directory and the `raw` sub-directory:

```diff
 .
 ├── generate_data.py
 ├── orders-performance
 │   ├── assets
+│   │   └── ingestion
+│   │       ├── raw.customers.asset.yml
+│   │       ├── raw.order_items.asset.yml
+│   │       ├── raw.orders.asset.yml
+│   │       ├── raw.products.asset.yml
+│   │       └── raw.product_variants.asset.yml
 │   │── README.md
 │   └── pipeline.yml
 ├── pyproject.toml
 └── uv.lock
```

## Designing Your Analytics Layer

## Implementing Quality Checks

## Running & Validating the Pipeline

## Troubleshooting Common Issues

## Next Steps & Production Considerations

## Glossary

| Term                  | Definition                                                                                 |
|-----------------------|--------------------------------------------------------------------------------------------|
| AOV                   | Average Order Value - The average amount spent per order                                   |
| DuckDB                | A lightweight, in-memory SQL database engine used for data analytics                       |
| SKU                   | Stock Keeping Unit - A unique identifier for a product variant                             |
| PostgreSQL            | An open-source relational database management system                                       |
| Data Pipeline         | A series of processes that move and transform data from source to destination              |
| Data Layer            | A logical grouping of data at a similar level of transformation (e.g., raw, staging, mart) |
| Raw Layer             | Exact copies of source data tables without modifications                                   |
| Staging Layer         | Cleaned, typed, and validated data ready for analysis                                      |
| Mart Layer            | Analytics-ready aggregations and business-focused data models                              |
| Incremental Loading   | A data loading strategy that processes only new or changed records                         |
| Referential Integrity | Ensuring relationships between tables are maintained and valid                             |
| Primary Key (PK)      | A unique identifier for each record in a table                                             |
| Foreign Key (FK)      | A field that links to a primary key in another table                                       |
| DSN                   | Data Source Name - Connection string used to connect to a database                         |

