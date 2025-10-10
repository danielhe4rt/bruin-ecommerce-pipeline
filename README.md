<h1 align="center"> Ecommerce Demo Data Stack (Postgres → DuckDB via Bruin) </h1>

<div>
<img src="./.github/assets/logo.png" width=100 align="left" />

<p style="padding-left: 4px;">
This repository contains a small ecommerce demo data stack you can run locally. It combines:
</p>

</div>

- A Python data generator that writes OLTP-style data into PostgreSQL
- DuckDB SQL assets (with @bruin metadata) for staging and mart layers
- A Bruin pipeline descriptor that orchestrates ingestion from Postgres to DuckDB and builds marts
- A local DuckDB database file for ad‑hoc exploration

If you are new to Bruin: the SQL assets in ecommerce/assets/** are executed in DuckDB, and the pipeline resolves
external connections by name (configured in your Bruin environment, not in this repo).

## Stack and Tooling

- Language: Python (>= 3.9)
- Package/dependency manager: uv (Astral) recommended; standard Python also works
- Runtime deps:
    - psycopg[binary] >= 3.1.18
    - Faker >= 25.0.0
- Databases:
    - PostgreSQL (target for generated OLTP data)
    - DuckDB (local analytics store; file duckdb.db in repo root)
- Orchestration/CLI: Bruin CLI (used to run pipeline, query DuckDB, and manage connections)

## Project Structure

- generate_data.py — Python script that initializes schema and generates/upserts demo data in PostgreSQL
- sql/ddl.sql — Postgres DDL for required tables (idempotent; enforced at runtime)
- ecommerce/pipeline.yml — Bruin pipeline descriptor
- ecommerce/assets/** — DuckDB SQL assets and ingestion descriptors with @bruin metadata
    - ecommerce/assets/ingestion/*.asset.yml — Ingestion assets pulling from Postgres (pg-default) into DuckDB raw.*
      tables
    - ecommerce/assets/staging/*.sql — Staging models (type: duckdb.sql, materialized as tables)
    - ecommerce/assets/mart/*.sql — Mart models (type: duckdb.sql, materialized as tables)
- duckdb.db — Local DuckDB database file for exploration
- Makefile — Convenience targets for data generation, pipeline runs, connection checks, and queries
- pyproject.toml — Project metadata and uv script alias for the data generator

## Requirements

- Python >= 3.9
- PostgreSQL instance you can connect to (DSN)
- DuckDB (no separate server required; duckdb.db in repo root is used by Bruin connection)
- Bruin CLI installed and configured
- uv (recommended) or a working Python environment capable of installing dependencies

## Installation

You can use uv to run without explicitly creating a virtualenv.

- Install uv: https://docs.astral.sh/uv/
- Ensure Bruin CLI is installed and you have connections configured (see Configuration below)

No manual dependency installation is required when using uv; it will resolve dependencies declared in pyproject.toml.

## Configuration

Connections are resolved by name in the pipeline. Configure these in your Bruin environment:

- duckdb-default → points to your local DuckDB database (e.g., file: ./duckdb.db)
- pg-default → points to your PostgreSQL database containing the OLTP tables

Notes:

- This repo does not include actual connection credentials. Set them up in Bruin per your environment.
- The data generator writes directly to PostgreSQL using a DSN string you provide at runtime.

## Usage

### 1) Generate demo data into PostgreSQL

The generator will ensure schema exists (applies sql/ddl.sql) and then upsert customers/products/variants and create
orders/order_items.

Using uv (script alias):

- Example:
  uv run generate-data -- --dsn postgresql://USER:PASS@HOST:PORT/DBNAME --start-date 2024-01-01

Flags (selected):

- --dsn postgresql://user:pass@host:port/dbname (required)
- --start-date YYYY-MM-DD (required; naive dates treated as UTC)
- --customers INT (default 100)
- --products INT (default 50)
- --variants-per-product MIN MAX (default 1 3)
- --orders INT (default 500)
- --items-per-order MIN MAX (default 1 5)
- --seed INT (default 42)
- --update-existing (touch subset of rows)
- --replace-orders (clear/regenerate orders + order_items only)
- --ddl PATH (override schema file; defaults to ./sql/ddl.sql; must exist)

Using Makefile helper (requires Make, Bruin not needed for this step):

- Required variables: DSN, START
- Examples:
  make generate-data DSN=postgresql://USER:PASS@localhost:5432/ecommerce START=2024-01-01
  make generate-data DSN=postgresql://USER:PASS@localhost:5432/ecommerce START=2024-01-01 EXTRA="--replace-orders --seed
  123"

Notes:

- The DDL file is required; generation fails if missing.
- Upserts are conflict-safe on unique keys (customers.email, products.sku, product_variants.variant_sku).

### 2) Run the Bruin pipeline (Postgres → DuckDB)

- Ensure bruin connections exist: duckdb-default and pg-default
- Execute:
  bruin run ecommerce/pipeline.yml

What it does:

- Ingestion assets (ecommerce/assets/ingestion/*.asset.yml) read from Postgres (source_connection: pg-default) and land
  data into DuckDB raw.
- Staging and mart SQL assets (type: duckdb.sql) materialize tables in DuckDB

### 3) Explore DuckDB marts

- Quick query via Makefile:
  make query # defaults to select * from mart.sales_daily limit 10
  make query TABLE=mart.sales_daily
  make query SQL="select count(*) from mart.product_performance"

- Preview common marts:
  make mart-list

## Scripts and Commands

Makefile targets:

- help — Show available commands
- generate-data — Generate demo data (requires DSN and START; optional EXTRA="...")
- run-pipeline — Run the Bruin pipeline defined at ecommerce/pipeline.yml
- connections-list — List available Bruin connections
- connections-test-duckdb — Test the duckdb-default connection
- connections-test-postgres — Test the pg-default connection
- query — Run a SQL query against DuckDB (via Bruin). Supports TABLE or SQL="..."
- mart-list — Preview first 10 rows from common mart tables

## TODO

- [ ] Docker Docs for Postgres
- [ ] Explore Incremental Loading
- [ ] Implement `depends` to chain all responsibilities from the pipeline
- [ ] Refine the `generate_data.py` to make more realistic data