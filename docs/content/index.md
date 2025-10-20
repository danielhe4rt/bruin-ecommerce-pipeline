---
seo:
  title: Bruin Tutorials
  description: Create stunning, fast and SEO-optimized documentation sites with Nuxt UI.
---

::u-page-hero{class="dark:bg-gradient-to-b from-neutral-900 to-neutral-950"}
---
orientation: horizontal
---
#top
:hero-background

#title
Bruin [University]{.text-primary}

#description
A documentation-driven learning hub for modern data engineers. Learn how to design, build, and validate pipelines with Bruin — from ingestion to analytics — all on your laptop.

#links
  :::u-button
  ---
  to: /getting-started
  size: xl
  trailing-icon: i-lucide-arrow-right
  ---
  Get started
  :::

  :::u-button
  ---
  icon: i-simple-icons-github
  color: neutral
  variant: outline
  size: xl
  to: https://github.com/bruin-data/bruin
  target: _blank
  ---
  Check on Github
  :::

#default
  :::prose-pre
  ---
  filename: customers.asset.yml
  ---

```yaml [orders-performance/assets/ingestion/raw.customers.asset.yml]
name: raw.customers
type: ingestr
description: Ingest data from Postgres into DuckDB raw layer.

parameters:
    source_connection: pg-default
    source_table: "public.customers"
    destination: duckdb
```





:::prose-code-tree-from-path
---
path: /samples/orders-performance/assets
default-value: staging/stg.orders.sql
expand-all: false
---
:::
