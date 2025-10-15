```diff
 .
├── generate_data.py
├── orders-performance
│   ├── assets
│   │   ├── ingestion
│   │   │   ├── raw.customers.asset.yml
│   │   │   ├── raw.order_items.asset.yml
│   │   │   ├── raw.orders.asset.yml
│   │   │   ├── raw.products.asset.yml
│   │   │   └── raw.product_variants.asset.yml
│   │   ├── mart
│   │   │   ├── customers
│   │   │   │   ├── mart.customers_by_age.asset.sql
│   │   │   │   └── mart.customers_by_county.asset.sql
│   │   │   ├── products
│   │   │   │   ├── mart.product_performance.sql
│   │   │   │   └── mart.variant_profitability.sql
│   │   │   └── sales
│   │   │       └── mart.sales_daily.sql
│   │   └── staging
│   │         ├── stg.customers.asset.sql
│   │         ├── stg.order_items.sql
│   │         ├── stg.orders.sql
│   │         ├── stg.products.sql
│   │         ├── stg.product_variants.sql
│   │         └── stg.test.asset.sql
│   └── pipeline.yml
├── pyproject.toml
└── uv.lock
```