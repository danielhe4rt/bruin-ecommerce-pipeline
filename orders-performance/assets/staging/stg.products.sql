/* @bruin
name: stg.products
type: duckdb.sql
materialization:
  type: table

tags:
  - staging

depends:
  - raw.products

columns:
  - extends: Product.ID
  - extends: Product.Name
  - extends: Product.Category
  - extends: Product.SKU
  - extends: Base.CreatedAt
  - extends: Base.UpdatedAt

@bruin */

SELECT
  id AS product_id,
  name,
  category,
  sku,
  created_at,
  updated_at
FROM raw.products;