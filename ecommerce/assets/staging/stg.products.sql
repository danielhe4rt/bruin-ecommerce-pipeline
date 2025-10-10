/* @bruin
name: stg.products
type: duckdb.sql
materialization:
  type: table

depends:
  - raw.products

columns:
  - name: product_id
    type: integer
    checks:
      - name: not_null
      - name: unique
  - name: name
    type: string
    checks:
      - name: not_null
  - name: category
    type: string
    checks:
      - name: not_null
  - name: sku
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: created_at
    type: timestamp
    checks:
      - name: not_null
  - name: updated_at
    type: timestamp
    checks:
      - name: not_null

@bruin */

SELECT
  id AS product_id,
  name,
  category,
  sku,
  created_at,
  updated_at
FROM raw.products;