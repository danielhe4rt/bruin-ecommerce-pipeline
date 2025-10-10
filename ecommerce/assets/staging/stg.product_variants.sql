/* @bruin
name: stg.product_variants
type: duckdb.sql
materialization:
  type: table

depends:
  - raw.product_variants
  - stg.products

columns:
  - name: variant_id
    type: integer
    checks:
      - name: not_null
      - name: unique
  - name: product_id
    type: integer
    checks:
      - name: not_null
  - name: variant_sku
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: color
    type: string
  - name: size
    type: string
  - name: manufacturing_price
    type: numeric
    checks:
      - name: not_null
      - name: range
        min: 0
  - name: selling_price
    type: numeric
    checks:
      - name: not_null
      - name: range
        min: 0
  - name: stock_quantity
    type: integer
    checks:
      - name: not_null
      - name: range
        min: 0
  - name: is_active
    type: boolean
    checks:
      - name: not_null
  - name: created_at
    type: timestamp
    checks:
      - name: not_null
  - name: updated_at
    type: timestamp
    checks:
      - name: not_null

@bruin */

-- Keep only variants belonging to known products, simple cleaning
SELECT
  pv.id AS variant_id,
  pv.product_id,
  pv.variant_sku,
  pv.color,
  pv.size,
  CASE WHEN pv.manufacturing_price < 0 THEN 0 ELSE pv.manufacturing_price END AS manufacturing_price,
  CASE WHEN pv.selling_price < 0 THEN 0 ELSE pv.selling_price END AS selling_price,
  CASE WHEN pv.stock_quantity < 0 THEN 0 ELSE pv.stock_quantity END AS stock_quantity,
  COALESCE(pv.is_active, TRUE) AS is_active,
  pv.created_at,
  pv.updated_at
FROM raw.product_variants pv
JOIN stg.products p ON p.product_id = pv.product_id;