/* @bruin

name: mart.variant_profitability
type: duckdb.sql

materialization:
  type: table
  strategy: create+replace

depends:
  - stg.product_variants
  - stg.order_items
  - stg.orders
  - stg.products

columns:
  - name: variant_id
    type: integer
    checks:
      - name: not_null
      - name: unique
  - name: variant_sku
    type: string
    checks:
      - name: not_null
  - name: product_id
    type: integer
    checks:
      - name: not_null
  - name: items_sold
    type: integer
    checks:
      - name: not_null
  - name: revenue
    type: numeric
    checks:
      - name: not_null
  - name: cost
    type: numeric
    checks:
      - name: not_null
  - name: profit
    type: numeric
    checks:
      - name: not_null
  - name: margin_pct
    type: numeric
    checks:
      - name: not_null

@bruin */

WITH paid_items AS (
  SELECT oi.*
  FROM stg.order_items oi
  JOIN stg.orders o ON o.order_id = oi.order_id
  WHERE o.status IN ('paid','shipped')
), joined AS (
  SELECT
    v.variant_id,
    p.name as product_name,
    v.variant_sku,
    v.product_id,
    v.manufacturing_price,
    v.selling_price,
    oi.quantity,
    oi.total_price
  FROM paid_items oi
  JOIN stg.product_variants v ON v.variant_id = oi.variant_id
  JOIN stg.products p ON p.product_id = v.product_id
)
SELECT
    j.product_name,
  j.variant_id,
  j.variant_sku,
  j.product_id,
  SUM(j.quantity) AS items_sold,
  SUM(j.total_price) AS revenue,
  SUM(j.quantity * j.manufacturing_price) AS cost,
  SUM(j.total_price) - SUM(j.quantity * j.manufacturing_price) AS profit,
  CASE WHEN SUM(j.total_price) = 0 THEN 0
       ELSE (SUM(j.total_price) - SUM(j.quantity * j.manufacturing_price)) / SUM(j.total_price)
  END AS margin_pct,
  BOOL_OR(j.selling_price < j.manufacturing_price) AS selling_price_below_cost
FROM joined j
GROUP BY 1,2,3,4
ORDER BY profit DESC;
