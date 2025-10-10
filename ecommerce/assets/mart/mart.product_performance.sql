/* @bruin
name: mart.product_performance
type: duckdb.sql
materialization:
  type: table

depends:
  - stg.products
  - stg.product_variants
  - stg.order_items
  - stg.orders

columns:
  - name: product_id
    type: integer
    checks:
      - name: not_null
  - name: product_name
    type: string
    checks:
      - name: not_null
  - name: category
    type: string
    checks:
      - name: not_null
  - name: items_sold
    type: integer
    checks:
      - name: not_null
      - name: range
        min: 0
  - name: gross_revenue
    type: numeric
    checks:
      - name: not_null
      - name: range
        min: 0
  - name: avg_item_price
    type: numeric
    checks:
      - name: not_null
      - name: range
        min: 0

custom_checks:
  - name: no negative prices or revenue
    query: |
      SELECT COUNT(*) FROM mart.product_performance WHERE avg_item_price < 0 OR gross_revenue < 0
    value: 0
@bruin */

-- Product performance aggregated at product level (variants rolled up to products)
WITH paid_items AS (
  SELECT oi.*
  FROM stg.order_items oi
  JOIN stg.orders o ON o.order_id = oi.order_id
  WHERE o.status IN ('paid','shipped')
), items_with_product AS (
  SELECT
    v.product_id,
    oi.quantity,
    oi.total_price
  FROM paid_items oi
  JOIN stg.product_variants v ON v.variant_id = oi.variant_id
)
SELECT
  p.product_id,
  p.name AS product_name,
  p.category,
  SUM(i.quantity) AS items_sold,
  SUM(i.total_price) AS gross_revenue,
  CASE WHEN SUM(i.quantity) = 0 THEN 0 ELSE SUM(i.total_price) / SUM(i.quantity) END AS avg_item_price
FROM items_with_product i
JOIN stg.products p ON p.product_id = i.product_id
GROUP BY 1,2,3
ORDER BY gross_revenue DESC;