/* @bruin
name: stg.order_items
type: duckdb.sql
materialization:
  type: table

depends:
  - raw.order_items

columns:
  - name: order_item_id
    type: integer
    checks:
      - name: not_null
      - name: unique
  - name: order_id
    type: integer
    checks:
      - name: not_null
  - name: variant_id
    type: integer
    checks:
      - name: not_null
  - name: quantity
    type: integer
    checks:
      - name: not_null
      - name: range
        min: 1
  - name: unit_price
    type: numeric
    checks:
      - name: not_null
      - name: range
        min: 0
  - name: total_price
    type: numeric
    checks:
      - name: not_null
      - name: range
        min: 0
  - name: created_at
    type: timestamp
    checks:
      - name: not_null

@bruin */

SELECT
  oi.id AS order_item_id,
  oi.order_id,
  oi.variant_id,
  oi.quantity,
  CASE WHEN oi.unit_price < 0 THEN 0 ELSE oi.unit_price END AS unit_price,
  CASE WHEN oi.total_price < 0 THEN 0 ELSE oi.total_price END AS total_price,
  oi.created_at
FROM raw.order_items oi;