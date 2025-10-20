/* @bruin
name: stg.order_items
type: duckdb.sql
materialization:
  type: table

tags:
  - staging

depends:
  - raw.order_items

columns:
  - extends: OrderItem.ID
  - extends: OrderItem.OrderID
  - extends: OrderItem.VariantID
  - extends: OrderItem.Quantity
  - extends: OrderItem.UnitPrice
  - extends: OrderItem.TotalPrice
  - extends: Base.CreatedAt

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