/* @bruin

name: stg.orders
type: duckdb.sql

materialization:
  type: table
  strategy: create+replace

tags:
  - staging

depends:
  - raw.orders
  - raw.customers

columns:
  - extends: Order.ID
  - extends: Order.CustomerID
  - extends: Order.OrderDate
  - extends: Order.Status
  - extends: Order.TotalAmount

@bruin */

WITH src AS (
  SELECT
    o.id              AS order_id,
    o.customer_id,
    o.order_date,
    o.status,
    CASE WHEN o.total_amount < 0 THEN 0 ELSE o.total_amount END AS total_amount
  FROM raw.orders o
)
SELECT * FROM src;
