/* @bruin
name: stg.orders
type: duckdb.sql
materialization:
  type: table

depends:
  - raw.orders
  - raw.customers

columns:
  - name: order_id
    type: integer
    checks:
      - name: not_null
      - name: unique
  - name: customer_id
    type: integer
    checks:
      - name: not_null
  - name: order_date
    type: timestamp
    checks:
      - name: not_null
  - name: status
    type: string
    checks:
      - name: not_null
      - name: accepted_values
        value: [ 'pending', 'paid', 'cancelled', 'shipped' ]
  - name: total_amount
    type: numeric
    checks:
      - name: not_null
      - name: range
        min: 0

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