/* @bruin

name: mart.sales_daily
type: duckdb.sql

materialization:
  type: table
  strategy: append

depends:
  - stg.order_items
  - stg.orders

columns:
  - name: sale_date
    type: date
    checks:
      - name: not_null
  - name: orders_count
    type: integer
    checks:
      - name: not_null
  - name: items_count
    type: integer
    checks:
      - name: not_null
  - name: revenue
    type: numeric
    checks:
      - name: not_null

custom_checks:
  - name: total revenue non-negative
    value: 0
    query: SELECT COUNT(*) FROM mart.sales_daily WHERE revenue < 0

@bruin */

WITH daily_orders AS (SELECT CAST(CAST(order_date AS TIMESTAMP) AS DATE)                               AS sale_date,
                             COUNT(*)                                                                  AS orders_count,
                             SUM(CASE WHEN status IN ('paid', 'shipped') THEN total_amount ELSE 0 END) AS revenue
                      FROM stg.orders
                      GROUP BY 1),
     daily_items AS (SELECT CAST(CAST(o.order_date AS TIMESTAMP) AS DATE) AS sale_date,
                            COUNT(*)                                      AS items_count
                     FROM stg.order_items oi
                              JOIN stg.orders o ON o.order_id = oi.order_id
                     GROUP BY 1)
SELECT d.sale_date,
       d.orders_count,
       COALESCE(i.items_count, 0) AS items_count,
       d.revenue
FROM daily_orders d  
ORDER BY sale_date;
