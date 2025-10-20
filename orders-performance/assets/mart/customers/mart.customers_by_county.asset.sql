/* @bruin

name: mart.customers_by_country
type: duckdb.sql

materialization:
  type: table
  strategy: create+replace

depends:
  - stg.customers

@bruin */

SELECT
    country,
    COUNT(*) AS total_customers
  FROM stg.customers
  GROUP BY country
  ORDER BY total_customers DESC
