/* @bruin

name: stg.test
type: duckdb.sql

materialization:
  type: table

@bruin */

SELECT '{{ end_date }}'
FROM raw.customers
WHERE email IS NOT NULL
