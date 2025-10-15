/* @bruin

name: mart.customers_by_age
type: duckdb.sql
tags:
  - mart
  - amazing-tag

materialization:
  type: table
  strategy: create+replace

depends:
  - stg.customers
owner: danielhe4rt@gmail.com

@bruin */

WITH src AS (SELECT CASE
                        WHEN age < 25 THEN '18-24'
                        WHEN age BETWEEN 25 AND 34 THEN '25-34'
                        WHEN age BETWEEN 35 AND 49 THEN '35-49'
                        ELSE '50+'
                        END AS age_group
             FROM stg.customers)
SELECT age_group,
       COUNT(*) AS total_customers
FROM src
GROUP BY age_group
ORDER BY total_customers DESC;
