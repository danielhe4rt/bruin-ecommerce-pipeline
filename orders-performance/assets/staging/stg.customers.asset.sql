/* @bruin

name: stg.customers
type: duckdb.sql

materialization:
  type: table

depends:
  - raw.customers

columns:
  - name: customer_id
    type: INTEGER
    owner: daniel@gmail.com
  - name: email
    type: VARCHAR
  - name: country
    type: VARCHAR
  - name: age
    type: BIGINT
  - name: created_at
    type: TIMESTAMPTZ
  - name: updated_at
    type: TIMESTAMPTZ

@bruin */

SELECT id::INT AS customer_id, COALESCE(TRIM(email), '') AS email,
       COALESCE(TRIM(country), 'Unknown') AS country,
       COALESCE(age, 0) AS age,
       created_at,
       updated_at
FROM raw.customers
WHERE email IS NOT NULL
