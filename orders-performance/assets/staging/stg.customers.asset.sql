/* @bruin

name: stg.customers
type: duckdb.sql

tags:
  - staging


materialization:
  type: table

depends:
  - raw.customers

owner: daniel@gmail.com

columns:
  - extends: Customer.ID
  - extends: Customer.Email
  - extends: Customer.Country
  - extends: Customer.Age
  - extends: Base.CreatedAt
  - extends: Base.UpdatedAt

@bruin */

SELECT id::INT AS customer_id, COALESCE(TRIM(email), '') AS email,
       COALESCE(TRIM(country), 'Unknown') AS country,
       COALESCE(age, 0) AS age,
       created_at,
       updated_at
FROM raw.customers
WHERE email IS NOT NULL
