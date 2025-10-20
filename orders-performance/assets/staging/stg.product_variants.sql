/* @bruin

name: stg.product_variants
type: duckdb.sql

tags:
  - staging

materialization:
  type: table
  strategy: create+replace

depends:
  - raw.product_variants
  - stg.products

columns:
  - extends: Variant.ID
  - extends: Variant.ProductID
  - extends: Variant.VariantSKU
  - extends: Variant.Color
  - extends: Variant.Size
  - extends: Variant.ManufacturingPrice
  - extends: Variant.SellingPrice
  - extends: Variant.StockQuantity
  - extends: Variant.IsActive
  - extends: Base.CreatedAt
  - extends: Base.UpdatedAt

custom_checks:
  - name: validate product variant sizes
    description: |
      Ensures that if the product category is 'shoes', the size must be numeric.
      Otherwise, the size must be one of ['S', 'M', 'L'].
    value: 0
    query: |
      SELECT  
        COUNT(*) AS invalid_count
      FROM raw.product_variants v
      JOIN stg.products p ON p.product_id = v.product_id
      WHERE
        (p.category = 'shoes' AND NOT (v.size ~ '^[0-9]+$'))
        OR (p.category != 'shoes' AND v.size NOT IN ('S', 'M', 'L'))
    

@bruin */

-- Keep only variants belonging to known products, simple cleaning
SELECT
  pv.id AS variant_id,
  pv.product_id,
  pv.variant_sku,
  pv.color,
  pv.size,
  CASE WHEN pv.manufacturing_price < 0 THEN 0 ELSE pv.manufacturing_price END AS manufacturing_price,
  CASE WHEN pv.selling_price < 0 THEN 0 ELSE pv.selling_price END AS selling_price,
  CASE WHEN pv.stock_quantity < 0 THEN 0 ELSE pv.stock_quantity END AS stock_quantity,
  COALESCE(pv.is_active, TRUE) AS is_active,
  pv.created_at,
  pv.updated_at
FROM raw.product_variants pv
JOIN stg.products p ON p.product_id = pv.product_id;
