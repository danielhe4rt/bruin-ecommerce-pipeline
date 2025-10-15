-- In case you want to recreate the database, run the following commands:
drop database if exists ecommerce;

create database ecommerce;
\c ecommerce;


CREATE TABLE IF NOT EXISTS customers
(
    id         BIGSERIAL PRIMARY KEY,
    full_name  TEXT        NOT NULL,
    email      TEXT        NOT NULL UNIQUE,
    country    TEXT        NOT NULL,
    age        INTEGER     NOT NULL CHECK (age >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);


CREATE TABLE IF NOT EXISTS products
(
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT        NOT NULL,
    category   TEXT        NOT NULL,
    sku        TEXT        NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);


CREATE TABLE IF NOT EXISTS product_variants
(
    id                  BIGSERIAL PRIMARY KEY,
    product_id          BIGINT         NOT NULL REFERENCES products (id) ON DELETE CASCADE,
    variant_sku         TEXT           NOT NULL UNIQUE,
    color               TEXT,
    size                TEXT,
    manufacturing_price NUMERIC(10, 2) NOT NULL,
    selling_price       NUMERIC(10, 2) NOT NULL,
    stock_quantity      INTEGER        NOT NULL,
    is_active           BOOLEAN        NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ    NOT NULL DEFAULT now()
);


CREATE TABLE IF NOT EXISTS orders
(
    id           BIGSERIAL PRIMARY KEY,
    customer_id  BIGINT      NOT NULL REFERENCES customers (id),
    order_date   TIMESTAMPTZ NOT NULL DEFAULT now(),
    status       TEXT        NOT NULL,
    total_amount NUMERIC(12, 2),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);


CREATE TABLE IF NOT EXISTS order_items
(
    id          BIGSERIAL PRIMARY KEY,
    order_id    BIGINT         NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    variant_id  BIGINT         NOT NULL REFERENCES product_variants (id),
    quantity    INTEGER        NOT NULL,
    unit_price  NUMERIC(10, 2) NOT NULL,
    total_price NUMERIC(12, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    created_at  TIMESTAMPTZ    NOT NULL DEFAULT now()
);


WITH paid_items AS (SELECT oi.*
                    FROM order_items oi
                             JOIN orders o ON o.id = oi.order_id
                    WHERE o.status IN ('paid', 'shipped')),
     joined AS (SELECT v.id,
                       p.name as product_name,
                       v.variant_sku,
                       v.product_id,
                       v.manufacturing_price,
                       v.selling_price,
                       oi.quantity,
                       oi.total_price
                FROM paid_items oi
                         JOIN product_variants v ON v.id = oi.variant_id
                         JOIN products p ON p.id = v.product_id)
SELECT j.id,
       j.product_name,
       j.variant_sku,
       j.product_id,
       SUM(j.quantity)                                              AS items_sold,
       SUM(j.total_price)                                           AS revenue,
       SUM(j.quantity * j.manufacturing_price)                      AS cost,
       SUM(j.total_price) - SUM(j.quantity * j.manufacturing_price) AS profit,
       CASE
           WHEN SUM(j.total_price) = 0 THEN 0
           ELSE (SUM(j.total_price) - SUM(j.quantity * j.manufacturing_price)) / SUM(j.total_price)
           END                                                      AS margin_pct,
       -- helper flag for custom check logic
       BOOL_OR(j.selling_price < j.manufacturing_price)             AS selling_price_below_cost
FROM joined j
GROUP BY 1, 2, 3, 4
ORDER BY profit DESC;