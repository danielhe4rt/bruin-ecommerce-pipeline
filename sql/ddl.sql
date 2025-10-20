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
