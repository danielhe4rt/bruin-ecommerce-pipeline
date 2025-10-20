#!/usr/bin/env python3
"""
E-commerce demo data generator (Postgres).

Usage:
    uv run generate-data --dsn postgresql://user:pass@host:5432/db
"""

import argparse, random
from datetime import datetime, timedelta, timezone
from faker import Faker
import psycopg

# -------------------------------------------------------------------
# GLOBAL CONFIGURATION
# -------------------------------------------------------------------

fake = Faker()

# --- Static categorical constants ---
PRODUCT_CATEGORIES = ["t-shirts", "hoodies", "shoes", "accessories", "jackets"]
COLORS = ["Red", "Blue", "Black", "White", "Green"]
SIZES_APPAREL = ["S", "M", "L", "XL"]
SIZES_SHOES = [36, 38, 40, 42, 44]
ORDER_STATUSES = ["pending", "paid", "cancelled", "shipped"]
ORDER_STATUS_WEIGHTS = [0.2, 0.5, 0.1, 0.2]

# --- SQL Statements ---
SQL_INSERT_CUSTOMERS = """
                       INSERT INTO customers(full_name, email, country, age, created_at, updated_at)
                       VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (email) DO
                       UPDATE
                           SET full_name=EXCLUDED.full_name, country=EXCLUDED.country,
                           age=EXCLUDED.age, updated_at=EXCLUDED.updated_at; \
                       """

SQL_INSERT_PRODUCTS = """
                      INSERT INTO products(name, category, sku, created_at, updated_at)
                      VALUES (%s, %s, %s, %s, %s) ON CONFLICT (sku) DO
                      UPDATE
                          SET name=EXCLUDED.name, category=EXCLUDED.category,
                          updated_at=EXCLUDED.updated_at; \
                      """

SQL_INSERT_VARIANTS = """
                      INSERT INTO product_variants
                      (product_id, variant_sku, color, size, manufacturing_price, selling_price,
                       stock_quantity, is_active, created_at, updated_at)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (variant_sku) DO
                      UPDATE
                          SET color=EXCLUDED.color, size =EXCLUDED.size,
                          manufacturing_price=EXCLUDED.manufacturing_price,
                          selling_price=EXCLUDED.selling_price,
                          stock_quantity=EXCLUDED.stock_quantity,
                          updated_at=EXCLUDED.updated_at; \
                      """

SQL_DELETE_ORDERS_IN_WINDOW = "DELETE FROM orders WHERE order_date BETWEEN %s AND %s;"

SQL_INSERT_ORDERS = """
                    INSERT INTO orders(customer_id, order_date, status, total_amount, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s); \
                    """

SQL_INSERT_ORDER_ITEMS = """
                         INSERT INTO order_items(order_id, variant_id, quantity, unit_price, created_at)
                         VALUES (%s, %s, %s, %s, %s); \
                         """

SQL_UPDATE_ORDER_TOTALS = """
                          UPDATE orders o
                          SET total_amount = COALESCE(oi.sum_total, 0) FROM (
  SELECT order_id, SUM(total_price) sum_total
  FROM order_items GROUP BY order_id
) oi
                          WHERE o.id = oi.order_id
                            AND o.order_date BETWEEN %s
                            AND %s; \
                          """

SQL_STATS_COUNTS = {
    "customers": "SELECT COUNT(*) FROM customers",
    "products": "SELECT COUNT(*) FROM products",
    "variants": "SELECT COUNT(*) FROM product_variants",
    "orders": "SELECT COUNT(*) FROM orders WHERE order_date BETWEEN %s AND %s",
    "items": """
             SELECT COUNT(*)
             FROM order_items oi
                      JOIN orders o ON o.id = oi.order_id
             WHERE o.order_date BETWEEN %s AND %s;
             """,
}

SQL_CHAOS_VALIDATION = """
                       SELECT p.category,
                              SUM(CASE
                                      WHEN (p.category = 'shoes' AND v.size !~ '^[0-9]+$')
                                          OR (p.category <> 'shoes' AND v.size NOT IN ('S', 'M', 'L', 'XL'))
                                          THEN 1
                                      ELSE 0 END) invalid
                       FROM product_variants v
                                JOIN products p ON p.id = v.product_id
                       GROUP BY 1
                       ORDER BY 1; \
                       """


def parse_args():
    now = datetime.now(timezone.utc)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = now.replace(hour=23, minute=59, second=59, microsecond=999999)

    p = argparse.ArgumentParser(description="Generate demo ecommerce data (idempotent).")
    p.add_argument("--dsn", required=True, help="PostgreSQL DSN")
    p.add_argument("--customers", type=int, default=100)
    p.add_argument("--products", type=int, default=50)
    p.add_argument("--orders", type=int, default=500)
    p.add_argument("--max-items-per-order", type=int, default=5)
    p.add_argument("--chaos-percent", type=float, default=0.0, help="%% of variants with invalid sizes")
    p.add_argument("--scale", type=int, default=1, help="Multiply base volumes")
    p.add_argument("--seed", type=int, default=42, help="Deterministic RNG seed")
    p.add_argument("--starting-at", type=str, help="ISO start (e.g. 2024-01-01)")
    p.add_argument("--ending-at", type=str, help="ISO end (e.g. 2024-06-30)")

    a = p.parse_args()
    a.starting_at = a.starting_at or day_start.isoformat()
    a.ending_at = a.ending_at or day_end.isoformat()
    return a


def iso(dt):
    return datetime.fromisoformat(dt).astimezone(timezone.utc)


def rdate(s, e):
    """Return random datetime between s and e."""
    return s + timedelta(seconds=random.randint(0, int((e - s).total_seconds())))


def execmany(conn, sql, rows):
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()

def gen_customers(n):
    for _ in range(n):
        yield (
            fake.name(),
            fake.unique.email(),
            fake.country(),
            random.randint(18, 70),
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
        )


def gen_products(n):
    for _ in range(n):
        cat = random.choice(PRODUCT_CATEGORIES)
        yield (
            f"{fake.word().capitalize()} {cat}",
            cat,
            fake.unique.bothify("SKU-####-??").upper(),
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
        )


def gen_variants(prod_rows, chaos):
    for pid, cat in prod_rows:
        for _ in range(random.randint(1, 4)):
            sku = fake.unique.bothify("VAR-####-??").upper()
            color = random.choice(COLORS)
            size = str(random.choice(SIZES_SHOES)) if cat == "shoes" else random.choice(SIZES_APPAREL)

            # Chaos mode
            if chaos > 0 and random.random() < (chaos / 100):
                if cat == "shoes":
                    size = random.choice(SIZES_APPAREL)
                else:
                    size = str(random.choice(SIZES_SHOES))

            manuf = round(random.uniform(10, 80), 2)
            sell = round(manuf * random.uniform(1.2, 2.0), 2)
            yield (
                pid, sku, color, size, manuf, sell,
                random.randint(0, 200), True,
                datetime.now(timezone.utc), datetime.now(timezone.utc),
            )


def gen_orders(cust_ids, n, s, e):
    for _ in range(n):
        yield (random.choice(cust_ids), rdate(s, e), random.choices(ORDER_STATUSES, ORDER_STATUS_WEIGHTS)[0], 0.0,
               datetime.now(timezone.utc), datetime.now(timezone.utc),)


def gen_items(order_ids, variant_ids, max_items):
    for oid in order_ids:
        for vid in random.sample(variant_ids, random.randint(1, max(1, max_items))):
            yield (oid, vid, random.randint(1, 3), round(random.uniform(20, 200), 2), datetime.now(timezone.utc))


def print_box_summary(S, E, c_cnt, p_cnt, v_cnt, o_cnt, i_cnt, max_items, bad_by_cat):
    print("\n✅ Data generation complete (idempotent window load)")
    print(f"Window: {S.isoformat()} → {E.isoformat()}")
    print(f"Counts — Customers: {c_cnt:,} | Products: {p_cnt:,} | Variants: {v_cnt:,}")
    print(f"Window Facts — Orders: {o_cnt:,} | Items: {i_cnt:,} | MaxItems/Order: {max_items}")
    print("Chaos check (invalid sizes per category):")
    for cat, bad in bad_by_cat:
        print(f"  • {cat}: {bad:,} invalid")


def main():
    args = parse_args()
    random.seed(args.seed)
    Faker.seed(args.seed)
    S, E = iso(args.starting_at), iso(args.ending_at)

    print("\n=== Generating E-commerce Demo Data ===")
    print(f"Window: {S.isoformat()} → {E.isoformat()}")
    print(f"Scale x{args.scale} | Seed {args.seed} | Chaos {args.chaos_percent:.1f}%\n")

    base_c, base_p, base_o = args.customers, args.products, args.orders
    n_c, n_p, n_o = base_c * args.scale, base_p * args.scale, base_o * args.scale

    with psycopg.connect(args.dsn) as conn:
        execmany(conn, SQL_INSERT_CUSTOMERS, gen_customers(n_c))
        execmany(conn, SQL_INSERT_PRODUCTS, gen_products(n_p))

        prod_rows = list(conn.execute("SELECT id, category FROM products"))
        execmany(conn, SQL_INSERT_VARIANTS, gen_variants(prod_rows, args.chaos_percent))

        with conn.cursor() as cur:
            cur.execute(SQL_DELETE_ORDERS_IN_WINDOW, (S, E))
        conn.commit()

        cust_ids = [r[0] for r in conn.execute("SELECT id FROM customers")]
        execmany(conn, SQL_INSERT_ORDERS, gen_orders(cust_ids, n_o, S, E))

        order_ids = [r[0] for r in conn.execute("SELECT id FROM orders WHERE order_date BETWEEN %s AND %s", (S, E))]
        variant_ids = [r[0] for r in conn.execute("SELECT id FROM product_variants")]
        execmany(conn, SQL_INSERT_ORDER_ITEMS, gen_items(order_ids, variant_ids, args.max_items_per_order))

        with conn.cursor() as cur:
            cur.execute(SQL_UPDATE_ORDER_TOTALS, (S, E))
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(SQL_STATS_COUNTS["customers"]);
            c_cnt = cur.fetchone()[0]
            cur.execute(SQL_STATS_COUNTS["products"]);
            p_cnt = cur.fetchone()[0]
            cur.execute(SQL_STATS_COUNTS["variants"]);
            v_cnt = cur.fetchone()[0]
            cur.execute(SQL_STATS_COUNTS["orders"], (S, E));
            o_cnt = cur.fetchone()[0]
            cur.execute(SQL_STATS_COUNTS["items"], (S, E));
            i_cnt = cur.fetchone()[0]
            cur.execute(SQL_CHAOS_VALIDATION);
            bad_by_cat = cur.fetchall()

    print_box_summary(S, E, c_cnt, p_cnt, v_cnt, o_cnt, i_cnt, args.max_items_per_order, bad_by_cat)


if __name__ == "__main__":
    main()
