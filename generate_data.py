#!/usr/bin/env python3
import argparse, random, sys
from datetime import datetime, timedelta, timezone
from typing import Tuple
from pathlib import Path
import psycopg
from psycopg.rows import tuple_row
from faker import Faker


def parse_args():
    p = argparse.ArgumentParser(description="Generate ecommerce demo data into PostgreSQL")
    p.add_argument("--dsn", required=True, help="PostgreSQL DSN")
    p.add_argument("--start-date", required=True, help="ISO date YYYY-MM-DD")
    p.add_argument("--customers", type=int, default=100)
    p.add_argument("--products", type=int, default=50)
    p.add_argument("--variants-per-product", nargs=2, type=int, metavar=("MIN", "MAX"), default=[1, 3])
    p.add_argument("--orders", type=int, default=500)
    p.add_argument("--items-per-order", nargs=2, type=int, metavar=("MIN", "MAX"), default=[1, 5])
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--update-existing", action="store_true")
    p.add_argument("--replace-orders", action="store_true")
    p.add_argument("--ddl", help="Path to DDL SQL file (defaults to ./sql/ddl.sql)")
    p.add_argument("--yes", action="store_true")
    return p.parse_args()


def rand_dt(start: datetime, end: datetime) -> datetime:
    return start + timedelta(seconds=random.uniform(0, (end - start).total_seconds()))


def status_pick() -> str:
    r = random.random()
    return "pending" if r < 0.15 else ("paid" if r < 0.65 else ("shipped" if r < 0.90 else "cancelled"))


def ensure_datestr(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def init_schema(conn, ddl_path: str | None):
    """Always run DDL on connect to ensure tables exist (idempotent)."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS public; SET search_path TO public;")

    default_path = Path(__file__).parent / "sql" / "ddl.sql"
    path = Path(ddl_path) if ddl_path else default_path
    if not path.exists():
        raise FileNotFoundError(f"DDL file not found at {path}")

    sql_text = path.read_text()
    with conn.cursor() as cur:
        for stmt in sql_text.split(";"):
            s = stmt.strip()
            if s:
                cur.execute(s + ";")


def upsert_customers(conn, fk: Faker, n: int, start: datetime, now: datetime, update_existing: bool):
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("SELECT email, id FROM customers")
        existing = {e: i for e, i in cur.fetchall()}

    to_insert, emails = [], set(existing.keys())
    while len(to_insert) + len(existing) < n:
        name, email = fk.name(), fk.unique.email()
        if email in emails:
            continue
        emails.add(email)
        to_insert.append((name, email, fk.country(), fk.city(), rand_dt(start, now), rand_dt(start, now)))

    if to_insert:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO customers(full_name, email, country, city, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (email) DO NOTHING
                """,
                to_insert,
            )

    if update_existing:
        with conn.cursor() as cur:
            cur.execute("UPDATE customers SET updated_at = now() WHERE id IN (SELECT id FROM customers TABLESAMPLE SYSTEM(10))")

    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("SELECT id, email FROM customers")
        return {e: i for i, e in cur.fetchall()}


def upsert_products(conn, fk: Faker, n: int, start: datetime, now: datetime, update_existing: bool):
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("SELECT sku, id FROM products")
        existing = {s: i for s, i in cur.fetchall()}

    to_insert = []
    cats = ["Apparel", "Electronics", "Home", "Sports", "Toys", "Beauty"]
    skus = set(existing.keys())

    while len(to_insert) + len(existing) < n:
        name, sku = fk.unique.word().title(), f"PROD-{fk.unique.random_number(digits=6)}"
        if sku in skus:
            continue
        skus.add(sku)
        to_insert.append((name, random.choice(cats), sku, rand_dt(start, now), rand_dt(start, now)))

    if to_insert:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO products(name, category, sku, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (sku) DO NOTHING
                """,
                to_insert,
            )

    if update_existing:
        with conn.cursor() as cur:
            cur.execute("UPDATE products SET updated_at = now() WHERE id IN (SELECT id FROM products TABLESAMPLE SYSTEM(10))")

    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("SELECT sku, id FROM products")
        return {s: i for s, i in cur.fetchall()}


def upsert_variants(conn, product_ids: dict, per_range: Tuple[int, int], start: datetime, now: datetime, update_existing: bool):
    colors = ["Red", "Blue", "Green", "Black", "White", "Yellow", "Purple", "Gray"]
    sizes = ["XS", "S", "M", "L", "XL"]

    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("SELECT variant_sku, id FROM product_variants")
        existing = {s: i for s, i in cur.fetchall()}

    to_insert = []
    for _, pid in product_ids.items():
        for _ in range(random.randint(per_range[0], per_range[1])):
            c, z = random.choice(colors), random.choice(sizes)
            vsku = f"VAR-{pid}-{c[:3].upper()}-{z}"
            if vsku in existing:
                continue
            mfg = round(random.uniform(5, 100), 2)
            sell = round(mfg * random.uniform(1.1, 1.8), 2)
            to_insert.append(
                (pid, vsku, c, z, mfg, sell, random.randint(0, 500), random.random() < 0.85, rand_dt(start, now), rand_dt(start, now))
            )

    if to_insert:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO product_variants
                    (product_id, variant_sku, color, size, manufacturing_price, selling_price, stock_quantity, is_active, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (variant_sku)
                DO UPDATE SET updated_at = EXCLUDED.updated_at
                """,
                to_insert,
            )

    if update_existing:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE product_variants
                SET stock_quantity = GREATEST(0, stock_quantity + (random() * 10)::int - 5),
                    updated_at = now()
                WHERE id IN (SELECT id FROM product_variants TABLESAMPLE SYSTEM(10))
                """
            )

    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("SELECT id, selling_price FROM product_variants")
        return cur.fetchall()


def generate_orders(conn, customer_map: dict, variants: list, n_orders: int, item_range: Tuple[int, int], start: datetime, now: datetime):
    cust_ids = list(customer_map.values())
    orders = [(random.choice(cust_ids), rand_dt(start, now), status_pick()) for _ in range(n_orders)]

    with conn.cursor() as cur:
        cur.executemany("INSERT INTO orders(customer_id, order_date, status) VALUES (%s, %s, %s)", orders)

        cur.execute("SELECT id FROM orders ORDER BY id DESC LIMIT %s", (n_orders,))
        order_ids = [r[0] for r in cur.fetchall()][::-1]

        items, totals = [], {oid: 0 for oid in order_ids}
        for oid in order_ids:
            for _ in range(random.randint(item_range[0], item_range[1])):
                vid, price = random.choice(variants)
                qty = random.randint(1, 5)
                items.append((oid, vid, qty, price))
                totals[oid] += round(qty * price, 2)

        if items:
            cur.executemany("INSERT INTO order_items(order_id, variant_id, quantity, unit_price) VALUES (%s, %s, %s, %s)", items)

        cur.executemany("UPDATE orders SET total_amount = %s WHERE id = %s", [(amt, oid) for oid, amt in totals.items()])


def main():
    a = parse_args()
    random.seed(a.seed)
    fk = Faker()
    fk.seed_instance(a.seed)
    start = ensure_datestr(a.start_date)
    now = datetime.now(timezone.utc)

    with psycopg.connect(a.dsn, autocommit=False) as conn:
        conn.execute("SET statement_timeout TO '5min';")
        conn.execute("SET synchronous_commit TO OFF;")
        conn.execute("SET client_min_messages TO WARNING;")

        try:
            init_schema(conn, a.ddl)

            cust = upsert_customers(conn, fk, a.customers, start, now, a.update_existing)
            prods = upsert_products(conn, fk, a.products, start, now, a.update_existing)
            variants = upsert_variants(conn, prods, tuple(a.variants_per_product), start, now, a.update_existing)

            if a.replace_orders:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        DELETE FROM order_items USING orders
                        WHERE order_items.order_id = orders.id AND orders.order_date >= %s;
                        DELETE FROM orders WHERE order_date >= %s;
                        """,
                        (start, start),
                    )

            generate_orders(conn, cust, variants, a.orders, tuple(a.items_per_order), start, now)
            conn.commit()

        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Transaction rolled back due to: {e}", file=sys.stderr)
            raise

    print(
        f"""
✅ Data generation complete
───────────────────────────────
  Customers: {a.customers}
  Products: {a.products}
  Variants per product: {tuple(a.variants_per_product)}
  Orders inserted: {a.orders}
  Items per order: {tuple(a.items_per_order)}
  Time window: {a.start_date} → {now.date()}
  Seed: {a.seed}
  Flags: update_existing={a.update_existing}, replace_orders={a.replace_orders}
"""
    )


if __name__ == "__main__":
    main()
