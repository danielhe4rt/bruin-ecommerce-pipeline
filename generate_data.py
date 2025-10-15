#!/usr/bin/env python3
"""
Generate realistic e-commerce demo data for PostgreSQL with time window and chaos injection.

Populates: customers, products, product_variants, orders, order_items
Idempotent & incremental: uses UPSERTs with conflict resolution.
Usage:
    uv run generate_data.py --dsn postgresql://user:pass@localhost:5432/ecommerce \
      --starting-at 2024-01-01 --ending-at 2024-06-30 --chaos-percent 5
"""

import argparse
import random
from datetime import datetime, timedelta, timezone
from faker import Faker
import psycopg

fake = Faker()
random.seed(42)  # deterministic output



def parse_args():
    now = datetime.now(timezone.utc)
    beginning_of_the_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_the_day = now.replace(hour=23, minute=59, second=59, microsecond=999999)

    p = argparse.ArgumentParser(description="Generate demo data for e-commerce tables.")
    p.add_argument("--dsn", required=True, help="PostgreSQL DSN")
    p.add_argument("--customers", type=int, default=100)
    p.add_argument("--products", type=int, default=50)
    p.add_argument("--orders", type=int, default=500)
    p.add_argument("--max-items-per-order", type=int, default=5)
    p.add_argument("--chaos-percent", type=float, default=0.0, help="Percentage of product variants with invalid sizes")
    p.add_argument("--starting-at", type=str,
                   default=beginning_of_the_day, help="ISO start date (e.g., 2024-01-01)")
    p.add_argument("--ending-at", type=str,
                   default=end_of_the_day,
                   help="ISO end date (e.g., 2024-06-30)")
    return p.parse_args()


def parse_date(date_str):
    if isinstance(date_str, str):
        return datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
    return date_str


def random_date(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))


def generate_customers(n):
    for _ in range(n):
        yield (
            fake.name(),
            fake.unique.email(),
            fake.country(),
            random.randint(18, 70),
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
        )


def generate_products(n):
    categories = ["t-shirts", "hoodies", "shoes", "accessories", "jackets"]
    for _ in range(n):
        cat = random.choice(categories)
        yield (
            f"{fake.word().capitalize()} {cat}",
            cat,
            fake.unique.bothify("SKU-####-??").upper(),
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
        )


def generate_product_variants(product_rows, chaos_percent):
    for pid, category in product_rows:
        for _ in range(random.randint(1, 4)):
            variant_sku = fake.unique.bothify("VAR-####-??").upper()
            color = random.choice(["Red", "Blue", "Black", "White", "Green", None])

            # Normal logic
            if category == "shoes":
                size = str(random.choice([36, 38, 40, 42, 44]))
            else:
                size = random.choice(["S", "M", "L", "XL"])

            # Controlled chaos
            if chaos_percent > 0 and random.random() < (chaos_percent / 100):
                if category == "shoes":
                    size = random.choice(["S", "M", "L"])  # wrong for shoes
                else:
                    size = str(random.choice([37, 41, 45]))  # wrong for non-shoes

            manuf = round(random.uniform(10, 80), 2)
            sell = round(manuf * random.uniform(1.2, 2.0), 2)
            yield (
                pid,
                variant_sku,
                color,
                size,
                manuf,
                sell,
                random.randint(0, 200),
                True,
                datetime.now(timezone.utc),
                datetime.now(timezone.utc),
            )


def generate_orders(customers, n, start, end):
    statuses = ["pending", "paid", "cancelled", "shipped"]
    for _ in range(n):
        yield (
            random.choice(customers),
            random_date(start, end),
            random.choices(statuses, weights=[0.2, 0.5, 0.1, 0.2])[0],
            0.0,
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
        )


def generate_order_items(order_ids, variant_ids, max_items):
    for oid in order_ids:
        for vid in random.sample(variant_ids, random.randint(1, max_items)):
            qty = random.randint(1, 3)
            yield (
                oid,
                vid,
                qty,
                round(random.uniform(20, 200), 2),
                datetime.now(timezone.utc),
            )


def upsert(conn, sql, data):
    with conn.cursor() as cur:
        cur.executemany(sql, data)
    conn.commit()


def main():
    args = parse_args()
    conn = psycopg.connect(args.dsn)

    start = parse_date(args.starting_at)
    end = parse_date(args.ending_at)
    print(f"Generating data between: \n Starting at: {start.strftime('%Y-%m-%d %H:%M:%S')} \n Ending at: {end.strftime('%Y-%m-%d %H:%M:%S')}")

    print(f"Inserting/updating {args.customers} customers...")
    upsert(
        conn,
        """
        INSERT INTO customers (full_name, email, country, age, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (email) DO
        UPDATE
            SET full_name = EXCLUDED.full_name,
            country = EXCLUDED.country,
            age = EXCLUDED.age,
            updated_at = EXCLUDED.updated_at;
        """,
        generate_customers(args.customers),
    )

    print(f"Inserting/updating {args.products} products...")
    upsert(
        conn,
        """
        INSERT INTO products (name, category, sku, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s) ON CONFLICT (sku) DO
        UPDATE
            SET name = EXCLUDED.name,
            category = EXCLUDED.category,
            updated_at = EXCLUDED.updated_at;
        """,
        generate_products(args.products),
    )

    product_rows = list(conn.execute("SELECT id, category FROM products"))
    print(f"Generating product variants (chaos: {args.chaos_percent}%)...")
    upsert(
        conn,
        """
        INSERT INTO product_variants
        (product_id, variant_sku, color, size, manufacturing_price, selling_price, stock_quantity, is_active,
         created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (variant_sku) DO
        UPDATE
            SET color = EXCLUDED.color,
            size = EXCLUDED.size,
            manufacturing_price = EXCLUDED.manufacturing_price,
            selling_price = EXCLUDED.selling_price,
            stock_quantity = EXCLUDED.stock_quantity,
            updated_at = EXCLUDED.updated_at;
        """,
        generate_product_variants(product_rows, args.chaos_percent),
    )

    customer_ids = [r[0] for r in conn.execute("SELECT id FROM customers")]
    print(f"Inserting/updating {args.orders} orders...")
    upsert(
        conn,
        """
        INSERT INTO orders (customer_id, order_date, status, total_amount, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO
        UPDATE
            SET status = EXCLUDED.status,
            total_amount = EXCLUDED.total_amount,
            updated_at = EXCLUDED.updated_at;
        """,
        generate_orders(customer_ids, args.orders, start, end),
    )

    order_ids = [r[0] for r in conn.execute("SELECT id FROM orders")]
    variant_ids = [r[0] for r in conn.execute("SELECT id FROM product_variants")]
    print("Inserting/updating order items...")
    upsert(
        conn,
        """
        INSERT INTO order_items (order_id, variant_id, quantity, unit_price, created_at)
        VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING;
        """,
        generate_order_items(order_ids, variant_ids, args.max_items_per_order),
    )

    print("✅ Data generation complete. Ready for incremental ingestion tests.")
    conn.close()


if __name__ == "__main__":
    main()
