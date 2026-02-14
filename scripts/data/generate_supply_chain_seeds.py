from __future__ import annotations
import random
from datetime import date, timedelta
from faker import Faker
import csv
import os

fake = Faker()
random.seed(42)

OUT_DIR = os.path.join("dbt", "supply_chain_dbt", "seeds")
os.makedirs(OUT_DIR, exist_ok=True)

# Size knobs (keep modest for now; we can scale later)
N_SUPPLIERS = 200
N_PRODUCTS = 800
N_WAREHOUSES = 8
N_ORDERS = 20000
N_SHIPMENTS = 24000
N_DAILY_INVENTORY_DAYS = 120  # snapshot-like table

START_DATE = date.today() - timedelta(days=180)
END_DATE = date.today()

def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)

def write_csv(path: str, header: list[str], rows: list[list]):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

# --- suppliers ---
suppliers = []
supplier_rows = []
for i in range(1, N_SUPPLIERS + 1):
    sid = f"S{i:04d}"
    name = fake.company().replace(",", "")
    country = fake.country_code()
    base_lead_time = random.randint(3, 25)
    reliability = round(random.uniform(0.75, 0.99), 3)
    suppliers.append((sid, base_lead_time, reliability))
    supplier_rows.append([sid, name, country, base_lead_time, reliability])

write_csv(
    os.path.join(OUT_DIR, "raw_suppliers.csv"),
    ["supplier_id", "supplier_name", "country_code", "base_lead_time_days", "reliability_score"],
    supplier_rows
)

# --- warehouses ---
warehouses = []
wh_rows = []
for i in range(1, N_WAREHOUSES + 1):
    wid = f"W{i:02d}"
    city = fake.city().replace(",", "")
    state = fake.state_abbr()
    warehouses.append(wid)
    wh_rows.append([wid, city, state])

write_csv(
    os.path.join(OUT_DIR, "raw_warehouses.csv"),
    ["warehouse_id", "city", "state"],
    wh_rows
)

# --- products ---
products = []
prod_rows = []
categories = ["Electronics", "Home", "Apparel", "Beauty", "Tools", "Sports", "Office", "Grocery"]
for i in range(1, N_PRODUCTS + 1):
    pid = f"P{i:05d}"
    category = random.choice(categories)
    supplier_id, _, _ = random.choice(suppliers)
    unit_cost = round(random.uniform(2.0, 250.0), 2)
    products.append((pid, supplier_id, unit_cost))
    prod_rows.append([pid, category, supplier_id, unit_cost])

write_csv(
    os.path.join(OUT_DIR, "raw_products.csv"),
    ["product_id", "category", "primary_supplier_id", "unit_cost_usd"],
    prod_rows
)

# --- orders ---
order_rows = []
orders = []
for i in range(1, N_ORDERS + 1):
    oid = f"O{i:07d}"
    order_date = START_DATE + timedelta(days=random.randint(0, (END_DATE - START_DATE).days))
    warehouse_id = random.choice(warehouses)
    product_id, supplier_id, unit_cost = random.choice(products)
    qty = max(1, int(random.gauss(12, 6)))
    order_value = round(qty * unit_cost, 2)
    orders.append((oid, order_date, warehouse_id, product_id, supplier_id, qty))
    order_rows.append([oid, order_date.isoformat(), warehouse_id, product_id, supplier_id, qty, order_value])

write_csv(
    os.path.join(OUT_DIR, "raw_orders.csv"),
    ["order_id", "order_date", "warehouse_id", "product_id", "supplier_id", "order_qty", "order_value_usd"],
    order_rows
)

# --- shipments (incremental-friendly; some orders ship split/late) ---
shipment_rows = []
status_choices = ["in_transit", "delivered", "delayed", "cancelled"]
for i in range(1, N_SHIPMENTS + 1):
    shid = f"SH{i:08d}"
    oid, order_date, warehouse_id, product_id, supplier_id, qty = random.choice(orders)

    # lead time influenced by supplier base lead time + randomness
    base_lt = next(x[1] for x in suppliers if x[0] == supplier_id)
    noise = int(random.gauss(2, 3))
    lead_time = max(1, base_lt + noise)

    shipped_date = order_date + timedelta(days=random.randint(0, 2))
    delivered_date = shipped_date + timedelta(days=lead_time)

    status = random.choices(
        population=status_choices,
        weights=[0.10, 0.80, 0.08, 0.02],
        k=1
    )[0]

    if status == "delivered":
        actual_delivery = delivered_date
    elif status == "delayed":
        actual_delivery = delivered_date + timedelta(days=random.randint(2, 10))
    elif status == "in_transit":
        actual_delivery = ""  # not yet delivered
    else:  # cancelled
        actual_delivery = ""

    carrier = random.choice(["UPS", "FedEx", "DHL", "USPS", "XPO", "Maersk"])
    shipped_qty = max(1, int(random.gauss(qty, max(1, qty * 0.2))))
    shipment_rows.append([
        shid,
        oid,
        supplier_id,
        warehouse_id,
        product_id,
        shipped_date.isoformat(),
        (actual_delivery if actual_delivery == "" else actual_delivery.isoformat()),
        status,
        carrier,
        shipped_qty
    ])

write_csv(
    os.path.join(OUT_DIR, "raw_shipments.csv"),
    ["shipment_id", "order_id", "supplier_id", "warehouse_id", "product_id",
     "shipped_date", "delivered_date", "shipment_status", "carrier", "shipped_qty"],
    shipment_rows
)

# --- daily inventory snapshots (acts like a snapshot source table) ---
inv_rows = []
for d in daterange(END_DATE - timedelta(days=N_DAILY_INVENTORY_DAYS), END_DATE):
    for _ in range(1500):  # rows per day
        warehouse_id = random.choice(warehouses)
        product_id, supplier_id, _ = random.choice(products)
        on_hand = max(0, int(random.gauss(120, 55)))
        reserved = max(0, int(random.gauss(15, 10)))
        available = max(0, on_hand - reserved)
        inv_rows.append([d.isoformat(), warehouse_id, product_id, supplier_id, on_hand, reserved, available])

write_csv(
    os.path.join(OUT_DIR, "raw_inventory_daily.csv"),
    ["snapshot_date", "warehouse_id", "product_id", "supplier_id", "on_hand_qty", "reserved_qty", "available_qty"],
    inv_rows
)

print(f"✅ Wrote seed CSVs to: {OUT_DIR}")
