import csv
import os
import random
from datetime import date, timedelta


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

RANDOM_SEED = 42

CUSTOMER_COUNT = 350
SALES_COUNT = 1000
SERVICE_COUNT = 500
APPOINTMENT_COUNT = 800
SURVEY_COUNT = 1200
FEEDBACK_COUNT = 900
MOVEMENT_COUNT = 1800
STORE_KPI_MONTHS = 12
STORE_IDS = list(range(501, 526))

REGIONS = ["North", "South", "East", "West", "Midwest"]

STATE_CITY_MAP = {
    "AZ": {"region": "West", "cities": ["Phoenix", "Tucson", "Mesa"]},
    "TX": {"region": "South", "cities": ["Dallas", "Houston", "Austin"]},
    "CA": {"region": "West", "cities": ["Los Angeles", "San Diego", "Sacramento"]},
    "CO": {"region": "West", "cities": ["Denver", "Colorado Springs", "Fort Collins"]},
    "FL": {"region": "South", "cities": ["Orlando", "Tampa", "Jacksonville"]},
    "GA": {"region": "South", "cities": ["Atlanta", "Savannah", "Augusta"]},
    "NC": {"region": "South", "cities": ["Charlotte", "Raleigh", "Greensboro"]},
    "TN": {"region": "South", "cities": ["Nashville", "Memphis", "Knoxville"]},
    "IL": {"region": "Midwest", "cities": ["Chicago", "Naperville", "Peoria"]},
    "OH": {"region": "Midwest", "cities": ["Columbus", "Cleveland", "Cincinnati"]},
}

PRODUCTS = [
    (1, "AllSeason Radial A/S", "Tire", 120.0),
    (2, "HighPerformance ZR", "Tire", 260.0),
    (3, "Truck Terrain LT", "Tire", 210.0),
    (4, "Eco Touring", "Tire", 140.0),
    (5, "WinterGrip Ice", "Tire", 180.0),
    (6, "Wheel Alloy 18in", "Wheel", 185.0),
    (7, "Wheel Sport 20in", "Wheel", 260.0),
    (8, "TPMS Sensor Accessory", "Accessory", 40.0),
    (9, "Lug Nut Kit", "Accessory", 25.0),
    (10, "Wiper Blades Premium", "Accessory", 30.0),
    (11, "Tire Rotation Service", "Service", 0.0),
    (12, "Flat Repair Service", "Service", 0.0),
]

SERVICE_TYPES = [
    ("Tire Rotation", 0.0),
    ("Flat Repair", 0.0),
    ("Wheel Balance", 20.0),
    ("TPMS Reset", 15.0),
]

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
]
PROMOTIONS = [
    (9001, "Winter Safety Event", "Percent", 10.0, "Tire", "Online"),
    (9002, "Wheel Upgrade Week", "Percent", 15.0, "Wheel", "InStore"),
    (9003, "Service Bundle", "Fixed", 20.0, "Service", "Online"),
    (9004, "Accessory Add-On", "Fixed", 10.0, "Accessory", "InStore"),
    (9005, "Spring Road Trip", "Percent", 12.0, "Tire", "Online"),
]

APPOINTMENT_TYPES = ["Install", "Rotation", "Repair", "Inspection"]
APPOINTMENT_STATUS = ["Scheduled", "Completed", "NoShow", "Cancelled"]
APPOINTMENT_CHANNELS = ["Online", "InStore", "Phone"]

SURVEY_CHANNELS = ["Email", "SMS", "InStore"]
FEEDBACK_SENTIMENTS = ["positive", "neutral", "negative"]
FEEDBACK_TOPICS = ["Service Quality", "Wait Time", "Pricing", "Staff Friendliness", "Product Selection"]
MOVEMENT_TYPES = ["Receipt", "Sale", "Transfer", "Adjustment", "Return"]


def random_date(start: date, end: date) -> date:
    days_range = (end - start).days
    return start + timedelta(days=random.randint(0, days_range))


def ensure_data_dir() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)


def generate_customers() -> list[dict]:
    customers = []
    start = date(2023, 1, 1)
    end = date(2025, 1, 15)
    for cid in range(101, 101 + CUSTOMER_COUNT):
        join = random_date(start, end)
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        customers.append(
            {
                "customer_id": cid,
                "first_name": first_name,
                "last_name": last_name,
                "region": random.choice(REGIONS),
                "join_date": join.isoformat(),
                "satisfaction_score": round(random.uniform(3.2, 5.0), 1),
            }
        )
    return customers


def generate_sales(customers: list[dict]) -> list[dict]:
    sales = []
    start = date(2025, 1, 1)
    end = date(2025, 12, 31)
    product_ids = [p[0] for p in PRODUCTS]
    service_product_ids = [p[0] for p in PRODUCTS if p[2] == "Service"]
    for sid in range(1001, 1001 + SALES_COUNT):
        customer_id = random.choice(customers)["customer_id"]
        product_id = random.choice(product_ids)
        product = next(p for p in PRODUCTS if p[0] == product_id)
        quantity = random.randint(1, 4)
        unit_price = product[3]
        if product_id in service_product_ids:
            quantity = 1
        promotion_id = random.choice([None, None, None] + [p[0] for p in PROMOTIONS])
        total_amount = round(quantity * unit_price, 2)
        sales.append(
            {
                "sale_id": sid,
                "date": random_date(start, end).isoformat(),
                "customer_id": customer_id,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": total_amount,
                "store_id": random.choice(STORE_IDS),
                "promotion_id": promotion_id,
            }
        )
    return sales


def generate_inventory() -> list[dict]:
    inventory = []
    for store_id in STORE_IDS:
        for product_id, _, category, _ in PRODUCTS:
            if category == "Service":
                continue
            stock_qty = random.randint(10, 120)
            reorder_threshold = random.randint(10, 35)
            inventory.append(
                {
                    "store_id": store_id,
                    "product_id": product_id,
                    "stock_qty": stock_qty,
                    "reorder_threshold": reorder_threshold,
                }
            )
    return inventory


def generate_services(customers: list[dict]) -> list[dict]:
    services = []
    start = date(2025, 1, 1)
    end = date(2025, 12, 31)
    for sid in range(2001, 2001 + SERVICE_COUNT):
        customer_id = random.choice(customers)["customer_id"]
        service_type, fee = random.choice(SERVICE_TYPES)
        services.append(
            {
                "service_id": sid,
                "customer_id": customer_id,
                "date": random_date(start, end).isoformat(),
                "service_type": service_type,
                "service_fee": round(fee, 2),
            }
        )
    return services


def generate_stores() -> list[dict]:
    stores = []
    start = date(1999, 1, 1)
    end = date(2024, 12, 31)
    states = list(STATE_CITY_MAP.keys())
    for store_id in STORE_IDS:
        state = random.choice(states)
        region = STATE_CITY_MAP[state]["region"]
        city = random.choice(STATE_CITY_MAP[state]["cities"])
        stores.append(
            {
                "store_id": store_id,
                "store_name": f"Discount Tire {city}, {state}",
                "region": region,
                "state": state,
                "city": city,
                "opened_date": random_date(start, end).isoformat(),
                "manager_name": f"Manager{store_id}",
            }
        )
    return stores


def generate_promotions() -> list[dict]:
    start = date(2025, 1, 1)
    promotions = []
    for promo_id, name, discount_type, discount_value, category, channel in PROMOTIONS:
        promo_start = random_date(start, date(2025, 9, 1))
        promo_end = promo_start + timedelta(days=random.randint(14, 45))
        promotions.append(
            {
                "promo_id": promo_id,
                "promo_name": name,
                "start_date": promo_start.isoformat(),
                "end_date": promo_end.isoformat(),
                "discount_type": discount_type,
                "discount_value": discount_value,
                "applies_to_category": category,
                "channel": channel,
            }
        )
    return promotions


def generate_appointments(customers: list[dict]) -> list[dict]:
    appointments = []
    start = date(2025, 1, 1)
    end = date(2025, 12, 31)
    for aid in range(3001, 3001 + APPOINTMENT_COUNT):
        customer_id = random.choice(customers)["customer_id"]
        appointment_date = random_date(start, end)
        booked_days_ahead = random.randint(0, 21)
        appointments.append(
            {
                "appointment_id": aid,
                "customer_id": customer_id,
                "store_id": random.choice(STORE_IDS),
                "appointment_date": appointment_date.isoformat(),
                "appointment_type": random.choice(APPOINTMENT_TYPES),
                "status": random.choice(APPOINTMENT_STATUS),
                "channel": random.choice(APPOINTMENT_CHANNELS),
                "booked_days_ahead": booked_days_ahead,
                "estimated_wait_minutes": random.randint(10, 90),
            }
        )
    return appointments


def generate_surveys(customers: list[dict]) -> list[dict]:
    surveys = []
    start = date(2025, 1, 1)
    end = date(2025, 12, 31)
    for sid in range(4001, 4001 + SURVEY_COUNT):
        customer_id = random.choice(customers)["customer_id"]
        responses = random.randint(1, 5)
        score = round(random.uniform(3.0, 5.0), 1)
        nps_category = random.choices(
            ["Promoter", "Passive", "Detractor"], weights=[0.6, 0.25, 0.15], k=1
        )[0]
        surveys.append(
            {
                "survey_id": sid,
                "customer_id": customer_id,
                "survey_date": random_date(start, end).isoformat(),
                "satisfaction_score": score,
                "nps_category": nps_category,
                "response_count": responses,
                "channel": random.choice(SURVEY_CHANNELS),
            }
        )
    return surveys


def generate_feedback_topics(customers: list[dict]) -> list[dict]:
    feedback = []
    start = date(2025, 1, 1)
    end = date(2025, 12, 31)
    for fid in range(5001, 5001 + FEEDBACK_COUNT):
        customer_id = random.choice(customers)["customer_id"]
        topic = random.choice(FEEDBACK_TOPICS)
        sentiment = random.choices(FEEDBACK_SENTIMENTS, weights=[0.6, 0.25, 0.15], k=1)[0]
        feedback.append(
            {
                "feedback_id": fid,
                "customer_id": customer_id,
                "feedback_date": random_date(start, end).isoformat(),
                "topic": topic,
                "sentiment": sentiment,
                "mentions": random.randint(1, 3),
            }
        )
    return feedback


def generate_inventory_movements() -> list[dict]:
    movements = []
    start = date(2025, 1, 1)
    end = date(2025, 12, 31)
    product_ids = [p[0] for p in PRODUCTS if p[2] != "Service"]
    for mid in range(6001, 6001 + MOVEMENT_COUNT):
        movement_type = random.choice(MOVEMENT_TYPES)
        quantity = random.randint(1, 20)
        if movement_type in {"Sale", "Return", "Adjustment"}:
            quantity = -abs(quantity) if movement_type != "Return" else abs(quantity)
        movements.append(
            {
                "movement_id": mid,
                "movement_date": random_date(start, end).isoformat(),
                "store_id": random.choice(STORE_IDS),
                "product_id": random.choice(product_ids),
                "movement_type": movement_type,
                "quantity": quantity,
                "reason": movement_type,
            }
        )
    return movements


def generate_store_kpis() -> list[dict]:
    kpis = []
    start_month = date(2025, 1, 1)
    for store_id in STORE_IDS:
        for month_offset in range(STORE_KPI_MONTHS):
            month_start = date(start_month.year, month_offset + 1, 1)
            kpis.append(
                {
                    "store_id": store_id,
                    "month": month_start.isoformat(),
                    "operational_efficiency": random.randint(80, 98),
                    "avg_wait_minutes": random.randint(10, 45),
                    "daily_throughput_units": random.randint(80, 180),
                    "service_attach_rate": round(random.uniform(0.35, 0.7), 2),
                }
            )
    return kpis


def write_csv(filename: str, rows: list[dict], fieldnames: list[str]) -> None:
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    random.seed(RANDOM_SEED)
    ensure_data_dir()

    customers = generate_customers()
    sales = generate_sales(customers)
    inventory = generate_inventory()
    services = generate_services(customers)
    stores = generate_stores()
    promotions = generate_promotions()
    appointments = generate_appointments(customers)
    surveys = generate_surveys(customers)
    feedback_topics = generate_feedback_topics(customers)
    inventory_movements = generate_inventory_movements()
    store_kpis = generate_store_kpis()

    write_csv(
        "customers.csv",
        customers,
        ["customer_id", "first_name", "last_name", "region", "join_date", "satisfaction_score"],
    )
    write_csv(
        "products.csv",
        [
            {
                "product_id": pid,
                "product_name": name,
                "category": category,
                "list_price": price,
            }
            for pid, name, category, price in PRODUCTS
        ],
        ["product_id", "product_name", "category", "list_price"],
    )
    write_csv(
        "sales.csv",
        sales,
        [
            "sale_id",
            "date",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "total_amount",
            "store_id",
            "promotion_id",
        ],
    )
    write_csv(
        "stores.csv",
        stores,
        [
            "store_id",
            "store_name",
            "region",
            "state",
            "city",
            "opened_date",
            "manager_name",
        ],
    )
    write_csv(
        "promotions.csv",
        promotions,
        [
            "promo_id",
            "promo_name",
            "start_date",
            "end_date",
            "discount_type",
            "discount_value",
            "applies_to_category",
            "channel",
        ],
    )
    write_csv(
        "inventory.csv",
        inventory,
        ["store_id", "product_id", "stock_qty", "reorder_threshold"],
    )
    write_csv(
        "services.csv",
        services,
        ["service_id", "customer_id", "date", "service_type", "service_fee"],
    )
    write_csv(
        "appointments.csv",
        appointments,
        [
            "appointment_id",
            "customer_id",
            "store_id",
            "appointment_date",
            "appointment_type",
            "status",
            "channel",
            "booked_days_ahead",
            "estimated_wait_minutes",
        ],
    )
    write_csv(
        "surveys.csv",
        surveys,
        [
            "survey_id",
            "customer_id",
            "survey_date",
            "satisfaction_score",
            "nps_category",
            "response_count",
            "channel",
        ],
    )
    write_csv(
        "feedback_topics.csv",
        feedback_topics,
        [
            "feedback_id",
            "customer_id",
            "feedback_date",
            "topic",
            "sentiment",
            "mentions",
        ],
    )
    write_csv(
        "inventory_movements.csv",
        inventory_movements,
        [
            "movement_id",
            "movement_date",
            "store_id",
            "product_id",
            "movement_type",
            "quantity",
            "reason",
        ],
    )
    write_csv(
        "store_kpis.csv",
        store_kpis,
        [
            "store_id",
            "month",
            "operational_efficiency",
            "avg_wait_minutes",
            "daily_throughput_units",
            "service_attach_rate",
        ],
    )

    print(f"Wrote CSV files to {DATA_DIR}")


if __name__ == "__main__":
    main()
