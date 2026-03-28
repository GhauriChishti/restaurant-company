import csv
import random
import math
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path
import os

SEED = 42
random.seed(SEED)

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = Path(os.environ.get("CUSTOMER_OUTPUT_DIR", ROOT)).expanduser().resolve()
BRANCHES_MASTER_PATH = Path(os.environ.get("BRANCHES_MASTER_PATH", ROOT / "branches_master.csv")).expanduser().resolve()
SALES_ORDERS_PATH = Path(os.environ.get("SALES_ORDERS_PATH", ROOT / "sales_orders.csv")).expanduser().resolve()
SALES_ORDER_LINES_PATH = Path(
    os.environ.get("SALES_ORDER_LINES_PATH", ROOT / "sales_order_lines.csv")
).expanduser().resolve()


def round2(value):
    return round(float(value), 2)


def write_csv(path, rows, columns):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column) for column in columns})


def load_references():
    for src in (BRANCHES_MASTER_PATH, SALES_ORDERS_PATH, SALES_ORDER_LINES_PATH):
        if not src.exists():
            raise FileNotFoundError(f"Missing required file: {src}")

    with BRANCHES_MASTER_PATH.open("r", newline="", encoding="utf-8") as handle:
        branch_rows = list(csv.DictReader(handle))

    with SALES_ORDERS_PATH.open("r", newline="", encoding="utf-8") as handle:
        order_rows = list(csv.DictReader(handle))

    with SALES_ORDER_LINES_PATH.open("r", newline="", encoding="utf-8") as handle:
        line_rows = list(csv.DictReader(handle))

    return branch_rows, order_rows, line_rows


def create_campaigns(min_date, max_date):
    channels = ["Social", "SMS", "Email", "Outdoor", "App Push"]
    names = [
        "Weeknight Saver",
        "Lunch Rush",
        "Family Feast",
        "Loyalty Booster",
        "Ramadan Specials",
        "Summer Cooler Deal",
        "Back-to-Office Bites",
        "Weekend Combo Fest",
    ]

    campaigns = []
    performance_index = defaultdict(list)

    campaign_count = max(4, min(8, int(math.sqrt((max_date - min_date).days + 1))))
    cursor = min_date + timedelta(days=5)
    for i in range(campaign_count):
        duration = random.randint(10, 26)
        if cursor > max_date:
            cursor = min_date + timedelta(days=random.randint(0, 20))
        start = cursor
        end = min(start + timedelta(days=duration), max_date)
        cursor = end + timedelta(days=random.randint(8, 24))

        campaign_id = f"MC{i+1:04d}"
        campaign = {
            "campaign_id": campaign_id,
            "campaign_name": names[i % len(names)],
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "channel": channels[i % len(channels)],
            "budget": round2(random.uniform(120000, 950000)),
        }
        campaigns.append(campaign)

        d = start
        while d <= end:
            performance_index[d.isoformat()].append(campaign_id)
            d += timedelta(days=1)

    return campaigns, performance_index


def build_customers(order_rows):
    order_counts = defaultdict(int)
    customer_orders = defaultdict(list)

    customer_rows = []
    link_rows = []

    for order in order_rows:
        order_id = order["order_id"]
        r = random.random()
        if r < 0.55:
            cid_num = random.randint(1, max(500, len(order_rows) // 6))
        elif r < 0.90:
            cid_num = random.randint(1, max(900, len(order_rows) // 4))
        else:
            cid_num = random.randint(1, max(1400, len(order_rows) // 2))

        customer_id = f"C{cid_num:07d}"
        order_counts[customer_id] += 1
        customer_orders[customer_id].append(order_id)
        link_rows.append({"customer_id": customer_id, "order_id": order_id})

    city_options = sorted({row.get("city", "Unknown") for row in order_rows if row.get("city")})
    if not city_options:
        city_options = ["Unknown"]

    for customer_id, orders in customer_orders.items():
        order_dates = []
        for oid in orders:
            # lookup through link map built from orders via order_id-index for registration timing fallback
            order_dates.append(oid)

        n_orders = order_counts[customer_id]
        if n_orders <= 2:
            segment = "New"
        elif n_orders <= 8:
            segment = "Regular"
        else:
            segment = "Loyal"

        customer_rows.append(
            {
                "customer_id": customer_id,
                "registration_date": "",
                "city": random.choice(city_options),
                "segment": segment,
            }
        )

    return customer_rows, link_rows, customer_orders


def finalize_customer_dates(customer_rows, customer_orders, order_date_by_id, branch_city_by_order):
    for row in customer_rows:
        cid = row["customer_id"]
        dates = [order_date_by_id[oid] for oid in customer_orders[cid] if oid in order_date_by_id]
        if dates:
            earliest = min(dates)
            delta = random.randint(0, 45)
            reg = earliest - timedelta(days=delta)
            row["registration_date"] = reg.isoformat()

            cities = [branch_city_by_order.get(oid) for oid in customer_orders[cid] if branch_city_by_order.get(oid)]
            if cities:
                row["city"] = random.choice(cities)
        else:
            row["registration_date"] = "2025-01-01"


def generate_performance(campaigns, performance_index, order_rows, branch_ids):
    daily_branch = defaultdict(lambda: {"orders": 0, "sales": 0.0})
    for order in order_rows:
        key = (order["order_date"], order["branch_id"])
        daily_branch[key]["orders"] += 1
        daily_branch[key]["sales"] += float(order.get("net_sales", 0.0) or 0.0)

    performance_rows = []
    campaign_map = {row["campaign_id"]: row for row in campaigns}

    for date_str, campaign_ids in sorted(performance_index.items()):
        for branch_id in branch_ids:
            for campaign_id in campaign_ids:
                base_orders = daily_branch[(date_str, branch_id)]["orders"]
                base_sales = daily_branch[(date_str, branch_id)]["sales"]

                impressions = max(100, int(random.gauss(3500 + base_orders * 14, 900)))
                conv_rate = random.uniform(0.008, 0.028)
                conversions = max(0, int(impressions * conv_rate))

                campaign = campaign_map[campaign_id]
                start = datetime.fromisoformat(campaign["start_date"]).date()
                end = datetime.fromisoformat(campaign["end_date"]).date()
                d = datetime.fromisoformat(date_str).date()
                if not (start <= d <= end):
                    raise ValueError("Campaign performance date outside campaign period")

                lift_factor = random.uniform(0.025, 0.11)
                incremental_sales = round2((base_sales * lift_factor) + conversions * random.uniform(2.0, 8.5))

                performance_rows.append(
                    {
                        "campaign_id": campaign_id,
                        "date": date_str,
                        "branch_id": branch_id,
                        "impressions": impressions,
                        "conversions": conversions,
                        "incremental_sales": incremental_sales,
                    }
                )

    return performance_rows


def generate_feedback_and_refunds(order_rows, link_rows, line_rows, branch_ids):
    order_ids = {row["order_id"] for row in order_rows}
    order_branch = {row["order_id"]: row["branch_id"] for row in order_rows}
    order_sales = {row["order_id"]: float(row.get("net_sales", 0.0) or 0.0) for row in order_rows}

    line_count_by_order = defaultdict(int)
    for line in line_rows:
        line_count_by_order[line["order_id"]] += int(line.get("quantity", 1) or 1)

    feedback_rows = []
    refund_rows = []
    feedback_id_seq = 1
    refund_id_seq = 1

    categories = ["Food Quality", "Service", "Delay", "Packaging", "Value", "Cleanliness"]

    complaint_target_rate = random.uniform(0.02, 0.05)
    complaint_count = 0

    for order in order_rows:
        order_id = order["order_id"]
        date_str = order["order_date"]
        branch_id = order["branch_id"]
        if branch_id not in branch_ids:
            raise ValueError("Orphan branch_id in orders")

        base = 3.1 + random.random() * 1.6
        if line_count_by_order[order_id] > 4:
            base -= random.uniform(0.1, 0.35)
        rating = int(min(5, max(1, round(base + random.uniform(-0.4, 0.5)))))

        complaint_flag = 1 if random.random() < complaint_target_rate else 0
        if rating <= 2 and random.random() < 0.55:
            complaint_flag = 1
        if rating >= 5 and random.random() < 0.90:
            complaint_flag = 0

        if complaint_flag == 1:
            complaint_count += 1

        feedback_rows.append(
            {
                "feedback_id": f"FB{feedback_id_seq:09d}",
                "date": date_str,
                "branch_id": branch_id,
                "order_id": order_id,
                "rating": rating,
                "complaint_flag": complaint_flag,
                "feedback_category": random.choice(categories),
            }
        )
        feedback_id_seq += 1

    observed_rate = complaint_count / max(1, len(feedback_rows))
    if observed_rate < 0.02 or observed_rate > 0.05:
        needed = int(len(feedback_rows) * max(0.02, min(0.05, complaint_target_rate)))
        complaint_rows = [r for r in feedback_rows if r["complaint_flag"] == 1]
        non_complaint_rows = [r for r in feedback_rows if r["complaint_flag"] == 0]
        if len(complaint_rows) > needed:
            to_flip = random.sample(complaint_rows, len(complaint_rows) - needed)
            for row in to_flip:
                row["complaint_flag"] = 0
        elif len(complaint_rows) < needed and non_complaint_rows:
            to_flip = random.sample(non_complaint_rows, min(len(non_complaint_rows), needed - len(complaint_rows)))
            for row in to_flip:
                row["complaint_flag"] = 1

    for row in feedback_rows:
        if row["complaint_flag"] != 1:
            continue
        if random.random() > 0.62:
            continue

        order_id = row["order_id"]
        gross = order_sales.get(order_id, 0.0)
        amount = round2(max(0.0, min(gross, gross * random.uniform(0.08, 0.85))))
        refund_rows.append(
            {
                "refund_id": f"RF{refund_id_seq:09d}",
                "order_id": order_id,
                "refund_amount": amount,
                "reason": row["feedback_category"],
            }
        )
        refund_id_seq += 1

    linked_order_ids = {row["order_id"] for row in link_rows}
    if not linked_order_ids.issubset(order_ids):
        raise ValueError("Orphan order_id in customer_orders_link")

    for row in feedback_rows:
        if row["order_id"] not in order_ids:
            raise ValueError("Orphan order_id in feedback")
        if row["branch_id"] not in branch_ids:
            raise ValueError("Orphan branch_id in feedback")
        if not (1 <= int(row["rating"]) <= 5):
            raise ValueError("Invalid rating value")

    for row in refund_rows:
        if row["order_id"] not in order_ids:
            raise ValueError("Orphan order_id in refunds")
        if float(row["refund_amount"]) < 0:
            raise ValueError("Negative refund_amount")

    return feedback_rows, refund_rows


def generate():
    branch_rows, order_rows, line_rows = load_references()
    branch_ids = {row["branch_id"] for row in branch_rows}
    branch_city = {row["branch_id"]: row.get("city", "Unknown") for row in branch_rows}

    if not order_rows:
        raise ValueError("sales_orders.csv is empty")

    min_date = min(datetime.fromisoformat(row["order_date"]).date() for row in order_rows)
    max_date = max(datetime.fromisoformat(row["order_date"]).date() for row in order_rows)

    order_date_by_id = {row["order_id"]: datetime.fromisoformat(row["order_date"]).date() for row in order_rows}
    branch_city_by_order = {row["order_id"]: branch_city.get(row["branch_id"], "Unknown") for row in order_rows}

    campaigns, perf_index = create_campaigns(min_date, max_date)
    campaign_perf = generate_performance(campaigns, perf_index, order_rows, sorted(branch_ids))

    customer_rows, link_rows, customer_orders = build_customers(order_rows)
    finalize_customer_dates(customer_rows, customer_orders, order_date_by_id, branch_city_by_order)

    feedback_rows, refund_rows = generate_feedback_and_refunds(order_rows, link_rows, line_rows, branch_ids)

    write_csv(
        OUTPUT_DIR / "marketing_campaigns.csv",
        campaigns,
        ["campaign_id", "campaign_name", "start_date", "end_date", "channel", "budget"],
    )
    write_csv(
        OUTPUT_DIR / "marketing_campaign_performance.csv",
        campaign_perf,
        ["campaign_id", "date", "branch_id", "impressions", "conversions", "incremental_sales"],
    )
    write_csv(
        OUTPUT_DIR / "customer_master.csv",
        sorted(customer_rows, key=lambda r: r["customer_id"]),
        ["customer_id", "registration_date", "city", "segment"],
    )
    write_csv(
        OUTPUT_DIR / "customer_orders_link.csv",
        link_rows,
        ["customer_id", "order_id"],
    )
    write_csv(
        OUTPUT_DIR / "customer_feedback.csv",
        feedback_rows,
        ["feedback_id", "date", "branch_id", "order_id", "rating", "complaint_flag", "feedback_category"],
    )
    write_csv(
        OUTPUT_DIR / "customer_refunds.csv",
        refund_rows,
        ["refund_id", "order_id", "refund_amount", "reason"],
    )

    print("Generates marketing/customer CSV datasets from branch and sales references with built-in validation checks.")


if __name__ == "__main__":
    generate()
