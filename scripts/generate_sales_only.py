import csv
import math
import random
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from pathlib import Path

SEED = 42
random.seed(SEED)

START_DATE = date(2025, 1, 1)
END_DATE = date(2025, 12, 31)

ROOT = Path(__file__).resolve().parents[1]
BRANCHES_MASTER_PATH = ROOT / "branches_master.csv"
SKU_MASTER_PATH = ROOT / "sku_master.csv"


def daterange(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def round2(value: float) -> float:
    return round(float(value), 2)


def poisson_sample(lam: float) -> int:
    lam = max(lam, 0.1)
    threshold = math.exp(-lam)
    k = 0
    product = 1.0
    while product > threshold:
        k += 1
        product *= random.random()
    return max(0, k - 1)


def weighted_pick(items, weights):
    total = sum(weights)
    pick = random.uniform(0, total)
    upto = 0.0
    for item, weight in zip(items, weights):
        upto += weight
        if upto >= pick:
            return item
    return items[-1]


def write_csv(path: Path, rows, columns):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column) for column in columns})


def seasonal_multiplier(day: date) -> dict:
    dow = day.weekday()
    month = day.month
    mult = {"orders": 1.0, "drink": 1.0, "broast": 1.0, "di_share": 1.0}

    if dow >= 5:
        mult["orders"] *= 1.20

    if date(2025, 3, 1) <= day <= date(2025, 4, 12):
        mult["orders"] *= 1.16
        mult["broast"] *= 1.28
        mult["drink"] *= 1.18
        mult["di_share"] *= 0.86

    if month in (5, 6, 7, 8):
        mult["drink"] *= 1.25
        mult["orders"] *= 1.03

    if day.day in (1, 2, 3, 27, 28, 29, 30, 31):
        mult["orders"] *= 1.08

    if random.random() < 0.08:
        mult["orders"] *= 0.90
        mult["di_share"] *= 0.82

    return mult


def load_master_data():
    if not BRANCHES_MASTER_PATH.exists():
        raise FileNotFoundError(f"Missing branches master file: {BRANCHES_MASTER_PATH}")
    if not SKU_MASTER_PATH.exists():
        raise FileNotFoundError(f"Missing SKU master file: {SKU_MASTER_PATH}")

    with BRANCHES_MASTER_PATH.open("r", encoding="utf-8", newline="") as f:
        branch_rows = list(csv.DictReader(f))

    with SKU_MASTER_PATH.open("r", encoding="utf-8", newline="") as f:
        sku_rows = list(csv.DictReader(f))

    branches = []
    for row in branch_rows:
        city = row["city"].strip()
        base_orders = {
            "Karachi": 330,
            "Lahore": 285,
            "Islamabad": 230,
            "Rawalpindi": 245,
            "Faisalabad": 170,
        }.get(city, 220)
        base_di_share = {
            "Karachi": 0.56,
            "Lahore": 0.54,
            "Islamabad": 0.58,
            "Rawalpindi": 0.52,
            "Faisalabad": 0.50,
        }.get(city, 0.53)
        branches.append(
            {
                "branch_id": row["branch_id"],
                "branch_name": row.get("branch_name", row["branch_id"]),
                "city": city,
                "base_orders": base_orders,
                "base_di_share": base_di_share,
            }
        )

    skus = []
    for row in sku_rows:
        di_col = "price_di" if "price_di" in row else "dine_in_price"
        dt_col = "price_dt" if "price_dt" in row else "takeaway_price"
        skus.append(
            {
                "sku_id": row["sku_id"],
                "category": row["category"],
                "price_di": float(row[di_col]),
                "price_dt": float(row[dt_col]),
            }
        )

    return branches, skus


def random_order_datetime(day: date):
    slot_ranges = {
        "Breakfast": (7, 10),
        "Lunch": (11, 15),
        "Evening": (16, 18),
        "Dinner": (19, 23),
    }
    slot_weights = [0.12, 0.37, 0.18, 0.33]
    slot = weighted_pick(list(slot_ranges.keys()), slot_weights)
    hour_start, hour_end = slot_ranges[slot]
    hour = random.randint(hour_start, hour_end)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    dt = datetime.combine(day, time(hour=hour, minute=minute, second=second))
    return dt, slot


def generate():
    branches, skus = load_master_data()
    sku_by_category = defaultdict(list)
    for sku in skus:
        sku_by_category[sku["category"]].append(sku)

    base_mix = {
        "Chicken Meals": 0.22,
        "Burgers/Wraps": 0.34,
        "Sides": 0.16,
        "Dips/Sauces": 0.11,
        "Drinks": 0.17,
    }

    campaign_months = {2, 5, 8, 11}

    orders = []
    lines = []

    order_counter = 1
    line_counter = 1

    payment_methods = ["Cash", "Card", "Wallet"]
    payment_weights = [0.30, 0.56, 0.14]

    for branch in branches:
        for day in daterange(START_DATE, END_DATE):
            season = seasonal_multiplier(day)
            growth = 1.0
            if branch["city"] == "Faisalabad":
                day_index = (day - START_DATE).days
                growth = 0.90 + 0.22 * (day_index / 364.0)

            lam = branch["base_orders"] * season["orders"] * growth
            total_orders = max(25, poisson_sample(lam))

            di_share = clip(
                branch["base_di_share"] * season["di_share"] + random.gauss(0, 0.02),
                0.30,
                0.72,
            )

            for _ in range(total_orders):
                channel = "DI" if random.random() < di_share else "DT"

                units_per_order = clip(random.gauss(1.57, 0.11), 1.0, 3.5)
                total_items = max(1, int(round(units_per_order)))

                category_mix = dict(base_mix)
                category_mix["Drinks"] *= season["drink"]
                category_mix["Chicken Meals"] *= season["broast"]
                total_mix = sum(category_mix.values())
                for key in category_mix:
                    category_mix[key] /= total_mix

                categories = list(category_mix.keys())
                order_lines = []
                gross_sales = 0.0

                for _line in range(total_items):
                    category = weighted_pick(categories, [category_mix[c] for c in categories])
                    catalog = sku_by_category.get(category) or skus
                    sku = random.choice(catalog)

                    quantity = 1 if random.random() < 0.90 else 2
                    unit_price = sku["price_di"] if channel == "DI" else sku["price_dt"]
                    gross_line_sales = quantity * unit_price
                    gross_sales += gross_line_sales

                    order_lines.append(
                        {
                            "sku_id": sku["sku_id"],
                            "quantity": quantity,
                            "unit_price": round2(unit_price),
                            "gross_line_sales": round2(gross_line_sales),
                        }
                    )

                discount_rate = clip(random.gauss(0.045, 0.015), 0.01, 0.09)
                if day.month in campaign_months:
                    discount_rate += 0.008
                discount_amount = round2(gross_sales * discount_rate)

                distributed_discount = 0.0
                for idx, row in enumerate(order_lines):
                    if idx == len(order_lines) - 1:
                        line_discount = round2(discount_amount - distributed_discount)
                    else:
                        share = row["gross_line_sales"] / gross_sales if gross_sales else 0
                        line_discount = round2(discount_amount * share)
                        distributed_discount += line_discount
                    net_line_sales = round2(row["gross_line_sales"] - line_discount)

                    lines.append(
                        {
                            "order_line_id": f"OL{line_counter:09d}",
                            "order_id": f"SO{order_counter:09d}",
                            "sku_id": row["sku_id"],
                            "quantity": row["quantity"],
                            "unit_price": row["unit_price"],
                            "gross_line_sales": row["gross_line_sales"],
                            "discount_line_amount": line_discount,
                            "net_line_sales": net_line_sales,
                        }
                    )
                    line_counter += 1

                order_dt, order_time_slot = random_order_datetime(day)
                refund_flag = 1 if random.random() < 0.015 else 0

                orders.append(
                    {
                        "order_id": f"SO{order_counter:09d}",
                        "order_datetime": order_dt.isoformat(sep=" "),
                        "order_date": day.isoformat(),
                        "order_time_slot": order_time_slot,
                        "branch_id": branch["branch_id"],
                        "channel": channel,
                        "total_items": sum(item["quantity"] for item in order_lines),
                        "gross_sales": round2(gross_sales),
                        "discount_amount": discount_amount,
                        "net_sales": round2(gross_sales - discount_amount),
                        "payment_method": weighted_pick(payment_methods, payment_weights),
                        "refund_flag": refund_flag,
                    }
                )
                order_counter += 1

    orders_columns = [
        "order_id",
        "order_datetime",
        "order_date",
        "order_time_slot",
        "branch_id",
        "channel",
        "total_items",
        "gross_sales",
        "discount_amount",
        "net_sales",
        "payment_method",
        "refund_flag",
    ]
    write_csv(ROOT / "sales_orders.csv", orders, orders_columns)

    lines_columns = [
        "order_line_id",
        "order_id",
        "sku_id",
        "quantity",
        "unit_price",
        "gross_line_sales",
        "discount_line_amount",
        "net_line_sales",
    ]
    write_csv(ROOT / "sales_order_lines.csv", lines, lines_columns)

    daily_summary = defaultdict(lambda: {"orders_count": 0, "units_sold": 0, "revenue": 0.0, "refunds_amount": 0.0})

    for order in orders:
        key = (order["order_date"], order["branch_id"], order["channel"])
        daily_summary[key]["orders_count"] += 1
        daily_summary[key]["units_sold"] += int(order["total_items"])
        if order["refund_flag"] == 0:
            daily_summary[key]["revenue"] += float(order["net_sales"])
        else:
            daily_summary[key]["refunds_amount"] += float(order["net_sales"])

    daily_rows = []
    for (dt, branch_id, channel), agg in sorted(daily_summary.items()):
        orders_count = agg["orders_count"]
        revenue = round2(agg["revenue"])
        daily_rows.append(
            {
                "date": dt,
                "branch_id": branch_id,
                "channel": channel,
                "orders_count": orders_count,
                "units_sold": int(agg["units_sold"]),
                "revenue": revenue,
                "avg_order_value": round2(revenue / orders_count if orders_count else 0.0),
                "refunds_amount": round2(agg["refunds_amount"]),
            }
        )

    daily_summary_columns = [
        "date",
        "branch_id",
        "channel",
        "orders_count",
        "units_sold",
        "revenue",
        "avg_order_value",
        "refunds_amount",
    ]
    write_csv(ROOT / "sales_daily_summary.csv", daily_rows, daily_summary_columns)

    # Validations
    assert orders, "sales_orders is empty"
    assert lines, "sales_order_lines is empty"

    for row in orders:
        assert row["gross_sales"] >= 0 and row["discount_amount"] >= 0 and row["net_sales"] >= 0, "Negative values in orders"
        assert row["payment_method"] in payment_methods, "Invalid payment method"
        assert row["order_time_slot"] in {"Breakfast", "Lunch", "Evening", "Dinner"}, "Invalid time slot"

    lines_by_order = defaultdict(list)
    for line in lines:
        for col in ("quantity", "unit_price", "gross_line_sales", "discount_line_amount", "net_line_sales"):
            assert float(line[col]) >= 0, f"Negative line value in {col}"
        lines_by_order[line["order_id"]].append(line)

    for order in orders:
        order_lines = lines_by_order[order["order_id"]]
        gross_sum = round2(sum(float(l["gross_line_sales"]) for l in order_lines))
        discount_sum = round2(sum(float(l["discount_line_amount"]) for l in order_lines))
        net_sum = round2(sum(float(l["net_line_sales"]) for l in order_lines))

        assert gross_sum == order["gross_sales"], "Order gross sales does not reconcile with lines"
        assert abs(discount_sum - order["discount_amount"]) <= 0.02, "Order discount does not reconcile with lines"
        assert abs(net_sum - order["net_sales"]) <= 0.02, "Order net sales does not reconcile with lines"
        assert sum(int(l["quantity"]) for l in order_lines) == order["total_items"], "Order total_items mismatch"

    recomputed_summary = defaultdict(lambda: {"orders_count": 0, "units_sold": 0, "revenue": 0.0, "refunds_amount": 0.0})
    for order in orders:
        key = (order["order_date"], order["branch_id"], order["channel"])
        recomputed_summary[key]["orders_count"] += 1
        recomputed_summary[key]["units_sold"] += int(order["total_items"])
        if order["refund_flag"] == 0:
            recomputed_summary[key]["revenue"] += float(order["net_sales"])
        else:
            recomputed_summary[key]["refunds_amount"] += float(order["net_sales"])

    for row in daily_rows:
        key = (row["date"], row["branch_id"], row["channel"])
        assert key in recomputed_summary, "Daily summary key missing in orders"
        assert row["orders_count"] == recomputed_summary[key]["orders_count"], "Daily summary orders_count mismatch"
        assert row["units_sold"] == recomputed_summary[key]["units_sold"], "Daily summary units_sold mismatch"
        assert abs(row["revenue"] - round2(recomputed_summary[key]["revenue"])) <= 0.02, "Daily summary revenue mismatch"
        assert abs(row["refunds_amount"] - round2(recomputed_summary[key]["refunds_amount"])) <= 0.02, "Daily summary refunds_amount mismatch"
        for col in ("orders_count", "units_sold", "revenue", "avg_order_value", "refunds_amount"):
            assert row[col] >= 0, f"Negative daily summary value: {col}"

    annual_revenue_by_city = defaultdict(float)
    branch_city = {b["branch_id"]: b["city"] for b in branches}
    for order in orders:
        if order["refund_flag"] == 0:
            annual_revenue_by_city[branch_city[order["branch_id"]]] += float(order["net_sales"])

    ranked_cities = sorted(annual_revenue_by_city.items(), key=lambda item: item[1], reverse=True)
    assert ranked_cities and ranked_cities[0][0] == "Karachi", "Karachi must have highest annual revenue"

    print("Generated files:")
    print("- sales_orders.csv")
    print("- sales_order_lines.csv")
    print("- sales_daily_summary.csv")
    print("Validations passed.")


if __name__ == "__main__":
    generate()
