import csv
import math
import random
from collections import defaultdict
from datetime import date, timedelta

SEED = 42
random.seed(SEED)

START_DATE = date(2025, 1, 1)
END_DATE = date(2025, 12, 31)


def daterange(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def poisson_sample(lam: float) -> int:
    """Knuth algorithm. Good enough for dashboard-level simulation."""
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


def round2(value: float) -> float:
    return round(float(value), 2)


def seasonal_multiplier(day: date) -> dict:
    dow = day.weekday()
    month = day.month
    mult = {
        "orders": 1.0,
        "drink": 1.0,
        "broast": 1.0,
        "aov": 1.0,
        "di_share": 1.0,
        "utilities": 1.0,
    }

    # Weekends
    if dow >= 5:
        mult["orders"] *= 1.20
        mult["aov"] *= 1.03

    # Ramzan / Eid season effect
    if date(2025, 3, 1) <= day <= date(2025, 4, 12):
        mult["orders"] *= 1.16
        mult["broast"] *= 1.28
        mult["drink"] *= 1.18
        mult["aov"] *= 1.07
        mult["di_share"] *= 0.86

    # Summer drink lift
    if month in (5, 6, 7, 8):
        mult["drink"] *= 1.25
        mult["orders"] *= 1.03
        mult["utilities"] *= 1.09

    # Cooler weather effect
    if month in (11, 12, 1):
        mult["drink"] *= 0.86
        mult["utilities"] *= 1.04

    # Payday lift
    if day.day in (1, 2, 3, 27, 28, 29, 30, 31):
        mult["orders"] *= 1.08
        mult["aov"] *= 1.02

    # Occasional rainy / low-footfall day
    if random.random() < 0.08:
        mult["orders"] *= 0.90
        mult["di_share"] *= 0.82

    return mult


def build_skus():
    """
    SKU cost assumptions are BOM-inspired executive-level approximations.
    Costs here represent variable food / recipe cost only.
    Packaging is modeled separately at transaction-simulation time.
    """
    skus = [
        ("SKU001", "Broast Quarter", "Chicken Meals", "chicken", 520, 560, 360, 398, False),
        ("SKU002", "Broast Half", "Chicken Meals", "chicken", 980, 1040, 695, 760, False),
        ("SKU003", "Broast Full", "Chicken Meals", "chicken", 1890, 1990, 1425, 1555, False),
        ("SKU004", "Smokey Laham", "Burgers/Wraps", "beef", 570, 620, 410, 465, False),
        ("SKU005", "Burger Al-Mushroom", "Burgers/Wraps", "beef", 640, 690, 505, 565, False),
        ("SKU006", "Smash Beiruti", "Burgers/Wraps", "beef", 620, 670, 645, 700, True),
        ("SKU007", "Chicken Batata", "Burgers/Wraps", "chicken", 520, 570, 385, 440, False),
        ("SKU008", "Wrap Al Beiruti", "Burgers/Wraps", "chicken", 560, 610, 430, 492, False),
        ("SKU009", "Chicken Shots Burger", "Burgers/Wraps", "chicken", 470, 520, 478, 535, True),
        ("SKU010", "Fries", "Sides", "veg", 240, 270, 112, 136, False),
        ("SKU011", "Garlic Dip", "Dips/Sauces", "veg", 75, 85, 38, 47, False),
        ("SKU012", "Tangy Dip", "Dips/Sauces", "veg", 75, 85, 36, 45, False),
        ("SKU013", "Honey Mustard Dip", "Dips/Sauces", "veg", 80, 90, 40, 50, False),
        ("SKU014", "Mustard Sauce Dip", "Dips/Sauces", "veg", 75, 85, 35, 44, False),
        ("SKU015", "Chipotle Dip", "Dips/Sauces", "veg", 85, 95, 44, 54, False),
        ("SKU016", "Garlic Creamy Sauce", "Dips/Sauces", "veg", 85, 95, 43, 53, False),
        ("SKU017", "Minty Whirl", "Dips/Sauces", "veg", 80, 90, 39, 49, False),
        ("SKU018", "Burger Glow", "Dips/Sauces", "veg", 85, 95, 42, 52, False),
        ("SKU019", "Mighty Mush", "Dips/Sauces", "veg", 90, 100, 47, 57, False),
        ("SKU020", "Mojito", "Drinks", "veg", 260, 290, 95, 124, False),
        ("SKU021", "Ice Tea", "Drinks", "veg", 220, 250, 82, 108, False),
        ("SKU022", "Blueberry", "Drinks", "veg", 280, 310, 102, 132, False),
        ("SKU023", "Mint Margarita", "Drinks", "veg", 250, 280, 92, 120, False),
    ]

    rows = []
    for sku in skus:
        row = {
            "sku_id": sku[0],
            "sku_name": sku[1],
            "category": sku[2],
            "protein_type": sku[3],
            "price_di": float(sku[4]),
            "price_dt": float(sku[5]),
            "cost_di": float(sku[6]),
            "cost_dt": float(sku[7]),
            "loss_leader_flag": bool(sku[8]),
        }
        row["gross_margin_di"] = (row["price_di"] - row["cost_di"]) / row["price_di"]
        row["gross_margin_dt"] = (row["price_dt"] - row["cost_dt"]) / row["price_dt"]
        rows.append(row)
    return rows


def write_csv(path, rows, columns):
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column) for column in columns})


def generate():
    skus = build_skus()
    sku_by_category = defaultdict(list)
    for sku in skus:
        sku_by_category[sku["category"]].append(sku)

    branches = [
        {
            "branch_id": "B001",
            "branch_name": "Falcon QSR Karachi",
            "city": "Karachi",
            "base_orders": 330,
            "base_di_share": 0.56,
            "labor_daily": 210,
            "overhead_daily": 155,
            "pack_di": 13,
        },
        {
            "branch_id": "B002",
            "branch_name": "Falcon QSR Lahore",
            "city": "Lahore",
            "base_orders": 285,
            "base_di_share": 0.54,
            "labor_daily": 195,
            "overhead_daily": 145,
            "pack_di": 12,
        },
        {
            "branch_id": "B003",
            "branch_name": "Falcon QSR Islamabad",
            "city": "Islamabad",
            "base_orders": 230,
            "base_di_share": 0.58,
            "labor_daily": 180,
            "overhead_daily": 138,
            "pack_di": 11,
        },
        {
            "branch_id": "B004",
            "branch_name": "Falcon QSR Rawalpindi",
            "city": "Rawalpindi",
            "base_orders": 245,
            "base_di_share": 0.52,
            "labor_daily": 185,
            "overhead_daily": 140,
            "pack_di": 11,
        },
        {
            "branch_id": "B005",
            "branch_name": "Falcon QSR Faisalabad",
            "city": "Faisalabad",
            "base_orders": 170,
            "base_di_share": 0.50,
            "labor_daily": 150,
            "overhead_daily": 120,
            "pack_di": 10,
        },
    ]

    base_mix = {
        "Chicken Meals": 0.22,
        "Burgers/Wraps": 0.34,
        "Sides": 0.16,
        "Dips/Sauces": 0.11,
        "Drinks": 0.17,
    }
    campaign_months = {2, 5, 8, 11}

    daily_rows = []
    channel_daily = []
    sku_perf = defaultdict(
        lambda: {
            "units": 0,
            "revenue": 0.0,
            "variable_cost": 0.0,
            "sku_name": "",
            "category": "",
        }
    )

    for branch in branches:
        for day in daterange(START_DATE, END_DATE):
            season = seasonal_multiplier(day)
            growth = 1.0
            if branch["city"] == "Faisalabad":
                day_index = (day - START_DATE).days
                growth = 0.90 + 0.22 * (day_index / 364.0)

            lam = branch["base_orders"] * season["orders"] * growth
            total_orders = max(50, poisson_sample(lam))
            di_share = clip(
                branch["base_di_share"] * season["di_share"] + random.gauss(0, 0.02),
                0.30,
                0.72,
            )
            dine_in_orders = int(round(total_orders * di_share))
            takeaway_orders = total_orders - dine_in_orders

            total_customers = max(
                total_orders,
                int(
                    round(
                        dine_in_orders * random.uniform(1.08, 1.24)
                        + takeaway_orders * random.uniform(0.95, 1.03)
                    )
                ),
            )

            units_per_order = clip(random.gauss(1.57, 0.11), 1.30, 1.90)
            units_total = int(round(total_orders * units_per_order))

            category_mix = dict(base_mix)
            category_mix["Drinks"] *= season["drink"]
            category_mix["Chicken Meals"] *= season["broast"]
            total_mix = sum(category_mix.values())
            for key in category_mix:
                category_mix[key] /= total_mix

            categories = list(base_mix.keys())
            category_units = {category: 0 for category in categories}
            for _ in range(units_total):
                category = weighted_pick(categories, [category_mix[name] for name in categories])
                category_units[category] += 1

            di_rev = dt_rev = 0.0
            di_food = dt_food = 0.0
            di_pack = dt_pack = 0.0
            di_units_sold = dt_units_sold = 0

            di_unit_share = clip(di_share + random.gauss(0.02, 0.02), 0.25, 0.78)

            for category, count in category_units.items():
                items = sku_by_category[category]
                if category == "Chicken Meals":
                    weights = [0.50, 0.32, 0.18]
                elif category == "Drinks":
                    weights = [0.28, 0.30, 0.20, 0.22]
                else:
                    weights = [1.0] * len(items)

                for _ in range(count):
                    sku = weighted_pick(items, weights)
                    di_units = 1 if random.random() < di_unit_share else 0
                    dt_units = 1 - di_units

                    di_units_sold += di_units
                    dt_units_sold += dt_units

                    di_rev += di_units * sku["price_di"]
                    dt_rev += dt_units * sku["price_dt"]
                    di_food += di_units * sku["cost_di"]
                    dt_food += dt_units * sku["cost_dt"]

                    di_pack_unit = 9 if category in ("Drinks", "Dips/Sauces") else 14
                    dt_pack_unit = 21 if category in ("Drinks", "Dips/Sauces") else 36
                    di_pack += di_units * di_pack_unit
                    dt_pack += dt_units * dt_pack_unit

                    perf = sku_perf[sku["sku_id"]]
                    perf["units"] += 1
                    perf["revenue"] += di_units * sku["price_di"] + dt_units * sku["price_dt"]
                    perf["variable_cost"] += di_units * sku["cost_di"] + dt_units * sku["cost_dt"]
                    perf["sku_name"] = sku["sku_name"]
                    perf["category"] = sku["category"]

            gross_revenue = di_rev + dt_rev
            discount_rate = clip(random.gauss(0.045, 0.015), 0.01, 0.09)
            if day.month in campaign_months:
                discount_rate += 0.008
            discount_amount = gross_revenue * discount_rate
            net_revenue = gross_revenue - discount_amount

            waste_factor = clip(random.gauss(1.028, 0.008), 1.01, 1.05)
            food_cost = (di_food + dt_food) * waste_factor
            packaging_cost = di_pack + dt_pack + (
                dine_in_orders * branch["pack_di"]
                + takeaway_orders * (branch["pack_di"] + 34)
            )
            labor_cost = branch["labor_daily"] * random.uniform(0.95, 1.08) * (
                1.06 if day.weekday() in (4, 5, 6) else 1.0
            )
            overhead_cost = branch["overhead_daily"] * season["utilities"] * random.uniform(0.97, 1.06)

            marketing_cost = 0.0
            if day.month in campaign_months:
                marketing_cost = random.uniform(2500, 8500) * (
                    1.15 if branch["city"] in ("Karachi", "Lahore") else 1.0
                )

            refunds_amount = clip(
                net_revenue * random.gauss(0.008, 0.003),
                0,
                net_revenue * 0.03,
            )

            # Cost-driven profitability (fixed from original version).
            total_cost = (
                food_cost
                + packaging_cost
                + labor_cost
                + overhead_cost
                + marketing_cost
                + refunds_amount
            )
            gross_profit = net_revenue - total_cost
            gross_margin_pct = gross_profit / net_revenue if net_revenue else 0.0

            avg_order_value = net_revenue / total_orders if total_orders else 0.0
            complaints_rate = clip(random.gauss(0.021, 0.007), 0.01, 0.04)
            complaints_count = int(round(total_orders * complaints_rate))
            avg_prep_time = clip(
                random.gauss(14.2, 2.1)
                + (0.9 if day.weekday() in (4, 5, 6) else 0)
                + (0.5 if branch["city"] in ("Karachi", "Lahore") else -0.3)
                - (0.4 if branch["city"] == "Faisalabad" else 0),
                8,
                24,
            )

            daily_rows.append(
                {
                    "date": day.isoformat(),
                    "branch_id": branch["branch_id"],
                    "branch_name": branch["branch_name"],
                    "city": branch["city"],
                    "total_orders": total_orders,
                    "total_customers": total_customers,
                    "dine_in_orders": dine_in_orders,
                    "takeaway_orders": takeaway_orders,
                    "gross_revenue": round2(gross_revenue),
                    "discount_amount": round2(discount_amount),
                    "net_revenue": round2(net_revenue),
                    "food_cost": round2(food_cost),
                    "packaging_cost": round2(packaging_cost),
                    "labor_cost": round2(labor_cost),
                    "overhead_cost": round2(overhead_cost),
                    "marketing_cost": round2(marketing_cost),
                    "total_cost": round2(total_cost),
                    "gross_profit": round2(gross_profit),
                    "gross_margin_pct": round(gross_margin_pct, 4),
                    "avg_order_value": round2(avg_order_value),
                    "avg_prep_time_min": round(avg_prep_time, 2),
                    "complaints_count": complaints_count,
                    "refunds_amount": round2(refunds_amount),
                }
            )

            month = day.strftime("%Y-%m")
            for channel in ("DI", "DT"):
                if channel == "DI":
                    orders = dine_in_orders
                    units = di_units_sold
                    revenue = di_rev * (1 - discount_rate)
                    channel_food_cost = di_food * waste_factor
                    channel_packaging_cost = di_pack + dine_in_orders * branch["pack_di"]
                else:
                    orders = takeaway_orders
                    units = dt_units_sold
                    revenue = dt_rev * (1 - discount_rate)
                    channel_food_cost = dt_food * waste_factor
                    channel_packaging_cost = dt_pack + takeaway_orders * (branch["pack_di"] + 34)

                channel_gross_profit = revenue - channel_food_cost - channel_packaging_cost
                channel_daily.append(
                    {
                        "month": month,
                        "branch_id": branch["branch_id"],
                        "city": branch["city"],
                        "channel": channel,
                        "total_orders": orders,
                        "units_sold": units,
                        "revenue": revenue,
                        "avg_order_value": revenue / orders if orders else 0.0,
                        "food_cost": channel_food_cost,
                        "packaging_cost": channel_packaging_cost,
                        "gross_profit": channel_gross_profit,
                        "gross_margin_pct": channel_gross_profit / revenue if revenue else 0.0,
                    }
                )

    daily_columns = [
        "date",
        "branch_id",
        "branch_name",
        "city",
        "total_orders",
        "total_customers",
        "dine_in_orders",
        "takeaway_orders",
        "gross_revenue",
        "discount_amount",
        "net_revenue",
        "food_cost",
        "packaging_cost",
        "labor_cost",
        "overhead_cost",
        "marketing_cost",
        "total_cost",
        "gross_profit",
        "gross_margin_pct",
        "avg_order_value",
        "avg_prep_time_min",
        "complaints_count",
        "refunds_amount",
    ]
    write_csv("daily_kpis.csv", daily_rows, daily_columns)

    monthly = defaultdict(lambda: defaultdict(float))
    complaints_month = defaultdict(int)
    for row in daily_rows:
        month = row["date"][:7]
        key = (month, row["branch_id"], row["branch_name"], row["city"])
        monthly[key]["total_orders"] += row["total_orders"]
        monthly[key]["total_revenue"] += row["net_revenue"]
        monthly[key]["food_cost"] += row["food_cost"]
        monthly[key]["packaging_cost"] += row["packaging_cost"]
        monthly[key]["labor_cost"] += row["labor_cost"]
        monthly[key]["overhead_cost"] += row["overhead_cost"]
        monthly[key]["marketing_cost"] += row["marketing_cost"]
        monthly[key]["total_cost"] += row["total_cost"]
        monthly[key]["gross_profit"] += row["gross_profit"]
        complaints_month[key] += row["complaints_count"]

    branch_monthly_rows = []
    for key, values in monthly.items():
        month, branch_id, branch_name, city = key
        orders = values["total_orders"]
        revenue = values["total_revenue"]
        branch_monthly_rows.append(
            {
                "month": month,
                "branch_id": branch_id,
                "branch_name": branch_name,
                "city": city,
                "total_orders": int(round(orders)),
                "total_revenue": round2(revenue),
                "food_cost": round2(values["food_cost"]),
                "packaging_cost": round2(values["packaging_cost"]),
                "labor_cost": round2(values["labor_cost"]),
                "overhead_cost": round2(values["overhead_cost"]),
                "marketing_cost": round2(values["marketing_cost"]),
                "total_cost": round2(values["total_cost"]),
                "gross_profit": round2(values["gross_profit"]),
                "gross_margin_pct": round(values["gross_profit"] / revenue if revenue else 0.0, 4),
                "avg_order_value": round2(revenue / orders if orders else 0.0),
                "complaint_rate_pct": round((complaints_month[key] / orders) * 100 if orders else 0.0, 2),
            }
        )
    branch_monthly_rows.sort(key=lambda row: (row["month"], row["branch_id"]))

    branch_monthly_columns = [
        "month",
        "branch_id",
        "branch_name",
        "city",
        "total_orders",
        "total_revenue",
        "food_cost",
        "packaging_cost",
        "labor_cost",
        "overhead_cost",
        "marketing_cost",
        "total_cost",
        "gross_profit",
        "gross_margin_pct",
        "avg_order_value",
        "complaint_rate_pct",
    ]
    write_csv("branch_monthly_summary.csv", branch_monthly_rows, branch_monthly_columns)

    sku_margin_rows = []
    for sku in skus:
        gross_profit_di = sku["price_di"] - sku["cost_di"]
        gross_profit_dt = sku["price_dt"] - sku["cost_dt"]
        sku_margin_rows.append(
            {
                "sku_id": sku["sku_id"],
                "sku_name": sku["sku_name"],
                "category": sku["category"],
                "protein_type": sku["protein_type"],
                "price_di": sku["price_di"],
                "price_dt": sku["price_dt"],
                "cost_di": sku["cost_di"],
                "cost_dt": sku["cost_dt"],
                "gross_profit_di": round2(gross_profit_di),
                "gross_profit_dt": round2(gross_profit_dt),
                "gross_margin_di_pct": round((gross_profit_di / sku["price_di"]) * 100, 2),
                "gross_margin_dt_pct": round((gross_profit_dt / sku["price_dt"]) * 100, 2),
                "loss_leader_flag": sku["loss_leader_flag"],
            }
        )

    sku_margin_columns = [
        "sku_id",
        "sku_name",
        "category",
        "protein_type",
        "price_di",
        "price_dt",
        "cost_di",
        "cost_dt",
        "gross_profit_di",
        "gross_profit_dt",
        "gross_margin_di_pct",
        "gross_margin_dt_pct",
        "loss_leader_flag",
    ]
    write_csv("sku_margin_summary.csv", sku_margin_rows, sku_margin_columns)

    channel_agg = defaultdict(lambda: defaultdict(float))
    for row in channel_daily:
        key = (row["month"], row["branch_id"], row["city"], row["channel"])
        for column in (
            "total_orders",
            "units_sold",
            "revenue",
            "food_cost",
            "packaging_cost",
            "gross_profit",
        ):
            channel_agg[key][column] += row[column]

    channel_rows = []
    for key, values in channel_agg.items():
        month, branch_id, city, channel = key
        revenue = values["revenue"]
        orders = values["total_orders"]
        channel_rows.append(
            {
                "month": month,
                "branch_id": branch_id,
                "city": city,
                "channel": channel,
                "total_orders": int(round(orders)),
                "units_sold": int(round(values["units_sold"])),
                "revenue": round2(revenue),
                "avg_order_value": round2(revenue / orders if orders else 0.0),
                "food_cost": round2(values["food_cost"]),
                "packaging_cost": round2(values["packaging_cost"]),
                "gross_profit": round2(values["gross_profit"]),
                "gross_margin_pct": round(values["gross_profit"] / revenue if revenue else 0.0, 4),
            }
        )
    channel_rows.sort(key=lambda row: (row["month"], row["branch_id"], row["channel"]))

    channel_columns = [
        "month",
        "branch_id",
        "city",
        "channel",
        "total_orders",
        "units_sold",
        "revenue",
        "avg_order_value",
        "food_cost",
        "packaging_cost",
        "gross_profit",
        "gross_margin_pct",
    ]
    write_csv("channel_sales_summary.csv", channel_rows, channel_columns)

    cost_rows = []
    for row in branch_monthly_rows:
        refunds = 0.0
        month = row["month"]
        branch_id = row["branch_id"]
        city = row["city"]
        for daily_row in daily_rows:
            if daily_row["date"].startswith(month) and daily_row["branch_id"] == branch_id:
                refunds += daily_row["refunds_amount"]
        cost_rows.append(
            {
                "month": month,
                "branch_id": branch_id,
                "city": city,
                "revenue": row["total_revenue"],
                "food_cost": row["food_cost"],
                "packaging_cost": row["packaging_cost"],
                "labor_cost": row["labor_cost"],
                "overhead_cost": row["overhead_cost"],
                "marketing_cost": row["marketing_cost"],
                "refunds": round2(refunds),
                "total_cost": row["total_cost"],
                "gross_profit": row["gross_profit"],
                "net_cash_like_surplus": round2(row["total_revenue"] - row["total_cost"]),
            }
        )

    cost_columns = [
        "month",
        "branch_id",
        "city",
        "revenue",
        "food_cost",
        "packaging_cost",
        "labor_cost",
        "overhead_cost",
        "marketing_cost",
        "refunds",
        "total_cost",
        "gross_profit",
        "net_cash_like_surplus",
    ]
    write_csv("cost_summary_monthly.csv", cost_rows, cost_columns)

    perf_rows = []
    for sku_id, perf in sku_perf.items():
        margin = (perf["revenue"] - perf["variable_cost"]) / perf["revenue"] if perf["revenue"] else 0.0
        perf_rows.append({"sku_id": sku_id, **perf, "margin": margin})

    units_sorted = sorted(row["units"] for row in perf_rows)
    q1 = units_sorted[len(units_sorted) // 4]
    q2 = units_sorted[len(units_sorted) // 2]
    q3 = units_sorted[(len(units_sorted) * 3) // 4]

    loss_flag_map = {sku["sku_id"]: sku["loss_leader_flag"] for sku in skus}
    menu_rows = []
    for perf in perf_rows:
        units = perf["units"]
        if units <= q1:
            popularity_band = "Low"
        elif units <= q2:
            popularity_band = "Medium"
        elif units <= q3:
            popularity_band = "High"
        else:
            popularity_band = "Very High"

        margin = perf["margin"]
        if margin < 0:
            margin_band = "Negative"
        elif margin < 0.20:
            margin_band = "Low"
        elif margin < 0.40:
            margin_band = "Medium"
        else:
            margin_band = "High"

        popularity_high = popularity_band in ("High", "Very High")
        margin_high = margin_band in ("Medium", "High")
        if popularity_high and margin_high:
            quadrant = "Star"
        elif popularity_high and not margin_high:
            quadrant = "Plowhorse"
        elif (not popularity_high) and margin_high:
            quadrant = "Puzzle"
        else:
            quadrant = "Dog"

        action_hint = {
            "Star": "Promote and maintain consistency",
            "Plowhorse": "Review recipe/price to lift margin",
            "Puzzle": "Increase visibility and bundle smartly",
            "Dog": "Consider reformulation or limited-time strategy",
        }[quadrant]

        menu_rows.append(
            {
                "sku_id": perf["sku_id"],
                "sku_name": perf["sku_name"],
                "category": perf["category"],
                "popularity_band": popularity_band,
                "margin_band": margin_band,
                "menu_engineering_quadrant": quadrant,
                "loss_leader_flag": loss_flag_map[perf["sku_id"]],
                "action_hint": action_hint,
            }
        )
    menu_rows.sort(key=lambda row: row["sku_id"])

    menu_columns = [
        "sku_id",
        "sku_name",
        "category",
        "popularity_band",
        "margin_band",
        "menu_engineering_quadrant",
        "loss_leader_flag",
        "action_hint",
    ]
    write_csv("menu_engineering_flags.csv", menu_rows, menu_columns)

    write_csv(
        "sku_master_internal.csv",
        skus,
        [
            "sku_id",
            "sku_name",
            "category",
            "protein_type",
            "price_di",
            "price_dt",
            "cost_di",
            "cost_dt",
            "loss_leader_flag",
            "gross_margin_di",
            "gross_margin_dt",
        ],
    )

    # Validation block
    expected_rows = 365 * 5
    assert len(daily_rows) == expected_rows, "daily_kpis row count mismatch"

    seen = set()
    for row in daily_rows:
        assert row["branch_id"] and row["date"], "Missing branch_id/date"
        unique_key = (row["date"], row["branch_id"])
        assert unique_key not in seen, f"Duplicate daily row: {unique_key}"
        seen.add(unique_key)
        assert row["total_orders"] >= 0, "Negative orders found"
        assert row["dine_in_orders"] + row["takeaway_orders"] == row["total_orders"], "DI/DT orders do not sum to total_orders"
        for column in ("net_revenue", "food_cost", "packaging_cost", "labor_cost", "overhead_cost"):
            assert row[column] >= 0, f"Negative value found in {column}"

        calculated_total_cost = (
            row["food_cost"]
            + row["packaging_cost"]
            + row["labor_cost"]
            + row["overhead_cost"]
            + row["marketing_cost"]
            + row["refunds_amount"]
        )
        assert abs(calculated_total_cost - row["total_cost"]) < 1.5, "Total cost reconciliation failed"
        assert abs((row["net_revenue"] - row["total_cost"]) - row["gross_profit"]) < 1.5, "Gross profit reconciliation failed"

    month_daily = defaultdict(lambda: {"orders": 0, "revenue": 0.0})
    for row in daily_rows:
        key = (row["date"][:7], row["branch_id"])
        month_daily[key]["orders"] += row["total_orders"]
        month_daily[key]["revenue"] += row["net_revenue"]

    for row in branch_monthly_rows:
        key = (row["month"], row["branch_id"])
        assert month_daily[key]["orders"] == row["total_orders"], "Monthly orders do not reconcile"
        assert abs(month_daily[key]["revenue"] - row["total_revenue"]) < 3.0, "Monthly revenue does not reconcile"

    month_channel_revenue = defaultdict(float)
    for row in channel_rows:
        month_channel_revenue[(row["month"], row["branch_id"])] += row["revenue"]

    for row in branch_monthly_rows:
        key = (row["month"], row["branch_id"])
        diff_ratio = abs(month_channel_revenue[key] - row["total_revenue"]) / max(row["total_revenue"], 1.0)
        assert diff_ratio < 0.06, "Channel revenue reconciliation outside tolerance"

    assert sum(1 for sku in skus if sku["loss_leader_flag"]) >= 2, "At least two loss leaders required"

    annual_revenue_by_city = defaultdict(float)
    for row in daily_rows:
        annual_revenue_by_city[row["city"]] += row["net_revenue"]
    ranked_cities = sorted(annual_revenue_by_city.items(), key=lambda item: item[1], reverse=True)
    assert ranked_cities[0][0] == "Karachi", "Karachi must lead annual revenue"

    print("=== Diagnostics ===")
    print("Total annual revenue by branch:")
    for city, revenue in ranked_cities:
        print(f"{city}: {revenue:,.2f}")

    average_margins = []
    for row in sku_margin_rows:
        avg_margin = (row["gross_margin_di_pct"] + row["gross_margin_dt_pct"]) / 2
        average_margins.append((row["sku_name"], avg_margin))
    average_margins.sort(key=lambda item: item[1], reverse=True)

    print("\nTop 5 highest-margin SKUs:")
    for name, margin in average_margins[:5]:
        print(f"{name}: {margin:.2f}%")

    print("\nBottom 5 lowest-margin SKUs:")
    for name, margin in average_margins[-5:]:
        print(f"{name}: {margin:.2f}%")

    category_margins = defaultdict(list)
    category_by_sku = {row["sku_name"]: next(sku["category"] for sku in skus if sku["sku_name"] == row["sku_name"]) for row in sku_margin_rows}
    for name, margin in average_margins:
        category_margins[category_by_sku[name]].append(margin)

    print("\nAverage gross margin by category:")
    for category, margins in sorted(
        category_margins.items(), key=lambda item: sum(item[1]) / len(item[1]), reverse=True
    ):
        print(f"{category}: {sum(margins) / len(margins):.2f}%")


if __name__ == "__main__":
    generate()
