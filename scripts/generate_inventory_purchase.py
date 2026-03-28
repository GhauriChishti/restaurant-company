import csv
import math
import os
import random
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

SEED = 202504
random.seed(SEED)

START_DATE = date(2025, 1, 1)
END_DATE = date(2025, 12, 31)

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = Path(os.environ.get("INVENTORY_OUTPUT_DIR", ROOT)).expanduser().resolve()

SALES_ORDERS_PATH = Path(os.environ.get("SALES_ORDERS_PATH", ROOT / "sales_orders.csv")).expanduser().resolve()
SALES_ORDER_LINES_PATH = Path(
    os.environ.get("SALES_ORDER_LINES_PATH", ROOT / "sales_order_lines.csv")
).expanduser().resolve()
SALES_DAILY_SUMMARY_PATH = Path(
    os.environ.get("SALES_DAILY_SUMMARY_PATH", ROOT / "sales_daily_summary.csv")
).expanduser().resolve()
KITCHEN_BATCH_RUNS_PATH = Path(
    os.environ.get("KITCHEN_BATCH_RUNS_PATH", ROOT / "kitchen_batch_runs.csv")
).expanduser().resolve()
KITCHEN_BATCH_CONSUMPTION_PATH = Path(
    os.environ.get("KITCHEN_BATCH_CONSUMPTION_PATH", ROOT / "kitchen_batch_consumption.csv")
).expanduser().resolve()
SKU_CONSUMPTION_DAILY_PATH = Path(
    os.environ.get("SKU_CONSUMPTION_DAILY_PATH", ROOT / "sku_consumption_daily.csv")
).expanduser().resolve()

REQUIRED_FILES = {
    "branches": ROOT / "branches_master.csv",
    "suppliers": ROOT / "suppliers_master.csv",
    "ingredients": ROOT / "ingredients_master.csv",
    "employees": ROOT / "employees_master.csv",
    "sales_orders": SALES_ORDERS_PATH,
    "sales_order_lines": SALES_ORDER_LINES_PATH,
    "sales_daily_summary": SALES_DAILY_SUMMARY_PATH,
    "kitchen_batch_runs": KITCHEN_BATCH_RUNS_PATH,
    "kitchen_batch_consumption": KITCHEN_BATCH_CONSUMPTION_PATH,
    "sku_consumption_daily": SKU_CONSUMPTION_DAILY_PATH,
}

INVENTORY_DAILY_STOCK_COLUMNS = [
    "stock_id",
    "date",
    "branch_id",
    "item_type",
    "item_id",
    "opening_stock",
    "purchase_in_qty",
    "production_in_qty",
    "sales_consumption_qty",
    "internal_use_qty",
    "wastage_qty",
    "closing_stock",
    "stockout_flag",
]

INVENTORY_MOVEMENTS_COLUMNS = [
    "movement_id",
    "movement_datetime",
    "branch_id",
    "item_type",
    "item_id",
    "movement_type",
    "quantity",
    "reference_id",
    "reference_source",
    "unit_cost",
]

WASTAGE_COLUMNS = [
    "wastage_id",
    "date",
    "branch_id",
    "item_type",
    "item_id",
    "wastage_reason",
    "quantity",
    "estimated_cost",
    "reported_by_employee_id",
]

REORDER_COLUMNS = [
    "signal_id",
    "date",
    "branch_id",
    "ingredient_id",
    "closing_stock",
    "reorder_point",
    "reorder_flag",
    "recommended_order_qty",
]

PO_COLUMNS = [
    "po_id",
    "po_date",
    "branch_id",
    "supplier_id",
    "po_status",
    "promised_delivery_date",
    "received_date",
    "payment_due_date",
    "total_po_value",
]

PO_LINE_COLUMNS = [
    "po_line_id",
    "po_id",
    "ingredient_id",
    "ordered_qty",
    "received_qty",
    "purchase_uom",
    "unit_cost",
    "line_total_cost",
    "quality_issue_flag",
    "rejection_qty",
]


def parse_float(value, default=0.0):
    try:
        return float(str(value).strip())
    except Exception:
        return default


def parse_int(value, default=0):
    try:
        return int(float(str(value).strip()))
    except Exception:
        return default


def parse_date(value):
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text[:19], fmt).date()
        except Exception:
            pass
    return None


def round4(value):
    return round(float(value), 4)


def daterange(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def read_csv_rows(path):
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path, columns, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({c: row.get(c, "") for c in columns})


def validate_required_files():
    missing = [str(path) for path in REQUIRED_FILES.values() if not path.exists()]
    if missing:
        raise FileNotFoundError("Missing required files:\n- " + "\n- ".join(sorted(missing)))


def branch_scale(row):
    name = (row.get("branch_name", "") + " " + row.get("city", "")).lower()
    if "karachi" in name:
        return 1.75
    if "lahore" in name:
        return 1.3
    if "islamabad" in name:
        return 1.15
    if "rawalpindi" in name:
        return 1.0
    if "faisalabad" in name:
        return 0.72
    return 0.95


def load_reference_data():
    validate_required_files()

    branches = read_csv_rows(REQUIRED_FILES["branches"])
    suppliers = read_csv_rows(REQUIRED_FILES["suppliers"])
    ingredients = [r for r in read_csv_rows(REQUIRED_FILES["ingredients"]) if str(r.get("active_flag", "1")) != "0"]
    employees = [r for r in read_csv_rows(REQUIRED_FILES["employees"]) if str(r.get("status", "active")).lower() != "inactive"]

    _ = read_csv_rows(REQUIRED_FILES["sales_orders"])
    _ = read_csv_rows(REQUIRED_FILES["sales_order_lines"])
    _ = read_csv_rows(REQUIRED_FILES["sales_daily_summary"])

    kitchen_runs = read_csv_rows(REQUIRED_FILES["kitchen_batch_runs"])
    kitchen_consumption = read_csv_rows(REQUIRED_FILES["kitchen_batch_consumption"])
    sku_consumption = read_csv_rows(REQUIRED_FILES["sku_consumption_daily"])

    branches_by_id = {b["branch_id"]: b for b in branches}
    suppliers_by_id = {s["supplier_id"]: s for s in suppliers}
    ingredients_by_id = {i["ingredient_id"]: i for i in ingredients}

    employees_by_branch = defaultdict(list)
    for emp in employees:
        employees_by_branch[emp.get("branch_id", "")].append(emp.get("employee_id", ""))

    kitchen_run_by_id = {}
    for run in kitchen_runs:
        run_date = parse_date(run.get("batch_date"))
        if not run_date:
            continue
        if run_date < START_DATE or run_date > END_DATE:
            continue
        kitchen_run_by_id[run.get("batch_run_id", "")] = run

    sku_sales_by_key = defaultdict(float)
    sku_ref_by_key = defaultdict(list)
    for row in sku_consumption:
        row_date = parse_date(row.get("date"))
        if not row_date or row_date < START_DATE or row_date > END_DATE:
            continue
        branch_id = row.get("branch_id", "")
        ingredient_id = row.get("ingredient_id", "")
        if branch_id not in branches_by_id or ingredient_id not in ingredients_by_id:
            continue
        qty = parse_float(row.get("actual_qty_consumed", row.get("theoretical_qty_consumed", 0.0)), 0.0)
        if qty <= 0:
            continue
        key = (row_date, branch_id, ingredient_id)
        sku_sales_by_key[key] += qty
        sku_ref_by_key[key].append(row.get("consumption_id", ""))

    batch_use_by_key = defaultdict(float)
    batch_ref_by_key = defaultdict(list)
    for row in kitchen_consumption:
        run_id = row.get("batch_run_id", "")
        run = kitchen_run_by_id.get(run_id)
        if not run:
            continue
        branch_id = run.get("branch_id", "")
        ingredient_id = row.get("ingredient_id", "")
        if branch_id not in branches_by_id or ingredient_id not in ingredients_by_id:
            continue
        qty = parse_float(row.get("quantity_used", 0.0), 0.0)
        if qty <= 0:
            continue
        row_date = parse_date(run.get("batch_date"))
        key = (row_date, branch_id, ingredient_id)
        batch_use_by_key[key] += qty
        batch_ref_by_key[key].append(run_id)

    return {
        "branches": branches,
        "branches_by_id": branches_by_id,
        "suppliers_by_id": suppliers_by_id,
        "ingredients": ingredients,
        "ingredients_by_id": ingredients_by_id,
        "employees_by_branch": employees_by_branch,
        "kitchen_run_by_id": kitchen_run_by_id,
        "sku_sales_by_key": sku_sales_by_key,
        "sku_ref_by_key": sku_ref_by_key,
        "batch_use_by_key": batch_use_by_key,
        "batch_ref_by_key": batch_ref_by_key,
    }


def build_model_universe(ref):
    used_pairs = set()
    for key, qty in ref["sku_sales_by_key"].items():
        if qty > 0:
            used_pairs.add((key[1], key[2]))
    for key, qty in ref["batch_use_by_key"].items():
        if qty > 0:
            used_pairs.add((key[1], key[2]))

    avg_daily = defaultdict(float)
    day_count = 366.0
    for day, branch_id, ingredient_id in list(ref["sku_sales_by_key"].keys()) + list(ref["batch_use_by_key"].keys()):
        _ = day
        avg_daily[(branch_id, ingredient_id)] += ref["sku_sales_by_key"].get((day, branch_id, ingredient_id), 0.0)
        avg_daily[(branch_id, ingredient_id)] += 0.65 * ref["batch_use_by_key"].get((day, branch_id, ingredient_id), 0.0)

    for key in list(avg_daily.keys()):
        avg_daily[key] = max(0.05, avg_daily[key] / day_count)

    # If a branch has no measured usage for ingredient but ingredient is core category, include a light baseline.
    for branch in ref["branches"]:
        branch_id = branch["branch_id"]
        for ing in ref["ingredients"]:
            if str(ing.get("is_packaging", "0")) == "1" or ing.get("ingredient_group", "").lower() in {
                "protein",
                "dairy",
                "bread",
                "sauce",
            }:
                key = (branch_id, ing["ingredient_id"])
                if key not in avg_daily and random.random() < 0.18:
                    used_pairs.add(key)
                    avg_daily[key] = 0.08 * branch_scale(branch)

    return used_pairs, avg_daily


def choose_wastage_reason(ingredient_row):
    is_perishable = str(ingredient_row.get("is_perishable", "0")) == "1"
    if is_perishable:
        pool = ["expiry", "overproduction", "spillage", "damage"]
        weights = [0.45, 0.22, 0.2, 0.13]
    else:
        pool = ["damage", "spillage", "overproduction", "expiry"]
        weights = [0.5, 0.27, 0.18, 0.05]
    pick = random.random() * sum(weights)
    running = 0.0
    for item, w in zip(pool, weights):
        running += w
        if pick <= running:
            return item
    return pool[0]


def payment_due_from_supplier(supplier_row, received_date):
    terms = parse_int(supplier_row.get("payment_terms_days", 14), 14)
    terms = max(7, min(60, terms))
    return received_date + timedelta(days=terms)


def generate_inventory_and_purchase(ref):
    branches = ref["branches"]
    branches_by_id = ref["branches_by_id"]
    suppliers_by_id = ref["suppliers_by_id"]
    ingredients_by_id = ref["ingredients_by_id"]
    employees_by_branch = ref["employees_by_branch"]

    used_pairs, avg_daily = build_model_universe(ref)

    inventory_daily_stock = []
    inventory_movements = []
    wastage_log = []
    reorder_signals = []
    purchase_orders = []
    po_lines = []

    stock_counter = 1
    movement_counter = 1
    wastage_counter = 1
    signal_counter = 1
    po_counter = 1
    po_line_counter = 1

    stock_state = {}
    reorder_point_map = {}
    target_stock_map = {}
    outstanding_po = defaultdict(list)
    receipts_schedule = defaultdict(list)

    po_total_received_qty_by_branch = defaultdict(float)

    for branch in branches:
        branch_id = branch["branch_id"]
        b_scale = branch_scale(branch)
        for ingredient in ref["ingredients"]:
            ing_id = ingredient["ingredient_id"]
            if (branch_id, ing_id) not in used_pairs:
                continue

            daily_use = avg_daily.get((branch_id, ing_id), 0.08 * b_scale)
            is_perishable = str(ingredient.get("is_perishable", "0")) == "1"
            is_packaging = str(ingredient.get("is_packaging", "0")) == "1"

            if is_packaging:
                reorder_cover = 14.0
                target_cover = 26.0
            elif is_perishable:
                reorder_cover = 2.8
                target_cover = 6.0
            else:
                reorder_cover = 5.5
                target_cover = 11.0

            reorder_point = max(0.25, daily_use * reorder_cover * b_scale)
            target_stock = max(reorder_point * 1.25, daily_use * target_cover * b_scale)
            reorder_point_map[(branch_id, ing_id)] = reorder_point
            target_stock_map[(branch_id, ing_id)] = target_stock

            jan_growth = 1.0
            if "faisalabad" in (branch.get("city", "") + " " + branch.get("branch_name", "")).lower():
                jan_growth = 0.83

            opening_stock = target_stock * random.uniform(0.8, 1.15) * jan_growth
            stock_state[(branch_id, ing_id)] = max(0.0, opening_stock)

    for day in daterange(START_DATE, END_DATE):
        day_index = (day - START_DATE).days
        for branch in branches:
            branch_id = branch["branch_id"]
            seasonal_scale = 1.0
            if "faisalabad" in (branch.get("city", "") + " " + branch.get("branch_name", "")).lower():
                seasonal_scale = 0.85 + (0.3 * (day_index / 364.0))

            for ingredient in ref["ingredients"]:
                ing_id = ingredient["ingredient_id"]
                pair = (branch_id, ing_id)
                if pair not in stock_state:
                    continue

                unit_cost = parse_float(ingredient.get("standard_unit_cost", 0.0), 0.0)
                opening_stock = stock_state[pair]
                purchase_in = 0.0
                production_in = 0.0

                day_receipts = receipts_schedule.get((day, branch_id, ing_id), [])
                for receipt in day_receipts:
                    receipt_qty = receipt["received_qty"]
                    purchase_in += receipt_qty
                    inventory_movements.append(
                        {
                            "movement_id": f"MOV{movement_counter:010d}",
                            "movement_datetime": f"{day.isoformat()} 10:00:00",
                            "branch_id": branch_id,
                            "item_type": "ingredient",
                            "item_id": ing_id,
                            "movement_type": "purchase_receipt",
                            "quantity": round4(receipt_qty),
                            "reference_id": receipt["po_id"],
                            "reference_source": "purchase_orders",
                            "unit_cost": round4(receipt["unit_cost"]),
                        }
                    )
                    movement_counter += 1

                sales_consumption = ref["sku_sales_by_key"].get((day, branch_id, ing_id), 0.0)
                batch_consumption = ref["batch_use_by_key"].get((day, branch_id, ing_id), 0.0)
                internal_use = batch_consumption
                baseline = avg_daily.get(pair, 0.06)

                if sales_consumption <= 0 and random.random() < 0.12:
                    sales_consumption = baseline * random.uniform(0.1, 0.55) * seasonal_scale
                if internal_use <= 0 and random.random() < 0.08:
                    internal_use = baseline * random.uniform(0.05, 0.35)

                wastage = 0.0
                is_perishable = str(ingredient.get("is_perishable", "0")) == "1"
                is_packaging = str(ingredient.get("is_packaging", "0")) == "1"
                waste_prob = 0.025 + (0.035 if is_perishable else 0.0) - (0.012 if is_packaging else 0.0)
                if random.random() < max(0.003, waste_prob):
                    wastage = max(0.01, baseline * random.uniform(0.04, 0.34))
                    reason = choose_wastage_reason(ingredient)
                    reporters = employees_by_branch.get(branch_id, [])
                    reporter = random.choice(reporters) if reporters else ""
                    wastage_log.append(
                        {
                            "wastage_id": f"WST{wastage_counter:010d}",
                            "date": day.isoformat(),
                            "branch_id": branch_id,
                            "item_type": "ingredient",
                            "item_id": ing_id,
                            "wastage_reason": reason,
                            "quantity": round4(wastage),
                            "estimated_cost": round4(wastage * unit_cost),
                            "reported_by_employee_id": reporter,
                        }
                    )
                    inventory_movements.append(
                        {
                            "movement_id": f"MOV{movement_counter:010d}",
                            "movement_datetime": f"{day.isoformat()} 20:00:00",
                            "branch_id": branch_id,
                            "item_type": "ingredient",
                            "item_id": ing_id,
                            "movement_type": "wastage",
                            "quantity": round4(wastage),
                            "reference_id": f"WST{wastage_counter:010d}",
                            "reference_source": "inventory_wastage_log",
                            "unit_cost": round4(unit_cost),
                        }
                    )
                    movement_counter += 1
                    wastage_counter += 1

                if day == START_DATE:
                    inventory_movements.append(
                        {
                            "movement_id": f"MOV{movement_counter:010d}",
                            "movement_datetime": f"{day.isoformat()} 00:05:00",
                            "branch_id": branch_id,
                            "item_type": "ingredient",
                            "item_id": ing_id,
                            "movement_type": "opening_balance",
                            "quantity": round4(opening_stock),
                            "reference_id": f"OPEN-{branch_id}-{ing_id}",
                            "reference_source": "inventory_daily_stock",
                            "unit_cost": round4(unit_cost),
                        }
                    )
                    movement_counter += 1

                if sales_consumption > 0:
                    sale_refs = ref["sku_ref_by_key"].get((day, branch_id, ing_id), [])
                    sale_ref = sale_refs[0] if sale_refs else f"SCON-{branch_id}-{ing_id}-{day.isoformat()}"
                    inventory_movements.append(
                        {
                            "movement_id": f"MOV{movement_counter:010d}",
                            "movement_datetime": f"{day.isoformat()} 14:00:00",
                            "branch_id": branch_id,
                            "item_type": "ingredient",
                            "item_id": ing_id,
                            "movement_type": "sales_consumption",
                            "quantity": round4(sales_consumption),
                            "reference_id": sale_ref,
                            "reference_source": "sku_consumption_daily",
                            "unit_cost": round4(unit_cost),
                        }
                    )
                    movement_counter += 1

                if internal_use > 0:
                    batch_refs = ref["batch_ref_by_key"].get((day, branch_id, ing_id), [])
                    batch_ref = batch_refs[0] if batch_refs else f"BATCH-{branch_id}-{ing_id}-{day.isoformat()}"
                    inventory_movements.append(
                        {
                            "movement_id": f"MOV{movement_counter:010d}",
                            "movement_datetime": f"{day.isoformat()} 16:00:00",
                            "branch_id": branch_id,
                            "item_type": "ingredient",
                            "item_id": ing_id,
                            "movement_type": "batch_consumption",
                            "quantity": round4(internal_use),
                            "reference_id": batch_ref,
                            "reference_source": "kitchen_batch_consumption",
                            "unit_cost": round4(unit_cost),
                        }
                    )
                    movement_counter += 1

                raw_closing = opening_stock + purchase_in + production_in - sales_consumption - internal_use - wastage
                stockout_flag = 1 if raw_closing <= 0 else 0
                closing_stock = max(0.0, raw_closing)

                row = {
                    "stock_id": f"STK{stock_counter:011d}",
                    "date": day.isoformat(),
                    "branch_id": branch_id,
                    "item_type": "ingredient",
                    "item_id": ing_id,
                    "opening_stock": round4(opening_stock),
                    "purchase_in_qty": round4(purchase_in),
                    "production_in_qty": round4(production_in),
                    "sales_consumption_qty": round4(sales_consumption),
                    "internal_use_qty": round4(internal_use),
                    "wastage_qty": round4(wastage),
                    "closing_stock": round4(closing_stock),
                    "stockout_flag": stockout_flag,
                }
                inventory_daily_stock.append(row)
                stock_counter += 1
                stock_state[pair] = closing_stock

                reorder_point = reorder_point_map[pair] * seasonal_scale
                target_stock = target_stock_map[pair] * seasonal_scale
                reorder_flag = 1 if closing_stock < reorder_point else 0
                recommended_qty = max(0.0, target_stock - closing_stock)

                reorder_signals.append(
                    {
                        "signal_id": f"RSG{signal_counter:010d}",
                        "date": day.isoformat(),
                        "branch_id": branch_id,
                        "ingredient_id": ing_id,
                        "closing_stock": round4(closing_stock),
                        "reorder_point": round4(reorder_point),
                        "reorder_flag": reorder_flag,
                        "recommended_order_qty": round4(recommended_qty if reorder_flag else 0.0),
                    }
                )
                signal_counter += 1

                if reorder_flag:
                    has_open_po = False
                    for po_id in outstanding_po[pair]:
                        if po_id:
                            has_open_po = True
                            break

                    if not has_open_po and recommended_qty > 0:
                        ingredient_supplier = ingredient.get("default_supplier_id", "")
                        if ingredient_supplier not in suppliers_by_id:
                            supplier_ids = sorted(suppliers_by_id.keys())
                            ingredient_supplier = random.choice(supplier_ids) if supplier_ids else ""

                        supplier = suppliers_by_id.get(ingredient_supplier, {})
                        lead_days = parse_int(supplier.get("lead_time_days", 2), 2)
                        lead_days = max(1, min(14, lead_days))
                        delay = 0
                        if random.random() < 0.18:
                            delay = random.randint(1, 3)

                        po_date = day
                        promised_delivery = po_date + timedelta(days=lead_days)
                        received_date = promised_delivery + timedelta(days=delay)

                        daily_use = avg_daily.get(pair, 0.08)
                        lot_multiplier = 2.4 if str(ingredient.get("is_packaging", "0")) == "1" else 1.0
                        ordered_qty = max(recommended_qty, daily_use * (lead_days + 3)) * lot_multiplier
                        ordered_qty *= random.uniform(0.9, 1.2)

                        quality_issue = 1 if random.random() < 0.07 else 0
                        rejection_qty = 0.0
                        if quality_issue:
                            rejection_qty = ordered_qty * random.uniform(0.02, 0.12)

                        if received_date > END_DATE:
                            po_status = "open"
                            rec_qty = 0.0
                            received_date_value = ""
                            payment_due = ""
                        else:
                            fill_rate = random.uniform(0.84, 1.0)
                            if random.random() < 0.11:
                                fill_rate = random.uniform(0.45, 0.83)
                            rec_qty = max(0.0, ordered_qty * fill_rate - rejection_qty)
                            if rec_qty >= ordered_qty * 0.96:
                                po_status = "completed"
                            else:
                                po_status = "partial"
                            if delay > 0 and po_status == "completed":
                                po_status = "delayed"
                            received_date_value = received_date.isoformat()
                            payment_due = payment_due_from_supplier(supplier, received_date).isoformat()

                        unit_cost = parse_float(ingredient.get("standard_unit_cost", 0.0), 0.0) * random.uniform(0.96, 1.08)
                        line_total_cost = rec_qty * unit_cost

                        po_id = f"PO{po_counter:010d}"
                        po_line_id = f"POL{po_line_counter:011d}"
                        po_counter += 1
                        po_line_counter += 1

                        po_lines.append(
                            {
                                "po_line_id": po_line_id,
                                "po_id": po_id,
                                "ingredient_id": ing_id,
                                "ordered_qty": round4(ordered_qty),
                                "received_qty": round4(rec_qty),
                                "purchase_uom": ingredient.get("buying_uom", ingredient.get("base_uom", "unit")),
                                "unit_cost": round4(unit_cost),
                                "line_total_cost": round4(line_total_cost),
                                "quality_issue_flag": quality_issue,
                                "rejection_qty": round4(rejection_qty),
                            }
                        )

                        # Optional extra line from same supplier when two items are simultaneously low.
                        if random.random() < 0.18:
                            peer_candidates = []
                            for other in ref["ingredients"]:
                                other_id = other["ingredient_id"]
                                if other_id == ing_id:
                                    continue
                                if other.get("default_supplier_id", "") != ingredient_supplier:
                                    continue
                                other_pair = (branch_id, other_id)
                                if other_pair not in stock_state:
                                    continue
                                other_target = target_stock_map.get(other_pair, 0.0) * seasonal_scale
                                other_stock = stock_state.get(other_pair, 0.0)
                                if other_stock < other_target * 0.55:
                                    peer_candidates.append(other)
                            if peer_candidates:
                                extra = random.choice(peer_candidates)
                                extra_id = extra["ingredient_id"]
                                extra_pair = (branch_id, extra_id)
                                extra_daily = avg_daily.get(extra_pair, 0.06)
                                extra_order = max(extra_daily * (lead_days + 4), target_stock_map.get(extra_pair, 1.0) * 0.38)
                                if str(extra.get("is_packaging", "0")) == "1":
                                    extra_order *= 1.6
                                extra_order *= random.uniform(0.86, 1.15)
                                extra_rej = 0.0
                                extra_qf = 0
                                if random.random() < 0.05:
                                    extra_qf = 1
                                    extra_rej = extra_order * random.uniform(0.01, 0.09)
                                if received_date > END_DATE:
                                    extra_rec = 0.0
                                else:
                                    extra_rec = max(0.0, extra_order * random.uniform(0.8, 1.0) - extra_rej)
                                extra_cost = parse_float(extra.get("standard_unit_cost", 0.0), 0.0) * random.uniform(0.95, 1.09)
                                po_lines.append(
                                    {
                                        "po_line_id": f"POL{po_line_counter:011d}",
                                        "po_id": po_id,
                                        "ingredient_id": extra_id,
                                        "ordered_qty": round4(extra_order),
                                        "received_qty": round4(extra_rec),
                                        "purchase_uom": extra.get("buying_uom", extra.get("base_uom", "unit")),
                                        "unit_cost": round4(extra_cost),
                                        "line_total_cost": round4(extra_rec * extra_cost),
                                        "quality_issue_flag": extra_qf,
                                        "rejection_qty": round4(extra_rej),
                                    }
                                )
                                po_line_counter += 1
                                if received_date <= END_DATE:
                                    receipts_schedule[(received_date, branch_id, extra_id)].append(
                                        {
                                            "po_id": po_id,
                                            "received_qty": extra_rec,
                                            "unit_cost": extra_cost,
                                        }
                                    )
                                    po_total_received_qty_by_branch[branch_id] += extra_rec

                        purchase_orders.append(
                            {
                                "po_id": po_id,
                                "po_date": po_date.isoformat(),
                                "branch_id": branch_id,
                                "supplier_id": ingredient_supplier,
                                "po_status": po_status,
                                "promised_delivery_date": promised_delivery.isoformat(),
                                "received_date": received_date_value,
                                "payment_due_date": payment_due,
                                "total_po_value": 0.0,
                            }
                        )

                        if received_date <= END_DATE:
                            receipts_schedule[(received_date, branch_id, ing_id)].append(
                                {
                                    "po_id": po_id,
                                    "received_qty": rec_qty,
                                    "unit_cost": unit_cost,
                                }
                            )
                            po_total_received_qty_by_branch[branch_id] += rec_qty
                        else:
                            outstanding_po[pair].append(po_id)

    # Resolve PO totals from line totals (received basis).
    line_total_by_po = defaultdict(float)
    for line in po_lines:
        line_total_by_po[line["po_id"]] += parse_float(line.get("line_total_cost", 0.0), 0.0)
    for po in purchase_orders:
        po["total_po_value"] = round4(line_total_by_po.get(po["po_id"], 0.0))

    outputs = {
        "inventory_daily_stock": inventory_daily_stock,
        "inventory_movements": inventory_movements,
        "inventory_wastage_log": wastage_log,
        "purchase_reorder_signals": reorder_signals,
        "purchase_orders": purchase_orders,
        "purchase_order_lines": po_lines,
        "po_total_received_qty_by_branch": po_total_received_qty_by_branch,
        "used_pairs": used_pairs,
    }
    return outputs


def run_validations(ref, out):
    branches = set(ref["branches_by_id"].keys())
    ingredients = set(ref["ingredients_by_id"].keys())
    employees_by_branch = ref["employees_by_branch"]
    suppliers = set(ref["suppliers_by_id"].keys())

    po_ids = {p["po_id"] for p in out["purchase_orders"]}
    po_line_po_ids = {l["po_id"] for l in out["purchase_order_lines"]}
    run_ids = set(ref["kitchen_run_by_id"].keys())

    # 1. no orphan foreign keys
    for row in out["inventory_daily_stock"]:
        if row["branch_id"] not in branches or row["item_id"] not in ingredients:
            raise ValueError("inventory_daily_stock foreign key violation")
    for row in out["inventory_movements"]:
        if row["branch_id"] not in branches or row["item_id"] not in ingredients:
            raise ValueError("inventory_movements foreign key violation")
    for row in out["inventory_wastage_log"]:
        if row["branch_id"] not in branches or row["item_id"] not in ingredients:
            raise ValueError("inventory_wastage_log foreign key violation")
        if row["reported_by_employee_id"] not in employees_by_branch.get(row["branch_id"], []):
            raise ValueError("wastage reporter foreign key violation")
    for row in out["purchase_reorder_signals"]:
        if row["branch_id"] not in branches or row["ingredient_id"] not in ingredients:
            raise ValueError("purchase_reorder_signals foreign key violation")
    for row in out["purchase_orders"]:
        if row["branch_id"] not in branches or row["supplier_id"] not in suppliers:
            raise ValueError("purchase_orders foreign key violation")
    for row in out["purchase_order_lines"]:
        if row["po_id"] not in po_ids or row["ingredient_id"] not in ingredients:
            raise ValueError("purchase_order_lines foreign key violation")

    # 2,3,4. stock checks
    for row in out["inventory_daily_stock"]:
        opening = parse_float(row["opening_stock"], 0.0)
        purchase_in = parse_float(row["purchase_in_qty"], 0.0)
        production_in = parse_float(row["production_in_qty"], 0.0)
        sales = parse_float(row["sales_consumption_qty"], 0.0)
        internal = parse_float(row["internal_use_qty"], 0.0)
        wastage = parse_float(row["wastage_qty"], 0.0)
        closing = parse_float(row["closing_stock"], 0.0)
        stockout = parse_int(row["stockout_flag"], 0)

        if closing < 0:
            raise ValueError("negative closing stock")
        if (stockout == 1 and closing != 0) or (stockout == 0 and closing == 0):
            raise ValueError("stockout_flag must match closing_stock")

        expected = opening + purchase_in + production_in - sales - internal - wastage
        expected = max(0.0, expected)
        if abs(expected - closing) > 1e-3:
            raise ValueError("inventory_daily_stock formula mismatch")

    # 5. purchase receipt movement must link to PO
    for row in out["inventory_movements"]:
        if row["movement_type"] == "purchase_receipt":
            if row["reference_source"] != "purchase_orders" or row["reference_id"] not in po_ids:
                raise ValueError("purchase_receipt movement reference mismatch")

    # 6. batch_consumption movement links to kitchen ref
    for row in out["inventory_movements"]:
        if row["movement_type"] == "batch_consumption":
            if row["reference_source"] != "kitchen_batch_consumption":
                raise ValueError("batch movement source mismatch")
            ref_id = row["reference_id"]
            if not (ref_id in run_ids or ref_id.startswith("BATCH-")):
                raise ValueError("batch movement reference mismatch")

    # 7. sales_consumption movement links to sku/sales ref
    for row in out["inventory_movements"]:
        if row["movement_type"] == "sales_consumption":
            if row["reference_source"] != "sku_consumption_daily":
                raise ValueError("sales movement source mismatch")
            ref_id = row["reference_id"]
            if not (ref_id.startswith("SCON-") or str(ref_id).strip()):
                raise ValueError("sales movement reference missing")

    # 8. PO line roll-up consistency
    po_totals = defaultdict(float)
    for line in out["purchase_order_lines"]:
        po_totals[line["po_id"]] += parse_float(line.get("line_total_cost", 0.0), 0.0)
    for po in out["purchase_orders"]:
        if abs(parse_float(po.get("total_po_value", 0.0), 0.0) - po_totals.get(po["po_id"], 0.0)) > 1e-3:
            raise ValueError("PO total mismatch")

    # 9. busiest branch should have more purchase than low-volume branch
    branch_name_map = {
        row["branch_id"]: (row.get("branch_name", "") + " " + row.get("city", "")).lower()
        for row in ref["branches"]
    }
    karachi_ids = [bid for bid, n in branch_name_map.items() if "karachi" in n]
    faisalabad_ids = [bid for bid, n in branch_name_map.items() if "faisalabad" in n]
    if karachi_ids and faisalabad_ids:
        karachi_total = sum(out["po_total_received_qty_by_branch"].get(x, 0.0) for x in karachi_ids)
        faisalabad_total = sum(out["po_total_received_qty_by_branch"].get(x, 0.0) for x in faisalabad_ids)
        if karachi_total <= faisalabad_total:
            raise ValueError("Branch volume realism violation: Karachi <= Faisalabad")

    # 10. full-year daily coverage for modeled branch/ingredient pairs
    days_required = (END_DATE - START_DATE).days + 1
    coverage = defaultdict(int)
    for row in out["inventory_daily_stock"]:
        key = (row["branch_id"], row["item_id"])
        coverage[key] += 1
    for key in out["used_pairs"]:
        if coverage.get(key, 0) != days_required:
            raise ValueError("Coverage violation for branch-ingredient pair")

    # Additional sanity: all PO ids referenced by lines
    if not po_line_po_ids.issubset(po_ids):
        raise ValueError("PO lines contain unknown PO ids")


def main():
    ref = load_reference_data()
    outputs = generate_inventory_and_purchase(ref)
    run_validations(ref, outputs)

    out_paths = {
        "inventory_daily_stock.csv": (INVENTORY_DAILY_STOCK_COLUMNS, outputs["inventory_daily_stock"]),
        "inventory_movements.csv": (INVENTORY_MOVEMENTS_COLUMNS, outputs["inventory_movements"]),
        "inventory_wastage_log.csv": (WASTAGE_COLUMNS, outputs["inventory_wastage_log"]),
        "purchase_reorder_signals.csv": (REORDER_COLUMNS, outputs["purchase_reorder_signals"]),
        "purchase_orders.csv": (PO_COLUMNS, outputs["purchase_orders"]),
        "purchase_order_lines.csv": (PO_LINE_COLUMNS, outputs["purchase_order_lines"]),
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for file_name, payload in out_paths.items():
        cols, rows = payload
        path = OUTPUT_DIR / file_name
        write_csv(path, cols, rows)
        print(f"{path} :: {len(rows)} rows")

    print("Generated inventory and purchase layer for 2025 with validated relationships and reconciled stock math.")


if __name__ == "__main__":
    main()
