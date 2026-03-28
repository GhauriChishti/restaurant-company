import csv
import math
import os
import random
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

SEED = 202503
random.seed(SEED)

START_DATE = date(2025, 1, 1)
END_DATE = date(2025, 12, 31)

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = Path(os.environ.get("KITCHEN_OUTPUT_DIR", ROOT)).expanduser().resolve()

REQUIRED_FILES = {
    "branches": ROOT / "branches_master.csv",
    "ingredients": ROOT / "ingredients_master.csv",
    "batches": ROOT / "batches_master.csv",
    "batch_recipe": ROOT / "batch_recipe_lines.csv",
    "processed_items": ROOT / "processed_items_master.csv",
    "skus": ROOT / "sku_master.csv",
    "sku_recipe": ROOT / "sku_recipe_lines.csv",
    "employees": ROOT / "employees_master.csv",
    "sales_orders": ROOT / "sales_orders.csv",
    "sales_order_lines": ROOT / "sales_order_lines.csv",
    "sales_daily_summary": ROOT / "sales_daily_summary.csv",
}

BATCH_RUNS_COLUMNS = [
    "batch_run_id",
    "batch_date",
    "branch_id",
    "batch_id",
    "output_qty",
    "output_uom",
    "waste_qty",
    "shift",
    "produced_by_employee_id",
    "total_batch_cost",
]

BATCH_CONSUMPTION_COLUMNS = [
    "batch_run_consumption_id",
    "batch_run_id",
    "ingredient_id",
    "quantity_used",
    "unit_cost",
    "total_cost",
]

SKU_CONSUMPTION_COLUMNS = [
    "consumption_id",
    "date",
    "branch_id",
    "sku_id",
    "ingredient_id",
    "theoretical_qty_consumed",
    "actual_qty_consumed",
    "variance_qty",
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
            writer.writerow({k: row.get(k, "") for k in columns})


def validate_required_files():
    missing = [str(path) for path in REQUIRED_FILES.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing required reference files:\n- " + "\n- ".join(sorted(missing))
        )


def load_reference_data():
    validate_required_files()

    branches = read_csv_rows(REQUIRED_FILES["branches"])
    ingredients = read_csv_rows(REQUIRED_FILES["ingredients"])
    batches = read_csv_rows(REQUIRED_FILES["batches"])
    batch_recipe_lines = read_csv_rows(REQUIRED_FILES["batch_recipe"])
    processed_items = read_csv_rows(REQUIRED_FILES["processed_items"])
    skus = read_csv_rows(REQUIRED_FILES["skus"])
    sku_recipe_lines = read_csv_rows(REQUIRED_FILES["sku_recipe"])
    employees = read_csv_rows(REQUIRED_FILES["employees"])
    sales_orders = read_csv_rows(REQUIRED_FILES["sales_orders"])
    sales_order_lines = read_csv_rows(REQUIRED_FILES["sales_order_lines"])
    _ = read_csv_rows(REQUIRED_FILES["sales_daily_summary"])

    branches_by_id = {row["branch_id"]: row for row in branches}
    ingredients_by_id = {row["ingredient_id"]: row for row in ingredients}
    batches_by_id = {row["batch_id"]: row for row in batches}
    processed_item_to_batch = {
        row["processed_item_id"]: row.get("source_batch_id", "")
        for row in processed_items
        if row.get("source_batch_id")
    }
    skus_by_id = {row["sku_id"]: row for row in skus}

    sku_recipe_by_sku = defaultdict(list)
    for line in sku_recipe_lines:
        sku_recipe_by_sku[line["sku_id"]].append(line)

    batch_recipe_by_batch = defaultdict(list)
    for line in batch_recipe_lines:
        batch_recipe_by_batch[line["batch_id"]].append(line)

    orders_by_id = {row["order_id"]: row for row in sales_orders}

    kitchen_employees_by_branch = defaultdict(list)
    for row in employees:
        dep = str(row.get("department", "")).strip().lower()
        role = str(row.get("role", "")).strip().lower()
        if dep == "kitchen" or "kitchen" in role or "helper" in role or "shift lead" in role:
            kitchen_employees_by_branch[row["branch_id"]].append(row["employee_id"])

    ingredient_unit_cost = {
        row["ingredient_id"]: parse_float(row.get("standard_unit_cost", 0.0), 0.0)
        for row in ingredients
    }

    return {
        "branches_by_id": branches_by_id,
        "ingredients_by_id": ingredients_by_id,
        "batches_by_id": batches_by_id,
        "processed_item_to_batch": processed_item_to_batch,
        "skus_by_id": skus_by_id,
        "sku_recipe_by_sku": sku_recipe_by_sku,
        "batch_recipe_by_batch": batch_recipe_by_batch,
        "orders_by_id": orders_by_id,
        "sales_order_lines": sales_order_lines,
        "kitchen_employees_by_branch": kitchen_employees_by_branch,
        "ingredient_unit_cost": ingredient_unit_cost,
    }


def extract_order_date(order_row):
    if order_row.get("order_date"):
        return datetime.strptime(order_row["order_date"], "%Y-%m-%d").date()
    if order_row.get("order_datetime"):
        return datetime.fromisoformat(order_row["order_datetime"]).date()
    raise ValueError(f"Order {order_row.get('order_id')} missing date fields")


def build_consumption_and_batch_demand(ref):
    ingredients_by_id = ref["ingredients_by_id"]
    processed_item_to_batch = ref["processed_item_to_batch"]
    sku_recipe_by_sku = ref["sku_recipe_by_sku"]
    orders_by_id = ref["orders_by_id"]

    sku_consumption_rows = []
    batch_demand = defaultdict(float)
    sales_qty_by_branch = defaultdict(float)
    sales_dates_by_branch = defaultdict(set)

    consumption_id = 1

    for line in ref["sales_order_lines"]:
        order = orders_by_id.get(line.get("order_id", ""))
        if not order:
            continue

        branch_id = order["branch_id"]
        order_date = extract_order_date(order)
        if order_date < START_DATE or order_date > END_DATE:
            continue

        sku_id = line.get("sku_id", "")
        sold_qty = parse_float(line.get("quantity", 0.0), 0.0)
        if sold_qty <= 0:
            continue

        sales_qty_by_branch[branch_id] += sold_qty
        sales_dates_by_branch[branch_id].add(order_date)

        for recipe in sku_recipe_by_sku.get(sku_id, []):
            component_type = str(recipe.get("component_type", "")).strip().lower()
            component_id = recipe.get("component_id", "")
            req_qty = parse_float(recipe.get("quantity_required", 0.0), 0.0)
            if req_qty <= 0:
                continue

            theoretical = sold_qty * req_qty

            if component_type in ("raw_ingredient", "packaging"):
                ingredient_id = component_id
                if ingredient_id not in ingredients_by_id:
                    continue

                pct_variance = random.uniform(0.03, 0.08)
                actual = theoretical * (1 + pct_variance if random.random() < 0.5 else 1 - pct_variance)
                variance = actual - theoretical

                sku_consumption_rows.append(
                    {
                        "consumption_id": f"SC{consumption_id:09d}",
                        "date": order_date.isoformat(),
                        "branch_id": branch_id,
                        "sku_id": sku_id,
                        "ingredient_id": ingredient_id,
                        "theoretical_qty_consumed": round4(theoretical),
                        "actual_qty_consumed": round4(max(0.0, actual)),
                        "variance_qty": round4(variance),
                    }
                )
                consumption_id += 1
            elif component_type == "batch":
                batch_demand[(order_date.isoformat(), branch_id, component_id)] += theoretical
            elif component_type == "processed_item":
                source_batch_id = processed_item_to_batch.get(component_id)
                if source_batch_id:
                    batch_demand[(order_date.isoformat(), branch_id, source_batch_id)] += theoretical

    return sku_consumption_rows, batch_demand, sales_qty_by_branch, sales_dates_by_branch


def batch_frequency_factor(batch_name):
    n = batch_name.lower()
    if "dip" in n or "sauce" in n:
        return 1.25
    if "breading" in n or "spicy" in n or "less spicy" in n:
        return 1.00
    if "tender" in n or "marination" in n or "marinade" in n:
        return 0.70
    return 0.90


def pick_shift(batch_name):
    n = batch_name.lower()
    if "dip" in n or "sauce" in n:
        return "Morning" if random.random() < 0.55 else "Evening"
    if "marination" in n or "tender" in n:
        return "Morning" if random.random() < 0.70 else "Evening"
    return "Morning" if random.random() < 0.50 else "Evening"


def generate_batch_runs_and_consumption(ref, batch_demand):
    batches_by_id = ref["batches_by_id"]
    batch_recipe_by_batch = ref["batch_recipe_by_batch"]
    kitchen_employees_by_branch = ref["kitchen_employees_by_branch"]
    ingredient_unit_cost = ref["ingredient_unit_cost"]

    batch_runs = []
    batch_consumption = []

    run_id_seq = 1
    cons_id_seq = 1

    for (batch_date, branch_id, batch_id), demand_qty in sorted(batch_demand.items()):
        batch = batches_by_id.get(batch_id)
        if not batch:
            continue
        if not kitchen_employees_by_branch.get(branch_id):
            continue

        recipe_lines = batch_recipe_by_batch.get(batch_id, [])
        if not recipe_lines:
            continue

        base_output = parse_float(batch.get("output_qty", 1.0), 1.0)
        waste_pct = parse_float(batch.get("waste_pct", 0.0), 0.0)
        batch_name = batch.get("batch_name", batch_id)

        frequency = batch_frequency_factor(batch_name)
        boosted_demand = demand_qty * frequency
        runs_needed = max(1, int(math.ceil(boosted_demand / max(base_output, 0.0001))))

        if frequency >= 1.2:
            runs_needed += 1 if random.random() < 0.40 else 0
        elif frequency <= 0.75:
            runs_needed = max(1, runs_needed - (1 if random.random() < 0.30 else 0))

        planned_total_output = boosted_demand * random.uniform(1.01, 1.12)
        output_per_run = max(base_output * 0.5, planned_total_output / runs_needed)

        for _ in range(runs_needed):
            output_qty = max(0.0001, output_per_run * random.uniform(0.92, 1.08))
            waste_qty = max(0.0, output_qty * waste_pct)
            employee_id = random.choice(kitchen_employees_by_branch[branch_id])
            shift = pick_shift(batch_name)

            run_id = f"BR{run_id_seq:09d}"
            run_id_seq += 1

            consumption_cost_total = 0.0
            for recipe in recipe_lines:
                ingredient_id = recipe.get("ingredient_id", "")
                base_qty = parse_float(recipe.get("quantity_required", 0.0), 0.0)
                if base_qty <= 0:
                    continue

                quantity_used = base_qty * (output_qty / max(base_output, 0.0001))
                unit_cost = ingredient_unit_cost.get(
                    ingredient_id,
                    parse_float(recipe.get("standard_unit_cost", 0.0), 0.0),
                )
                total_cost = quantity_used * unit_cost
                consumption_cost_total += total_cost

                batch_consumption.append(
                    {
                        "batch_run_consumption_id": f"BRC{cons_id_seq:010d}",
                        "batch_run_id": run_id,
                        "ingredient_id": ingredient_id,
                        "quantity_used": round4(quantity_used),
                        "unit_cost": round4(unit_cost),
                        "total_cost": round4(total_cost),
                    }
                )
                cons_id_seq += 1

            total_batch_cost = consumption_cost_total * random.uniform(0.995, 1.005)
            batch_runs.append(
                {
                    "batch_run_id": run_id,
                    "batch_date": batch_date,
                    "branch_id": branch_id,
                    "batch_id": batch_id,
                    "output_qty": round4(output_qty),
                    "output_uom": batch.get("output_uom", "unit"),
                    "waste_qty": round4(waste_qty),
                    "shift": shift,
                    "produced_by_employee_id": employee_id,
                    "total_batch_cost": round4(total_batch_cost),
                }
            )

    return batch_runs, batch_consumption


def run_validations(ref, sku_consumption_rows, batch_runs, batch_consumption, sales_dates_by_branch, sales_qty_by_branch):
    branches = set(ref["branches_by_id"].keys())
    ingredients = set(ref["ingredients_by_id"].keys())
    batches = set(ref["batches_by_id"].keys())
    skus = set(ref["skus_by_id"].keys())
    employees_by_branch = ref["kitchen_employees_by_branch"]

    run_ids = set()
    for row in batch_runs:
        assert row["branch_id"] in branches, "Orphan branch_id in kitchen_batch_runs"
        assert row["batch_id"] in batches, "Orphan batch_id in kitchen_batch_runs"
        assert row["produced_by_employee_id"] in set(employees_by_branch[row["branch_id"]]), "Invalid/branch-incompatible kitchen employee"
        assert parse_float(row["output_qty"]) >= 0 and parse_float(row["waste_qty"]) >= 0 and parse_float(row["total_batch_cost"]) >= 0, "Negative batch run measure"
        run_ids.add(row["batch_run_id"])

    cost_by_run = defaultdict(float)
    for row in batch_consumption:
        assert row["batch_run_id"] in run_ids, "Batch consumption points to non-existent batch run"
        assert row["ingredient_id"] in ingredients, "Orphan ingredient_id in batch consumption"
        qty = parse_float(row["quantity_used"])
        unit_cost = parse_float(row["unit_cost"])
        total_cost = parse_float(row["total_cost"])
        assert qty >= 0 and unit_cost >= 0 and total_cost >= 0, "Negative quantity or cost in batch consumption"
        cost_by_run[row["batch_run_id"]] += total_cost

    for row in sku_consumption_rows:
        assert row["branch_id"] in branches, "Invalid branch in sku consumption"
        assert row["sku_id"] in skus, "Invalid sku_id in sku consumption"
        assert row["ingredient_id"] in ingredients, "Invalid ingredient_id in sku consumption"
        t = parse_float(row["theoretical_qty_consumed"])
        a = parse_float(row["actual_qty_consumed"])
        assert t >= 0 and a >= 0, "Negative sku consumption quantities"

    for run in batch_runs:
        run_id = run["batch_run_id"]
        expected = cost_by_run.get(run_id, 0.0)
        actual = parse_float(run["total_batch_cost"])
        if expected == 0.0:
            continue
        rel_gap = abs(actual - expected) / expected
        assert rel_gap <= 0.03, "Batch run cost reconciliation failed"

    demand_days = {k: len(v) for k, v in sales_dates_by_branch.items()}
    run_days = defaultdict(set)
    run_count_by_branch = defaultdict(int)
    for row in batch_runs:
        run_days[row["branch_id"]].add(row["batch_date"])
        run_count_by_branch[row["branch_id"]] += 1

    for branch_id, days in demand_days.items():
        if days == 0:
            continue
        coverage_ratio = len(run_days.get(branch_id, set())) / float(days)
        assert coverage_ratio >= 0.30, "Insufficient yearly kitchen coverage for active sales demand"

    ranked_sales = sorted(sales_qty_by_branch.items(), key=lambda x: x[1], reverse=True)
    if len(ranked_sales) >= 2:
        high_branch = ranked_sales[0][0]
        low_branch = ranked_sales[-1][0]
        assert run_count_by_branch.get(high_branch, 0) >= run_count_by_branch.get(low_branch, 0), "Busiest branch has fewer batch runs than low-volume branch"


def main():
    ref = load_reference_data()

    sku_consumption_rows, batch_demand, sales_qty_by_branch, sales_dates_by_branch = build_consumption_and_batch_demand(ref)
    batch_runs, batch_consumption = generate_batch_runs_and_consumption(ref, batch_demand)

    run_validations(
        ref,
        sku_consumption_rows,
        batch_runs,
        batch_consumption,
        sales_dates_by_branch,
        sales_qty_by_branch,
    )

    batch_runs_path = OUTPUT_DIR / "kitchen_batch_runs.csv"
    batch_cons_path = OUTPUT_DIR / "kitchen_batch_consumption.csv"
    sku_cons_path = OUTPUT_DIR / "sku_consumption_daily.csv"

    write_csv(batch_runs_path, BATCH_RUNS_COLUMNS, batch_runs)
    write_csv(batch_cons_path, BATCH_CONSUMPTION_COLUMNS, batch_consumption)
    write_csv(sku_cons_path, SKU_CONSUMPTION_COLUMNS, sku_consumption_rows)

    print("Kitchen layer generation complete:")
    print(f"- {batch_runs_path}")
    print(f"- {batch_cons_path}")
    print(f"- {sku_cons_path}")
    print(f"Rows: batch_runs={len(batch_runs)}, batch_consumption={len(batch_consumption)}, sku_consumption={len(sku_consumption_rows)}")


if __name__ == "__main__":
    main()
