import csv
import os
import random
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

SEED = 202506
random.seed(SEED)

ROOT = Path(__file__).resolve().parents[1]

REQUIRED_FILES = {
    "branches": Path(os.environ.get("BRANCHES_MASTER_PATH", ROOT / "branches_master.csv")),
    "employees": Path(os.environ.get("EMPLOYEES_MASTER_PATH", ROOT / "employees_master.csv")),
    "sales_orders": Path(os.environ.get("SALES_ORDERS_PATH", ROOT / "sales_orders.csv")),
    "sales_order_lines": Path(os.environ.get("SALES_ORDER_LINES_PATH", ROOT / "sales_order_lines.csv")),
    "kitchen_batch_runs": Path(os.environ.get("KITCHEN_BATCH_RUNS_PATH", ROOT / "kitchen_batch_runs.csv")),
    "inventory_daily_stock": Path(os.environ.get("INVENTORY_DAILY_STOCK_PATH", ROOT / "inventory_daily_stock.csv")),
}

OUTPUT_DIR = Path(os.environ.get("OPERATIONS_HR_OUTPUT_DIR", ROOT))
OUTPUT_FILES = {
    "operations_shift_log": OUTPUT_DIR / "operations_shift_log.csv",
    "operations_store_daily_metrics": OUTPUT_DIR / "operations_store_daily_metrics.csv",
    "hr_attendance": OUTPUT_DIR / "hr_attendance.csv",
    "hr_payroll": OUTPUT_DIR / "hr_payroll.csv",
    "hr_employee_productivity": OUTPUT_DIR / "hr_employee_productivity.csv",
}

SHIFT_TYPES = ("Morning", "Evening", "Night")
SHIFT_ORDER_SHARE = {"Morning": 0.44, "Evening": 0.46, "Night": 0.10}
SHIFT_HOURS = {
    "Morning": ("08:00:00", "16:00:00"),
    "Evening": ("16:00:00", "00:00:00"),
    "Night": ("00:00:00", "08:00:00"),
}


def read_csv(path):
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path, rows, headers):
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({h: row.get(h, "") for h in headers})


def parse_date(value):
    text = str(value).strip()
    if not text:
        return None
    if " " in text:
        text = text.split(" ", 1)[0]
    return datetime.strptime(text, "%Y-%m-%d").date()


def parse_float(value, default=0.0):
    try:
        return float(str(value).strip())
    except Exception:
        return default


def daterange(start, end):
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)


def employee_role_bucket(employee):
    dep = str(employee.get("department", "")).lower()
    role = str(employee.get("role", "")).lower()
    if "kitchen" in dep or "kitchen" in role or "cook" in role or "chef" in role or "helper" in role:
        return "kitchen"
    if "manager" in role or "operations" in dep:
        return "manager"
    return "front"


def validate_inputs(paths):
    missing = [str(p) for p in paths.values() if not p.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing required upstream datasets. Create base datasets first (without regenerating masters):\n- "
            + "\n- ".join(sorted(missing))
        )


def load_inputs():
    validate_inputs(REQUIRED_FILES)

    branches = read_csv(REQUIRED_FILES["branches"])
    employees = read_csv(REQUIRED_FILES["employees"])
    sales_orders = read_csv(REQUIRED_FILES["sales_orders"])
    _ = read_csv(REQUIRED_FILES["sales_order_lines"])
    kitchen_batch_runs = read_csv(REQUIRED_FILES["kitchen_batch_runs"])
    inventory_daily_stock = read_csv(REQUIRED_FILES["inventory_daily_stock"])

    active_branches = {b["branch_id"] for b in branches if str(b.get("status", "Active")).lower() == "active"}
    employees = [
        e
        for e in employees
        if e.get("branch_id") in active_branches and str(e.get("status", "Active")).lower() != "inactive"
    ]

    return branches, employees, sales_orders, kitchen_batch_runs, inventory_daily_stock


def aggregate_daily_sales(sales_orders):
    daily = defaultdict(lambda: {"orders": 0, "revenue": 0.0})
    all_dates = set()
    for row in sales_orders:
        dt = parse_date(row.get("order_date") or row.get("order_datetime"))
        if not dt:
            continue
        branch_id = row.get("branch_id", "")
        key = (dt.isoformat(), branch_id)
        daily[key]["orders"] += 1
        if str(row.get("refund_flag", "0")).strip() != "1":
            daily[key]["revenue"] += parse_float(row.get("net_sales", 0.0), 0.0)
        all_dates.add(dt)

    if not all_dates:
        return daily, None, None
    return daily, min(all_dates), max(all_dates)


def aggregate_kitchen_batches(kitchen_batch_runs):
    kitchen_output = defaultdict(float)
    batches_by_emp_day = defaultdict(int)

    for row in kitchen_batch_runs:
        dt = parse_date(row.get("batch_date"))
        if not dt:
            continue
        day = dt.isoformat()
        branch_id = row.get("branch_id", "")
        kitchen_output[(day, branch_id)] += parse_float(row.get("output_qty", 0.0), 0.0)
        emp = row.get("produced_by_employee_id", "")
        if emp:
            batches_by_emp_day[(day, emp)] += 1

    return kitchen_output, batches_by_emp_day


def aggregate_inventory(inventory_daily_stock):
    stockouts = defaultdict(int)
    wastage_qty = defaultdict(float)
    inbound_qty = defaultdict(float)

    for row in inventory_daily_stock:
        dt = parse_date(row.get("date"))
        if not dt:
            continue
        key = (dt.isoformat(), row.get("branch_id", ""))
        stockouts[key] += int(str(row.get("stockout_flag", "0")).strip() == "1")
        wastage_qty[key] += max(0.0, parse_float(row.get("wastage_qty", 0.0), 0.0))
        inbound_qty[key] += max(0.0, parse_float(row.get("purchase_in_qty", 0.0), 0.0))

    return stockouts, wastage_qty, inbound_qty


def pick_manager_for_branch(branch_id, employees_by_branch):
    managers = [
        e["employee_id"]
        for e in employees_by_branch.get(branch_id, [])
        if employee_role_bucket(e) == "manager"
    ]
    if managers:
        return random.choice(managers)
    fallback = employees_by_branch.get(branch_id, [])
    return random.choice(fallback)["employee_id"] if fallback else ""


def generate_attendance(employees, start_date, end_date):
    rows = []
    seq = 1

    for employee in sorted(employees, key=lambda x: x["employee_id"]):
        emp_id = employee["employee_id"]
        branch_id = employee["branch_id"]
        base_shift = employee.get("shift_type", "Morning")
        role_bucket = employee_role_bucket(employee)

        status = str(employee.get("status", "Active")).lower()
        abs_prob = 0.02 if status == "active" else 0.08
        if employee.get("employment_type", "").lower() == "contract":
            abs_prob += 0.01

        for d in daterange(start_date, end_date):
            absence_flag = 1 if random.random() < abs_prob else 0
            check_in = ""
            check_out = ""
            working_hours = 0.0
            overtime_hours = 0.0

            if not absence_flag:
                assigned_shift = base_shift if base_shift in SHIFT_TYPES else random.choice(SHIFT_TYPES)
                in_time, out_time = SHIFT_HOURS[assigned_shift]

                base_start = datetime.strptime(in_time, "%H:%M:%S")
                check_in_dt = base_start + timedelta(minutes=random.randint(-20, 25))

                standard_hours = 8.0
                overtime_hours = max(0.0, round(random.gauss(0.8, 0.9), 2)) if role_bucket == "kitchen" else max(0.0, round(random.gauss(0.4, 0.7), 2))
                overtime_hours = min(overtime_hours, 4.0)
                working_hours = round(standard_hours + overtime_hours, 2)

                check_out_dt = check_in_dt + timedelta(hours=working_hours)
                check_in = check_in_dt.time().strftime("%H:%M:%S")
                check_out = check_out_dt.time().strftime("%H:%M:%S")

            rows.append(
                {
                    "attendance_id": f"ATT{seq:010d}",
                    "date": d.isoformat(),
                    "employee_id": emp_id,
                    "branch_id": branch_id,
                    "check_in_time": check_in,
                    "check_out_time": check_out,
                    "working_hours": f"{max(0.0, working_hours):.2f}",
                    "overtime_hours": f"{max(0.0, overtime_hours):.2f}",
                    "absence_flag": absence_flag,
                }
            )
            seq += 1

    return rows


def generate_payroll(employees, attendance_rows):
    attendance_by_emp_month = defaultdict(lambda: {"overtime": 0.0, "absences": 0})

    for row in attendance_rows:
        month = row["date"][:7]
        key = (row["employee_id"], month)
        attendance_by_emp_month[key]["overtime"] += parse_float(row.get("overtime_hours", 0.0), 0.0)
        attendance_by_emp_month[key]["absences"] += int(str(row.get("absence_flag", "0")).strip() == "1")

    rows = []
    seq = 1
    for emp in sorted(employees, key=lambda x: x["employee_id"]):
        emp_id = emp["employee_id"]
        branch_id = emp["branch_id"]
        base_salary = parse_float(emp.get("salary_monthly", 0.0), 0.0)
        hourly_rate = base_salary / 208.0 if base_salary > 0 else 0.0

        months = sorted({m for (eid, m) in attendance_by_emp_month if eid == emp_id})
        for month in months:
            agg = attendance_by_emp_month[(emp_id, month)]
            overtime = max(0.0, agg["overtime"])
            absences = max(0, agg["absences"])
            overtime_pay = round(overtime * hourly_rate * 1.5, 2)
            bonus = round(min(base_salary * 0.08, overtime_pay * 0.25 + random.uniform(500.0, 2000.0)), 2)
            deductions = round(min(base_salary * 0.22, absences * hourly_rate * 8.0 * 0.6), 2)
            net_salary = round(base_salary + overtime_pay + bonus - deductions, 2)

            rows.append(
                {
                    "payroll_id": f"PAY{seq:010d}",
                    "month": month,
                    "employee_id": emp_id,
                    "branch_id": branch_id,
                    "base_salary": f"{base_salary:.2f}",
                    "overtime_pay": f"{overtime_pay:.2f}",
                    "bonus": f"{bonus:.2f}",
                    "deductions": f"{deductions:.2f}",
                    "net_salary": f"{max(0.0, net_salary):.2f}",
                }
            )
            seq += 1

    return rows


def generate_shift_log(daily_sales, employees_by_branch):
    rows = []
    seq = 1

    for (day, branch_id), agg in sorted(daily_sales.items()):
        base_orders = agg["orders"]
        base_revenue = agg["revenue"]

        staff_pool = employees_by_branch.get(branch_id, [])
        active_count = len(staff_pool)
        manager_id = pick_manager_for_branch(branch_id, employees_by_branch)

        order_splits = {}
        sales_splits = {}
        running_orders = 0
        running_sales = 0.0

        for shift in SHIFT_TYPES[:-1]:
            share = SHIFT_ORDER_SHARE[shift] + random.uniform(-0.03, 0.03)
            o = int(round(base_orders * max(0.0, share)))
            s = round(base_revenue * max(0.0, share), 2)
            order_splits[shift] = o
            sales_splits[shift] = s
            running_orders += o
            running_sales += s

        night_orders = max(0, base_orders - running_orders)
        night_sales = round(max(0.0, base_revenue - running_sales), 2)
        order_splits["Night"] = night_orders
        sales_splits["Night"] = night_sales

        for shift in SHIFT_TYPES:
            shift_factor = {"Morning": 0.92, "Evening": 1.0, "Night": 0.55}[shift]
            staff_count = int(round(active_count * shift_factor))
            staff_count = max(1, staff_count) if active_count else 0

            rows.append(
                {
                    "shift_id": f"SHF{seq:010d}",
                    "date": day,
                    "branch_id": branch_id,
                    "shift_type": shift,
                    "manager_employee_id": manager_id,
                    "staff_count": staff_count,
                    "total_sales": f"{max(0.0, sales_splits[shift]):.2f}",
                    "total_orders": max(0, order_splits[shift]),
                }
            )
            seq += 1

    return rows


def generate_store_daily_metrics(daily_sales, kitchen_output, stockouts, wastage_qty, inbound_qty):
    rows = []

    for (day, branch_id), sales in sorted(daily_sales.items()):
        orders = max(0, sales["orders"])
        revenue = max(0.0, round(sales["revenue"], 2))
        aov = round(revenue / orders, 2) if orders else 0.0
        ko = round(max(0.0, kitchen_output.get((day, branch_id), 0.0)), 4)
        so = max(0, stockouts.get((day, branch_id), 0))
        wastage = max(0.0, wastage_qty.get((day, branch_id), 0.0))
        inbound = max(0.0, inbound_qty.get((day, branch_id), 0.0))
        proxy_unit_cost = (revenue / orders * 0.08) if orders else 5.0
        stability_discount = 0.98 if inbound > 0 else 1.0
        wastage_cost = round(wastage * proxy_unit_cost * stability_discount, 2)

        rows.append(
            {
                "date": day,
                "branch_id": branch_id,
                "orders": orders,
                "revenue": f"{revenue:.2f}",
                "avg_order_value": f"{aov:.2f}",
                "kitchen_output": f"{ko:.4f}",
                "stockouts": so,
                "wastage_cost": f"{max(0.0, wastage_cost):.2f}",
            }
        )

    return rows


def generate_employee_productivity(employees, attendance_rows, daily_sales, batches_by_emp_day):
    attendance_present = [r for r in attendance_rows if str(r.get("absence_flag", "0")) == "0"]
    present_by_day_branch = defaultdict(list)
    for row in attendance_present:
        present_by_day_branch[(row["date"], row["branch_id"])].append(row["employee_id"])

    employees_by_id = {e["employee_id"]: e for e in employees}
    rows = []
    seq = 1

    for (day, branch_id), present_ids in sorted(present_by_day_branch.items()):
        total_orders = max(0, int(daily_sales.get((day, branch_id), {}).get("orders", 0)))
        front_ids = [eid for eid in present_ids if employee_role_bucket(employees_by_id[eid]) in ("front", "manager")]
        kitchen_ids = [eid for eid in present_ids if employee_role_bucket(employees_by_id[eid]) == "kitchen"]

        front_total = len(front_ids)
        for eid in front_ids:
            weight = random.uniform(0.85, 1.2)
            expected = (total_orders / front_total) if front_total else 0.0
            handled = int(round(max(0.0, expected * weight)))
            score = min(100.0, round(45.0 + handled * 0.65 + random.uniform(-4.0, 7.0), 2))
            rows.append(
                {
                    "productivity_id": f"PRD{seq:010d}",
                    "date": day,
                    "employee_id": eid,
                    "branch_id": branch_id,
                    "orders_handled": handled,
                    "kitchen_batches_handled": 0,
                    "productivity_score": f"{max(0.0, score):.2f}",
                }
            )
            seq += 1

        for eid in kitchen_ids:
            batches = max(0, batches_by_emp_day.get((day, eid), 0))
            orders_hint = int(round((total_orders / max(1, len(kitchen_ids))) * 0.15))
            score = min(100.0, round(50.0 + batches * 8.0 + orders_hint * 0.4 + random.uniform(-5.0, 6.0), 2))
            rows.append(
                {
                    "productivity_id": f"PRD{seq:010d}",
                    "date": day,
                    "employee_id": eid,
                    "branch_id": branch_id,
                    "orders_handled": max(0, orders_hint),
                    "kitchen_batches_handled": batches,
                    "productivity_score": f"{max(0.0, score):.2f}",
                }
            )
            seq += 1

    return rows


def validate_outputs(employees, attendance, payroll, productivity):
    emp_branch = {e["employee_id"]: e["branch_id"] for e in employees}

    for row in attendance:
        assert row["employee_id"] in emp_branch, "Unknown employee in attendance"
        assert row["branch_id"] == emp_branch[row["employee_id"]], "Attendance employee-branch mismatch"
        assert parse_float(row["working_hours"], 0.0) >= 0.0, "Negative working hours"
        assert parse_float(row["overtime_hours"], 0.0) >= 0.0, "Negative overtime hours"

    attendance_month = defaultdict(lambda: {"ot": 0.0})
    for row in attendance:
        key = (row["employee_id"], row["date"][:7])
        attendance_month[key]["ot"] += parse_float(row["overtime_hours"], 0.0)

    payroll_keys = set()
    for row in payroll:
        key = (row["employee_id"], row["month"])
        payroll_keys.add(key)
        assert key in attendance_month, "Payroll record without attendance month"
        assert row["branch_id"] == emp_branch[row["employee_id"]], "Payroll employee-branch mismatch"
        assert parse_float(row["net_salary"], 0.0) >= 0.0, "Negative net salary"

    for key in attendance_month:
        assert key in payroll_keys, "Attendance month missing payroll"

    role_by_emp = {e["employee_id"]: employee_role_bucket(e) for e in employees}
    for row in productivity:
        emp_id = row["employee_id"]
        role = role_by_emp.get(emp_id, "front")
        orders_handled = int(row["orders_handled"])
        batches = int(row["kitchen_batches_handled"])
        score = parse_float(row["productivity_score"], 0.0)

        assert row["branch_id"] == emp_branch[emp_id], "Productivity employee-branch mismatch"
        assert 0.0 <= score <= 100.0, "Productivity score out of range"
        if role == "kitchen":
            assert batches >= 0 and orders_handled >= 0, "Kitchen productivity negative values"
            assert orders_handled <= 80, "Kitchen orders handled unrealistic"
        else:
            assert orders_handled >= 0 and batches == 0, "Front productivity mapping invalid"
            assert orders_handled <= 350, "Front orders handled unrealistic"


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    branches, employees, sales_orders, kitchen_batch_runs, inventory_daily_stock = load_inputs()
    _ = branches

    daily_sales, min_day, max_day = aggregate_daily_sales(sales_orders)
    if min_day is None or max_day is None:
        raise ValueError("sales_orders.csv has no valid dates; cannot generate Ops/HR datasets")

    employees_by_branch = defaultdict(list)
    for employee in employees:
        employees_by_branch[employee["branch_id"]].append(employee)

    kitchen_output, batches_by_emp_day = aggregate_kitchen_batches(kitchen_batch_runs)
    stockouts, wastage_qty, inbound_qty = aggregate_inventory(inventory_daily_stock)

    attendance = generate_attendance(employees, min_day, max_day)
    payroll = generate_payroll(employees, attendance)
    shift_log = generate_shift_log(daily_sales, employees_by_branch)
    store_daily_metrics = generate_store_daily_metrics(daily_sales, kitchen_output, stockouts, wastage_qty, inbound_qty)
    productivity = generate_employee_productivity(employees, attendance, daily_sales, batches_by_emp_day)

    validate_outputs(employees, attendance, payroll, productivity)

    write_csv(
        OUTPUT_FILES["operations_shift_log"],
        shift_log,
        ["shift_id", "date", "branch_id", "shift_type", "manager_employee_id", "staff_count", "total_sales", "total_orders"],
    )
    write_csv(
        OUTPUT_FILES["operations_store_daily_metrics"],
        store_daily_metrics,
        ["date", "branch_id", "orders", "revenue", "avg_order_value", "kitchen_output", "stockouts", "wastage_cost"],
    )
    write_csv(
        OUTPUT_FILES["hr_attendance"],
        attendance,
        ["attendance_id", "date", "employee_id", "branch_id", "check_in_time", "check_out_time", "working_hours", "overtime_hours", "absence_flag"],
    )
    write_csv(
        OUTPUT_FILES["hr_payroll"],
        payroll,
        ["payroll_id", "month", "employee_id", "branch_id", "base_salary", "overtime_pay", "bonus", "deductions", "net_salary"],
    )
    write_csv(
        OUTPUT_FILES["hr_employee_productivity"],
        productivity,
        ["productivity_id", "date", "employee_id", "branch_id", "orders_handled", "kitchen_batches_handled", "productivity_score"],
    )

    print("Generated operations + HR datasets:")
    for key, path in OUTPUT_FILES.items():
        print(f"- {key}: {path}")


if __name__ == "__main__":
    main()
