#!/usr/bin/env python3
"""Generate finance and risk datasets from core operational CSVs.

Uses only Python standard library modules:
- csv
- random
- datetime
- collections
- pathlib
- os
- math
"""

import csv
import random
import datetime
import collections
import pathlib
import os
import math


DEFAULT_FILES = {
    "PURCHASE_ORDERS_PATH": "purchase_orders.csv",
    "PURCHASE_ORDER_LINES_PATH": "purchase_order_lines.csv",
    "SUPPLIERS_MASTER_PATH": "suppliers_master.csv",
    "INVENTORY_STOCK_PATH": "inventory_daily_stock.csv",
    "HR_PAYROLL_PATH": "hr_payroll.csv",
    "MARKETING_CAMPAIGNS_PATH": "marketing_campaigns.csv",
}

OUTPUT_FILES = {
    "supplier_performance": "supplier_performance.csv",
    "cash_flow_daily": "cash_flow_daily.csv",
    "expense_register": "expense_register.csv",
    "expansion_plan": "expansion_plan.csv",
    "risk_incidents": "risk_incidents.csv",
    "compliance_audit_log": "compliance_audit_log.csv",
}


class ValidationError(Exception):
    """Raised when required input data quality checks fail."""


def get_input_path(env_name):
    return pathlib.Path(os.getenv(env_name, DEFAULT_FILES[env_name]))


def get_output_dir():
    return pathlib.Path(os.getenv("FINANCE_OUTPUT_DIR", "."))


def read_csv(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing required input file: {path}")

    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        headers = reader.fieldnames or []
    if not headers:
        raise ValidationError(f"CSV has no headers: {path}")
    return rows, headers


def normalize_key(key):
    return (key or "").strip().lower()


def pick_column(headers, candidates, required=True):
    normalized = {normalize_key(h): h for h in headers}
    for candidate in candidates:
        if normalize_key(candidate) in normalized:
            return normalized[normalize_key(candidate)]
    if required:
        raise ValidationError(f"Could not find required column. Tried: {candidates}")
    return None


def parse_date(value):
    text = (value or "").strip()
    if not text:
        return None

    formats = [
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%m/%d/%Y",
        "%Y/%m/%d",
        "%Y-%m-%d %H:%M:%S",
    ]
    for fmt in formats:
        try:
            return datetime.datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValidationError(f"Unrecognized date format: {value}")


def parse_float(value, default=0.0):
    text = (value or "").strip()
    if not text:
        return default
    cleaned = text.replace(",", "")
    try:
        return float(cleaned)
    except ValueError:
        return default


def parse_int(value, default=0):
    return int(round(parse_float(value, default=default)))


def detect_branch_ids(data_map):
    candidates = ["branch_id", "branch", "location_id", "store_id"]
    branch_values_by_source = {}

    for source_name, (rows, headers) in data_map.items():
        column = pick_column(headers, candidates, required=False)
        values = set()
        if column:
            for row in rows:
                value = (row.get(column) or "").strip()
                if value:
                    values.add(value)
        branch_values_by_source[source_name] = values

    authoritative_sources = ["purchase_orders", "inventory_stock", "hr_payroll"]
    master = set()
    for source_name in authoritative_sources:
        master.update(branch_values_by_source.get(source_name, set()))

    if not master:
        for source_name, values in branch_values_by_source.items():
            if source_name != "suppliers_master":
                master.update(values)

    if not master:
        raise ValidationError("No branch_id values found in branch-bearing input files")

    for source_name, values in branch_values_by_source.items():
        if not values:
            continue
        invalid = values - master
        if invalid:
            sample = sorted(list(invalid))[:5]
            raise ValidationError(
                f"Invalid branch_id(s) in {source_name}: {sample}. "
                "Each branch_id must be valid everywhere."
            )

    return master


def validate_supplier_and_purchase_linkage(po_rows, po_headers, line_rows, line_headers, supplier_rows, supplier_headers):
    supplier_col = pick_column(supplier_headers, ["supplier_id", "vendor_id", "id"])
    po_supplier_col = pick_column(po_headers, ["supplier_id", "vendor_id", "supplier"])
    po_id_col = pick_column(po_headers, ["purchase_order_id", "po_id", "order_id", "id"])
    line_po_col = pick_column(line_headers, ["purchase_order_id", "po_id", "order_id"])

    supplier_ids = {
        (row.get(supplier_col) or "").strip()
        for row in supplier_rows
        if (row.get(supplier_col) or "").strip()
    }
    if not supplier_ids:
        raise ValidationError("No supplier IDs found in suppliers_master.csv")

    po_ids = set()
    missing_supplier = []
    for row in po_rows:
        po_id = (row.get(po_id_col) or "").strip()
        supplier_id = (row.get(po_supplier_col) or "").strip()
        if po_id:
            po_ids.add(po_id)
        if supplier_id and supplier_id not in supplier_ids:
            missing_supplier.append((po_id, supplier_id))

    if missing_supplier:
        sample = missing_supplier[:5]
        raise ValidationError(f"Supplier must exist. Missing supplier references in purchase_orders: {sample}")

    invalid_lines = []
    for row in line_rows:
        linked_po = (row.get(line_po_col) or "").strip()
        if linked_po and linked_po not in po_ids:
            invalid_lines.append(linked_po)

    if invalid_lines:
        sample = sorted(set(invalid_lines))[:5]
        raise ValidationError(f"Purchase linkage invalid. Unknown purchase_order_id(s): {sample}")

    return {
        "supplier_col": supplier_col,
        "po_supplier_col": po_supplier_col,
        "po_id_col": po_id_col,
        "line_po_col": line_po_col,
    }


def build_supplier_performance(po_rows, po_headers, supplier_rows, supplier_headers, linkage_cols):
    supplier_col = linkage_cols["supplier_col"]
    po_supplier_col = linkage_cols["po_supplier_col"]

    expected_col = pick_column(po_headers, ["expected_delivery_date", "eta_date", "promised_date"], required=False)
    actual_col = pick_column(po_headers, ["actual_delivery_date", "received_date", "delivery_date"], required=False)
    quality_col = pick_column(po_headers, ["quality_score", "inspection_score"], required=False)

    supplier_name_col = pick_column(supplier_headers, ["supplier_name", "vendor_name", "name"], required=False)

    supplier_names = {}
    for row in supplier_rows:
        sid = (row.get(supplier_col) or "").strip()
        if sid:
            supplier_names[sid] = (row.get(supplier_name_col) or sid).strip() if supplier_name_col else sid

    stats = collections.defaultdict(lambda: {
        "orders": 0,
        "on_time": 0,
        "delay_days": 0.0,
        "quality_sum": 0.0,
        "quality_count": 0,
    })

    for row in po_rows:
        sid = (row.get(po_supplier_col) or "").strip()
        if not sid:
            continue
        stat = stats[sid]
        stat["orders"] += 1

        expected = parse_date(row.get(expected_col)) if expected_col else None
        actual = parse_date(row.get(actual_col)) if actual_col else None
        if expected and actual:
            delay = (actual - expected).days
            if delay <= 0:
                stat["on_time"] += 1
            stat["delay_days"] += max(0, delay)
        else:
            proxy_delay = max(0, random.gauss(1.8, 2.2))
            if proxy_delay <= 1:
                stat["on_time"] += 1
            stat["delay_days"] += proxy_delay

        if quality_col:
            q = parse_float(row.get(quality_col), default=0.0)
            if q > 0:
                stat["quality_sum"] += q
                stat["quality_count"] += 1
            else:
                synth_q = min(100.0, max(70.0, random.gauss(89, 6)))
                stat["quality_sum"] += synth_q
                stat["quality_count"] += 1
        else:
            synth_q = min(100.0, max(70.0, random.gauss(88, 7)))
            stat["quality_sum"] += synth_q
            stat["quality_count"] += 1

    rows = []
    for sid in sorted(stats.keys()):
        s = stats[sid]
        orders = max(1, s["orders"])
        quality_avg = s["quality_sum"] / max(1, s["quality_count"])
        on_time_rate = 100.0 * s["on_time"] / orders
        avg_delay = s["delay_days"] / orders
        reliability = max(0.0, min(100.0, 100.0 - avg_delay * 4.5 + (quality_avg - 85.0) * 0.8))
        rows.append({
            "supplier_id": sid,
            "supplier_name": supplier_names.get(sid, sid),
            "total_orders": str(orders),
            "on_time_rate_pct": f"{on_time_rate:.2f}",
            "avg_delay_days": f"{avg_delay:.2f}",
            "quality_score_avg": f"{quality_avg:.2f}",
            "reliability_index": f"{reliability:.2f}",
        })

    return rows


def collect_purchase_cash_out(po_rows, po_headers):
    amount_col = pick_column(po_headers, ["total_amount", "order_total", "amount", "grand_total"], required=False)
    expected_col = pick_column(po_headers, ["actual_delivery_date", "received_date", "delivery_date", "expected_delivery_date"], required=False)

    cash_out = collections.defaultdict(float)
    min_date = None
    max_date = None

    for row in po_rows:
        if not amount_col:
            continue
        amount = max(0.0, parse_float(row.get(amount_col), default=0.0))
        if amount <= 0:
            continue

        delivery_date = parse_date(row.get(expected_col)) if expected_col else None
        if delivery_date is None:
            continue

        payment_lag = max(0, int(round(random.gauss(8, 5))))
        payment_date = delivery_date + datetime.timedelta(days=payment_lag)
        cash_out[payment_date] += amount

        min_date = payment_date if min_date is None else min(min_date, payment_date)
        max_date = payment_date if max_date is None else max(max_date, payment_date)

    return cash_out, min_date, max_date


def collect_payroll_cash_out(hr_rows, hr_headers):
    date_col = pick_column(hr_headers, ["pay_date", "payroll_date", "date", "month"])
    amount_col = pick_column(
        hr_headers,
        ["net_salary", "net pay", "netpay", "net_salary ", "net_pay", "gross_pay", "pay_amount", "amount"],
        required=False
    )
    base_col = None
    overtime_col = None
    bonus_col = None
    deductions_col = None
    if amount_col is None:
        base_col = pick_column(hr_headers, ["base_salary", "basic_salary"], required=False)
        overtime_col = pick_column(hr_headers, ["overtime_pay", "overtime"], required=False)
        bonus_col = pick_column(hr_headers, ["bonus"], required=False)
        deductions_col = pick_column(hr_headers, ["deductions"], required=False)

    cash_out = collections.defaultdict(float)
    min_date = None
    max_date = None

    for row in hr_rows:
        if normalize_key(date_col) == "month":
            month_text = (row.get(date_col) or "").strip()
            if not month_text:
                continue
            d = parse_date(f"{month_text}-28")
        else:
            d = parse_date(row.get(date_col))

        if d is None:
            continue
        if amount_col is not None:
            amount = parse_float(row.get(amount_col), default=0.0)
        else:
            base = parse_float(row.get(base_col) if base_col else None, default=0.0)
            overtime = parse_float(row.get(overtime_col) if overtime_col else None, default=0.0)
            bonus = parse_float(row.get(bonus_col) if bonus_col else None, default=0.0)
            deductions = parse_float(row.get(deductions_col) if deductions_col else None, default=0.0)
            amount = base + overtime + bonus - deductions
        amount = max(0.0, amount)
        cash_out[d] += amount
        min_date = d if min_date is None else min(min_date, d)
        max_date = d if max_date is None else max(max_date, d)

    return cash_out, min_date, max_date


def collect_marketing_cash_out(marketing_rows, marketing_headers):
    spend_col = pick_column(marketing_headers, ["spend", "budget", "amount", "cost"])
    start_col = pick_column(marketing_headers, ["campaign_date", "start_date", "launch_date", "date"], required=False)
    end_col = pick_column(marketing_headers, ["end_date", "campaign_end_date"], required=False)

    cash_out = collections.defaultdict(float)
    min_date = None
    max_date = None

    for row in marketing_rows:
        total = max(0.0, parse_float(row.get(spend_col), default=0.0))
        if total <= 0:
            continue

        start_date = parse_date(row.get(start_col)) if start_col else None
        end_date = parse_date(row.get(end_col)) if end_col else None

        if start_date is None:
            continue
        if end_date is None or end_date < start_date:
            end_date = start_date

        days = (end_date - start_date).days + 1
        daily = total / max(1, days)
        for offset in range(days):
            d = start_date + datetime.timedelta(days=offset)
            cash_out[d] += daily
            min_date = d if min_date is None else min(min_date, d)
            max_date = d if max_date is None else max(max_date, d)

    return cash_out, min_date, max_date


def build_cash_in_series(inventory_rows, inventory_headers, date_start, date_end):
    date_col = pick_column(inventory_headers, ["date", "stock_date", "inventory_date"])
    branch_col = pick_column(inventory_headers, ["branch_id", "branch", "store_id", "location_id"])
    stock_value_col = pick_column(inventory_headers, ["stock_value", "closing_stock_value", "inventory_value", "value"])

    stock_by_branch_day = collections.defaultdict(dict)
    for row in inventory_rows:
        d = parse_date(row.get(date_col))
        branch_id = (row.get(branch_col) or "").strip()
        if not d or not branch_id:
            continue
        stock_by_branch_day[branch_id][d] = parse_float(row.get(stock_value_col), default=0.0)

    cash_in_by_day = collections.defaultdict(float)

    for branch_id, day_map in stock_by_branch_day.items():
        days_sorted = sorted(day_map.keys())
        prev_val = None
        for day in days_sorted:
            stock_val = max(0.0, day_map[day])
            if prev_val is not None:
                stock_drop = max(0.0, prev_val - stock_val)
            else:
                stock_drop = 0.0

            turnover_proxy = stock_drop * random.uniform(1.25, 1.9)
            baseline = max(150.0, math.sqrt(stock_val + 1.0) * random.uniform(18.0, 36.0))
            weekday = day.weekday()
            weekday_factor = 1.1 if weekday in (4, 5) else (0.93 if weekday == 0 else 1.0)

            sales = max(0.0, (turnover_proxy + baseline) * weekday_factor)
            cash_in_by_day[day] += sales
            prev_val = stock_val

    cursor = date_start
    while cursor <= date_end:
        cash_in_by_day[cursor] += random.uniform(1200.0, 3200.0)
        cursor += datetime.timedelta(days=1)

    return cash_in_by_day


def build_cash_flow_daily(cash_in_by_day, purchase_out, payroll_out, marketing_out, date_start, date_end):
    rows = []
    opening = max(50000.0, sum(cash_in_by_day.values()) * 0.08)

    cursor = date_start
    while cursor <= date_end:
        cash_in = cash_in_by_day.get(cursor, 0.0)
        purchases = purchase_out.get(cursor, 0.0)
        payroll = payroll_out.get(cursor, 0.0)
        marketing = marketing_out.get(cursor, 0.0)
        cash_out = purchases + payroll + marketing

        projected = opening + cash_in - cash_out
        financing = 0.0

        if projected < -20000:
            financing = abs(projected) + random.uniform(10000, 25000)
        elif projected < 0:
            if random.random() < 0.45:
                financing = abs(projected) + random.uniform(1000, 6000)

        closing = projected + financing

        rows.append({
            "date": cursor.isoformat(),
            "opening_cash": f"{opening:.2f}",
            "cash_in_sales": f"{cash_in:.2f}",
            "cash_out_purchases": f"{purchases:.2f}",
            "cash_out_payroll": f"{payroll:.2f}",
            "cash_out_marketing": f"{marketing:.2f}",
            "financing_inflow": f"{financing:.2f}",
            "closing_cash": f"{closing:.2f}",
        })

        opening = closing
        cursor += datetime.timedelta(days=1)

    return rows


def build_expense_register(purchase_out, payroll_out, marketing_out):
    all_days = sorted(set(purchase_out) | set(payroll_out) | set(marketing_out))
    rows = []
    seq = 1
    for day in all_days:
        if purchase_out.get(day, 0.0) > 0:
            rows.append({
                "expense_id": f"EXP-{seq:06d}",
                "date": day.isoformat(),
                "category": "COGS_PURCHASES",
                "sub_category": "Supplier Payments",
                "amount": f"{purchase_out[day]:.2f}",
                "payment_mode": random.choice(["Bank Transfer", "Credit Terms"]),
            })
            seq += 1

        if payroll_out.get(day, 0.0) > 0:
            rows.append({
                "expense_id": f"EXP-{seq:06d}",
                "date": day.isoformat(),
                "category": "PAYROLL",
                "sub_category": "Staff Salaries",
                "amount": f"{payroll_out[day]:.2f}",
                "payment_mode": "Bank Transfer",
            })
            seq += 1

        if marketing_out.get(day, 0.0) > 0:
            rows.append({
                "expense_id": f"EXP-{seq:06d}",
                "date": day.isoformat(),
                "category": "MARKETING",
                "sub_category": random.choice(["Digital Ads", "Promotions", "Local Activations"]),
                "amount": f"{marketing_out[day]:.2f}",
                "payment_mode": random.choice(["Credit Card", "Bank Transfer"]),
            })
            seq += 1

    return rows


def build_expansion_plan(branch_ids, anchor_date):
    branches = sorted(branch_ids)
    current_count = len(branches)
    additions = max(2, int(round(current_count * 0.35)))

    rows = []
    for idx in range(1, additions + 1):
        launch = anchor_date + datetime.timedelta(days=idx * random.randint(45, 75))
        capex = random.uniform(180000, 350000)
        month1_sales = random.uniform(40000, 90000)
        ramp_factor = random.uniform(1.04, 1.12)
        month6_sales = month1_sales * (ramp_factor ** 5)

        rows.append({
            "new_branch_id": f"NB-{idx:03d}",
            "planned_launch_date": launch.isoformat(),
            "capex_budget": f"{capex:.2f}",
            "month_1_revenue": f"{month1_sales:.2f}",
            "month_6_revenue": f"{month6_sales:.2f}",
            "ramp_profile": random.choice(["Slow-Start", "Balanced", "Aggressive"]),
            "status": random.choice(["Approved", "Under Review"]),
        })

    return rows


def build_risk_incidents(branch_ids, supplier_perf_rows, date_start, date_end):
    suppliers_ranked = sorted(
        supplier_perf_rows,
        key=lambda r: parse_float(r.get("reliability_index"), default=100.0)
    )
    riskier_suppliers = [row["supplier_id"] for row in suppliers_ranked[:max(1, len(suppliers_ranked) // 3)]]

    rows = []
    incident_id = 1

    total_days = (date_end - date_start).days + 1
    baseline_events = max(3, int(total_days * len(branch_ids) * 0.006))

    for _ in range(baseline_events):
        day_offset = random.randint(0, max(0, total_days - 1))
        day = date_start + datetime.timedelta(days=day_offset)
        branch = random.choice(sorted(list(branch_ids)))
        event_type = random.choices(
            ["supplier_delay", "quality_failure", "cash_shortfall", "compliance_gap", "inventory_loss"],
            weights=[30, 22, 18, 15, 15],
            k=1,
        )[0]

        supplier_id = ""
        if event_type in ("supplier_delay", "quality_failure") and riskier_suppliers:
            supplier_id = random.choice(riskier_suppliers)

        impact = {
            "supplier_delay": random.uniform(1200, 9000),
            "quality_failure": random.uniform(1800, 12000),
            "cash_shortfall": random.uniform(1000, 8000),
            "compliance_gap": random.uniform(500, 6000),
            "inventory_loss": random.uniform(700, 7000),
        }[event_type]

        rows.append({
            "incident_id": f"RISK-{incident_id:05d}",
            "incident_date": day.isoformat(),
            "branch_id": branch,
            "risk_type": event_type,
            "supplier_id": supplier_id,
            "severity": random.choices(["Low", "Medium", "High"], weights=[45, 40, 15], k=1)[0],
            "estimated_financial_impact": f"{impact:.2f}",
            "status": random.choice(["Open", "Mitigated", "Closed"]),
        })
        incident_id += 1

    rows.sort(key=lambda r: r["incident_date"])
    return rows


def build_compliance_audit_log(branch_ids, date_start, date_end):
    rows = []
    seq = 1

    cursor = datetime.date(date_start.year, date_start.month, 1)
    end_month = datetime.date(date_end.year, date_end.month, 1)

    branches_sorted = sorted(list(branch_ids))

    while cursor <= end_month:
        for branch in branches_sorted:
            score = min(100.0, max(72.0, random.gauss(88.0, 5.5)))
            findings = 0
            if score < 80:
                findings = random.randint(2, 5)
            elif score < 88:
                findings = random.randint(1, 3)
            else:
                findings = random.randint(0, 1)

            rows.append({
                "audit_id": f"AUD-{seq:06d}",
                "audit_date": cursor.isoformat(),
                "branch_id": branch,
                "audit_area": random.choice(["Finance Controls", "Procurement", "Payroll", "Data Governance"]),
                "compliance_score": f"{score:.2f}",
                "findings_count": str(findings),
                "result": "Pass" if score >= 80 else "Conditional",
                "owner": random.choice(["Finance Ops", "Internal Audit", "Risk Office"]),
            })
            seq += 1

        next_month = cursor.month + 1
        next_year = cursor.year
        if next_month > 12:
            next_month = 1
            next_year += 1
        cursor = datetime.date(next_year, next_month, 1)

    return rows


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with path.open("w", newline="", encoding="utf-8") as handle:
            handle.write("")
        return

    headers = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def main():
    random.seed(42)

    po_rows, po_headers = read_csv(get_input_path("PURCHASE_ORDERS_PATH"))
    line_rows, line_headers = read_csv(get_input_path("PURCHASE_ORDER_LINES_PATH"))
    supplier_rows, supplier_headers = read_csv(get_input_path("SUPPLIERS_MASTER_PATH"))
    inventory_rows, inventory_headers = read_csv(get_input_path("INVENTORY_STOCK_PATH"))
    hr_rows, hr_headers = read_csv(get_input_path("HR_PAYROLL_PATH"))
    marketing_rows, marketing_headers = read_csv(get_input_path("MARKETING_CAMPAIGNS_PATH"))

    data_map = {
        "purchase_orders": (po_rows, po_headers),
        "purchase_order_lines": (line_rows, line_headers),
        "suppliers_master": (supplier_rows, supplier_headers),
        "inventory_stock": (inventory_rows, inventory_headers),
        "hr_payroll": (hr_rows, hr_headers),
        "marketing_campaigns": (marketing_rows, marketing_headers),
    }

    branch_ids = detect_branch_ids(data_map)

    linkage_cols = validate_supplier_and_purchase_linkage(
        po_rows,
        po_headers,
        line_rows,
        line_headers,
        supplier_rows,
        supplier_headers,
    )

    supplier_performance = build_supplier_performance(
        po_rows,
        po_headers,
        supplier_rows,
        supplier_headers,
        linkage_cols,
    )

    purchase_out, p_min, p_max = collect_purchase_cash_out(po_rows, po_headers)
    payroll_out, pay_min, pay_max = collect_payroll_cash_out(hr_rows, hr_headers)
    marketing_out, m_min, m_max = collect_marketing_cash_out(marketing_rows, marketing_headers)

    dates = [d for d in [p_min, p_max, pay_min, pay_max, m_min, m_max] if d is not None]
    if not dates:
        raise ValidationError("Unable to derive date range from purchases/payroll/marketing")
    date_start = min(dates)
    date_end = max(dates)

    cash_in = build_cash_in_series(inventory_rows, inventory_headers, date_start, date_end)
    cash_flow = build_cash_flow_daily(cash_in, purchase_out, payroll_out, marketing_out, date_start, date_end)

    for row in cash_flow:
        opening = parse_float(row["opening_cash"])
        closing = parse_float(row["closing_cash"])
        if opening < -50000 or closing < -50000:
            raise ValidationError(
                "No negative cash unless realistic: cash balance dropped below tolerance "
                "without financing mitigation"
            )

    expense_register = build_expense_register(purchase_out, payroll_out, marketing_out)
    expansion_plan = build_expansion_plan(branch_ids, date_end + datetime.timedelta(days=30))
    risk_incidents = build_risk_incidents(branch_ids, supplier_performance, date_start, date_end)
    compliance_log = build_compliance_audit_log(branch_ids, date_start, date_end)

    output_dir = get_output_dir()
    write_csv(output_dir / OUTPUT_FILES["supplier_performance"], supplier_performance)
    write_csv(output_dir / OUTPUT_FILES["cash_flow_daily"], cash_flow)
    write_csv(output_dir / OUTPUT_FILES["expense_register"], expense_register)
    write_csv(output_dir / OUTPUT_FILES["expansion_plan"], expansion_plan)
    write_csv(output_dir / OUTPUT_FILES["risk_incidents"], risk_incidents)
    write_csv(output_dir / OUTPUT_FILES["compliance_audit_log"], compliance_log)


if __name__ == "__main__":
    main()
