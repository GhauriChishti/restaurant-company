import csv
import random
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

SEED = 20260328
random.seed(SEED)

ROOT = Path(__file__).resolve().parents[1]

INPUT_FILES = {
    "purchase_orders": ROOT / "purchase_orders.csv",
    "purchase_order_lines": ROOT / "purchase_order_lines.csv",
    "suppliers": ROOT / "suppliers_master.csv",
    "inventory_daily_stock": ROOT / "inventory_daily_stock.csv",
    "hr_payroll": ROOT / "hr_payroll.csv",
    "marketing_campaigns": ROOT / "marketing_campaigns.csv",
    "branches": ROOT / "branches_master.csv",
    "employees": ROOT / "employees_master.csv",
    "sales_orders": ROOT / "sales_orders.csv",
}

OUTPUT_FILES = {
    "supplier_performance": ROOT / "supplier_performance.csv",
    "cash_flow_daily": ROOT / "cash_flow_daily.csv",
    "expense_register": ROOT / "expense_register.csv",
    "expansion_plan": ROOT / "expansion_plan.csv",
    "risk_incidents": ROOT / "risk_incidents.csv",
    "compliance_audit_log": ROOT / "compliance_audit_log.csv",
}


def read_csv(path: Path):
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, fieldnames, rows):
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fieldnames})


def parse_date(v):
    t = str(v or "").strip()
    if not t:
        return None
    t = t.split(" ")[0]
    return datetime.strptime(t, "%Y-%m-%d").date()


def parse_float(v, default=0.0):
    try:
        return float(str(v).strip())
    except Exception:
        return default


def daterange(start_date, end_date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)


def active_branches(branches):
    return [b for b in branches if str(b.get("status", "Active")).lower() == "active"]


def build_purchase_fallback(branches, suppliers, start_date, end_date):
    po_rows = []
    line_rows = []
    po_seq = 1
    pol_seq = 1
    for d in daterange(start_date, end_date):
        for br in branches:
            # 1-2 purchase orders every third day per branch to avoid unrealistic spikes
            if (d.toordinal() + int(br["branch_id"][-1])) % 3 != 0:
                continue
            for _ in range(1 if random.random() < 0.7 else 2):
                supplier = random.choice(suppliers)
                lead = max(1, int(parse_float(supplier.get("lead_time_days"), 2)))
                promised = d + timedelta(days=lead)
                delay = random.choices([0, 1, 2, 3], weights=[0.64, 0.23, 0.1, 0.03])[0]
                received = promised + timedelta(days=delay)
                po_id = f"POF{po_seq:07d}"
                total = 0.0
                for _i in range(random.randint(2, 4)):
                    ordered = random.uniform(40, 220)
                    rejection_rate = random.uniform(0.0, 0.06)
                    received_qty = max(0.0, ordered * (1 - rejection_rate))
                    unit_cost = random.uniform(2.5, 9.8)
                    line_total = received_qty * unit_cost
                    total += line_total
                    line_rows.append(
                        {
                            "po_line_id": f"POLF{pol_seq:08d}",
                            "po_id": po_id,
                            "ingredient_id": f"ING{random.randint(1, 80):04d}",
                            "ordered_qty": f"{ordered:.2f}",
                            "received_qty": f"{received_qty:.2f}",
                            "purchase_uom": "kg",
                            "unit_cost": f"{unit_cost:.2f}",
                            "line_total_cost": f"{line_total:.2f}",
                            "quality_issue_flag": 1 if rejection_rate > 0.035 else 0,
                            "rejection_qty": f"{max(0.0, ordered - received_qty):.2f}",
                        }
                    )
                    pol_seq += 1

                po_rows.append(
                    {
                        "po_id": po_id,
                        "po_date": d.isoformat(),
                        "branch_id": br["branch_id"],
                        "supplier_id": supplier["supplier_id"],
                        "po_status": "Received",
                        "promised_delivery_date": promised.isoformat(),
                        "received_date": received.isoformat(),
                        "payment_due_date": (received + timedelta(days=14)).isoformat(),
                        "total_po_value": f"{total:.2f}",
                    }
                )
                po_seq += 1
    return po_rows, line_rows


def build_supplier_performance(po_rows, line_rows):
    line_by_po = defaultdict(list)
    for line in line_rows:
        line_by_po[line.get("po_id", "")].append(line)

    daily_supplier = defaultdict(lambda: {"delay": 0.0, "fulfillment": 0.0, "quality": 0.0, "n": 0})
    for po in po_rows:
        po_date = parse_date(po.get("po_date"))
        promised = parse_date(po.get("promised_delivery_date"))
        received = parse_date(po.get("received_date")) or promised or po_date
        if not po_date:
            continue
        delay = max(0, (received - promised).days) if promised else 0

        ordered = 0.0
        received_qty = 0.0
        quality_penalty = 0.0
        for line in line_by_po.get(po.get("po_id", ""), []):
            ordered += parse_float(line.get("ordered_qty"), 0.0)
            received_qty += parse_float(line.get("received_qty"), 0.0)
            rejection = parse_float(line.get("rejection_qty"), 0.0)
            base = max(parse_float(line.get("ordered_qty"), 0.0), 1e-6)
            quality_penalty += (rejection / base) * 35.0
            if str(line.get("quality_issue_flag", "0")) == "1":
                quality_penalty += 4.0

        fulfillment = min(1.0, received_qty / ordered) if ordered > 0 else 1.0
        quality = max(60.0, min(99.5, 97.0 - quality_penalty - delay * 1.7))

        key = (po.get("supplier_id", ""), po_date.isoformat())
        rec = daily_supplier[key]
        rec["delay"] += delay
        rec["fulfillment"] += fulfillment
        rec["quality"] += quality
        rec["n"] += 1

    rows = []
    for (supplier_id, day), vals in sorted(daily_supplier.items(), key=lambda x: (x[0][1], x[0][0])):
        n = max(1, vals["n"])
        rows.append(
            {
                "supplier_id": supplier_id,
                "date": day,
                "delivery_delay_days": f"{vals['delay']/n:.2f}",
                "quality_score": f"{vals['quality']/n:.2f}",
                "order_fulfillment_rate": f"{vals['fulfillment']/n:.4f}",
            }
        )
    return rows


def monthly_payroll_by_branch(payroll_rows, employees_rows):
    out = defaultdict(float)
    if payroll_rows:
        for p in payroll_rows:
            month = str(p.get("payroll_month", ""))[:7]
            branch_id = p.get("branch_id", "")
            out[(branch_id, month)] += parse_float(p.get("net_salary", 0.0), 0.0)
    else:
        branch_employees = defaultdict(list)
        for e in employees_rows:
            if str(e.get("status", "Active")).lower() != "inactive":
                branch_employees[e.get("branch_id", "")].append(e)
        for month in [f"2025-{m:02d}" for m in range(1, 13)]:
            for branch_id, emps in branch_employees.items():
                total = 0.0
                for e in emps:
                    sal = parse_float(e.get("salary_monthly"), 0.0)
                    adj = random.uniform(0.97, 1.06)
                    total += sal * adj
                out[(branch_id, month)] += total
    return out


def marketing_daily(marketing_rows, branches, start_date, end_date):
    out = defaultdict(float)
    if marketing_rows:
        for r in marketing_rows:
            d = parse_date(r.get("date") or r.get("campaign_date"))
            if not d:
                continue
            branch_id = r.get("branch_id", "")
            out[(d.isoformat(), branch_id)] += parse_float(r.get("spend", r.get("campaign_spend", 0.0)), 0.0)
    else:
        promo_months = {2, 5, 8, 11}
        branch_base = {b["branch_id"]: random.uniform(2400, 5200) for b in branches}
        for d in daterange(start_date, end_date):
            boost = 1.35 if d.month in promo_months else 1.0
            for b in branches:
                if random.random() < 0.43:
                    spend = branch_base[b["branch_id"]] * random.uniform(0.75, 1.2) * boost
                    out[(d.isoformat(), b["branch_id"])] += spend
    return out


def sales_cash_in(po_rows, branches, start_date, end_date):
    # Build branch-level proxy sales using purchase activity to keep internal consistency
    purchase_daily = defaultdict(float)
    for po in po_rows:
        d = parse_date(po.get("po_date"))
        if not d:
            continue
        purchase_daily[(d.isoformat(), po.get("branch_id", ""))] += parse_float(po.get("total_po_value"), 0.0)

    out = defaultdict(float)
    for d in daterange(start_date, end_date):
        for b in branches:
            purchases = purchase_daily[(d.isoformat(), b["branch_id"])]
            baseline = 128000 if b.get("city") in {"Karachi", "Lahore"} else 98000
            sales = baseline + purchases * random.uniform(3.2, 4.2)
            dow = d.weekday()
            if dow >= 5:
                sales *= 1.1
            if d.month in {3, 4, 12}:
                sales *= 1.06
            out[(d.isoformat(), b["branch_id"])] = max(52000.0, sales)
    return out


def build_cash_and_expenses(po_rows, payroll_by_month, marketing_by_day, branches, start_date, end_date):
    purchases_by_day = defaultdict(float)
    for po in po_rows:
        d = parse_date(po.get("po_date"))
        if not d:
            continue
        purchases_by_day[(d.isoformat(), po.get("branch_id", ""))] += parse_float(po.get("total_po_value"), 0.0)

    cash_in_by_day = sales_cash_in(po_rows, branches, start_date, end_date)

    cash_rows = []
    expense_rows = []
    expense_seq = 1
    cumulative_by_branch = defaultdict(lambda: random.uniform(180000, 260000))

    for d in daterange(start_date, end_date):
        day = d.isoformat()
        month = day[:7]
        for b in branches:
            bid = b["branch_id"]
            purchase = purchases_by_day[(day, bid)]
            payroll_daily = payroll_by_month[(bid, month)] / 30.0
            marketing = marketing_by_day[(day, bid)]
            cash_out = purchase + payroll_daily + marketing
            cash_in = cash_in_by_day[(day, bid)]
            net = cash_in - cash_out
            cumulative_by_branch[bid] += net

            cash_rows.append(
                {
                    "date": day,
                    "branch_id": bid,
                    "cash_in": f"{cash_in:.2f}",
                    "cash_out": f"{cash_out:.2f}",
                    "net_cash_flow": f"{net:.2f}",
                    "cumulative_cash": f"{cumulative_by_branch[bid]:.2f}",
                }
            )

            for exp_type, amt in (
                ("purchase", purchase),
                ("payroll", payroll_daily),
                ("marketing", marketing),
            ):
                if amt <= 0.0:
                    continue
                expense_rows.append(
                    {
                        "expense_id": f"EXP{expense_seq:010d}",
                        "date": day,
                        "branch_id": bid,
                        "expense_type": exp_type,
                        "amount": f"{amt:.2f}",
                    }
                )
                expense_seq += 1

    return cash_rows, expense_rows


def build_expansion_plan(branches):
    cities = ["Multan", "Peshawar", "Sialkot", "Hyderabad"]
    rows = []
    start = date(2026, 5, 1)
    for i, city in enumerate(cities, start=1):
        flagship_bias = 1.2 if city in {"Multan", "Peshawar"} else 1.0
        cost = random.uniform(340000, 620000) * flagship_bias
        rows.append(
            {
                "new_branch_id": f"NB{i:03d}",
                "city": city,
                "opening_date": (start + timedelta(days=75 * (i - 1))).isoformat(),
                "investment_cost": f"{cost:.2f}",
                "ramp_up_months": random.choice([4, 5, 6, 7]),
            }
        )
    return rows


def build_risk_and_compliance(branches, start_date, end_date):
    risk_rows = []
    audit_rows = []
    risk_seq = 1
    audit_seq = 1

    types = ["Food Safety", "Workplace Injury", "POS Downtime", "Theft", "Fire Drill Gap"]
    severities = ["Low", "Medium", "High"]

    for d in daterange(start_date, end_date):
        day = d.isoformat()
        for b in branches:
            bid = b["branch_id"]
            # occasional incidents only
            if random.random() < 0.018:
                sev = random.choices(severities, weights=[0.64, 0.29, 0.07])[0]
                risk_rows.append(
                    {
                        "incident_id": f"RISK{risk_seq:08d}",
                        "date": day,
                        "branch_id": bid,
                        "incident_type": random.choice(types),
                        "severity": sev,
                    }
                )
                risk_seq += 1

            compliance = max(70.0, min(99.8, random.gauss(91.8, 3.4)))
            if random.random() < 0.06:
                compliance -= random.uniform(6.0, 12.0)
            violation = 1 if compliance < 85.0 else 0
            audit_rows.append(
                {
                    "audit_id": f"AUD{audit_seq:09d}",
                    "date": day,
                    "branch_id": bid,
                    "compliance_score": f"{max(60.0, compliance):.2f}",
                    "violation_flag": violation,
                }
            )
            audit_seq += 1

    return risk_rows, audit_rows


def run_validations(supplier_rows, cash_rows, expense_rows, po_rows):
    # supplier linked to purchase
    po_supplier_pairs = {(po.get("supplier_id", ""), po.get("po_date", "")) for po in po_rows}
    for row in supplier_rows:
        key = (row["supplier_id"], row["date"])
        if key not in po_supplier_pairs:
            raise ValueError(f"supplier_performance row not linked to purchase_orders: {key}")

    # cash flow consistency
    for row in cash_rows[:2000] + cash_rows[-2000:]:
        cin = parse_float(row["cash_in"])
        cout = parse_float(row["cash_out"])
        net = parse_float(row["net_cash_flow"])
        if abs((cin - cout) - net) > 0.05:
            raise ValueError("cash flow consistency failed")

    # no extreme spikes: check 99th percentile vs median
    by_branch = defaultdict(list)
    for row in cash_rows:
        by_branch[row["branch_id"]].append(parse_float(row["cash_in"]))
    for branch, vals in by_branch.items():
        vals = sorted(vals)
        med = vals[len(vals) // 2]
        p99 = vals[int(len(vals) * 0.99)]
        if med > 0 and p99 / med > 3.2:
            raise ValueError(f"unrealistic spike detected in cash_in for {branch}")

    # cumulative cash warning threshold (allow negative if realistic)
    neg_count = sum(1 for r in cash_rows if parse_float(r["cumulative_cash"]) < 0)
    if neg_count > int(0.2 * len(cash_rows)):
        raise ValueError("too many negative cumulative cash rows; unrealistic scenario")

    if not expense_rows:
        raise ValueError("expense_register cannot be empty")


def main():
    suppliers = read_csv(INPUT_FILES["suppliers"])
    branches = active_branches(read_csv(INPUT_FILES["branches"]))
    employees = read_csv(INPUT_FILES["employees"])

    if not suppliers or not branches:
        raise FileNotFoundError("Required supplier/branch masters are missing or empty.")

    po_rows = read_csv(INPUT_FILES["purchase_orders"])
    pol_rows = read_csv(INPUT_FILES["purchase_order_lines"])

    start_date, end_date = date(2025, 1, 1), date(2025, 12, 31)
    if po_rows:
        po_dates = sorted([parse_date(r.get("po_date")) for r in po_rows if parse_date(r.get("po_date"))])
        if po_dates:
            start_date, end_date = po_dates[0], po_dates[-1]
    else:
        po_rows, pol_rows = build_purchase_fallback(branches, suppliers, start_date, end_date)

    supplier_perf = build_supplier_performance(po_rows, pol_rows)
    payroll_by_month = monthly_payroll_by_branch(read_csv(INPUT_FILES["hr_payroll"]), employees)
    marketing_by_day = marketing_daily(read_csv(INPUT_FILES["marketing_campaigns"]), branches, start_date, end_date)

    cash_rows, expense_rows = build_cash_and_expenses(po_rows, payroll_by_month, marketing_by_day, branches, start_date, end_date)
    expansion_rows = build_expansion_plan(branches)
    risk_rows, audit_rows = build_risk_and_compliance(branches, start_date, end_date)

    run_validations(supplier_perf, cash_rows, expense_rows, po_rows)

    write_csv(
        OUTPUT_FILES["supplier_performance"],
        ["supplier_id", "date", "delivery_delay_days", "quality_score", "order_fulfillment_rate"],
        supplier_perf,
    )
    write_csv(
        OUTPUT_FILES["cash_flow_daily"],
        ["date", "branch_id", "cash_in", "cash_out", "net_cash_flow", "cumulative_cash"],
        cash_rows,
    )
    write_csv(
        OUTPUT_FILES["expense_register"],
        ["expense_id", "date", "branch_id", "expense_type", "amount"],
        expense_rows,
    )
    write_csv(
        OUTPUT_FILES["expansion_plan"],
        ["new_branch_id", "city", "opening_date", "investment_cost", "ramp_up_months"],
        expansion_rows,
    )
    write_csv(
        OUTPUT_FILES["risk_incidents"],
        ["incident_id", "date", "branch_id", "incident_type", "severity"],
        risk_rows,
    )
    write_csv(
        OUTPUT_FILES["compliance_audit_log"],
        ["audit_id", "date", "branch_id", "compliance_score", "violation_flag"],
        audit_rows,
    )

    print("Generated files:")
    for key, path in OUTPUT_FILES.items():
        print(f"- {key}: {path}")


if __name__ == "__main__":
    main()
