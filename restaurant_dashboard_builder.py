#!/usr/bin/env python3
"""
restaurant_dashboard_builder.py

Exact-column restaurant dashboard builder.
Uses real columns from the validated dataset profile.

Expected files in the same folder:
- daily_kpis.csv
- branch_monthly_summary.csv
- channel_sales_summary.csv
- cost_summary_monthly.csv
- sku_margin_summary.csv
- menu_engineering_flags.csv
- cash_flow_daily.csv
- expense_register.csv
- branches_master.csv
- sku_master.csv

Outputs:
- output/executive_summary.png
- output/sales_performance.png
- output/menu_engineering.png
- output/cost_profitability.png
- output/financial_health.png
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# =========================================================
# CONFIG
# =========================================================

BASE_DIR = Path(".")
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FILES = {
    "daily_kpis": "daily_kpis.csv",
    "branch_monthly_summary": "branch_monthly_summary.csv",
    "channel_sales_summary": "channel_sales_summary.csv",
    "cost_summary_monthly": "cost_summary_monthly.csv",
    "sku_margin_summary": "sku_margin_summary.csv",
    "menu_engineering_flags": "menu_engineering_flags.csv",
    "cash_flow_daily": "cash_flow_daily.csv",
    "expense_register": "expense_register.csv",
    "branches_master": "branches_master.csv",
    "sku_master": "sku_master.csv",
}

BRAND = {
    "bg": "#F7F8FA",
    "card": "#FFFFFF",
    "text": "#1F2937",
    "muted": "#6B7280",
    "grid": "#E5E7EB",
    "primary": "#0F766E",
    "secondary": "#2563EB",
    "accent": "#F59E0B",
    "danger": "#DC2626",
    "success": "#16A34A",
    "purple": "#7C3AED",
    "pink": "#DB2777",
    "teal": "#0D9488",
}

sns.set_theme(style="whitegrid")
plt.rcParams["figure.facecolor"] = BRAND["bg"]
plt.rcParams["axes.facecolor"] = BRAND["card"]
plt.rcParams["savefig.facecolor"] = BRAND["bg"]
plt.rcParams["axes.edgecolor"] = BRAND["grid"]
plt.rcParams["grid.color"] = BRAND["grid"]
plt.rcParams["axes.labelcolor"] = BRAND["text"]
plt.rcParams["xtick.color"] = BRAND["muted"]
plt.rcParams["ytick.color"] = BRAND["muted"]
plt.rcParams["text.color"] = BRAND["text"]
plt.rcParams["font.size"] = 10


# =========================================================
# LOADERS
# =========================================================

def read_csv_exact(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path.name}")

    for enc in ["utf-8", "utf-8-sig", "latin1"]:
        try:
            df = pd.read_csv(path, encoding=enc)
            if df.shape[1] >= 1:
                return clean_df(df)
        except Exception:
            continue

    raise ValueError(f"Could not read file: {path.name}")


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [
        str(c).strip().lower().replace(" ", "_").replace("-", "_").replace("/", "_")
        for c in out.columns
    ]
    return out


def parse_date_col(df: pd.DataFrame, col: str) -> pd.DataFrame:
    out = df.copy()
    out[col] = pd.to_datetime(out[col], errors="coerce")
    return out


def month_floor(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.to_period("M").dt.to_timestamp()


# =========================================================
# FORMATTERS
# =========================================================

def format_currency(x: float) -> str:
    if pd.isna(x):
        return "0"
    x = float(x)
    ax = abs(x)
    if ax >= 1_000_000_000:
        return f"PKR {x/1_000_000_000:.2f}B"
    if ax >= 1_000_000:
        return f"PKR {x/1_000_000:.2f}M"
    if ax >= 1_000:
        return f"PKR {x/1_000:.1f}K"
    return f"PKR {x:,.0f}"


def format_number(x: float) -> str:
    if pd.isna(x):
        return "0"
    x = float(x)
    ax = abs(x)
    if ax >= 1_000_000:
        return f"{x/1_000_000:.2f}M"
    if ax >= 1_000:
        return f"{x/1_000:.1f}K"
    return f"{x:,.0f}"


def format_pct(x: float) -> str:
    if pd.isna(x):
        return "0.0%"
    return f"{float(x):.1f}%"


# =========================================================
# VISUAL HELPERS
# =========================================================

def page_title(fig, title: str, subtitle: str) -> None:
    fig.text(0.03, 0.96, title, fontsize=22, fontweight="bold", color=BRAND["text"])
    fig.text(0.03, 0.925, subtitle, fontsize=10, color=BRAND["muted"])


def draw_kpi_card(ax, title: str, value: str, subtitle: str, color: str) -> None:
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.text(0.04, 0.80, title, fontsize=10, color=BRAND["muted"], transform=ax.transAxes)
    ax.text(0.04, 0.46, value, fontsize=21, fontweight="bold", color=color, transform=ax.transAxes)
    ax.text(0.04, 0.18, subtitle, fontsize=9, color=BRAND["muted"], transform=ax.transAxes)


def style_axis(ax, title: str) -> None:
    ax.set_title(title, loc="left", fontsize=12, fontweight="bold", pad=10)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(True, axis="y", linestyle="--", alpha=0.35)


def top_n(df: pd.DataFrame, value_col: str, n: int = 10) -> pd.DataFrame:
    return df.sort_values(value_col, ascending=False).head(n).copy()


# =========================================================
# DATA MODEL
# =========================================================

class Model:
    def __init__(self) -> None:
        self.daily = parse_date_col(read_csv_exact(BASE_DIR / FILES["daily_kpis"]), "date")
        self.branch_monthly = parse_date_col(read_csv_exact(BASE_DIR / FILES["branch_monthly_summary"]), "month")
        self.channel = parse_date_col(read_csv_exact(BASE_DIR / FILES["channel_sales_summary"]), "month")
        self.cost = parse_date_col(read_csv_exact(BASE_DIR / FILES["cost_summary_monthly"]), "month")
        self.sku_margin = read_csv_exact(BASE_DIR / FILES["sku_margin_summary"])
        self.menu_flags = read_csv_exact(BASE_DIR / FILES["menu_engineering_flags"])
        self.cash = parse_date_col(read_csv_exact(BASE_DIR / FILES["cash_flow_daily"]), "date")
        self.expense = parse_date_col(read_csv_exact(BASE_DIR / FILES["expense_register"]), "date")
        self.branches = parse_date_col(read_csv_exact(BASE_DIR / FILES["branches_master"]), "opening_date")
        self.sku_master = read_csv_exact(BASE_DIR / FILES["sku_master"])

        self.prepare()

    def prepare(self) -> None:
        self.daily["month"] = month_floor(self.daily["date"])
        self.cash["month"] = month_floor(self.cash["date"])
        self.expense["month"] = month_floor(self.expense["date"])

        branch_lookup = self.branches[["branch_id", "branch_name", "city", "region"]].drop_duplicates()

        self.channel = self.channel.merge(
            branch_lookup[["branch_id", "branch_name"]],
            on="branch_id",
            how="left",
        )

        self.cost = self.cost.merge(
            branch_lookup[["branch_id", "branch_name"]],
            on="branch_id",
            how="left",
        )

        self.cash = self.cash.merge(
            branch_lookup[["branch_id", "branch_name"]],
            on="branch_id",
            how="left",
        )

        self.expense = self.expense.merge(
            branch_lookup[["branch_id", "branch_name"]],
            on="branch_id",
            how="left",
        )

        # Exact derived KPIs
        self.cost["margin_pct"] = np.where(
            self.cost["revenue"] != 0,
            (self.cost["gross_profit"] / self.cost["revenue"]) * 100,
            0,
        )
        self.cost["food_cost_pct"] = np.where(
            self.cost["revenue"] != 0,
            (self.cost["food_cost"] / self.cost["revenue"]) * 100,
            0,
        )

        self.branch_monthly["food_cost_pct"] = np.where(
            self.branch_monthly["total_revenue"] != 0,
            (self.branch_monthly["food_cost"] / self.branch_monthly["total_revenue"]) * 100,
            0,
        )

        self.branch_monthly["labor_pct"] = np.where(
            self.branch_monthly["total_revenue"] != 0,
            (self.branch_monthly["labor_cost"] / self.branch_monthly["total_revenue"]) * 100,
            0,
        )

        self.daily["food_cost_pct"] = np.where(
            self.daily["net_revenue"] != 0,
            (self.daily["food_cost"] / self.daily["net_revenue"]) * 100,
            0,
        )

        self.daily["refund_pct"] = np.where(
            self.daily["gross_revenue"] != 0,
            (self.daily["refunds_amount"] / self.daily["gross_revenue"]) * 100,
            0,
        )

        self.channel["food_cost_pct"] = np.where(
            self.channel["revenue"] != 0,
            (self.channel["food_cost"] / self.channel["revenue"]) * 100,
            0,
        )

        # Rename overlapping menu flag column before merge
        menu_flags_renamed = self.menu_flags.rename(
            columns={"loss_leader_flag": "loss_leader_flag_menu"}
        )

        self.sku = self.sku_margin.merge(
            menu_flags_renamed[
                [
                    "sku_id",
                    "popularity_band",
                    "margin_band",
                    "menu_engineering_quadrant",
                    "loss_leader_flag_menu",
                    "action_hint",
                ]
            ],
            on="sku_id",
            how="left",
        )

        # Use sku_margin_summary.loss_leader_flag as primary since it already exists there
        self.sku["loss_leader_label"] = np.where(
            self.sku["loss_leader_flag"] == 1,
            "Loss Leader",
            "Standard",
        )


# =========================================================
# PAGE 1: EXECUTIVE SUMMARY
# =========================================================

def build_executive_summary(m: Model) -> None:
    total_net_revenue = m.daily["net_revenue"].sum()
    total_orders = m.daily["total_orders"].sum()
    total_gp = m.daily["gross_profit"].sum()
    margin_pct = (total_gp / total_net_revenue * 100) if total_net_revenue else 0
    food_cost_pct = (m.daily["food_cost"].sum() / total_net_revenue * 100) if total_net_revenue else 0
    closing_cash = m.cash.sort_values("date")["cumulative_cash"].iloc[-1]

    monthly_exec = (
        m.branch_monthly.groupby("month", as_index=False)
        .agg(
            revenue=("total_revenue", "sum"),
            gross_profit=("gross_profit", "sum"),
            orders=("total_orders", "sum"),
            complaints=("complaint_rate_pct", "mean"),
        )
        .sort_values("month")
    )
    monthly_exec["margin_pct"] = np.where(
        monthly_exec["revenue"] != 0,
        monthly_exec["gross_profit"] / monthly_exec["revenue"] * 100,
        0,
    )

    branch_rank = (
        m.branch_monthly.groupby("branch_name", as_index=False)
        .agg(revenue=("total_revenue", "sum"), gross_profit=("gross_profit", "sum"))
        .sort_values("revenue", ascending=False)
    )

    expense_monthly = (
        m.expense.groupby("month", as_index=False)["amount"].sum().sort_values("month")
    )

    cash_monthly = (
        m.cash.groupby("month", as_index=False)
        .agg(net_cash_flow=("net_cash_flow", "sum"), cumulative_cash=("cumulative_cash", "last"))
        .sort_values("month")
    )

    fig = plt.figure(figsize=(18, 10))
    gs = fig.add_gridspec(12, 12, left=0.03, right=0.98, top=0.90, bottom=0.05, hspace=0.9, wspace=0.7)
    page_title(fig, "Executive Summary", "Net revenue, profit, margin, cost discipline, and liquidity overview")

    cards = [fig.add_subplot(gs[0:2, i * 2:(i + 1) * 2]) for i in range(6)]
    draw_kpi_card(cards[0], "Net Revenue", format_currency(total_net_revenue), "daily_kpis.net_revenue", BRAND["primary"])
    draw_kpi_card(cards[1], "Orders", format_number(total_orders), "daily_kpis.total_orders", BRAND["secondary"])
    draw_kpi_card(cards[2], "Gross Profit", format_currency(total_gp), "daily_kpis.gross_profit", BRAND["success"])
    draw_kpi_card(cards[3], "Margin %", format_pct(margin_pct), "gross_profit / net_revenue", BRAND["purple"])
    draw_kpi_card(cards[4], "Food Cost %", format_pct(food_cost_pct), "food_cost / net_revenue", BRAND["accent"])
    draw_kpi_card(cards[5], "Closing Cash", format_currency(closing_cash), "cash_flow_daily.cumulative_cash", BRAND["teal"])

    ax1 = fig.add_subplot(gs[2:6, 0:6])
    sns.lineplot(data=monthly_exec, x="month", y="revenue", marker="o", linewidth=2.6, color=BRAND["primary"], ax=ax1)
    sns.lineplot(data=monthly_exec, x="month", y="gross_profit", marker="o", linewidth=2.1, color=BRAND["success"], ax=ax1)
    style_axis(ax1, "Monthly Revenue and Gross Profit")
    ax1.set_xlabel("")
    ax1.set_ylabel("Amount")
    ax1.legend(["Revenue", "Gross Profit"], frameon=False, loc="upper left")

    ax2 = fig.add_subplot(gs[2:6, 6:12])
    top_branches = top_n(branch_rank, "revenue", n=5)
    sns.barplot(data=top_branches, y="branch_name", x="revenue", color=BRAND["secondary"], ax=ax2)
    style_axis(ax2, "Top Branches by Revenue")
    ax2.set_xlabel("Revenue")
    ax2.set_ylabel("")

    ax3 = fig.add_subplot(gs[6:12, 0:6])
    sns.lineplot(data=expense_monthly, x="month", y="amount", marker="o", linewidth=2.4, color=BRAND["danger"], ax=ax3)
    style_axis(ax3, "Monthly Expense Trend")
    ax3.set_xlabel("")
    ax3.set_ylabel("Expense Amount")

    ax4 = fig.add_subplot(gs[6:12, 6:12])
    sns.lineplot(data=cash_monthly, x="month", y="net_cash_flow", marker="o", linewidth=2.4, color=BRAND["secondary"], ax=ax4)
    sns.lineplot(data=cash_monthly, x="month", y="cumulative_cash", marker="o", linewidth=2.1, color=BRAND["teal"], ax=ax4)
    style_axis(ax4, "Monthly Cash Flow and Closing Cash")
    ax4.set_xlabel("")
    ax4.set_ylabel("Amount")
    ax4.legend(["Net Cash Flow", "Closing Cash"], frameon=False, loc="upper left")

    fig.savefig(OUTPUT_DIR / "executive_summary.png", dpi=220, bbox_inches="tight")
    plt.close(fig)


# =========================================================
# PAGE 2: SALES PERFORMANCE
# =========================================================

def build_sales_performance(m: Model) -> None:
    monthly_sales = (
        m.branch_monthly.groupby("month", as_index=False)
        .agg(
            revenue=("total_revenue", "sum"),
            orders=("total_orders", "sum"),
            aov=("avg_order_value", "mean"),
        )
        .sort_values("month")
    )

    branch_sales = (
        m.branch_monthly.groupby("branch_name", as_index=False)
        .agg(
            revenue=("total_revenue", "sum"),
            orders=("total_orders", "sum"),
            aov=("avg_order_value", "mean"),
        )
        .sort_values("revenue", ascending=False)
    )

    channel_month = (
        m.channel.groupby(["month", "channel"], as_index=False)
        .agg(revenue=("revenue", "sum"))
        .sort_values(["month", "channel"])
    )

    fig = plt.figure(figsize=(18, 10))
    gs = fig.add_gridspec(
        12, 12,
        left=0.03, right=0.98, top=0.90, bottom=0.05,
        hspace=0.9, wspace=0.7
    )
    page_title(
        fig,
        "Sales Performance",
        "Branch sales, monthly trajectory, channel mix, and AOV dynamics"
    )

    # 1) Monthly Revenue Trend
    ax1 = fig.add_subplot(gs[0:6, 0:6])
    sns.lineplot(
        data=monthly_sales,
        x="month",
        y="revenue",
        marker="o",
        linewidth=2.6,
        color=BRAND["primary"],
        ax=ax1
    )
    style_axis(ax1, "Monthly Revenue Trend")
    ax1.set_xlabel("")
    ax1.set_ylabel("Revenue")

    # 2) Orders vs Revenue
    # Use numeric x positions so bar + twinx line are compatible
    ax2 = fig.add_subplot(gs[0:6, 6:12])
    ax2b = ax2.twinx()

    plot_df = monthly_sales.copy()
    plot_df["month_label"] = plot_df["month"].dt.strftime("%Y-%m")
    x = np.arange(len(plot_df))

    ax2.bar(
        x,
        plot_df["orders"],
        color=BRAND["secondary"],
        alpha=0.75,
        width=0.65,
        label="Orders"
    )
    ax2b.plot(
        x,
        plot_df["revenue"],
        color=BRAND["danger"],
        marker="o",
        linewidth=2.3,
        label="Revenue"
    )

    ax2.set_xticks(x)
    ax2.set_xticklabels(plot_df["month_label"], rotation=30)
    style_axis(ax2, "Orders vs Revenue")
    ax2.set_xlabel("")
    ax2.set_ylabel("Orders")
    ax2b.set_ylabel("Revenue")
    ax2b.grid(False)

    # Combined legend
    h1, l1 = ax2.get_legend_handles_labels()
    h2, l2 = ax2b.get_legend_handles_labels()
    ax2.legend(h1 + h2, l1 + l2, frameon=False, loc="upper left")

    # 3) Branch Revenue Ranking
    ax3 = fig.add_subplot(gs[6:12, 0:6])
    top_branch_sales = top_n(branch_sales, "revenue", n=5)
    sns.barplot(
        data=top_branch_sales,
        y="branch_name",
        x="revenue",
        color=BRAND["teal"],
        ax=ax3
    )
    style_axis(ax3, "Branch Revenue Ranking")
    ax3.set_xlabel("Revenue")
    ax3.set_ylabel("")

        # 4) Channel Sales Mix by Month
    ax4 = fig.add_subplot(gs[6:12, 6:12])

    pivot = (
        channel_month.pivot(index="month", columns="channel", values="revenue")
        .fillna(0)
        .sort_index()
        .reset_index()
    )

    pivot["month_label"] = pd.to_datetime(pivot["month"]).dt.strftime("%Y-%m")

    channels = [c for c in pivot.columns if c not in ["month", "month_label"]]
    x = np.arange(len(pivot))
    bottom = np.zeros(len(pivot))

    colors = sns.color_palette("viridis", n_colors=len(channels))

    for color, ch in zip(colors, channels):
        ax4.bar(
            x,
            pivot[ch].values,
            bottom=bottom,
            width=0.75,
            label=ch,
            color=color,
        )
        bottom += pivot[ch].values

    ax4.set_xticks(x)
    ax4.set_xticklabels(pivot["month_label"], rotation=30)
    style_axis(ax4, "Channel Sales Mix by Month")
    ax4.set_xlabel("")
    ax4.set_ylabel("Revenue")
    ax4.legend(frameon=False, title="", loc="upper left")

    fig.savefig(OUTPUT_DIR / "sales_performance.png", dpi=220, bbox_inches="tight")
    plt.close(fig)


# =========================================================
# PAGE 3: PRODUCT / MENU ENGINEERING
# =========================================================

def build_menu_engineering(m: Model) -> None:
    di_profit = m.sku[["sku_name", "gross_profit_di"]].sort_values("gross_profit_di", ascending=False)
    dt_profit = m.sku[["sku_name", "gross_profit_dt"]].sort_values("gross_profit_dt", ascending=False)

    quadrant_counts = (
        m.sku.groupby("menu_engineering_quadrant", as_index=False)
        .agg(count=("sku_id", "count"))
        .sort_values("count", ascending=False)
    )

    compare = (
        m.sku[
            [
                "sku_name",
                "price_di",
                "cost_di",
                "gross_margin_di_pct",
                "menu_engineering_quadrant",
            ]
        ]
        .sort_values("gross_margin_di_pct", ascending=False)
        .head(10)
    )

    action_counts = (
        m.sku.groupby("action_hint", as_index=False)
        .agg(count=("sku_id", "count"))
        .sort_values("count", ascending=False)
    )

    fig = plt.figure(figsize=(18, 10))
    gs = fig.add_gridspec(12, 12, left=0.03, right=0.98, top=0.90, bottom=0.05, hspace=0.9, wspace=0.7)
    page_title(fig, "Product / Menu Engineering", "Dine-in profitability, quadrant mix, and action guidance by SKU")

    ax1 = fig.add_subplot(gs[0:6, 0:4])
    sns.barplot(data=di_profit.head(10), y="sku_name", x="gross_profit_di", color=BRAND["primary"], ax=ax1)
    style_axis(ax1, "Top 10 SKUs by Dine-In Gross Profit")
    ax1.set_xlabel("Gross Profit DI")
    ax1.set_ylabel("")

    ax2 = fig.add_subplot(gs[0:6, 4:8])
    sns.barplot(data=dt_profit.head(10), y="sku_name", x="gross_profit_dt", color=BRAND["secondary"], ax=ax2)
    style_axis(ax2, "Top 10 SKUs by Takeaway Gross Profit")
    ax2.set_xlabel("Gross Profit DT")
    ax2.set_ylabel("")

    ax3 = fig.add_subplot(gs[0:6, 8:12])
    sns.barplot(data=quadrant_counts, x="menu_engineering_quadrant", y="count", color=BRAND["accent"], ax=ax3)
    style_axis(ax3, "Menu Engineering Quadrant Count")
    ax3.set_xlabel("")
    ax3.set_ylabel("SKU Count")
    ax3.tick_params(axis="x", rotation=20)

    ax4 = fig.add_subplot(gs[6:12, 0:8])
    plot_df = compare.sort_values("gross_margin_di_pct", ascending=True)
    y = np.arange(len(plot_df))
    ax4.barh(y + 0.18, plot_df["price_di"], height=0.35, color=BRAND["primary"], label="Price DI")
    ax4.barh(y - 0.18, plot_df["cost_di"], height=0.35, color=BRAND["danger"], label="Cost DI")
    ax4.set_yticks(y)
    ax4.set_yticklabels(plot_df["sku_name"])
    style_axis(ax4, "Dine-In Price vs Cost (Top Margin SKUs)")
    ax4.set_xlabel("Amount")
    ax4.legend(frameon=False, loc="lower right")

    ax5 = fig.add_subplot(gs[6:12, 8:12])
    sns.barplot(data=action_counts, y="action_hint", x="count", color=BRAND["purple"], ax=ax5)
    style_axis(ax5, "Recommended Action Hints")
    ax5.set_xlabel("SKU Count")
    ax5.set_ylabel("")

    fig.savefig(OUTPUT_DIR / "menu_engineering.png", dpi=220, bbox_inches="tight")
    plt.close(fig)


# =========================================================
# PAGE 4: COST & PROFITABILITY
# =========================================================

def build_cost_profitability(m: Model) -> None:
    monthly_cost = (
        m.cost.groupby("month", as_index=False)
        .agg(
            revenue=("revenue", "sum"),
            food_cost=("food_cost", "sum"),
            packaging_cost=("packaging_cost", "sum"),
            labor_cost=("labor_cost", "sum"),
            overhead_cost=("overhead_cost", "sum"),
            marketing_cost=("marketing_cost", "sum"),
            refunds=("refunds", "sum"),
            total_cost=("total_cost", "sum"),
            gross_profit=("gross_profit", "sum"),
            surplus=("net_cash_like_surplus", "sum"),
        )
        .sort_values("month")
    )
    monthly_cost["margin_pct"] = np.where(
        monthly_cost["revenue"] != 0,
        monthly_cost["gross_profit"] / monthly_cost["revenue"] * 100,
        0,
    )
    monthly_cost["food_cost_pct"] = np.where(
        monthly_cost["revenue"] != 0,
        monthly_cost["food_cost"] / monthly_cost["revenue"] * 100,
        0,
    )

    branch_profit = (
        m.cost.groupby("branch_name", as_index=False)
        .agg(gross_profit=("gross_profit", "sum"))
        .sort_values("gross_profit", ascending=False)
    )

    expense_breakdown = (
        m.expense.groupby("expense_type", as_index=False)["amount"].sum().sort_values("amount", ascending=False)
    )

    fig = plt.figure(figsize=(18, 10))
    gs = fig.add_gridspec(12, 12, left=0.03, right=0.98, top=0.90, bottom=0.05, hspace=0.9, wspace=0.7)
    page_title(fig, "Cost & Profitability", "Margin, food cost discipline, branch profitability, and operating expense burden")

    ax1 = fig.add_subplot(gs[0:6, 0:4])
    sns.lineplot(data=monthly_cost, x="month", y="gross_profit", marker="o", linewidth=2.5, color=BRAND["success"], ax=ax1)
    style_axis(ax1, "Monthly Gross Profit")
    ax1.set_xlabel("")
    ax1.set_ylabel("Gross Profit")

    ax2 = fig.add_subplot(gs[0:6, 4:8])
    sns.lineplot(data=monthly_cost, x="month", y="food_cost_pct", marker="o", linewidth=2.5, color=BRAND["accent"], ax=ax2)
    style_axis(ax2, "Monthly Food Cost %")
    ax2.set_xlabel("")
    ax2.set_ylabel("Food Cost %")

    ax3 = fig.add_subplot(gs[0:6, 8:12])
    sns.barplot(data=branch_profit, y="branch_name", x="gross_profit", color=BRAND["secondary"], ax=ax3)
    style_axis(ax3, "Branch Gross Profit Ranking")
    ax3.set_xlabel("Gross Profit")
    ax3.set_ylabel("")

    ax4 = fig.add_subplot(gs[6:12, 0:6])
    top_exp = expense_breakdown.head(10)
    sns.barplot(data=top_exp, y="expense_type", x="amount", color=BRAND["danger"], ax=ax4)
    style_axis(ax4, "Expense Breakdown by Category")
    ax4.set_xlabel("Expense Amount")
    ax4.set_ylabel("")

    ax5 = fig.add_subplot(gs[6:12, 6:12])
    stacked = monthly_cost[
        ["month", "food_cost", "packaging_cost", "labor_cost", "overhead_cost", "marketing_cost"]
    ].set_index("month")
    stacked.plot(kind="area", stacked=True, ax=ax5, alpha=0.75)
    style_axis(ax5, "Monthly Cost Mix")
    ax5.set_xlabel("")
    ax5.set_ylabel("Cost")
    ax5.legend(frameon=False, title="", loc="upper left")

    fig.savefig(OUTPUT_DIR / "cost_profitability.png", dpi=220, bbox_inches="tight")
    plt.close(fig)


# =========================================================
# PAGE 5: FINANCIAL HEALTH
# =========================================================

def build_financial_health(m: Model) -> None:
    daily_cash = m.cash.sort_values("date").copy()

    expense_type = (
        m.expense.groupby("expense_type", as_index=False)["amount"].sum().sort_values("amount", ascending=False)
    )

    closing_cash_monthly = (
        m.cash.groupby("month", as_index=False)
        .agg(closing_cash=("cumulative_cash", "last"), net_cash_flow=("net_cash_flow", "sum"))
        .sort_values("month")
    )

    fig = plt.figure(figsize=(18, 10))
    gs = fig.add_gridspec(12, 12, left=0.03, right=0.98, top=0.90, bottom=0.05, hspace=0.9, wspace=0.7)
    page_title(fig, "Financial Health", "Cash movement, liquidity trend, and expense pressure points")

    ax1 = fig.add_subplot(gs[0:6, 0:6])
    sns.lineplot(data=daily_cash, x="date", y="net_cash_flow", color=BRAND["secondary"], linewidth=2.2, ax=ax1)
    style_axis(ax1, "Daily Net Cash Flow")
    ax1.set_xlabel("")
    ax1.set_ylabel("Net Cash Flow")

    ax2 = fig.add_subplot(gs[0:6, 6:12])
    sns.lineplot(data=daily_cash, x="date", y="cash_in", color=BRAND["success"], linewidth=2.2, ax=ax2, label="Cash In")
    sns.lineplot(data=daily_cash, x="date", y="cash_out", color=BRAND["danger"], linewidth=2.2, ax=ax2, label="Cash Out")
    style_axis(ax2, "Cash In vs Cash Out")
    ax2.set_xlabel("")
    ax2.set_ylabel("Amount")
    ax2.legend(frameon=False, loc="upper left")

    ax3 = fig.add_subplot(gs[6:12, 0:6])
    sns.lineplot(data=closing_cash_monthly, x="month", y="closing_cash", color=BRAND["teal"], linewidth=2.4, marker="o", ax=ax3)
    style_axis(ax3, "Monthly Closing Cash")
    ax3.set_xlabel("")
    ax3.set_ylabel("Closing Cash")

    ax4 = fig.add_subplot(gs[6:12, 6:12])
    sns.barplot(data=expense_type.head(10), y="expense_type", x="amount", color=BRAND["accent"], ax=ax4)
    style_axis(ax4, "Top Expense Outflow Categories")
    ax4.set_xlabel("Expense Amount")
    ax4.set_ylabel("")

    fig.savefig(OUTPUT_DIR / "financial_health.png", dpi=220, bbox_inches="tight")
    plt.close(fig)


# =========================================================
# MAIN
# =========================================================

def main() -> None:
    print("Loading exact-column datasets...")
    m = Model()

    print("Building executive_summary.png")
    build_executive_summary(m)

    print("Building sales_performance.png")
    build_sales_performance(m)

    print("Building menu_engineering.png")
    build_menu_engineering(m)

    print("Building cost_profitability.png")
    build_cost_profitability(m)

    print("Building financial_health.png")
    build_financial_health(m)

    print(f"Done. Files saved to: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()