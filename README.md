# 🍔 Multi-Branch Fast Food Restaurant Data Platform (Pakistan)

## 📌 Project Overview

This project simulates a **complete data ecosystem** for a multi-branch fast food restaurant operating in Pakistan.

It is designed to replicate **real-world restaurant operations**, including:
- Sales behavior
- Cost structures
- Menu engineering
- Customer trends
- Channel performance (Dine-In vs Takeaway)

The dataset spans **1 full year (2025)** across **multiple branches**, making it suitable for:
- Business intelligence dashboards
- Data analytics portfolios
- Financial analysis
- Operational optimization

---

## 🎯 Objectives

- Build a **realistic, interconnected dataset**
- Reflect **actual restaurant business logic**
- Enable **executive-level decision making**
- Prepare data for **dashboarding & analytics**

---

## 🧠 Key Features

### 📊 Realistic Business Simulation
- Daily sales variation (weekday, weekend, seasonality)
- Ramzan & seasonal demand shifts
- Campaign-driven spikes
- City-wise performance differences

### 🍗 BOM-Based Costing
- Recipe-level cost structure
- Ingredient-based pricing
- Waste margins included
- Separate dine-in vs takeaway costs

### 🧾 Financial Accuracy
- Cost-driven profit calculation (NOT artificially forced)
- Full cost breakdown:
  - Food cost
  - Packaging
  - Labor
  - Overheads
  - Marketing
  - Refunds

### 🛵 Channel Intelligence
- Dine-In vs Takeaway split
- Different packaging costs
- Different pricing strategies

### 📈 Menu Engineering
- SKU-level performance tracking
- Margin vs popularity classification:
  - ⭐ Star
  - 🐎 Plowhorse
  - 🧩 Puzzle
  - 🐶 Dog

---

## 🗂️ Repository Structure
restaurant-company/

├── data/
│ ├── raw/ # (Future use)
│ ├── processed/ # (Future use)
│ └── final/ # Final datasets (READY FOR DASHBOARDS)
│
├── scripts/
│ └── generate_executive_datasets.py # Data generator
│
├── docs/
│ └── data_dictionary.md # (To be added)
│
├── README.md


---

## 📦 Datasets Included

| File | Description |
|-----|------------|
| `daily_kpis.csv` | Daily branch-level operations |
| `branch_monthly_summary.csv` | Monthly financial performance |
| `channel_sales_summary.csv` | Dine-in vs takeaway analysis |
| `cost_summary_monthly.csv` | Full cost breakdown |
| `sku_margin_summary.csv` | SKU-level profitability |
| `menu_engineering_flags.csv` | Menu performance classification |
| `sku_master_internal.csv` | Master SKU dataset |

---

## 📊 Data Highlights

- 📅 **365 days of data**
- 🏪 **5 branches across Pakistan**
- 🍔 **20+ menu items**
- 📦 **Realistic cost & pricing models**
- 📉 Includes **loss leaders**
- 📈 Includes **seasonality & campaigns**

---

## ⚙️ How to Run

### 1. Clone the repository

```bash
git clone https://github.com/GhauriChishti/restaurant-company
cd restaurant-company

2. Run the script
python scripts/generate_executive_datasets.py

3. Output

All datasets will be generated in:
/data/final/

🔥 Business Logic (Important)

Profit is calculated using real-world accounting logic:

total_cost = food_cost + packaging_cost + labor_cost + overhead_cost + marketing_cost + refunds

gross_profit = net_revenue - total_cost

✅ No artificial margin forcing
✅ Fully cost-driven profitability

🚀 Use Cases
Power BI / Tableau dashboards
SQL analytics projects
Financial modeling
Menu optimization
Operational efficiency analysis

📌 Future Enhancements
Inventory management dataset
Supplier performance tracking
HR & workforce analytics
Customer feedback system
Marketing campaign attribution

👤 Author

Farooq Ghauri
Data Analyst / Data Engineer (Portfolio Project)