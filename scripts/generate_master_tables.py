import csv
import random
import math
from datetime import date, timedelta
from collections import defaultdict

SEED = 20260328
random.seed(SEED)

OUTPUT_FILES = [
    "branches_master.csv",
    "suppliers_master.csv",
    "ingredients_master.csv",
    "batches_master.csv",
    "batch_recipe_lines.csv",
    "processed_items_master.csv",
    "sku_master.csv",
    "sku_recipe_lines.csv",
    "employees_master.csv",
]


def write_csv(filename, fieldnames, rows):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def money(val):
    return round(float(val), 4)


def build_branches():
    rows = [
        {
            "branch_id": "B001",
            "branch_name": "Karachi Clifton Flagship",
            "city": "Karachi",
            "region": "Sindh",
            "opening_date": "2018-04-14",
            "branch_type": "Flagship",
            "size_band": "XL",
            "seating_capacity": 180,
            "kitchen_capacity_score": 98,
            "status": "Active",
        },
        {
            "branch_id": "B002",
            "branch_name": "Lahore Gulberg Central",
            "city": "Lahore",
            "region": "Punjab",
            "opening_date": "2019-09-01",
            "branch_type": "High Street",
            "size_band": "L",
            "seating_capacity": 140,
            "kitchen_capacity_score": 90,
            "status": "Active",
        },
        {
            "branch_id": "B003",
            "branch_name": "Islamabad F-7 Express",
            "city": "Islamabad",
            "region": "ICT",
            "opening_date": "2020-11-20",
            "branch_type": "Express",
            "size_band": "M",
            "seating_capacity": 110,
            "kitchen_capacity_score": 85,
            "status": "Active",
        },
        {
            "branch_id": "B004",
            "branch_name": "Rawalpindi Saddar Drive-Thru",
            "city": "Rawalpindi",
            "region": "Punjab",
            "opening_date": "2021-07-05",
            "branch_type": "Drive-Thru",
            "size_band": "M",
            "seating_capacity": 95,
            "kitchen_capacity_score": 80,
            "status": "Active",
        },
        {
            "branch_id": "B005",
            "branch_name": "Faisalabad D-Ground",
            "city": "Faisalabad",
            "region": "Punjab",
            "opening_date": "2022-03-18",
            "branch_type": "High Street",
            "size_band": "S",
            "seating_capacity": 72,
            "kitchen_capacity_score": 72,
            "status": "Active",
        },
    ]
    return rows


def build_suppliers():
    rows = [
        ("S001", "Pak Proteins Karachi", "protein", "Karachi", "Muhammad Bilal", 2, 15, 0.95, 1),
        ("S002", "Punjab Meat Works", "protein", "Lahore", "Sajid Hussain", 3, 21, 0.93, 1),
        ("S003", "Metro Dry Goods", "dry goods", "Lahore", "Adeel Rauf", 4, 30, 0.89, 1),
        ("S004", "Spice Route Foods", "sauces_spices", "Karachi", "Farhan Qadir", 3, 21, 0.91, 1),
        ("S005", "WrapPack Industries", "packaging", "Faisalabad", "Usman Tahir", 5, 30, 0.87, 1),
        ("S006", "Refresh Beverages", "beverages", "Islamabad", "Hamza Iqbal", 2, 14, 0.92, 1),
        ("S007", "Northern Agri Fresh", "dry goods", "Rawalpindi", "Hassan Naeem", 2, 15, 0.88, 1),
        ("S008", "National Packaging Corp", "packaging", "Karachi", "Rizwan Ali", 4, 30, 0.9, 1),
        ("S009", "Urban Sauces Co", "sauces_spices", "Lahore", "Tariq Shabbir", 2, 21, 0.94, 1),
        ("S010", "Punjab Beverage Traders", "beverages", "Faisalabad", "Kamran Yousaf", 3, 21, 0.84, 0),
    ]
    return [
        {
            "supplier_id": r[0],
            "supplier_name": r[1],
            "supplier_category": r[2],
            "city": r[3],
            "contact_person": r[4],
            "lead_time_days": r[5],
            "payment_terms_days": r[6],
            "reliability_score": r[7],
            "active_flag": r[8],
        }
        for r in rows
    ]


def build_ingredients():
    data = [
        ("Salt", "spice", "mineral", "g", "kg", 1000, 0.09, 0, 0, 365, "S004", 1),
        ("Chicken Whole", "protein", "meat", "g", "kg", 1000, 0.62, 0, 1, 3, "S001", 1),
        ("Beef Strips", "protein", "meat", "g", "kg", 1000, 0.78, 0, 1, 3, "S002", 1),
        ("Oil", "frying", "oil", "ml", "ltr", 1000, 0.035, 0, 0, 270, "S003", 1),
        ("Fries Frozen", "starch", "frozen", "g", "kg", 1000, 0.29, 0, 1, 120, "S007", 1),
        ("Bun", "bakery", "bread", "pcs", "pack", 12, 16.0, 0, 1, 4, "S003", 1),
        ("Potato Bun", "bakery", "bread", "pcs", "pack", 12, 18.0, 0, 1, 4, "S003", 1),
        ("Tortilla Wrap", "bakery", "bread", "pcs", "pack", 10, 21.0, 0, 1, 5, "S003", 1),
        ("Cheddar Cheese", "dairy", "cheese", "g", "kg", 1000, 0.95, 0, 1, 25, "S007", 1),
        ("Cheese Slice", "dairy", "cheese", "pcs", "pack", 20, 26.0, 0, 1, 35, "S007", 1),
        ("Mayo Premium", "sauce", "condiment", "g", "kg", 1000, 0.44, 0, 1, 60, "S009", 1),
        ("Mustard Paste", "sauce", "condiment", "g", "kg", 1000, 0.39, 0, 0, 180, "S009", 1),
        ("Ketchup", "sauce", "condiment", "g", "kg", 1000, 0.26, 0, 0, 180, "S004", 1),
        ("Chilli Ketchup", "sauce", "condiment", "g", "kg", 1000, 0.31, 0, 0, 180, "S004", 1),
        ("Jalapeno", "vegetable", "pickle", "g", "kg", 1000, 0.41, 0, 1, 14, "S009", 1),
        ("Lettuce", "vegetable", "fresh", "g", "kg", 1000, 0.19, 0, 1, 4, "S007", 1),
        ("Onion", "vegetable", "fresh", "g", "kg", 1000, 0.13, 0, 1, 10, "S007", 1),
        ("Garlic", "vegetable", "fresh", "g", "kg", 1000, 0.24, 0, 1, 20, "S007", 1),
        ("Garlic Powder", "spice", "powder", "g", "kg", 1000, 0.59, 0, 0, 360, "S004", 1),
        ("Ginger Powder", "spice", "powder", "g", "kg", 1000, 0.53, 0, 0, 360, "S004", 1),
        ("Black Pepper", "spice", "powder", "g", "kg", 1000, 0.82, 0, 0, 365, "S004", 1),
        ("White Pepper", "spice", "powder", "g", "kg", 1000, 0.88, 0, 0, 365, "S004", 1),
        ("Chicken Powder", "seasoning", "powder", "g", "kg", 1000, 0.49, 0, 0, 300, "S004", 1),
        ("Soya Sauce", "sauce", "liquid", "ml", "ltr", 1000, 0.19, 0, 0, 365, "S009", 1),
        ("Sunflower Oil", "frying", "oil", "ml", "ltr", 1000, 0.04, 0, 0, 270, "S003", 1),
        ("Honey", "sweetener", "liquid", "g", "kg", 1000, 0.68, 0, 0, 365, "S009", 1),
        ("Mustard Seed", "spice", "seed", "g", "kg", 1000, 0.46, 0, 0, 365, "S004", 1),
        ("Hot Sauce", "sauce", "condiment", "g", "kg", 1000, 0.34, 0, 0, 180, "S009", 1),
        ("Sprite", "beverage", "soft_drink", "ml", "crate_ml", 30000, 0.015, 0, 0, 120, "S006", 1),
        ("Tea Bag", "beverage", "tea", "pcs", "box", 100, 8.5, 0, 0, 540, "S006", 1),
        ("Blueberry Syrup", "beverage", "syrup", "ml", "ltr", 1000, 0.27, 0, 0, 240, "S006", 1),
        ("Lemon", "fruit", "citrus", "g", "kg", 1000, 0.22, 0, 1, 10, "S007", 1),
        ("Mint", "herb", "fresh", "g", "kg", 1000, 0.31, 0, 1, 5, "S007", 1),
        ("Sugar", "sweetener", "dry", "g", "kg", 1000, 0.16, 0, 0, 365, "S003", 1),
        ("Dip Container", "packaging", "container", "pcs", "carton", 500, 4.2, 1, 0, 730, "S008", 1),
        ("Tissue Paper", "packaging", "consumable", "pcs", "pack", 200, 0.7, 1, 0, 730, "S008", 1),
        ("Butter Paper", "packaging", "wrap", "pcs", "pack", 500, 1.4, 1, 0, 730, "S005", 1),
        ("Foil", "packaging", "wrap", "pcs", "roll", 600, 2.4, 1, 0, 730, "S005", 1),
        ("Quarter Box", "packaging", "box", "pcs", "carton", 250, 14.0, 1, 0, 730, "S008", 1),
        ("F3 Box", "packaging", "box", "pcs", "carton", 250, 16.5, 1, 0, 730, "S008", 1),
        ("Lid F3", "packaging", "lid", "pcs", "carton", 250, 6.5, 1, 0, 730, "S008", 1),
        ("Fries Pouch", "packaging", "pouch", "pcs", "carton", 300, 5.2, 1, 0, 730, "S005", 1),
        ("Burger Box", "packaging", "box", "pcs", "carton", 250, 10.5, 1, 0, 730, "S005", 1),
        ("Food Bag", "packaging", "bag", "pcs", "bundle", 500, 3.1, 1, 0, 730, "S008", 1),
        ("Shopping Bag Small", "packaging", "bag", "pcs", "bundle", 300, 8.0, 1, 0, 730, "S008", 1),
        ("Shopping Bag Large", "packaging", "bag", "pcs", "bundle", 250, 11.5, 1, 0, 730, "S008", 1),
        ("Glass", "packaging", "drinkware", "pcs", "carton", 1000, 2.3, 1, 0, 730, "S005", 1),
        ("Straw", "packaging", "drinkware", "pcs", "box", 1000, 0.9, 1, 0, 730, "S005", 1),
    ]
    rows = []
    for i, item in enumerate(data, start=1):
        rows.append(
            {
                "ingredient_id": f"I{i:03d}",
                "ingredient_name": item[0],
                "ingredient_group": item[1],
                "ingredient_category": item[2],
                "base_uom": item[3],
                "buying_uom": item[4],
                "conversion_to_base": item[5],
                "standard_unit_cost": money(item[6]),
                "is_packaging": item[7],
                "is_perishable": item[8],
                "shelf_life_days": item[9],
                "default_supplier_id": item[10],
                "active_flag": item[11],
            }
        )
    return rows


def build_batches():
    defs = [
        ("Tenderizer", "protein_prep", 2000, "g", 0.02, "Classic", 1),
        ("Breading", "coating", 2500, "g", 0.03, "Golden", 1),
        ("Spicy", "coating", 1200, "g", 0.02, "Fiery", 1),
        ("Less Spicy", "coating", 1200, "g", 0.02, "Mild", 1),
        ("Beef Marination", "protein_prep", 1800, "g", 0.03, "Laham", 1),
        ("Garlic Dip Batch", "dip", 3000, "g", 0.02, "Signature", 1),
        ("Tangy Dip Batch", "dip", 2500, "g", 0.02, "Tangy", 1),
        ("Honey Mustard Batch", "dip", 2200, "g", 0.02, "SweetHeat", 1),
        ("Chipotle Dip Batch", "dip", 2200, "g", 0.02, "Smoky", 1),
        ("Garlic Creamy Sauce Batch", "sauce", 2600, "g", 0.02, "Creamy", 1),
        ("Minty Whirl Batch", "sauce", 2400, "g", 0.02, "Mint", 1),
        ("Burger Glow Batch", "sauce", 2400, "g", 0.02, "Special", 1),
        ("Mighty Mush Batch", "sauce", 2400, "g", 0.02, "Mushroom", 1),
    ]
    rows = []
    for i, d in enumerate(defs, start=1):
        rows.append(
            {
                "batch_id": f"BA{i:03d}",
                "batch_name": d[0],
                "batch_group": d[1],
                "output_qty": d[2],
                "output_uom": d[3],
                "waste_pct": d[4],
                "standard_cost_per_output_unit": 0.0,
                "variant_name": d[5],
                "active_flag": d[6],
            }
        )
    return rows


def build_batch_recipe_lines(ingredients_by_name, batches):
    recipes = {
        "Tenderizer": [("Soya Sauce", 100, "ml"), ("Garlic", 80, "g"), ("Ginger Powder", 40, "g"), ("Chicken Powder", 50, "g"), ("Black Pepper", 15, "g")],
        "Breading": [("Garlic Powder", 120, "g"), ("Black Pepper", 25, "g"), ("White Pepper", 20, "g"), ("Chicken Powder", 80, "g"), ("Ginger Powder", 40, "g"), ("Sugar", 45, "g")],
        "Spicy": [("Chilli Ketchup", 220, "g"), ("Hot Sauce", 130, "g"), ("Black Pepper", 25, "g"), ("Mustard Paste", 70, "g")],
        "Less Spicy": [("Ketchup", 240, "g"), ("Mayo Premium", 180, "g"), ("Black Pepper", 15, "g"), ("Mustard Paste", 60, "g")],
        "Beef Marination": [("Soya Sauce", 130, "ml"), ("Mustard Paste", 100, "g"), ("Garlic", 90, "g"), ("Black Pepper", 30, "g"), ("White Pepper", 20, "g"), ("Sunflower Oil", 150, "ml")],
        "Garlic Dip Batch": [("Mayo Premium", 900, "g"), ("Garlic", 180, "g"), ("Garlic Powder", 90, "g"), ("Lemon", 150, "g"), ("Salt", 10, "g")],
        "Tangy Dip Batch": [("Mayo Premium", 700, "g"), ("Ketchup", 380, "g"), ("Mustard Paste", 130, "g"), ("Lemon", 130, "g")],
        "Honey Mustard Batch": [("Honey", 480, "g"), ("Mustard Paste", 360, "g"), ("Mayo Premium", 420, "g"), ("Lemon", 100, "g"), ("Mustard Seed", 70, "g")],
        "Chipotle Dip Batch": [("Mayo Premium", 680, "g"), ("Hot Sauce", 260, "g"), ("Chilli Ketchup", 220, "g"), ("Garlic Powder", 40, "g")],
        "Garlic Creamy Sauce Batch": [("Mayo Premium", 720, "g"), ("Garlic", 210, "g"), ("Cheddar Cheese", 140, "g"), ("Black Pepper", 15, "g"), ("Lemon", 90, "g")],
        "Minty Whirl Batch": [("Mayo Premium", 640, "g"), ("Mint", 220, "g"), ("Lemon", 180, "g"), ("Sugar", 70, "g"), ("Mustard Paste", 50, "g")],
        "Burger Glow Batch": [("Mayo Premium", 650, "g"), ("Ketchup", 250, "g"), ("Mustard Paste", 130, "g"), ("Chilli Ketchup", 120, "g"), ("Sugar", 60, "g")],
        "Mighty Mush Batch": [("Mayo Premium", 560, "g"), ("Cheddar Cheese", 180, "g"), ("Black Pepper", 18, "g"), ("Garlic Powder", 32, "g"), ("Soya Sauce", 90, "ml")],
    }

    # Add a minimal synthetic seasoning ingredient if not listed in base BOM
    #if "Salt" not in ingredients_by_name:
    #    pass

    batch_id_by_name = {b["batch_name"]: b["batch_id"] for b in batches}
    rows = []
    idx = 1
    for batch_name, lines in recipes.items():
        for ingredient_name, qty, q_uom in lines:
            if ingredient_name not in ingredients_by_name:
                raise ValueError(f"Missing ingredient in master: {ingredient_name}")
            ing = ingredients_by_name[ingredient_name]
            unit_cost = float(ing["standard_unit_cost"])
            line_cost = money(qty * unit_cost)
            rows.append(
                {
                    "batch_recipe_line_id": f"BRL{idx:04d}",
                    "batch_id": batch_id_by_name[batch_name],
                    "ingredient_id": ing["ingredient_id"],
                    "quantity_required": qty,
                    "quantity_uom": q_uom,
                    "standard_unit_cost": unit_cost,
                    "line_cost": line_cost,
                }
            )
            idx += 1
    return rows


def assign_batch_costs(batches, batch_recipe_lines):
    grouped = defaultdict(float)
    for r in batch_recipe_lines:
        grouped[r["batch_id"]] += float(r["line_cost"])
    for b in batches:
        effective_output = float(b["output_qty"]) * (1.0 - float(b["waste_pct"]))
        b["standard_cost_per_output_unit"] = money(grouped[b["batch_id"]] / effective_output)


def build_processed_items(batches):
    batch_id_by_name = {b["batch_name"]: b["batch_id"] for b in batches}
    cost_by_id = {b["batch_id"]: float(b["standard_cost_per_output_unit"]) for b in batches}
    defs = [
        ("Marinated Beef", "Beef Marination", "g", 0.95),
        ("Injected Chicken", "Tenderizer", "g", 0.96),
        ("Garlic Dip Portion Base", "Garlic Dip Batch", "g", 0.98),
        ("Tangy Dip Portion Base", "Tangy Dip Batch", "g", 0.98),
        ("Honey Mustard Portion Base", "Honey Mustard Batch", "g", 0.98),
        ("Chipotle Dip Portion Base", "Chipotle Dip Batch", "g", 0.98),
        ("Garlic Creamy Sauce Portion Base", "Garlic Creamy Sauce Batch", "g", 0.98),
        ("Minty Whirl Portion Base", "Minty Whirl Batch", "g", 0.98),
        ("Burger Glow Portion Base", "Burger Glow Batch", "g", 0.98),
        ("Mighty Mush Portion Base", "Mighty Mush Batch", "g", 0.98),
    ]
    rows = []
    for i, d in enumerate(defs, start=1):
        source_id = batch_id_by_name[d[1]]
        rows.append(
            {
                "processed_item_id": f"PI{i:03d}",
                "processed_item_name": d[0],
                "source_batch_id": source_id,
                "output_uom": d[2],
                "yield_factor": d[3],
                "standard_cost_per_unit": money(cost_by_id[source_id] / d[3]),
                "active_flag": 1,
            }
        )
    return rows


def build_skus():
    defs = [
        ("Broast Quarter", "Chicken Meals", "Broast", "chicken", 680, 700),
        ("Broast Half", "Chicken Meals", "Broast", "chicken", 1280, 1320),
        ("Broast Full", "Chicken Meals", "Broast", "chicken", 2460, 2520),
        ("Smokey Laham", "Burgers/Wraps", "Burger", "beef", 640, 670),
        ("Burger Al-Mushroom", "Burgers/Wraps", "Burger", "beef", 690, 720),
        ("Smash Beiruti", "Burgers/Wraps", "Burger", "beef", 620, 650),
        ("Chicken Batata", "Burgers/Wraps", "Burger", "chicken", 590, 620),
        ("Wrap Al Beiruti", "Burgers/Wraps", "Wrap", "chicken", 560, 590),
        ("Chicken Shots Burger", "Burgers/Wraps", "Burger", "chicken", 430, 450),
        ("Fries", "Sides", "Fries", "veg", 220, 240),
        ("Loaded Fries", "Sides", "Fries", "chicken", 390, 420),
        ("Garlic Dip", "Dips/Sauces", "Dip", "veg", 65, 70),
        ("Tangy Dip", "Dips/Sauces", "Dip", "veg", 65, 70),
        ("Honey Mustard Dip", "Dips/Sauces", "Dip", "veg", 70, 75),
        ("Mustard Sauce Dip", "Dips/Sauces", "Dip", "veg", 60, 65),
        ("Chipotle Dip", "Dips/Sauces", "Dip", "veg", 70, 75),
        ("Garlic Creamy Sauce", "Dips/Sauces", "Sauce", "veg", 75, 80),
        ("Minty Whirl", "Dips/Sauces", "Sauce", "veg", 75, 80),
        ("Burger Glow", "Dips/Sauces", "Sauce", "veg", 75, 80),
        ("Mighty Mush", "Dips/Sauces", "Sauce", "veg", 85, 90),
        ("Mojito", "Drinks", "Cold Beverage", "veg", 180, 195),
        ("Ice Tea", "Drinks", "Cold Beverage", "veg", 150, 165),
        ("Blueberry", "Drinks", "Cold Beverage", "veg", 210, 225),
        ("Mint Margarita", "Drinks", "Cold Beverage", "veg", 170, 185),
    ]
    rows = []
    for i, d in enumerate(defs, start=1):
        rows.append(
            {
                "sku_id": f"SKU{i:03d}",
                "sku_name": d[0],
                "category": d[1],
                "subcategory": d[2],
                "protein_type": d[3],
                "dine_in_price": d[4],
                "takeaway_price": d[5],
                "active_flag": 1,
                "launch_date": "2021-01-01",
                "discontinue_date": "",
            }
        )
    return rows


def build_sku_recipe_lines(skus, ingredients_by_name, batches_by_name, processed_by_name):
    p = lambda n: processed_by_name[n]["processed_item_id"]
    i = lambda n: ingredients_by_name[n]["ingredient_id"]
    b = lambda n: batches_by_name[n]["batch_id"]

    rules = {
        "Broast Quarter": [
            ("raw_ingredient", i("Chicken Whole"), 220, "g", "prep", 0),
            ("batch", b("Tenderizer"), 18, "g", "prep", 0),
            ("batch", b("Breading"), 26, "g", "coat", 0),
            ("raw_ingredient", i("Fries Frozen"), 120, "g", "fry", 0),
            ("packaging", i("Quarter Box"), 1, "pcs", "pack", 1),
            ("packaging", i("Food Bag"), 1, "pcs", "pack", 1),
        ],
        "Broast Half": [
            ("raw_ingredient", i("Chicken Whole"), 420, "g", "prep", 0),
            ("batch", b("Tenderizer"), 30, "g", "prep", 0),
            ("batch", b("Breading"), 45, "g", "coat", 0),
            ("raw_ingredient", i("Fries Frozen"), 220, "g", "fry", 0),
            ("packaging", i("F3 Box"), 1, "pcs", "pack", 1),
            ("packaging", i("Lid F3"), 1, "pcs", "pack", 1),
        ],
        "Broast Full": [
            ("raw_ingredient", i("Chicken Whole"), 820, "g", "prep", 0),
            ("batch", b("Tenderizer"), 58, "g", "prep", 0),
            ("batch", b("Breading"), 88, "g", "coat", 0),
            ("raw_ingredient", i("Fries Frozen"), 420, "g", "fry", 0),
            ("packaging", i("F3 Box"), 2, "pcs", "pack", 1),
            ("packaging", i("Lid F3"), 2, "pcs", "pack", 1),
            ("packaging", i("Shopping Bag Large"), 1, "pcs", "pack", 1),
        ],
        "Smokey Laham": [
            ("raw_ingredient", i("Potato Bun"), 1, "pcs", "assemble", 0),
            ("processed_item", p("Marinated Beef"), 140, "g", "grill", 0),
            ("batch", b("Burger Glow Batch"), 24, "g", "assemble", 0),
            ("raw_ingredient", i("Onion"), 25, "g", "assemble", 0),
            ("packaging", i("Burger Box"), 1, "pcs", "pack", 1),
        ],
        "Burger Al-Mushroom": [
            ("raw_ingredient", i("Bun"), 1, "pcs", "assemble", 0),
            ("processed_item", p("Marinated Beef"), 135, "g", "grill", 0),
            ("batch", b("Mighty Mush Batch"), 30, "g", "assemble", 0),
            ("raw_ingredient", i("Cheese Slice"), 1, "pcs", "assemble", 0),
            ("packaging", i("Burger Box"), 1, "pcs", "pack", 1),
        ],
        "Smash Beiruti": [
            ("raw_ingredient", i("Bun"), 1, "pcs", "assemble", 0),
            ("processed_item", p("Marinated Beef"), 120, "g", "grill", 0),
            ("batch", b("Chipotle Dip Batch"), 20, "g", "assemble", 0),
            ("raw_ingredient", i("Jalapeno"), 18, "g", "assemble", 0),
            ("packaging", i("Burger Box"), 1, "pcs", "pack", 1),
        ],
        "Chicken Batata": [
            ("raw_ingredient", i("Potato Bun"), 1, "pcs", "assemble", 0),
            ("processed_item", p("Injected Chicken"), 125, "g", "fry", 0),
            ("batch", b("Less Spicy"), 22, "g", "assemble", 0),
            ("raw_ingredient", i("Lettuce"), 20, "g", "assemble", 0),
            ("packaging", i("Burger Box"), 1, "pcs", "pack", 1),
        ],
        "Wrap Al Beiruti": [
            ("raw_ingredient", i("Tortilla Wrap"), 1, "pcs", "assemble", 0),
            ("processed_item", p("Injected Chicken"), 130, "g", "grill", 0),
            ("batch", b("Spicy"), 24, "g", "assemble", 0),
            ("raw_ingredient", i("Lettuce"), 22, "g", "assemble", 0),
            ("packaging", i("Foil"), 1, "pcs", "pack", 1),
        ],
        "Chicken Shots Burger": [
            ("raw_ingredient", i("Bun"), 1, "pcs", "assemble", 0),
            ("processed_item", p("Injected Chicken"), 90, "g", "fry", 0),
            ("batch", b("Garlic Creamy Sauce Batch"), 15, "g", "assemble", 0),
            ("packaging", i("Burger Box"), 1, "pcs", "pack", 1),
        ],
        "Fries": [
            ("raw_ingredient", i("Fries Frozen"), 120, "g", "fry", 0),
            ("raw_ingredient", i("Oil"), 30, "ml", "fry", 0),
            ("packaging", i("Fries Pouch"), 1, "pcs", "pack", 1),
        ],
        "Loaded Fries": [
            ("raw_ingredient", i("Fries Frozen"), 180, "g", "fry", 0),
            ("processed_item", p("Injected Chicken"), 60, "g", "top", 0),
            ("batch", b("Garlic Creamy Sauce Batch"), 24, "g", "top", 0),
            ("packaging", i("Fries Pouch"), 1, "pcs", "pack", 1),
        ],
        "Garlic Dip": [
            ("processed_item", p("Garlic Dip Portion Base"), 35, "g", "portion", 0),
            ("packaging", i("Dip Container"), 1, "pcs", "pack", 1),
            ("packaging", i("Tissue Paper"), 1, "pcs", "pack", 1),
        ],
        "Tangy Dip": [
            ("processed_item", p("Tangy Dip Portion Base"), 35, "g", "portion", 0),
            ("packaging", i("Dip Container"), 1, "pcs", "pack", 1),
            ("packaging", i("Tissue Paper"), 1, "pcs", "pack", 1),
        ],
        "Honey Mustard Dip": [
            ("processed_item", p("Honey Mustard Portion Base"), 35, "g", "portion", 0),
            ("packaging", i("Dip Container"), 1, "pcs", "pack", 1),
            ("packaging", i("Tissue Paper"), 1, "pcs", "pack", 1),
        ],
        "Mustard Sauce Dip": [
            ("batch", b("Honey Mustard Batch"), 28, "g", "portion", 0),
            ("raw_ingredient", i("Mustard Paste"), 8, "g", "portion", 0),
            ("packaging", i("Dip Container"), 1, "pcs", "pack", 1),
        ],
        "Chipotle Dip": [
            ("processed_item", p("Chipotle Dip Portion Base"), 35, "g", "portion", 0),
            ("packaging", i("Dip Container"), 1, "pcs", "pack", 1),
            ("packaging", i("Tissue Paper"), 1, "pcs", "pack", 1),
        ],
        "Garlic Creamy Sauce": [
            ("processed_item", p("Garlic Creamy Sauce Portion Base"), 30, "g", "portion", 0),
            ("packaging", i("Dip Container"), 1, "pcs", "pack", 1),
            ("packaging", i("Tissue Paper"), 1, "pcs", "pack", 1),
        ],
        "Minty Whirl": [
            ("processed_item", p("Minty Whirl Portion Base"), 30, "g", "portion", 0),
            ("packaging", i("Dip Container"), 1, "pcs", "pack", 1),
            ("packaging", i("Tissue Paper"), 1, "pcs", "pack", 1),
        ],
        "Burger Glow": [
            ("processed_item", p("Burger Glow Portion Base"), 30, "g", "portion", 0),
            ("packaging", i("Dip Container"), 1, "pcs", "pack", 1),
            ("packaging", i("Tissue Paper"), 1, "pcs", "pack", 1),
        ],
        "Mighty Mush": [
            ("processed_item", p("Mighty Mush Portion Base"), 30, "g", "portion", 0),
            ("packaging", i("Dip Container"), 1, "pcs", "pack", 1),
            ("packaging", i("Tissue Paper"), 1, "pcs", "pack", 1),
        ],
        "Mojito": [
            ("raw_ingredient", i("Sprite"), 240, "ml", "mix", 0),
            ("raw_ingredient", i("Mint"), 12, "g", "mix", 0),
            ("raw_ingredient", i("Lemon"), 22, "g", "mix", 0),
            ("raw_ingredient", i("Sugar"), 14, "g", "mix", 0),
            ("packaging", i("Glass"), 1, "pcs", "serve", 0),
            ("packaging", i("Straw"), 1, "pcs", "serve", 0),
        ],
        "Ice Tea": [
            ("raw_ingredient", i("Tea Bag"), 1, "pcs", "brew", 0),
            ("raw_ingredient", i("Sugar"), 18, "g", "mix", 0),
            ("raw_ingredient", i("Lemon"), 16, "g", "mix", 0),
            ("packaging", i("Glass"), 1, "pcs", "serve", 0),
            ("packaging", i("Straw"), 1, "pcs", "serve", 0),
        ],
        "Blueberry": [
            ("raw_ingredient", i("Blueberry Syrup"), 40, "ml", "mix", 0),
            ("raw_ingredient", i("Sprite"), 220, "ml", "mix", 0),
            ("packaging", i("Glass"), 1, "pcs", "serve", 0),
            ("packaging", i("Straw"), 1, "pcs", "serve", 0),
        ],
        "Mint Margarita": [
            ("raw_ingredient", i("Mint"), 14, "g", "mix", 0),
            ("raw_ingredient", i("Lemon"), 24, "g", "mix", 0),
            ("raw_ingredient", i("Sugar"), 18, "g", "mix", 0),
            ("raw_ingredient", i("Sprite"), 200, "ml", "mix", 0),
            ("packaging", i("Glass"), 1, "pcs", "serve", 0),
            ("packaging", i("Straw"), 1, "pcs", "serve", 0),
        ],
    }

    sku_id_by_name = {s["sku_name"]: s["sku_id"] for s in skus}
    rows = []
    idx = 1
    for sku_name, lines in rules.items():
        for line in lines:
            rows.append(
                {
                    "sku_recipe_line_id": f"SRL{idx:05d}",
                    "sku_id": sku_id_by_name[sku_name],
                    "component_type": line[0],
                    "component_id": line[1],
                    "quantity_required": line[2],
                    "quantity_uom": line[3],
                    "stage": line[4],
                    "channel_specific_flag": line[5],
                }
            )
            idx += 1
    return rows


def build_employees(branches):
    names = [
        "Ahmed Raza", "Muhammad Ali", "Usman Tariq", "Bilal Ahmed", "Hamza Khan", "Ali Haider", "Saad Qureshi", "Fahad Malik",
        "Ahsan Javed", "Shahid Iqbal", "Imran Bashir", "Noman Aslam", "Taha Siddiqui", "Faraz Hussain", "Waqar Abbas", "Sohail Akram",
        "Kashif Nawaz", "Yasir Mahmood", "Danish Arif", "Talha Noor", "Ayesha Malik", "Sana Jabeen", "Iqra Khan", "Hina Rauf",
        "Maham Asif", "Nida Faisal", "Rabia Tahir", "Komal Shah", "Zainab Yousuf", "Amna Latif", "Kiran Sheikh", "Anum Waheed",
    ]
    roles = [
        ("Operations", "manager", 180000),
        ("Front Counter", "cashier", 55000),
        ("Kitchen", "kitchen staff", 60000),
        ("Operations", "shift lead", 85000),
        ("Kitchen", "helper", 42000),
        ("Housekeeping", "cleaner", 38000),
        ("Delivery", "rider", 50000),
    ]
    emp_rows = []
    idx = 1
    start_date = date(2019, 1, 1)

    for branch in branches:
        n = random.randint(11, 18)
        for j in range(n):
            nm = names[(idx + j) % len(names)]
            dept, role, base_salary = roles[j % len(roles)]
            join_dt = start_date + timedelta(days=random.randint(0, 2400))
            emp_rows.append(
                {
                    "employee_id": f"E{idx:04d}",
                    "employee_name": nm,
                    "branch_id": branch["branch_id"],
                    "department": dept,
                    "role": role,
                    "joining_date": join_dt.isoformat(),
                    "employment_type": "Full-Time" if j % 5 != 0 else "Contract",
                    "salary_monthly": int(base_salary + (branch["kitchen_capacity_score"] * 35) + random.randint(-5000, 7000)),
                    "shift_type": ["Morning", "Evening", "Rotational"][j % 3],
                    "status": "Active" if j % 12 != 0 else "On Leave",
                }
            )
            idx += 1
    return emp_rows


def validate_all(data):
    warnings = []
    summary = []

    # 1. Duplicate IDs
    id_fields = {
        "branches": (data["branches"], "branch_id"),
        "suppliers": (data["suppliers"], "supplier_id"),
        "ingredients": (data["ingredients"], "ingredient_id"),
        "batches": (data["batches"], "batch_id"),
        "batch_recipe_lines": (data["batch_recipe_lines"], "batch_recipe_line_id"),
        "processed_items": (data["processed_items"], "processed_item_id"),
        "skus": (data["skus"], "sku_id"),
        "sku_recipe_lines": (data["sku_recipe_lines"], "sku_recipe_line_id"),
        "employees": (data["employees"], "employee_id"),
    }
    for table, (rows, field) in id_fields.items():
        ids = [r[field] for r in rows]
        unique = len(set(ids)) == len(ids)
        summary.append((f"No duplicate IDs in {table}", unique))

    # 2. FK checks
    supplier_ids = {r["supplier_id"] for r in data["suppliers"]}
    ingredient_ids = {r["ingredient_id"] for r in data["ingredients"]}
    batch_ids = {r["batch_id"] for r in data["batches"]}
    processed_ids = {r["processed_item_id"] for r in data["processed_items"]}
    sku_ids = {r["sku_id"] for r in data["skus"]}
    branch_ids = {r["branch_id"] for r in data["branches"]}
    packaging_ids = {r["ingredient_id"] for r in data["ingredients"] if int(r["is_packaging"]) == 1}

    fk_ok = True
    fk_ok = fk_ok and all(r["default_supplier_id"] in supplier_ids for r in data["ingredients"])
    fk_ok = fk_ok and all(r["batch_id"] in batch_ids and r["ingredient_id"] in ingredient_ids for r in data["batch_recipe_lines"])
    fk_ok = fk_ok and all(r["source_batch_id"] in batch_ids for r in data["processed_items"])
    fk_ok = fk_ok and all(r["sku_id"] in sku_ids for r in data["sku_recipe_lines"])
    fk_ok = fk_ok and all(r["branch_id"] in branch_ids for r in data["employees"])

    for r in data["sku_recipe_lines"]:
        ctype = r["component_type"]
        cid = r["component_id"]
        if ctype == "raw_ingredient" and cid not in ingredient_ids:
            fk_ok = False
        elif ctype == "batch" and cid not in batch_ids:
            fk_ok = False
        elif ctype == "processed_item" and cid not in processed_ids:
            fk_ok = False
        elif ctype == "packaging" and cid not in packaging_ids:
            fk_ok = False
    summary.append(("No orphan foreign keys", fk_ok))

    # 3. Every SKU has at least 2 lines
    srl_cnt = defaultdict(int)
    for r in data["sku_recipe_lines"]:
        srl_cnt[r["sku_id"]] += 1
    sku_rule = all(srl_cnt[sku] >= 2 for sku in sku_ids)
    summary.append(("Every SKU has at least 2 recipe lines", sku_rule))

    # 4. Every batch has at least 3 lines
    brl_cnt = defaultdict(int)
    for r in data["batch_recipe_lines"]:
        brl_cnt[r["batch_id"]] += 1
    batch_rule = all(brl_cnt[bid] >= 3 for bid in batch_ids)
    summary.append(("Every batch has at least 3 recipe lines", batch_rule))

    # 5. Every branch has employees
    emp_cnt = defaultdict(int)
    for r in data["employees"]:
        emp_cnt[r["branch_id"]] += 1
    branch_rule = all(emp_cnt[bid] > 0 for bid in branch_ids)
    summary.append(("Every branch has employees", branch_rule))

    # 6. Non-negative costs/prices
    non_neg = True
    for r in data["ingredients"]:
        non_neg = non_neg and float(r["standard_unit_cost"]) >= 0
    for r in data["batches"]:
        non_neg = non_neg and float(r["standard_cost_per_output_unit"]) >= 0
    for r in data["processed_items"]:
        non_neg = non_neg and float(r["standard_cost_per_unit"]) >= 0
    for r in data["batch_recipe_lines"]:
        non_neg = non_neg and float(r["line_cost"]) >= 0
    for r in data["skus"]:
        non_neg = non_neg and float(r["dine_in_price"]) >= 0 and float(r["takeaway_price"]) >= 0
    summary.append(("All prices and costs are non-negative", non_neg))

    # 7. required files written successfully (checked by row counts > 0 except exactly-5 table)
    required_ok = all(len(data[k]) > 0 for k in ["suppliers", "ingredients", "batches", "batch_recipe_lines", "processed_items", "skus", "sku_recipe_lines", "employees"]) and len(data["branches"]) == 5
    summary.append(("All required CSVs are populated", required_ok))

    # Additional warnings
    if not all(r["status"] == "Active" for r in data["branches"]):
        warnings.append("One or more branches are not Active.")
    if min(int(r["seating_capacity"]) for r in data["branches"]) < 60:
        warnings.append("A branch has very low seating capacity.")

    overall = all(ok for _, ok in summary)
    return overall, summary, warnings


def main():
    branches = build_branches()
    suppliers = build_suppliers()
    ingredients = build_ingredients()

    ingredients_by_name = {r["ingredient_name"]: r for r in ingredients}

    batches = build_batches()
    batch_recipe_lines = build_batch_recipe_lines(ingredients_by_name, batches)
    assign_batch_costs(batches, batch_recipe_lines)

    processed_items = build_processed_items(batches)
    skus = build_skus()

    batches_by_name = {r["batch_name"]: r for r in batches}
    processed_by_name = {r["processed_item_name"]: r for r in processed_items}

    sku_recipe_lines = build_sku_recipe_lines(skus, ingredients_by_name, batches_by_name, processed_by_name)
    employees = build_employees(branches)

    table_map = {
        "branches": branches,
        "suppliers": suppliers,
        "ingredients": ingredients,
        "batches": batches,
        "batch_recipe_lines": batch_recipe_lines,
        "processed_items": processed_items,
        "skus": skus,
        "sku_recipe_lines": sku_recipe_lines,
        "employees": employees,
    }

    write_csv("branches_master.csv", [
        "branch_id", "branch_name", "city", "region", "opening_date", "branch_type", "size_band",
        "seating_capacity", "kitchen_capacity_score", "status"
    ], branches)
    write_csv("suppliers_master.csv", [
        "supplier_id", "supplier_name", "supplier_category", "city", "contact_person", "lead_time_days",
        "payment_terms_days", "reliability_score", "active_flag"
    ], suppliers)
    write_csv("ingredients_master.csv", [
        "ingredient_id", "ingredient_name", "ingredient_group", "ingredient_category", "base_uom", "buying_uom",
        "conversion_to_base", "standard_unit_cost", "is_packaging", "is_perishable", "shelf_life_days",
        "default_supplier_id", "active_flag"
    ], ingredients)
    write_csv("batches_master.csv", [
        "batch_id", "batch_name", "batch_group", "output_qty", "output_uom", "waste_pct",
        "standard_cost_per_output_unit", "variant_name", "active_flag"
    ], batches)
    write_csv("batch_recipe_lines.csv", [
        "batch_recipe_line_id", "batch_id", "ingredient_id", "quantity_required", "quantity_uom",
        "standard_unit_cost", "line_cost"
    ], batch_recipe_lines)
    write_csv("processed_items_master.csv", [
        "processed_item_id", "processed_item_name", "source_batch_id", "output_uom", "yield_factor",
        "standard_cost_per_unit", "active_flag"
    ], processed_items)
    write_csv("sku_master.csv", [
        "sku_id", "sku_name", "category", "subcategory", "protein_type", "dine_in_price", "takeaway_price",
        "active_flag", "launch_date", "discontinue_date"
    ], skus)
    write_csv("sku_recipe_lines.csv", [
        "sku_recipe_line_id", "sku_id", "component_type", "component_id", "quantity_required", "quantity_uom",
        "stage", "channel_specific_flag"
    ], sku_recipe_lines)
    write_csv("employees_master.csv", [
        "employee_id", "employee_name", "branch_id", "department", "role", "joining_date", "employment_type",
        "salary_monthly", "shift_type", "status"
    ], employees)

    ok, checks, warnings = validate_all(table_map)

    print("Execution Status:", "SUCCESS" if ok else "FAILED")
    print("Generated Files:")
    for f in OUTPUT_FILES:
        print("-", f)

    print("Row Counts:")
    print("- branches_master.csv:", len(branches))
    print("- suppliers_master.csv:", len(suppliers))
    print("- ingredients_master.csv:", len(ingredients))
    print("- batches_master.csv:", len(batches))
    print("- batch_recipe_lines.csv:", len(batch_recipe_lines))
    print("- processed_items_master.csv:", len(processed_items))
    print("- sku_master.csv:", len(skus))
    print("- sku_recipe_lines.csv:", len(sku_recipe_lines))
    print("- employees_master.csv:", len(employees))

    print("Assumptions Made:")
    assumptions = [
        "All generated master files are written to the repository root for easy downstream ingestion.",
        "Standard unit costs are maintained at base_uom level (g/ml/pcs) to keep BOM math consistent.",
        "'Mustard Sauce Dip' is modeled as a distinct SKU using Honey Mustard + mustard paste blend.",
        "Takeaway pricing includes mild packaging uplift; dine-in is priced slightly lower for most SKUs.",
        "Employee statuses are mostly Active with a small On Leave proportion for realism.",
    ]
    for a in assumptions:
        print("-", a)

    print("Validation Summary:")
    for name, passed in checks:
        print(f"- {name}: {'PASS' if passed else 'FAIL'}")

    print("Warnings:")
    if warnings:
        for w in warnings:
            print("-", w)
    else:
        print("- None")


if __name__ == "__main__":
    main()
