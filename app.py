import streamlit as st
import sqlite_utils
from datetime import datetime
from pathlib import Path
import pandas as pd

# Database file path
DB_PATH = "tracker.db"

# Constants
MI_PER_GALLON = 3785.41
MI_PER_CASE = 4800.00
GALLONS_PER_CASE = 1.27
UNITS_PER_CASE = 24

def calculate_total_cases_from_stock(singles, bottled_s, bottled_i):
    """Return total cases based on singles and 24-pack bottled counts."""
    return (singles + bottled_s * UNITS_PER_CASE + bottled_i * UNITS_PER_CASE) / UNITS_PER_CASE

def derive_finished_good_metrics(singles, bottled_s, bottled_i, abv):
    """Compute current_stock, proof_gallons, and excise_tax_due for a finished good."""
    total_cases = calculate_total_cases_from_stock(singles, bottled_s, bottled_i)
    proof_gallons = 0.0
    excise_tax_due = 0.0
    if abv > 0:
        total_gallons = total_cases * GALLONS_PER_CASE
        proof = abv * 2
        proof_gallons = calculate_proof_gallons(total_gallons, proof)
        excise_tax_due = calculate_excise_tax(proof_gallons)
    return {
        "current_stock": int(total_cases),
        "proof_gallons": proof_gallons,
        "excise_tax_due": excise_tax_due
    }

def update_finished_good(db, fg_id, singles, bottled_s, bottled_i, abv, extra_updates=None):
    """
    Update a finished good with provided stock numbers plus derived metrics.
    extra_updates can include fields like sold that should be merged.
    """
    metrics = derive_finished_good_metrics(singles, bottled_s, bottled_i, abv)
    payload = {
        "singles": singles,
        "bottled_s": bottled_s,
        "bottled_i": bottled_i,
        **metrics
    }
    if extra_updates:
        payload.update(extra_updates)
    db["finished_goods"].update(fg_id, payload)
    return metrics["current_stock"]

def calculate_proof_gallons(wine_gallons, proof):
    """Calculate proof gallons from wine gallons and proof"""
    return wine_gallons * (proof / 100.0)

def calculate_excise_tax(proof_gallons, tax_rate=13.50):
    """Calculate excise tax (default rate $13.50 per proof gallon)"""
    return proof_gallons * tax_rate

def calculate_density_from_abv(abv):
    """
    Calculate density (lbs/gallon) from ABV using mixture properties
    ABV is percentage (e.g., 40.0 for 40%)
    
    Formula based on ethanol-water mixture properties:
    - Pure ethanol: 0.789 g/mL
    - Pure water: 1.0 g/mL
    - Convert to lbs/gallon by multiplying by 8.345
    """
    abv_fraction = abv / 100.0
    # Density in g/mL
    density_g_ml = (abv_fraction * 0.789) + ((1 - abv_fraction) * 1.0)
    # Convert to lbs/gallon
    density_lbs_gal = density_g_ml * 8.345
    return density_lbs_gal

def calculate_gallons_from_weight(weight_lbs, abv):
    """
    Calculate wine gallons from weight (lbs) and ABV
    """
    if weight_lbs == 0:
        return 0.0
    density = calculate_density_from_abv(abv)
    wine_gallons = weight_lbs / density
    return wine_gallons

def calculate_weight_from_gallons(wine_gallons, abv):
    """
    Calculate weight (lbs) from wine gallons and ABV
    """
    if wine_gallons == 0:
        return 0.0
    density = calculate_density_from_abv(abv)
    weight_lbs = wine_gallons * density
    return weight_lbs

def init_database():
    """Initialize database and create tables if they don't exist"""
    db = sqlite_utils.Database(DB_PATH)
    
    # Create raw_materials table (enhanced)
    if "raw_materials" not in db.table_names():
        db["raw_materials"].create({
            "id": int,
            "name": str,
            "current_stock": int,
            "reorder_point": int,
            "wastage_factor": float,
            "units_per_case": int,  # e.g., 1000/carton, 2320/case
            "started": int,  # Initial quantity
            "depleted": int,  # Can be negative (additions)
            "added": int,  # Additional stock
            "units_remaining": int,  # Calculated: started - depleted + added
            "cases_remaining": int  # Calculated based on units_per_case
        }, pk="id")
    
    # Create finished_goods table (enhanced)
    if "finished_goods" not in db.table_names():
        db["finished_goods"].create({
            "id": int,
            "name": str,
            "current_stock": int,  # Total stock (for backward compatibility)
            "case_size": int,
            "singles": int,  # Individual units not yet packaged
            "bottled_s": int,  # Shipping boxes
            "bottled_i": int,  # In-store boxes
            "sold": int,  # Quantity sold
            "samples": int,  # Quantity pulled for samples
            "abv": float,  # Alcohol by Volume percentage
            "proof_gallons": float,  # Calculated field
            "excise_tax_due": float  # Calculated field
        }, pk="id")
    
    # Create bulk_spirits table (for raw bulk spirit in storage)
    if "bulk_spirits" not in db.table_names():
        db["bulk_spirits"].create({
            "id": int,
            "name": str,
            "proof": int,  # Proof value (e.g., 80, 100, 121)
            "abv": float,  # Alcohol by Volume (proof / 2)
            "wine_gallons": float,  # Current stock in wine gallons
            "proof_gallons": float  # Calculated: wine_gallons * (proof / 100)
        }, pk="id")
    
    # Create batches table (for mixed/blended bulk batches)
    if "batches" not in db.table_names():
        db["batches"].create({
            "id": int,
            "name": str,
            "gallons": float,  # Current gallons in processing
            "bottled_gallons": float,  # Gallons that have been bottled
            "ending": float,  # Ending stock
            "abv": float,  # Alcohol by Volume percentage
            "proof_gallons": float  # Calculated field
        }, pk="id")
    
    # Create inventory_tracking table (for non-bonded inventory and depletions)
    if "inventory_tracking" not in db.table_names():
        db["inventory_tracking"].create({
            "id": int,
            "item_name": str,
            "units_per_case": int,  # Extracted from name like "1000/carton"
            "started": int,
            "depleted": int,  # Can be negative
            "added": int,
            "units_remaining": int,  # Calculated
            "cases_remaining": int  # Calculated
        }, pk="id")
    
    # Create orders table
    if "orders" not in db.table_names():
        db["orders"].create({
            "id": int,
            "customer_name": str,
            "order_date": str,
            "status": str
        }, pk="id")
    
    # Create order_items table (multiple items per order)
    if "order_items" not in db.table_names():
        db["order_items"].create({
            "id": int,
            "order_id": int,
            "product_name": str,
            "quantity_cases": int
        }, pk="id", foreign_keys=[
            ("order_id", "orders", "id")
        ])
    
    # Create recipes table (junction table)
    if "recipes" not in db.table_names():
        db["recipes"].create({
            "finished_good_id": int,
            "raw_material_id": int,
            "qty_per_case": int
        }, foreign_keys=[
            ("finished_good_id", "finished_goods", "id"),
            ("raw_material_id", "raw_materials", "id")
        ])
    
    # Create production_recipes table (links finished goods to inventory items)
    if "production_recipes" not in db.table_names():
        db["production_recipes"].create({
            "id": int,
            "finished_good_id": int,
            "inventory_item_id": int,
            "qty_per_case": float,  # How many units of inventory item per case of finished good
            "wastage_factor": float  # Optional wastage factor (default 0.0 = 0%)
        }, pk="id", foreign_keys=[
            ("finished_good_id", "finished_goods", "id"),
            ("inventory_item_id", "inventory_tracking", "id")
        ])
    
    # Create production_history table (track all production events)
    if "production_history" not in db.table_names():
        db["production_history"].create({
            "id": int,
            "production_date": str,
            "finished_good_id": int,
            "finished_good_name": str,
            "cases_produced": int,
            "packaging_type": str,
            "units_produced": int,
            "proof_gallons_produced": float,
            "excise_tax_incurred": float,
            "batch_name": str,
            "notes": str
        }, pk="id", foreign_keys=[
            ("finished_good_id", "finished_goods", "id")
        ])
    
    # Create physical_inventory_counts table (physical vs system counts for finished goods)
    if "physical_inventory_counts" not in db.table_names():
        db["physical_inventory_counts"].create({
            "id": int,
            "count_date": str,
            "finished_good_id": int,
            "finished_good_name": str,
            "system_singles": int,
            "actual_singles": int,
            "system_bottled_s": int,
            "actual_bottled_s": int,
            "system_bottled_i": int,
            "actual_bottled_i": int,
            "variance_units": int,
            "variance_cases": float,
            "variance_percentage": float,
            "notes": str
        }, pk="id", foreign_keys=[
            ("finished_good_id", "finished_goods", "id")
        ])
    
    # Create inventory_physical_counts_raw table (physical counts for raw materials/inventory)
    if "inventory_physical_counts_raw" not in db.table_names():
        db["inventory_physical_counts_raw"].create({
            "id": int,
            "count_date": str,
            "inventory_item_id": int,
            "inventory_item_name": str,
            "system_units": int,
            "actual_units": int,
            "variance_units": int,
            "variance_percentage": float,
            "notes": str
        }, pk="id", foreign_keys=[
            ("inventory_item_id", "inventory_tracking", "id")
        ])
    
    # Create monthly_snapshots table (automated month-end inventory saves)
    if "monthly_snapshots" not in db.table_names():
        db["monthly_snapshots"].create({
            "id": int,
            "snapshot_date": str,
            "snapshot_month": str,
            "finished_goods_json": str,
            "bulk_spirits_json": str,
            "inventory_json": str,
            "total_proof_gallons": float,
            "total_excise_tax_liability": float,
            "total_finished_cases": int
        }, pk="id")
    
    # Create batch_recipes table (weight-based recipes for batches)
    if "batch_recipes" not in db.table_names():
        db["batch_recipes"].create({
            "id": int,
            "batch_id": int,
            "bulk_spirit_id": int,
            "ingredient_name": str,
            "weight_lbs": float,
            "percentage": float,
            "notes": str
        }, pk="id", foreign_keys=[
            ("batch_id", "batches", "id"),
            ("bulk_spirit_id", "bulk_spirits", "id")
        ])
    
    # Create bulk_spirit_receipts table (track each receipt with ABV variations)
    if "bulk_spirit_receipts" not in db.table_names():
        db["bulk_spirit_receipts"].create({
            "id": int,
            "bulk_spirit_id": int,
            "receipt_date": str,
            "weight_lbs": float,
            "wine_gallons": float,
            "abv": float,
            "proof": int,
            "supplier": str,
            "batch_number": str,
            "notes": str
        }, pk="id", foreign_keys=[
            ("bulk_spirit_id", "bulk_spirits", "id")
        ])
    
    # Create batch_production_log table (track batch production events)
    if "batch_production_log" not in db.table_names():
        db["batch_production_log"].create({
            "id": int,
            "production_date": str,
            "batch_id": int,
            "batch_name": str,
            "weight_produced_lbs": float,
            "gallons_produced": float,
            "abv": float,
            "proof_gallons": float,
            "notes": str
        }, pk="id", foreign_keys=[
            ("batch_id", "batches", "id")
        ])
    
    return db

def remove_duplicates(db):
    """Remove duplicate entries from all tables"""
    # Remove duplicates from inventory_tracking
    if "inventory_tracking" in db.table_names():
        seen_names = set()
        items_to_delete = []
        for item in db["inventory_tracking"].rows:
            if item["item_name"] in seen_names:
                items_to_delete.append(item["id"])
            else:
                seen_names.add(item["item_name"])
        for item_id in items_to_delete:
            db["inventory_tracking"].delete(item_id)
    
    # Remove duplicates from finished_goods
    if "finished_goods" in db.table_names():
        seen_names = set()
        items_to_delete = []
        for item in db["finished_goods"].rows:
            if item["name"] in seen_names:
                items_to_delete.append(item["id"])
            else:
                seen_names.add(item["name"])
        for item_id in items_to_delete:
            db["finished_goods"].delete(item_id)
    
    # Remove duplicates from bulk_spirits
    if "bulk_spirits" in db.table_names():
        seen_names = set()
        items_to_delete = []
        for item in db["bulk_spirits"].rows:
            if item["name"] in seen_names:
                items_to_delete.append(item["id"])
            else:
                seen_names.add(item["name"])
        for item_id in items_to_delete:
            db["bulk_spirits"].delete(item_id)
    
    # Remove duplicates from batches
    if "batches" in db.table_names():
        seen_names = set()
        items_to_delete = []
        for item in db["batches"].rows:
            if item["name"] in seen_names:
                items_to_delete.append(item["id"])
            else:
                seen_names.add(item["name"])
        for item_id in items_to_delete:
            db["batches"].delete(item_id)

def seed_database(db):
    """Seed database with actual inventory data from Google Sheets"""
    # First, remove any duplicates
    remove_duplicates(db)
    
    # Check if we already have the correct inventory items
    existing_items = set()
    if db["inventory_tracking"].count > 0:
        for item in db["inventory_tracking"].rows:
            existing_items.add(item["item_name"])
    
    # Expected inventory items (24 items from spreadsheet)
    expected_items = {
        "Shrink Bands - 75mm x 28mm (1000/carton)",
        "Shrink Film Bags (250/carton) 100 gauge, 16 x 20\" (Singles)",
        "Shrink Film Bags (300/carton) 100 gauge, 18 x 24\" (4packs)",
        "Custom Gold Lids (Fit PET) (2320/case)",
        "Case Tray (4packs) (90/Case)",
        "Case Tray (Singles) (92/Case)",
        "4pack Espresso (87/case)",
        "4pack Daiquiri (87/case)",
        "4pack Gimlet (87/case)",
        "4pack Typhoon (87/case)",
        "4pack Manhattan (87/case)",
        "4pack 1887 (87/case)",
        "4pack Mixed (Gim/Daiq) (90/case)",
        "Custom PET bottles (200ml-352/case)",
        "Typhoon Labels (PET-1700/wh)",
        "Typhoon Labels (GLASS-1700/wh)",
        "Espresso Labels (PET-1700/wh)",
        "Espresso Labels (GLASS-1700/wh)",
        "Gimlet Labels (PET-1700/wh)",
        "Gimlet Labels (GLASS-1700/wh)",
        "Manhattan Labels (PET-1700/wh)",
        "Manhattan Labels (GLASS-1700/wh)",
        "Daiquiri Labels (PET-1270/wh)",
        "Daiquiri Labels (GLASS-1270/wh)"
    }
    
    # If we already have all expected items, skip seeding
    if existing_items.issuperset(expected_items):
        return
    
    # Insert inventory tracking items (Non-bonded inventory and depletions)
    # All 24 items from the Starting inventory spreadsheet
    inventory_items = [
        {
            "item_name": "Shrink Bands - 75mm x 28mm (1000/carton)",
            "units_per_case": 1000,
            "started": 11576,
            "depleted": 0,
            "added": 0,
            "units_remaining": 11576,
            "cases_remaining": 482
        },
        {
            "item_name": "Shrink Film Bags (250/carton) 100 gauge, 16 x 20\" (Singles)",
            "units_per_case": 250,
            "started": 561,
            "depleted": 0,
            "added": 0,
            "units_remaining": 561,
            "cases_remaining": 561
        },
        {
            "item_name": "Shrink Film Bags (300/carton) 100 gauge, 18 x 24\" (4packs)",
            "units_per_case": 300,
            "started": 605,
            "depleted": 0,
            "added": 0,
            "units_remaining": 605,
            "cases_remaining": 605
        },
        {
            "item_name": "Custom Gold Lids (Fit PET) (2320/case)",
            "units_per_case": 2320,
            "started": 38808,
            "depleted": -480,  # Negative means addition
            "added": 0,
            "units_remaining": 39288,
            "cases_remaining": 1637
        },
        {
            "item_name": "Case Tray (4packs) (90/Case)",
            "units_per_case": 90,
            "started": 611,
            "depleted": 0,
            "added": 0,
            "units_remaining": 611,
            "cases_remaining": 611
        },
        {
            "item_name": "Case Tray (Singles) (92/Case)",
            "units_per_case": 92,
            "started": 414,
            "depleted": 0,
            "added": 0,
            "units_remaining": 414,
            "cases_remaining": 414
        },
        {
            "item_name": "4pack Espresso (87/case)",
            "units_per_case": 87,
            "started": 723,
            "depleted": 0,
            "added": 0,
            "units_remaining": 723,
            "cases_remaining": 121
        },
        {
            "item_name": "4pack Daiquiri (87/case)",
            "units_per_case": 87,
            "started": 809,
            "depleted": 0,
            "added": 0,
            "units_remaining": 809,
            "cases_remaining": 135
        },
        {
            "item_name": "4pack Gimlet (87/case)",
            "units_per_case": 87,
            "started": 739,
            "depleted": 0,
            "added": 0,
            "units_remaining": 739,
            "cases_remaining": 123
        },
        {
            "item_name": "4pack Typhoon (87/case)",
            "units_per_case": 87,
            "started": 970,
            "depleted": 0,
            "added": 0,
            "units_remaining": 970,
            "cases_remaining": 162
        },
        {
            "item_name": "4pack Manhattan (87/case)",
            "units_per_case": 87,
            "started": 995,
            "depleted": 0,
            "added": 0,
            "units_remaining": 995,
            "cases_remaining": 166
        },
        {
            "item_name": "4pack 1887 (87/case)",
            "units_per_case": 87,
            "started": 1044,
            "depleted": 0,
            "added": 0,
            "units_remaining": 1044,
            "cases_remaining": 174
        },
        {
            "item_name": "4pack Mixed (Gim/Daiq) (90/case)",
            "units_per_case": 90,
            "started": 1080,
            "depleted": 0,
            "added": 0,
            "units_remaining": 1080,
            "cases_remaining": 180
        },
        {
            "item_name": "Custom PET bottles (200ml-352/case)",
            "units_per_case": 352,
            "started": 32304,
            "depleted": -480,  # Negative means addition
            "added": 0,
            "units_remaining": 32784,
            "cases_remaining": 1366
        },
        {
            "item_name": "Typhoon Labels (PET-1700/wh)",
            "units_per_case": 1700,
            "started": 11180,
            "depleted": 0,
            "added": 0,
            "units_remaining": 11180,
            "cases_remaining": 466
        },
        {
            "item_name": "Typhoon Labels (GLASS-1700/wh)",
            "units_per_case": 1700,
            "started": 10000,
            "depleted": 0,
            "added": 0,
            "units_remaining": 10000,
            "cases_remaining": 417
        },
        {
            "item_name": "Espresso Labels (PET-1700/wh)",
            "units_per_case": 1700,
            "started": 8488,
            "depleted": 0,
            "added": 0,
            "units_remaining": 8488,
            "cases_remaining": 354
        },
        {
            "item_name": "Espresso Labels (GLASS-1700/wh)",
            "units_per_case": 1700,
            "started": 10000,
            "depleted": 0,
            "added": 0,
            "units_remaining": 10000,
            "cases_remaining": 417
        },
        {
            "item_name": "Gimlet Labels (PET-1700/wh)",
            "units_per_case": 1700,
            "started": 11608,
            "depleted": -480,  # Negative means addition
            "added": 0,
            "units_remaining": 12088,
            "cases_remaining": 504
        },
        {
            "item_name": "Gimlet Labels (GLASS-1700/wh)",
            "units_per_case": 1700,
            "started": 10000,
            "depleted": 480,
            "added": 0,
            "units_remaining": 9520,
            "cases_remaining": 397
        },
        {
            "item_name": "Manhattan Labels (PET-1700/wh)",
            "units_per_case": 1700,
            "started": 10200,
            "depleted": 0,
            "added": 0,
            "units_remaining": 10200,
            "cases_remaining": 425
        },
        {
            "item_name": "Manhattan Labels (GLASS-1700/wh)",
            "units_per_case": 1700,
            "started": 10000,
            "depleted": 0,
            "added": 0,
            "units_remaining": 10000,
            "cases_remaining": 417
        },
        {
            "item_name": "Daiquiri Labels (PET-1270/wh)",
            "units_per_case": 1270,
            "started": 6350,
            "depleted": 0,
            "added": 0,
            "units_remaining": 6350,
            "cases_remaining": 265
        },
        {
            "item_name": "Daiquiri Labels (GLASS-1270/wh)",
            "units_per_case": 1270,
            "started": 10000,
            "depleted": 0,
            "added": 0,
            "units_remaining": 10000,
            "cases_remaining": 417
        }
    ]
    
    # Only insert items that don't already exist
    for item in inventory_items:
        if item["item_name"] not in existing_items:
            db["inventory_tracking"].insert(item)
    
    # Insert finished goods (from Finished Goods tracking sheet)
    # Each cocktail has both GLASS and PET versions
    finished_goods_data = [
        # Typhoon versions
        {
            "name": "Typhoon (GLASS)",
            "current_stock": 0,  # Will be calculated
            "case_size": 24,
            "singles": 164,
            "bottled_s": 6,
            "bottled_i": 13,
            "sold": 0,
            "samples": 0,
            "abv": 17.01,
            "proof_gallons": 0.0,  # Will be calculated
            "excise_tax_due": 0.0  # Will be calculated
        },
        {
            "name": "Typhoon (PET)",
            "current_stock": 0,
            "case_size": 24,
            "singles": 164,
            "bottled_s": 5,
            "bottled_i": 12,
            "sold": 0,
            "samples": 0,
            "abv": 17.01,
            "proof_gallons": 0.0,
            "excise_tax_due": 0.0
        },
        # Gimlet versions
        {
            "name": "Gimlet (GLASS)",
            "current_stock": 0,
            "case_size": 24,
            "singles": 96,
            "bottled_s": 10,
            "bottled_i": 10,
            "sold": 0,
            "samples": 0,
            "abv": 16.13,
            "proof_gallons": 0.0,
            "excise_tax_due": 0.0
        },
        {
            "name": "Gimlet (PET)",
            "current_stock": 0,
            "case_size": 24,
            "singles": 95,
            "bottled_s": 10,
            "bottled_i": 10,
            "sold": 0,
            "samples": 0,
            "abv": 16.13,
            "proof_gallons": 0.0,
            "excise_tax_due": 0.0
        },
        # Daiquiri versions
        {
            "name": "Daiquiri (GLASS)",
            "current_stock": 0,
            "case_size": 24,
            "singles": 12,
            "bottled_s": 5,
            "bottled_i": 8,
            "sold": 0,
            "samples": 0,
            "abv": 19.12,
            "proof_gallons": 0.0,
            "excise_tax_due": 0.0
        },
        {
            "name": "Daiquiri (PET)",
            "current_stock": 0,
            "case_size": 24,
            "singles": 12,
            "bottled_s": 5,
            "bottled_i": 7,
            "sold": 0,
            "samples": 0,
            "abv": 19.12,
            "proof_gallons": 0.0,
            "excise_tax_due": 0.0
        },
        # Espresso Martini versions
        {
            "name": "Espresso Martini (GLASS)",
            "current_stock": 0,
            "case_size": 24,
            "singles": 81,
            "bottled_s": 0,
            "bottled_i": 20,
            "sold": 0,
            "samples": 0,
            "abv": 19.47,
            "proof_gallons": 0.0,
            "excise_tax_due": 0.0
        },
        {
            "name": "Espresso Martini (PET)",
            "current_stock": 0,
            "case_size": 24,
            "singles": 80,
            "bottled_s": 0,
            "bottled_i": 20,
            "sold": 0,
            "samples": 0,
            "abv": 19.47,
            "proof_gallons": 0.0,
            "excise_tax_due": 0.0
        },
        # Manhattan versions
        {
            "name": "Manhattan (GLASS)",
            "current_stock": 0,
            "case_size": 24,
            "singles": 50,
            "bottled_s": 5,
            "bottled_i": 0,
            "sold": 0,
            "samples": 0,
            "abv": 31.47,
            "proof_gallons": 0.0,
            "excise_tax_due": 0.0
        },
        {
            "name": "Manhattan (PET)",
            "current_stock": 0,
            "case_size": 24,
            "singles": 49,
            "bottled_s": 5,
            "bottled_i": 0,
            "sold": 0,
            "samples": 0,
            "abv": 31.47,
            "proof_gallons": 0.0,
            "excise_tax_due": 0.0
        },
        # Dateviator versions
        {
            "name": "Dateviator (GLASS)",
            "current_stock": 0,
            "case_size": 24,
            "singles": 0,
            "bottled_s": 0,
            "bottled_i": 0,
            "sold": 0,
            "samples": 0,
            "abv": 16.51,
            "proof_gallons": 0.0,
            "excise_tax_due": 0.0
        },
        {
            "name": "Dateviator (PET)",
            "current_stock": 0,
            "case_size": 24,
            "singles": 0,
            "bottled_s": 0,
            "bottled_i": 0,
            "sold": 0,
            "samples": 0,
            "abv": 16.51,
            "proof_gallons": 0.0,
            "excise_tax_due": 0.0
        },
        # Piña Colada versions
        {
            "name": "Piña Colada (GLASS)",
            "current_stock": 0,
            "case_size": 24,
            "singles": 0,
            "bottled_s": 0,
            "bottled_i": 0,
            "sold": 0,
            "samples": 0,
            "abv": 0.0,  # Not specified in sheet
            "proof_gallons": 0.0,
            "excise_tax_due": 0.0
        },
        {
            "name": "Piña Colada (PET)",
            "current_stock": 0,
            "case_size": 24,
            "singles": 0,
            "bottled_s": 0,
            "bottled_i": 0,
            "sold": 0,
            "samples": 0,
            "abv": 0.0,  # Not specified in sheet
            "proof_gallons": 0.0,
            "excise_tax_due": 0.0
        }
    ]
    
    # Insert finished goods (only if they don't exist)
    existing_fg_names = set()
    if db["finished_goods"].count > 0:
        for fg in db["finished_goods"].rows:
            existing_fg_names.add(fg["name"])
    
    for fg in finished_goods_data:
        if fg["name"] not in existing_fg_names:
            db["finished_goods"].insert(fg)
    
    # Insert bulk spirits (Raw Bulk Spirit in Storage)
    bulk_spirits_data = [
        {
            "name": "Skeptic Vodka (100 Proof)",
            "proof": 100,
            "abv": 50.0,
            "wine_gallons": 106.76,
            "proof_gallons": 106.76
        },
        {
            "name": "Skeptic Vodka (80 Proof)",
            "proof": 80,
            "abv": 40.0,
            "wine_gallons": 0.0,
            "proof_gallons": 0.0
        },
        {
            "name": "Skeptic Gin",
            "proof": 96,
            "abv": 48.0,
            "wine_gallons": 367.32,
            "proof_gallons": 352.6272
        },
        {
            "name": "5th Article 6yr Rye",
            "proof": 121,
            "abv": 60.5,
            "wine_gallons": 186.0,
            "proof_gallons": 225.06
        },
        {
            "name": "Persedo 2yr Bourbon",
            "proof": 120,
            "abv": 60.0,
            "wine_gallons": 150.0,
            "proof_gallons": 180.0
        },
        {
            "name": "Thrasher White Rum",
            "proof": 96,
            "abv": 48.0,
            "wine_gallons": 0.0,
            "proof_gallons": 0.0
        },
        {
            "name": "Thrashers Gold Rum",
            "proof": 80,
            "abv": 40.0,
            "wine_gallons": 0.0,
            "proof_gallons": 0.0
        },
        {
            "name": "Thrasher Coconut Rum",
            "proof": 80,
            "abv": 40.0,
            "wine_gallons": 0.0,
            "proof_gallons": 0.0
        }
    ]
    
    # Insert bulk spirits (only if they don't exist)
    existing_spirit_names = set()
    if db["bulk_spirits"].count > 0:
        for spirit in db["bulk_spirits"].rows:
            existing_spirit_names.add(spirit["name"])
    
    for spirit in bulk_spirits_data:
        if spirit["name"] not in existing_spirit_names:
            db["bulk_spirits"].insert(spirit)
    
    # Insert batches (Mixed/Blended Bulk Batches in Processing)
    batches_data = [
        {
            "name": "Typhoon Batch",
            "gallons": 0.0,
            "bottled_gallons": 0.0,
            "ending": 0.0,
            "abv": 16.02,
            "proof_gallons": 0.0
        },
        {
            "name": "Gimlet Batch",
            "gallons": 0.0,
            "bottled_gallons": 0.0,
            "ending": 0.0,
            "abv": 16.13,
            "proof_gallons": 0.0
        },
        {
            "name": "Daiquiri Batch",
            "gallons": 0.0,
            "bottled_gallons": 0.0,
            "ending": 0.0,
            "abv": 19.12,
            "proof_gallons": 0.0
        },
        {
            "name": "Espresso Batch",
            "gallons": 0.0,
            "bottled_gallons": 0.0,
            "ending": 0.0,
            "abv": 19.47,
            "proof_gallons": 0.0
        },
        {
            "name": "Manhattan Batch",
            "gallons": 0.0,
            "bottled_gallons": 0.0,
            "ending": 0.0,
            "abv": 31.47,
            "proof_gallons": 0.0
        },
        {
            "name": "PIÑA COLADA Batch",
            "gallons": 0.0,
            "bottled_gallons": 0.0,
            "ending": 0.0,
            "abv": 0.0,  # Not specified in sheet
            "proof_gallons": 0.0
        },
        {
            "name": "1887 Batch",
            "gallons": 0.0,
            "bottled_gallons": 0.0,
            "ending": 0.0,
            "abv": 0.0,  # Not specified in sheet
            "proof_gallons": 0.0
        },
        {
            "name": "Dateviator Batch",
            "gallons": 0.0,
            "bottled_gallons": 0.0,
            "ending": 0.0,
            "abv": 16.51,
            "proof_gallons": 0.0
        }
    ]
    
    # Insert batches (only if they don't exist)
    existing_batch_names = set()
    if db["batches"].count > 0:
        for batch in db["batches"].rows:
            existing_batch_names.add(batch["name"])
    
    for batch in batches_data:
        if batch["name"] not in existing_batch_names:
            db["batches"].insert(batch)
    
    # Raw materials table is left empty - can be populated later for production recipes
    # All inventory items are tracked in the inventory_tracking table above

def migrate_database(db):
    """Migrate existing database to add new columns"""
    # Migrate orders table - convert to new structure with order_items
    if "orders" in db.table_names() and "order_items" not in db.table_names():
        # Check if old structure exists (has product_name_ordered)
        try:
            test_order = next(db["orders"].rows, None)
            if test_order and "product_name_ordered" in test_order:
                # Migrate old orders to new structure
                old_orders = list(db["orders"].rows)
                
                # Create order_items table
                db["order_items"].create({
                    "id": int,
                    "order_id": int,
                    "product_name": str,
                    "quantity_cases": int
                }, pk="id", foreign_keys=[
                    ("order_id", "orders", "id")
                ])
                
                # Migrate data
                for old_order in old_orders:
                    if old_order.get("product_name_ordered"):
                        # Insert order item
                        db["order_items"].insert({
                            "order_id": old_order["id"],
                            "product_name": old_order["product_name_ordered"],
                            "quantity_cases": old_order.get("quantity_cases", 0)
                        })
                
                # Remove old columns (we'll keep them for now but they won't be used)
                # The new code will use order_items table
        except:
            pass
    
    # Migrate production_recipes table - add id if missing
    if "production_recipes" in db.table_names():
        try:
            # Try to get a row to check if id exists
            test_row = next(db["production_recipes"].rows, None)
            if test_row and "id" not in test_row:
                # Need to recreate table with id
                # First, backup data
                backup_data = list(db["production_recipes"].rows)
                # Drop and recreate
                db.execute("DROP TABLE production_recipes")
                db["production_recipes"].create({
                    "id": int,
                    "finished_good_id": int,
                    "inventory_item_id": int,
                    "qty_per_case": float,
                    "wastage_factor": float
                }, pk="id", foreign_keys=[
                    ("finished_good_id", "finished_goods", "id"),
                    ("inventory_item_id", "inventory_tracking", "id")
                ])
                # Restore data
                for row in backup_data:
                    db["production_recipes"].insert(row)
        except:
            pass  # Table might be empty or already has id
    
    # Migrate production_recipes table - add packaging_type column
    if "production_recipes" in db.table_names():
        production_recipes_table = db["production_recipes"]
        columns = [col.name for col in production_recipes_table.columns]
        
        if "packaging_type" not in columns:
            db.execute("ALTER TABLE production_recipes ADD COLUMN packaging_type TEXT DEFAULT 'Singles'")
            # Update all existing recipes to have 'Singles' as packaging_type
            for recipe in production_recipes_table.rows:
                if not recipe.get("packaging_type"):
                    production_recipes_table.update(recipe["id"], {"packaging_type": "Singles"})
    
    # Migrate bulk_spirits table - add weight-based tracking columns
    if "bulk_spirits" in db.table_names():
        bulk_spirits_table = db["bulk_spirits"]
        columns = [col.name for col in bulk_spirits_table.columns]
        
        if "weight_lbs" not in columns:
            db.execute("ALTER TABLE bulk_spirits ADD COLUMN weight_lbs REAL DEFAULT 0.0")
        if "density_lbs_per_gal" not in columns:
            db.execute("ALTER TABLE bulk_spirits ADD COLUMN density_lbs_per_gal REAL DEFAULT 8.345")
        if "weight_unit" not in columns:
            db.execute("ALTER TABLE bulk_spirits ADD COLUMN weight_unit TEXT DEFAULT 'lbs'")
        if "last_updated" not in columns:
            db.execute("ALTER TABLE bulk_spirits ADD COLUMN last_updated TEXT")
        
        # Migrate existing data: calculate weight from gallons
        for spirit in bulk_spirits_table.rows:
            if spirit.get("weight_lbs", 0.0) == 0.0 and spirit.get("wine_gallons", 0.0) > 0:
                # Calculate weight from existing gallons
                abv = spirit.get("abv", 40.0)
                wine_gallons = spirit.get("wine_gallons", 0.0)
                density = calculate_density_from_abv(abv)
                weight_lbs = wine_gallons * density
                
                bulk_spirits_table.update(spirit["id"], {
                    "weight_lbs": weight_lbs,
                    "density_lbs_per_gal": density,
                    "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
    
    # Migrate raw_materials table
    if "raw_materials" in db.table_names():
        raw_materials_table = db["raw_materials"]
        columns = [col.name for col in raw_materials_table.columns]
        
        # Add missing columns with ALTER TABLE
        if "units_per_case" not in columns:
            db.execute("ALTER TABLE raw_materials ADD COLUMN units_per_case INTEGER DEFAULT 24")
        if "started" not in columns:
            db.execute("ALTER TABLE raw_materials ADD COLUMN started INTEGER DEFAULT 0")
        if "depleted" not in columns:
            db.execute("ALTER TABLE raw_materials ADD COLUMN depleted INTEGER DEFAULT 0")
        if "added" not in columns:
            db.execute("ALTER TABLE raw_materials ADD COLUMN added INTEGER DEFAULT 0")
        if "units_remaining" not in columns:
            db.execute("ALTER TABLE raw_materials ADD COLUMN units_remaining INTEGER DEFAULT 0")
        if "cases_remaining" not in columns:
            db.execute("ALTER TABLE raw_materials ADD COLUMN cases_remaining INTEGER DEFAULT 0")
        
        # Update existing records with default values
        for material in raw_materials_table.rows:
            update_data = {}
            if material.get("started") is None:
                update_data["started"] = material.get("current_stock", 0)
            if material.get("depleted") is None:
                update_data["depleted"] = 0
            if material.get("added") is None:
                update_data["added"] = 0
            if material.get("units_per_case") is None:
                update_data["units_per_case"] = 24
            if material.get("units_remaining") is None:
                started = material.get("started", material.get("current_stock", 0))
                update_data["units_remaining"] = started
            if material.get("cases_remaining") is None:
                units_remaining = material.get("units_remaining", material.get("current_stock", 0))
                units_per_case = material.get("units_per_case", 24)
                update_data["cases_remaining"] = int(units_remaining / units_per_case) if units_per_case > 0 else 0
            
            if update_data:
                raw_materials_table.update(material["id"], update_data)
    
    # Migrate finished_goods table
    if "finished_goods" in db.table_names():
        finished_goods_table = db["finished_goods"]
        columns = [col.name for col in finished_goods_table.columns]
        
        # Add missing columns
        if "singles" not in columns:
            db.execute("ALTER TABLE finished_goods ADD COLUMN singles INTEGER DEFAULT 0")
        if "bottled_s" not in columns:
            db.execute("ALTER TABLE finished_goods ADD COLUMN bottled_s INTEGER DEFAULT 0")
        if "bottled_i" not in columns:
            db.execute("ALTER TABLE finished_goods ADD COLUMN bottled_i INTEGER DEFAULT 0")
        if "sold" not in columns:
            db.execute("ALTER TABLE finished_goods ADD COLUMN sold INTEGER DEFAULT 0")
        if "samples" not in columns:
            db.execute("ALTER TABLE finished_goods ADD COLUMN samples INTEGER DEFAULT 0")
        if "abv" not in columns:
            db.execute("ALTER TABLE finished_goods ADD COLUMN abv REAL DEFAULT 0.0")
        if "proof_gallons" not in columns:
            db.execute("ALTER TABLE finished_goods ADD COLUMN proof_gallons REAL DEFAULT 0.0")
        if "excise_tax_due" not in columns:
            db.execute("ALTER TABLE finished_goods ADD COLUMN excise_tax_due REAL DEFAULT 0.0")
        
        # Update existing records
        for fg in finished_goods_table.rows:
            update_data = {}
            if fg.get("singles") is None:
                update_data["singles"] = 0
            if fg.get("bottled_s") is None:
                update_data["bottled_s"] = 0
            if fg.get("bottled_i") is None:
                update_data["bottled_i"] = 0
            if fg.get("sold") is None:
                update_data["sold"] = 0
            if fg.get("samples") is None:
                update_data["samples"] = 0
            if fg.get("abv") is None:
                update_data["abv"] = 0.0
            if fg.get("proof_gallons") is None:
                update_data["proof_gallons"] = 0.0
            if fg.get("excise_tax_due") is None:
                update_data["excise_tax_due"] = 0.0
            
            if update_data:
                finished_goods_table.update(fg["id"], update_data)
    
    # Migrate orders table - add new columns
    if "orders" in db.table_names():
        orders_table = db["orders"]
        columns = [col.name for col in orders_table.columns]
        
        if "shipped_date" not in columns:
            db.execute("ALTER TABLE orders ADD COLUMN shipped_date TEXT")
        if "total_revenue" not in columns:
            db.execute("ALTER TABLE orders ADD COLUMN total_revenue REAL DEFAULT 0.0")
    
    # Migrate finished_goods table - add pricing columns
    if "finished_goods" in db.table_names():
        finished_goods_table = db["finished_goods"]
        columns = [col.name for col in finished_goods_table.columns]
        
        if "price_per_case" not in columns:
            db.execute("ALTER TABLE finished_goods ADD COLUMN price_per_case REAL DEFAULT 0.0")
        if "retail_price_per_case" not in columns:
            db.execute("ALTER TABLE finished_goods ADD COLUMN retail_price_per_case REAL DEFAULT 0.0")
    
    # Migrate order_items table - add pricing columns
    if "order_items" in db.table_names():
        order_items_table = db["order_items"]
        columns = [col.name for col in order_items_table.columns]
        
        if "unit_price" not in columns:
            db.execute("ALTER TABLE order_items ADD COLUMN unit_price REAL DEFAULT 0.0")
        if "line_total" not in columns:
            db.execute("ALTER TABLE order_items ADD COLUMN line_total REAL DEFAULT 0.0")

def get_db():
    """Get database connection"""
    return sqlite_utils.Database(DB_PATH)

def update_calculated_fields(db):
    """Update calculated fields across all tables"""
    # Update raw_materials calculated fields
    for material in db["raw_materials"].rows:
        # Safely get values with defaults
        started = material.get("started", material.get("current_stock", 0))
        depleted = material.get("depleted", 0)
        added = material.get("added", 0)
        units_per_case = material.get("units_per_case", 24)
        
        units_remaining = started - depleted + added
        cases_remaining = int(units_remaining / units_per_case) if units_per_case > 0 else 0
        
        db["raw_materials"].update(material["id"], {
            "units_remaining": units_remaining,
            "cases_remaining": cases_remaining,
            "current_stock": units_remaining
        })
    
    # Update bulk_spirits proof_gallons
    for spirit in db["bulk_spirits"].rows:
        wine_gallons = spirit.get("wine_gallons", 0.0)
        proof = spirit.get("proof", 80)
        proof_gallons = calculate_proof_gallons(wine_gallons, proof)
        db["bulk_spirits"].update(spirit["id"], {"proof_gallons": proof_gallons})
    
    # Update batches proof_gallons
    for batch in db["batches"].rows:
        ending = batch.get("ending", 0.0)
        abv = batch.get("abv", 0.0)
        proof_gallons = calculate_proof_gallons(ending, abv * 2) if ending > 0 and abv > 0 else 0.0
        db["batches"].update(batch["id"], {"proof_gallons": proof_gallons})
    
    # Update finished_goods calculated fields
    for fg in db["finished_goods"].rows:
        # Safely get values with defaults
        singles = fg.get("singles", 0)
        bottled_s = fg.get("bottled_s", 0)
        bottled_i = fg.get("bottled_i", 0)
        abv = fg.get("abv", 0.0)
        
        metrics = derive_finished_good_metrics(singles, bottled_s, bottled_i, abv)
        db["finished_goods"].update(fg["id"], metrics)
    
    # Update inventory_tracking calculated fields
    for item in db["inventory_tracking"].rows:
        # Safely get values with defaults
        started = item.get("started", 0)
        depleted = item.get("depleted", 0)
        added = item.get("added", 0)
        units_per_case = item.get("units_per_case", 24)
        
        units_remaining = started - depleted + added
        cases_remaining = int(units_remaining / units_per_case) if units_per_case > 0 else 0
        db["inventory_tracking"].update(item["id"], {
            "units_remaining": units_remaining,
            "cases_remaining": cases_remaining
        })

def create_monthly_snapshot(db, snapshot_date=None):
    """Create a monthly inventory snapshot for historical tracking"""
    import json
    from datetime import datetime
    
    if snapshot_date is None:
        snapshot_date = datetime.now().strftime("%Y-%m-%d")
    
    # Extract year-month for easy querying
    snapshot_month = snapshot_date[:7]  # e.g., "2025-01"
    
    # Capture finished goods
    finished_goods_data = []
    total_finished_cases = 0
    total_proof_gallons = 0
    total_excise_tax_liability = 0
    
    for fg in db["finished_goods"].rows:
        fg_dict = dict(fg)
        finished_goods_data.append(fg_dict)
        total_finished_cases += fg_dict.get("current_stock", 0)
        total_proof_gallons += fg_dict.get("proof_gallons", 0)
        total_excise_tax_liability += fg_dict.get("excise_tax_due", 0)
    
    # Capture bulk spirits
    bulk_spirits_data = []
    for spirit in db["bulk_spirits"].rows:
        spirit_dict = dict(spirit)
        bulk_spirits_data.append(spirit_dict)
        total_proof_gallons += spirit_dict.get("proof_gallons", 0)
    
    # Capture inventory items
    inventory_data = []
    for item in db["inventory_tracking"].rows:
        inventory_data.append(dict(item))
    
    # Insert snapshot
    db["monthly_snapshots"].insert({
        "snapshot_date": snapshot_date,
        "snapshot_month": snapshot_month,
        "finished_goods_json": json.dumps(finished_goods_data),
        "bulk_spirits_json": json.dumps(bulk_spirits_data),
        "inventory_json": json.dumps(inventory_data),
        "total_proof_gallons": total_proof_gallons,
        "total_excise_tax_liability": total_excise_tax_liability,
        "total_finished_cases": total_finished_cases
    })
    
    return snapshot_month

def cleanup_example_data(db):
    """Remove example/test data that shouldn't be in the database"""
    if "raw_materials" in db.table_names():
        example_items = ["Flour", "Bread Loaf"]
        for item in list(db["raw_materials"].rows):
            if item["name"] in example_items:
                db["raw_materials"].delete(item["id"])
    
    if "finished_goods" in db.table_names():
        example_items = ["Bread Loaf"]
        for item in list(db["finished_goods"].rows):
            if item["name"] in example_items:
                db["finished_goods"].delete(item["id"])
    
    if "orders" in db.table_names():
        # Remove example orders
        for order in list(db["orders"].rows):
            if order.get("customer_name") == "ABC Bakery":
                db["orders"].delete(order["id"])

# Initialize database
db = init_database()
migrate_database(db)  # Migrate existing database
cleanup_example_data(db)  # Always clean up example data
remove_duplicates(db)  # Remove any duplicates before seeding
seed_database(db)
update_calculated_fields(db)

# Page configuration
st.set_page_config(
    page_title="One Handed Bartender - Business Tracker", 
    layout="wide",
    page_icon="🍸",
    initial_sidebar_state="expanded"
)

# Custom CSS for brand styling
st.markdown("""
    <style>
        /* Brand colors - elegant dark cocktail bar aesthetic */
        .main {
            background-color: #0f0f0f;
        }
        .stApp {
            background: linear-gradient(180deg, #1a1a1a 0%, #0f0f0f 100%);
        }
        h1 {
            color: #d4af37;
            font-family: 'Georgia', serif;
            font-weight: 600;
            border-bottom: 2px solid #d4af37;
            padding-bottom: 0.5rem;
        }
        h2 {
            color: #f5f5f5;
            font-weight: 500;
        }
        h3 {
            color: #d4af37;
        }
        .stMetric {
            background-color: #1a1a1a;
            border: 1px solid #333;
            border-radius: 8px;
            padding: 1rem;
        }
        .stMetric label {
            color: #d4af37;
            font-weight: 600;
        }
        .stMetric [data-testid="stMetricValue"] {
            color: #f5f5f5;
        }
        .stSidebar {
            background-color: #1a1a1a;
        }
        .stSidebar .stRadio label {
            color: #f5f5f5;
        }
        .stSidebar .stRadio [role="radiogroup"] label[data-baseweb="radio"] {
            background-color: #2a2a2a;
        }
        .stSidebar .stRadio [role="radiogroup"] label[data-baseweb="radio"]:hover {
            background-color: #3a3a3a;
        }
        .stButton>button {
            background-color: #d4af37;
            color: #0f0f0f;
            font-weight: 600;
            border-radius: 6px;
            border: none;
        }
        .stButton>button:hover {
            background-color: #f5d896;
            color: #0f0f0f;
        }
        .stExpander {
            background-color: #1a1a1a;
            border: 1px solid #333;
        }
        .stExpanderHeader {
            color: #d4af37;
        }
        .stSuccess {
            background-color: #1a3a1a;
            border-left: 4px solid #4caf50;
        }
        .stInfo {
            background-color: #1a2a3a;
            border-left: 4px solid #2196F3;
        }
        .stWarning {
            background-color: #3a2a1a;
            border-left: 4px solid #ff9800;
        }
    </style>
""", unsafe_allow_html=True)

# Main title with brand name
st.title("🍸 One Handed Bartender - Business Tracker")

# Sidebar navigation with better styling
st.sidebar.markdown("### 🍹 Navigation")
st.sidebar.markdown("---")
page = st.sidebar.radio(
    "Select Page", 
    [
        "📊 Dashboard", 
        "🍾 Finished Goods", 
        "🥃 Bulk Spirits",
        "🔄 Batches",
        "📦 Inventory Tracking",
        "📋 Recipes",
        "⚙️ Production",
        "🔍 Physical Counts & Waste",
        "📈 Reports & Analytics",
        "💼 CRM/Sales"
    ],
    label_visibility="collapsed"
)

if page == "📊 Dashboard":
    st.header("📊 Overview Dashboard")
    
    db = get_db()
    
    # Quick date range for recent activity
    from datetime import datetime, timedelta
    today = datetime.now()
    last_30_days = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    today_str = today.strftime("%Y-%m-%d")
    
    # Key metrics with better visual presentation
    st.markdown("### Key Metrics")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # Current inventory
        finished_goods = list(db["finished_goods"].rows)
        total_cases = sum([fg.get("current_stock", 0) for fg in finished_goods])
        st.metric("🍾 Finished Goods", f"{total_cases:,} cases", help="Total finished goods inventory")
    
    with col2:
        # Recent orders (last 30 days)
        recent_orders = list(db["orders"].rows_where(
            "order_date >= ?", [last_30_days]
        ))
        st.metric("📋 Orders (30d)", len(recent_orders), help="Orders in last 30 days")
    
    with col3:
        pending_orders = len(list(db["orders"].rows_where("status = ?", ["Pending"])))
        st.metric("⏳ Pending Orders", pending_orders, help="Orders awaiting fulfillment")
    
    with col4:
        # Total proof gallons
        total_pg = sum([fg.get("proof_gallons", 0.0) for fg in finished_goods])
        st.metric("🥃 Proof Gallons", f"{total_pg:,.1f}", help="Total proof gallons in inventory")
    
    # Financial metrics
    st.markdown("### Financial Overview (Last 30 Days)")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # Revenue from shipped orders
        shipped_orders = list(db["orders"].rows_where(
            "status = 'Shipped' AND shipped_date >= ?", [last_30_days]
        ))
        total_revenue = sum([o.get("total_revenue", 0.0) for o in shipped_orders])
        st.metric("💰 Revenue", f"${total_revenue:,.2f}", help="Revenue from shipped orders")
    
    with col2:
        # Cases sold
        cases_sold = 0
        for order in shipped_orders:
            order_items = list(db["order_items"].rows_where("order_id = ?", [order["id"]]))
            cases_sold += sum([item["quantity_cases"] for item in order_items])
        st.metric("📦 Cases Sold", f"{cases_sold:,}", help="Total cases shipped")
    
    with col3:
        # Tax liability
        total_tax = sum([fg.get("excise_tax_due", 0.0) for fg in finished_goods])
        st.metric("🏛️ Tax Liability", f"${total_tax:,.2f}", help="Current excise tax liability")
    
    with col4:
        # Average order value
        avg_order = total_revenue / len(shipped_orders) if shipped_orders else 0
        st.metric("📊 Avg Order", f"${avg_order:,.2f}", help="Average order value")
    
    # Recent activity
    st.markdown("### Recent Activity")
    
    # Recent production
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Recent Production")
        production_records = list(db["production_history"].rows_where(
            "production_date >= ?", [last_30_days]
        ))
        
        if production_records:
            prod_display = []
            for p in sorted(production_records, key=lambda x: x["production_date"], reverse=True)[:5]:
                prod_display.append({
                    "Date": p["production_date"],
                    "Product": p["finished_good_name"],
                    "Cases": p["cases_produced"]
                })
            st.dataframe(pd.DataFrame(prod_display), use_container_width=True, hide_index=True)
        else:
            st.info("No recent production")
    
    with col2:
        st.markdown("#### Recent Orders")
        if recent_orders:
            order_display = []
            for o in sorted(recent_orders, key=lambda x: x["order_date"], reverse=True)[:5]:
                order_display.append({
                    "Date": o["order_date"],
                    "Customer": o["customer_name"],
                    "Status": o["status"],
                    "Value": f"${o.get('total_revenue', 0.0):,.2f}"
                })
            st.dataframe(pd.DataFrame(order_display), use_container_width=True, hide_index=True)
        else:
            st.info("No recent orders")
    
    # Low stock alerts
    st.markdown("### 🚨 Low Stock Alerts")
    low_stock_items = []
    for fg in finished_goods:
        stock = fg.get("current_stock", 0)
        if stock < 10:  # Alert if less than 10 cases
            low_stock_items.append({
                "Product": fg["name"],
                "Current Stock": f"{stock} cases",
                "Status": "🔴 Critical" if stock < 5 else "🟡 Low"
            })
    
    if low_stock_items:
        st.dataframe(pd.DataFrame(low_stock_items), use_container_width=True, hide_index=True)
    else:
        st.success("✅ All products are well stocked!")
    
    # Quick actions
    st.markdown("### ⚡ Quick Actions")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("📸 Create Inventory Snapshot", use_container_width=True):
            snapshot_month = create_monthly_snapshot(db)
            st.success(f"✅ Snapshot created for {snapshot_month}!")
            st.rerun()
    
    with col2:
        st.button("⚙️ Record Production", use_container_width=True, key="goto_production")
    
    with col3:
        st.button("💼 Create Order", use_container_width=True, key="goto_orders")
    
    with col4:
        st.button("📈 View Reports", use_container_width=True, key="goto_reports")

elif page == "🍾 Finished Goods":
    st.header("🍾 Finished Goods Inventory")
    
    db = get_db()
    
    # Add new finished good
    with st.expander("➕ Add New Finished Good"):
        with st.form("add_finished_good"):
            col1, col2 = st.columns(2)
            with col1:
                name = st.text_input("Name")
                case_size = st.number_input("Case Size", min_value=1, value=24)
                abv = st.number_input("ABV (%)", min_value=0.0, max_value=100.0, value=0.0, step=0.01)
            with col2:
                singles = st.number_input("Singles", min_value=0, value=0)
                bottled_s = st.number_input("Bottled (S) - Shipping", min_value=0, value=0)
                bottled_i = st.number_input("Bottled (I) - In-store", min_value=0, value=0)
            
            if st.form_submit_button("Add Finished Good"):
                if name:
                    metrics = derive_finished_good_metrics(singles, bottled_s, bottled_i, abv)
                    
                    db["finished_goods"].insert({
                        "name": name,
                        "current_stock": metrics["current_stock"],
                        "case_size": case_size,
                        "singles": singles,
                        "bottled_s": bottled_s,
                        "bottled_i": bottled_i,
                        "sold": 0,
                        "samples": 0,
                        "abv": abv,
                        "proof_gallons": metrics["proof_gallons"],
                        "excise_tax_due": metrics["excise_tax_due"]
                    })
                    st.success(f"✅ Added {name}")
                    st.rerun()
    
    # Display finished goods
    finished_goods = list(db["finished_goods"].rows)
    
    if finished_goods:
        # Summary table
        st.subheader("Finished Goods Summary")
        display_data = []
        for fg in finished_goods:
            all_cases = (fg["singles"] + fg["bottled_s"] * UNITS_PER_CASE + fg["bottled_i"] * UNITS_PER_CASE) / UNITS_PER_CASE
            
            display_data.append({
                "ID": fg["id"],
                "Name": fg["name"],
                "Singles": fg["singles"],
                "Bottled (S)": fg["bottled_s"],
                "Bottled (I)": fg["bottled_i"],
                "Sold": fg["sold"],
                "Samples": fg["samples"],
                "All Cases": f"{all_cases:.1f}",
                "ABV": f"{fg['abv']:.2f}%" if fg["abv"] > 0 else "N/A",
                "Proof Gallons": f"{fg['proof_gallons']:.2f}",
                "Excise Tax Due": f"${fg['excise_tax_due']:.2f}"
            })
        
        st.subheader("Finished Goods - Click to Edit")
        st.caption("Edit values directly in the table. All Cases, Proof Gallons, and Excise Tax are auto-calculated.")
        
        # Prepare editable dataframe
        editable_data = []
        for fg in finished_goods:
            editable_data.append({
                "ID": fg["id"],
                "Name": fg["name"],
                "Case Size": fg["case_size"],
                "Singles": fg["singles"],
                "Bottled (S)": fg["bottled_s"],
                "Bottled (I)": fg["bottled_i"],
                "Sold": fg["sold"],
                "Samples": fg["samples"],
                "ABV (%)": fg["abv"],
                "Price/Case ($)": fg.get("price_per_case", 0.0),
                "Retail Price ($)": fg.get("retail_price_per_case", 0.0)
            })
        
        df = pd.DataFrame(editable_data)
        
        edited_df = st.data_editor(
            df,
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            key="edit_finished_goods",
            column_config={
                "ID": st.column_config.NumberColumn("ID", disabled=True),
                "Name": st.column_config.TextColumn("Name"),
                "Case Size": st.column_config.NumberColumn("Case Size", min_value=1, step=1),
                "Singles": st.column_config.NumberColumn("Singles", min_value=0, step=1),
                "Bottled (S)": st.column_config.NumberColumn("Bottled (S)", min_value=0, step=1),
                "Bottled (I)": st.column_config.NumberColumn("Bottled (I)", min_value=0, step=1),
                "Sold": st.column_config.NumberColumn("Sold", min_value=0, step=1),
                "Samples": st.column_config.NumberColumn("Samples", min_value=0, step=1),
                "ABV (%)": st.column_config.NumberColumn("ABV (%)", min_value=0.0, max_value=100.0, step=0.01, format="%.2f"),
                "Price/Case ($)": st.column_config.NumberColumn("Price/Case ($)", min_value=0.0, step=0.01, format="$%.2f", help="Wholesale price per case"),
                "Retail Price ($)": st.column_config.NumberColumn("Retail Price ($)", min_value=0.0, step=0.01, format="$%.2f", help="Retail price per case")
            }
        )
        
        if st.button("💾 Save All Changes", type="primary", use_container_width=True):
            for idx, row in edited_df.iterrows():
                fg_id = int(row["ID"])
                singles = int(row["Singles"])
                bottled_s = int(row["Bottled (S)"])
                bottled_i = int(row["Bottled (I)"])
                abv = float(row["ABV (%)"])
                price_per_case = float(row["Price/Case ($)"])
                retail_price = float(row["Retail Price ($)"])
                
                metrics = derive_finished_good_metrics(singles, bottled_s, bottled_i, abv)
                
                db["finished_goods"].update(fg_id, {
                    "name": row["Name"],
                    "case_size": int(row["Case Size"]),
                    "singles": singles,
                    "bottled_s": bottled_s,
                    "bottled_i": bottled_i,
                    "sold": int(row["Sold"]),
                    "samples": int(row["Samples"]),
                    "abv": abv,
                    "current_stock": metrics["current_stock"],
                    "proof_gallons": metrics["proof_gallons"],
                    "excise_tax_due": metrics["excise_tax_due"],
                    "price_per_case": price_per_case,
                    "retail_price_per_case": retail_price
                })
            
            st.success("✅ All changes saved!")
            st.rerun()
        
        # Delete functionality
        st.subheader("Delete Items")
        delete_options = ["Select item to delete..."] + [f"{fg['id']}: {fg['name']}" for fg in finished_goods]
        delete_selection = st.selectbox("Select item to delete", delete_options, key="delete_fg")
        if delete_selection != "Select item to delete...":
            fg_id_to_delete = int(delete_selection.split(":")[0])
            if st.button("🗑️ Delete Selected Item", type="secondary", key="btn_delete_fg"):
                fg_name = db["finished_goods"].get(fg_id_to_delete)["name"]
                db["finished_goods"].delete(fg_id_to_delete)
                st.success(f"✅ Deleted {fg_name}")
                st.rerun()
    else:
        st.info("No finished goods found in the database.")

elif page == "🥃 Bulk Spirits":
    st.header("🥃 Raw Bulk Spirit in Storage (Weight-Based Tracking)")
    st.caption("⚖️ Primary tracking by WEIGHT (lbs) for legal/tax compliance. Gallons auto-calculated.")
    st.info("💡 Track by weight for accuracy. System automatically calculates wine gallons and proof gallons for TTB reporting.")
    
    db = get_db()
    
    # Add new bulk spirit
    with st.expander("➕ Receive New Bulk Spirit", expanded=False):
        st.markdown("### Enter Spirit Details")
        
        # Input method selector
        input_method = st.radio(
            "How do you want to enter the quantity?",
            ["By Weight (Recommended)", "By Volume (Gallons)"],
            help="Weight is more accurate for mixed alcohol. System converts either way."
        )
        
        with st.form("add_bulk_spirit"):
            col1, col2 = st.columns(2)
            with col1:
                name = st.text_input("Spirit Name", placeholder="e.g., Skeptic Vodka 100 Proof")
                abv = st.number_input("ABV (%)", min_value=0.0, max_value=100.0, value=40.0, step=0.01,
                                     help="Alcohol by Volume percentage from supplier")
            
            with col2:
                proof = st.number_input("Proof", min_value=0, max_value=200, value=int(abv*2), 
                                       help="Proof = ABV × 2")
                receipt_date = st.date_input("Receipt Date", value=datetime.now().date())
            
            # Show appropriate input based on method
            if input_method == "By Weight (Recommended)":
                col1, col2 = st.columns(2)
                with col1:
                    weight_lbs = st.number_input("Weight (lbs)", min_value=0.0, value=0.0, step=0.1,
                                                help="Total weight of spirit received")
                with col2:
                    # Calculate and show gallons
                    if weight_lbs > 0 and abv > 0:
                        calc_gallons = calculate_gallons_from_weight(weight_lbs, abv)
                        st.metric("📊 Calculated Wine Gallons", f"{calc_gallons:.2f}",
                                 help="Auto-calculated from weight and ABV")
                        wine_gallons = calc_gallons
                    else:
                        st.metric("📊 Calculated Wine Gallons", "0.00")
                        wine_gallons = 0.0
            else:  # By Volume
                col1, col2 = st.columns(2)
                with col1:
                    wine_gallons = st.number_input("Wine Gallons", min_value=0.0, value=0.0, step=0.01,
                                                   help="Volume of spirit received")
                with col2:
                    # Calculate and show weight
                    if wine_gallons > 0 and abv > 0:
                        calc_weight = calculate_weight_from_gallons(wine_gallons, abv)
                        st.metric("⚖️ Calculated Weight (lbs)", f"{calc_weight:.2f}",
                                 help="Auto-calculated from gallons and ABV")
                        weight_lbs = calc_weight
                    else:
                        st.metric("⚖️ Calculated Weight (lbs)", "0.00")
                        weight_lbs = 0.0
            
            # Optional fields
            col1, col2 = st.columns(2)
            with col1:
                supplier = st.text_input("Supplier (optional)", "")
            with col2:
                batch_number = st.text_input("Batch Number (optional)", "")
            
            notes = st.text_area("Notes (optional)", placeholder="Any special notes about this receipt...")
            
            if st.form_submit_button("💾 Receive Spirit", type="primary"):
                if name and weight_lbs > 0:
                    # Calculate all values
                    density = calculate_density_from_abv(abv)
                    wine_gallons = calculate_gallons_from_weight(weight_lbs, abv)
                    proof_gallons = calculate_proof_gallons(wine_gallons, proof)
                    
                    # Insert bulk spirit
                    spirit_id = db["bulk_spirits"].insert({
                        "name": name,
                        "proof": proof,
                        "abv": abv,
                        "wine_gallons": wine_gallons,
                        "proof_gallons": proof_gallons,
                        "weight_lbs": weight_lbs,
                        "density_lbs_per_gal": density,
                        "weight_unit": "lbs",
                        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }).last_pk
                    
                    # Log the receipt
                    db["bulk_spirit_receipts"].insert({
                        "bulk_spirit_id": spirit_id,
                        "receipt_date": receipt_date.strftime("%Y-%m-%d"),
                        "weight_lbs": weight_lbs,
                        "wine_gallons": wine_gallons,
                        "abv": abv,
                        "proof": proof,
                        "supplier": supplier,
                        "batch_number": batch_number,
                        "notes": notes
                    })
                    
                    st.success(f"✅ Received {name}: {weight_lbs:.2f} lbs ({wine_gallons:.2f} gal, {proof_gallons:.2f} PG)")
                    st.rerun()
                else:
                    st.error("Please enter spirit name and weight/volume")
    
    # Display bulk spirits
    bulk_spirits = list(db["bulk_spirits"].rows)
    
    if bulk_spirits:
        # Summary metrics at top
        st.markdown("### Current Inventory Summary")
        total_weight = sum([s.get("weight_lbs", 0.0) for s in bulk_spirits])
        total_wine_gallons = sum([s.get("wine_gallons", 0.0) for s in bulk_spirits])
        total_proof_gallons = sum([s.get("proof_gallons", 0.0) for s in bulk_spirits])
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("⚖️ Total Weight", f"{total_weight:,.2f} lbs", help="Total weight of all bulk spirits")
        with col2:
            st.metric("📊 Total Wine Gallons", f"{total_wine_gallons:,.2f} gal", help="Calculated from weight and ABV")
        with col3:
            st.metric("🏛️ Total Proof Gallons", f"{total_proof_gallons:,.2f} PG", help="For TTB reporting")
        
        # Detailed inventory table
        st.markdown("### Bulk Spirit Inventory (Weight-Based)")
        st.caption("⚖️ **Primary Unit: Weight (lbs)** | Gallons are auto-calculated for TTB compliance")
        
        # Prepare editable dataframe
        editable_data = []
        for spirit in bulk_spirits:
            weight = spirit.get("weight_lbs", 0.0)
            abv = spirit.get("abv", 40.0)
            wine_gal = spirit.get("wine_gallons", 0.0)
            proof_gal = spirit.get("proof_gallons", 0.0)
            density = spirit.get("density_lbs_per_gal", calculate_density_from_abv(abv))
            
            editable_data.append({
                "ID": spirit["id"],
                "Name": spirit["name"],
                "Weight (lbs)": weight,
                "ABV (%)": abv,
                "Proof": spirit["proof"],
                "Wine Gallons": wine_gal,
                "Proof Gallons": proof_gal,
                "Density (lb/gal)": density
            })
        
        df = pd.DataFrame(editable_data)
        
        edited_df = st.data_editor(
            df,
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            key="edit_bulk_spirits",
            column_config={
                "ID": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                "Name": st.column_config.TextColumn("Name", width="medium"),
                "Weight (lbs)": st.column_config.NumberColumn("Weight (lbs)", min_value=0.0, step=0.1, format="%.2f", help="Primary tracking unit"),
                "ABV (%)": st.column_config.NumberColumn("ABV (%)", min_value=0.0, max_value=100.0, step=0.01, format="%.2f", help="From supplier"),
                "Proof": st.column_config.NumberColumn("Proof", disabled=True, width="small", help="ABV × 2"),
                "Wine Gallons": st.column_config.NumberColumn("Wine Gallons", disabled=True, format="%.2f", help="Auto-calculated"),
                "Proof Gallons": st.column_config.NumberColumn("Proof Gallons", disabled=True, format="%.2f", help="For TTB"),
                "Density (lb/gal)": st.column_config.NumberColumn("Density", disabled=True, format="%.3f", help="Auto-calculated from ABV")
            }
        )
        
        if st.button("💾 Save All Changes", type="primary", use_container_width=True):
            for idx, row in edited_df.iterrows():
                spirit_id = int(row["ID"])
                weight_lbs = float(row["Weight (lbs)"])
                abv = float(row["ABV (%)"])
                proof = int(abv * 2)
                
                # Recalculate everything from weight and ABV
                density = calculate_density_from_abv(abv)
                wine_gallons = calculate_gallons_from_weight(weight_lbs, abv)
                proof_gallons = calculate_proof_gallons(wine_gallons, proof)
                
                db["bulk_spirits"].update(spirit_id, {
                    "name": row["Name"],
                    "weight_lbs": weight_lbs,
                    "abv": abv,
                    "proof": proof,
                    "wine_gallons": wine_gallons,
                    "proof_gallons": proof_gallons,
                    "density_lbs_per_gal": density,
                    "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
            
            st.success("✅ All changes saved! Gallons and proof gallons recalculated.")
            st.rerun()
        
        # Receipt history
        st.markdown("### Receipt History")
        st.caption("Track ABV variations and supplier batches")
        
        receipts = list(db["bulk_spirit_receipts"].rows)
        if receipts:
            # Show last 10 receipts
            recent_receipts = sorted(receipts, key=lambda x: x["receipt_date"], reverse=True)[:10]
            receipt_data = []
            for r in recent_receipts:
                spirit = db["bulk_spirits"].get(r["bulk_spirit_id"])
                spirit_name = spirit["name"] if spirit else "Unknown"
                receipt_data.append({
                    "Date": r["receipt_date"],
                    "Spirit": spirit_name,
                    "Weight (lbs)": f"{r['weight_lbs']:.2f}",
                    "Gallons": f"{r['wine_gallons']:.2f}",
                    "ABV": f"{r['abv']:.2f}%",
                    "Supplier": r.get("supplier", "—"),
                    "Batch #": r.get("batch_number", "—")
                })
            
            st.dataframe(pd.DataFrame(receipt_data), use_container_width=True, hide_index=True)
        else:
            st.info("No receipt history yet. Receipts are logged when you add new spirits.")
        
        # Delete functionality
        st.markdown("---")
        st.subheader("🗑️ Remove Spirit")
        delete_options = ["Select spirit to remove..."] + [f"{spirit['id']}: {spirit['name']}" for spirit in bulk_spirits]
        delete_selection = st.selectbox("Select item to delete", delete_options, key="delete_spirit")
        if delete_selection != "Select spirit to remove...":
            spirit_id_to_delete = int(delete_selection.split(":")[0])
            spirit = db["bulk_spirits"].get(spirit_id_to_delete)
            col1, col2 = st.columns([3, 1])
            with col1:
                st.warning(f"⚠️ This will remove {spirit['name']} ({spirit.get('weight_lbs', 0):.2f} lbs) from inventory.")
            with col2:
                if st.button("🗑️ Confirm Delete", type="secondary", key="btn_delete_spirit"):
                    spirit_name = spirit["name"]
                    db["bulk_spirits"].delete(spirit_id_to_delete)
                    st.success(f"✅ Removed {spirit_name}")
                    st.rerun()
    else:
        st.info("No bulk spirits in inventory. Receive your first spirit above!")

elif page == "🔄 Batches":
    st.header("🔄 Mixed/Blended Bulk Batches (Weight-Based Production)")
    st.caption("⚖️ Track batch production by weight. System automatically calculates gallons for inventory.")
    
    db = get_db()
    
    # Create tabs for different functions
    tab1, tab2, tab3 = st.tabs(["🏭 Produce Batch", "📋 Batch Recipes", "📊 Current Batches"])
    
    with tab1:  # Batch Production
        st.subheader("🏭 Produce a New Batch")
        st.caption("Mix bulk spirits and ingredients by weight to create cocktail batches")
        
        batches = list(db["batches"].rows)
        if not batches:
            st.warning("⚠️ No batch types configured. Create batch types in the 'Current Batches' tab first.")
        else:
            with st.form("produce_batch"):
                # Select batch type
                batch_names = [b["name"] for b in batches]
                selected_batch_name = st.selectbox("Batch Type", batch_names)
                selected_batch = next(b for b in batches if b["name"] == selected_batch_name)
                batch_id = selected_batch["id"]
                
                # Get recipe for this batch
                recipes = list(db["batch_recipes"].rows_where("batch_id = ?", [batch_id]))
                
                if not recipes:
                    st.warning(f"⚠️ No recipe configured for {selected_batch_name}. Configure recipe in 'Batch Recipes' tab.")
                    st.form_submit_button("Produce Batch", disabled=True)
                else:
                    # Show recipe summary
                    st.markdown(f"**Recipe for {selected_batch_name}:**")
                    recipe_summary = []
                    for recipe in recipes:
                        if recipe.get("bulk_spirit_id"):
                            spirit = db["bulk_spirits"].get(recipe["bulk_spirit_id"])
                            ingredient_name = spirit["name"] if spirit else "Unknown Spirit"
                        else:
                            ingredient_name = recipe.get("ingredient_name", "Unknown")
                        
                        recipe_summary.append(f"• {ingredient_name}: {recipe['percentage']:.1f}%")
                    
                    for line in recipe_summary:
                        st.caption(line)
                    
                    # Production inputs
                    col1, col2 = st.columns(2)
                    with col1:
                        weight_to_produce = st.number_input(
                            "Weight to Produce (lbs)", 
                            min_value=0.1, 
                            value=100.0, 
                            step=1.0,
                            help="Total weight of batch to produce"
                        )
                    
                    with col2:
                        production_date = st.date_input("Production Date", value=datetime.now().date())
                    
                    notes = st.text_area("Production Notes (optional)", placeholder="Any notes about this production run...")
                    
                    # Calculate depletions
                    st.markdown("**Depletions Preview:**")
                    depletion_preview = []
                    for recipe in recipes:
                        depletion_weight = weight_to_produce * (recipe['percentage'] / 100.0)
                        
                        if recipe.get("bulk_spirit_id"):
                            spirit = db["bulk_spirits"].get(recipe["bulk_spirit_id"])
                            ingredient_name = spirit["name"] if spirit else "Unknown Spirit"
                            current_weight = spirit.get("weight_lbs", 0.0) if spirit else 0.0
                            new_weight = current_weight - depletion_weight
                            
                            status = "✅" if new_weight >= 0 else "❌ INSUFFICIENT"
                            depletion_preview.append(f"{status} {ingredient_name}: -{depletion_weight:.2f} lbs (remaining: {new_weight:.2f} lbs)")
                        else:
                            ingredient_name = recipe.get("ingredient_name", "Unknown")
                            depletion_preview.append(f"• {ingredient_name}: {depletion_weight:.2f} lbs")
                    
                    for line in depletion_preview:
                        st.caption(line)
                    
                    # Check if we have enough
                    can_produce = True
                    for recipe in recipes:
                        if recipe.get("bulk_spirit_id"):
                            spirit = db["bulk_spirits"].get(recipe["bulk_spirit_id"])
                            if spirit:
                                depletion_weight = weight_to_produce * (recipe['percentage'] / 100.0)
                                if spirit.get("weight_lbs", 0.0) < depletion_weight:
                                    can_produce = False
                                    break
                    
                    if st.form_submit_button("🏭 Produce Batch", type="primary", disabled=not can_produce):
                        # Calculate batch ABV (weighted average)
                        total_alcohol_weight = 0
                        for recipe in recipes:
                            if recipe.get("bulk_spirit_id"):
                                spirit = db["bulk_spirits"].get(recipe["bulk_spirit_id"])
                                if spirit:
                                    ingredient_weight = weight_to_produce * (recipe['percentage'] / 100.0)
                                    alcohol_content = ingredient_weight * (spirit.get("abv", 0.0) / 100.0)
                                    total_alcohol_weight += alcohol_content
                        
                        batch_abv = (total_alcohol_weight / weight_to_produce) * 100.0 if weight_to_produce > 0 else 0.0
                        
                        # Calculate gallons
                        batch_gallons = calculate_gallons_from_weight(weight_to_produce, batch_abv)
                        proof = batch_abv * 2
                        proof_gallons = calculate_proof_gallons(batch_gallons, proof)
                        
                        # Deplete bulk spirits
                        depletions_made = []
                        for recipe in recipes:
                            if recipe.get("bulk_spirit_id"):
                                spirit = db["bulk_spirits"].get(recipe["bulk_spirit_id"])
                                if spirit:
                                    depletion_weight = weight_to_produce * (recipe['percentage'] / 100.0)
                                    new_weight = spirit.get("weight_lbs", 0.0) - depletion_weight
                                    
                                    # Recalculate gallons
                                    new_gallons = calculate_gallons_from_weight(new_weight, spirit.get("abv", 40.0))
                                    new_proof_gallons = calculate_proof_gallons(new_gallons, spirit.get("proof", 80))
                                    
                                    # Update spirit
                                    db["bulk_spirits"].update(recipe["bulk_spirit_id"], {
                                        "weight_lbs": new_weight,
                                        "wine_gallons": new_gallons,
                                        "proof_gallons": new_proof_gallons,
                                        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                    })
                                    
                                    depletions_made.append({
                                        "Spirit": spirit["name"],
                                        "Depleted (lbs)": f"-{depletion_weight:.2f}",
                                        "Remaining (lbs)": f"{new_weight:.2f}",
                                        "Remaining (gal)": f"{new_gallons:.2f}"
                                    })
                        
                        # Update batch inventory
                        current_gallons = selected_batch.get("gallons", 0.0)
                        current_weight_lbs = calculate_weight_from_gallons(current_gallons, selected_batch.get("abv", batch_abv))
                        new_total_weight = current_weight_lbs + weight_to_produce
                        new_total_gallons = calculate_gallons_from_weight(new_total_weight, batch_abv)
                        new_proof_gallons = calculate_proof_gallons(new_total_gallons, proof)
                        
                        db["batches"].update(batch_id, {
                            "gallons": new_total_gallons,
                            "ending": new_total_gallons - selected_batch.get("bottled_gallons", 0.0),
                            "abv": batch_abv,
                            "proof_gallons": new_proof_gallons
                        })
                        
                        # Log production
                        db["batch_production_log"].insert({
                            "production_date": production_date.strftime("%Y-%m-%d"),
                            "batch_id": batch_id,
                            "batch_name": selected_batch_name,
                            "weight_produced_lbs": weight_to_produce,
                            "gallons_produced": batch_gallons,
                            "abv": batch_abv,
                            "proof_gallons": proof_gallons,
                            "notes": notes
                        })
                        
                        st.success(f"✅ Produced {weight_to_produce:.2f} lbs of {selected_batch_name} ({batch_gallons:.2f} gal, {batch_abv:.2f}% ABV)")
                        
                        if depletions_made:
                            st.markdown("**Bulk Spirit Depletions:**")
                            st.dataframe(pd.DataFrame(depletions_made), use_container_width=True, hide_index=True)
                        
                        st.rerun()
    
    with tab2:  # Batch Recipes
        st.subheader("📋 Configure Batch Recipes (by Weight)")
        st.caption("Define the weight percentage of each ingredient in your cocktail batches")
        
        batches = list(db["batches"].rows)
        if not batches:
            st.warning("⚠️ No batch types exist. Create a batch type in the 'Current Batches' tab first.")
        else:
            for batch in batches:
                batch_id = batch["id"]
                batch_name = batch["name"]
                
                # Get existing recipe
                existing_recipes = list(db["batch_recipes"].rows_where("batch_id = ?", [batch_id]))
                
                with st.expander(f"🧪 {batch_name} Recipe", expanded=len(existing_recipes) == 0):
                    st.write(f"**Configure recipe for {batch_name}** (by weight %)")
                    
                    with st.form(f"recipe_{batch_id}"):
                        # Get all bulk spirits
                        bulk_spirits = list(db["bulk_spirits"].rows)
                        
                        recipe_items = []
                        total_percentage = 0.0
                        
                        # Show existing recipe items or allow adding new
                        st.markdown("**Bulk Spirits:**")
                        for spirit in bulk_spirits:
                            existing = next((r for r in existing_recipes if r.get("bulk_spirit_id") == spirit["id"]), None)
                            default_pct = existing["percentage"] if existing else 0.0
                            
                            col1, col2 = st.columns([3, 1])
                            with col1:
                                st.write(f"**{spirit['name']}** ({spirit.get('abv', 0):.1f}% ABV)")
                            with col2:
                                pct = st.number_input(
                                    "% by weight",
                                    min_value=0.0,
                                    max_value=100.0,
                                    value=default_pct,
                                    step=0.1,
                                    key=f"spirit_pct_{batch_id}_{spirit['id']}",
                                    label_visibility="collapsed"
                                )
                            
                            if pct > 0:
                                recipe_items.append({
                                    "bulk_spirit_id": spirit["id"],
                                    "ingredient_name": spirit["name"],
                                    "percentage": pct
                                })
                                total_percentage += pct
                        
                        # Allow adding non-alcohol ingredients
                        st.markdown("**Other Ingredients (non-alcohol):**")
                        num_other = st.number_input("Number of other ingredients", min_value=0, max_value=10, value=len([r for r in existing_recipes if not r.get("bulk_spirit_id")]), key=f"num_other_{batch_id}")
                        
                        for i in range(int(num_other)):
                            existing = existing_recipes[i] if i < len(existing_recipes) and not existing_recipes[i].get("bulk_spirit_id") else None
                            
                            col1, col2 = st.columns([3, 1])
                            with col1:
                                ingredient_name = st.text_input(
                                    f"Ingredient {i+1}",
                                    value=existing.get("ingredient_name", "") if existing else "",
                                    key=f"other_name_{batch_id}_{i}",
                                    placeholder="e.g., Lime Juice, Simple Syrup"
                                )
                            with col2:
                                pct = st.number_input(
                                    "% by weight",
                                    min_value=0.0,
                                    max_value=100.0,
                                    value=existing.get("percentage", 0.0) if existing else 0.0,
                                    step=0.1,
                                    key=f"other_pct_{batch_id}_{i}",
                                    label_visibility="collapsed"
                                )
                            
                            if ingredient_name and pct > 0:
                                recipe_items.append({
                                    "bulk_spirit_id": None,
                                    "ingredient_name": ingredient_name,
                                    "percentage": pct
                                })
                                total_percentage += pct
                        
                        # Show total
                        if total_percentage > 0:
                            if abs(total_percentage - 100.0) < 0.1:
                                st.success(f"✅ Total: {total_percentage:.1f}% (Perfect!)")
                            elif total_percentage < 100.0:
                                st.warning(f"⚠️ Total: {total_percentage:.1f}% (Need {100.0 - total_percentage:.1f}% more)")
                            else:
                                st.error(f"❌ Total: {total_percentage:.1f}% (Over by {total_percentage - 100.0:.1f}%)")
                        
                        if st.form_submit_button("💾 Save Recipe"):
                            if abs(total_percentage - 100.0) < 0.1:
                                # Delete existing recipes
                                for recipe in existing_recipes:
                                    db["batch_recipes"].delete(recipe["id"])
                                
                                # Insert new recipes
                                for item in recipe_items:
                                    db["batch_recipes"].insert({
                                        "batch_id": batch_id,
                                        "bulk_spirit_id": item.get("bulk_spirit_id"),
                                        "ingredient_name": item["ingredient_name"],
                                        "weight_lbs": 0.0,  # This is percentage-based
                                        "percentage": item["percentage"],
                                        "notes": ""
                                    })
                                
                                st.success(f"✅ Recipe saved for {batch_name}!")
                                st.rerun()
                            else:
                                st.error("Recipe must total 100%. Please adjust percentages.")
    
    with tab3:  # Current Batches
        st.subheader("📊 Current Batch Inventory")
        
        # Add new batch type
        with st.expander("➕ Create New Batch Type"):
            with st.form("add_batch"):
                col1, col2 = st.columns(2)
                with col1:
                    name = st.text_input("Batch Name", placeholder="e.g., Gimlet Batch")
                with col2:
                    abv = st.number_input("Target ABV (%)", min_value=0.0, max_value=100.0, value=16.0, step=0.01,
                                         help="Approximate ABV for this batch type")
                
                if st.form_submit_button("Create Batch Type"):
                    if name:
                        db["batches"].insert({
                            "name": name,
                            "gallons": 0.0,
                            "bottled_gallons": 0.0,
                            "ending": 0.0,
                            "abv": abv,
                            "proof_gallons": 0.0
                        })
                        st.success(f"✅ Created {name}. Now configure its recipe in 'Batch Recipes' tab.")
                        st.rerun()
        
        # Display current batches
        batches = list(db["batches"].rows)
        
        if batches:
            st.markdown("### Current Batch Inventory")
            st.caption("Edit values directly in the table. Proof Gallons are auto-calculated.")
            
            # Prepare editable dataframe
            editable_data = []
            for batch in batches:
                editable_data.append({
                    "ID": batch["id"],
                    "Batch Name": batch["name"],
                    "Gallons": batch["gallons"],
                    "Bottled Gallons": batch["bottled_gallons"],
                    "ABV (%)": batch["abv"]
                })
            
            df = pd.DataFrame(editable_data)
            
            edited_df = st.data_editor(
                df,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                key="edit_batch_inventory",
                column_config={
                    "ID": st.column_config.NumberColumn("ID", disabled=True),
                    "Batch Name": st.column_config.TextColumn("Batch Name"),
                    "Gallons": st.column_config.NumberColumn("Gallons", min_value=0.0, step=0.01, format="%.2f"),
                    "Bottled Gallons": st.column_config.NumberColumn("Bottled Gallons", min_value=0.0, step=0.01, format="%.2f"),
                    "ABV (%)": st.column_config.NumberColumn("ABV (%)", min_value=0.0, max_value=100.0, step=0.01, format="%.2f")
                }
            )
            
            if st.button("💾 Save All Changes", type="primary", use_container_width=True, key="save_batch_inventory"):
                for idx, row in edited_df.iterrows():
                    batch_id = int(row["ID"])
                    gallons = float(row["Gallons"])
                    bottled_gallons = float(row["Bottled Gallons"])
                    abv = float(row["ABV (%)"])
                    
                    ending = gallons - bottled_gallons
                    proof = abv * 2
                    proof_gallons = calculate_proof_gallons(ending, proof) if ending > 0 and abv > 0 else 0.0
                    
                    db["batches"].update(batch_id, {
                        "name": row["Batch Name"],
                        "gallons": gallons,
                        "bottled_gallons": bottled_gallons,
                        "ending": ending,
                        "abv": abv,
                        "proof_gallons": proof_gallons
                    })
                
                st.success("✅ All changes saved!")
                st.rerun()
            
            # Production history
            st.markdown("### Production History")
            production_logs = list(db["batch_production_log"].rows)
            
            if production_logs:
                recent_logs = sorted(production_logs, key=lambda x: x["production_date"], reverse=True)[:10]
                log_data = []
                for log in recent_logs:
                    log_data.append({
                        "Date": log["production_date"],
                        "Batch": log["batch_name"],
                        "Weight (lbs)": f"{log['weight_produced_lbs']:.2f}",
                        "Gallons": f"{log['gallons_produced']:.2f}",
                        "ABV": f"{log['abv']:.2f}%",
                        "Proof Gallons": f"{log['proof_gallons']:.2f}",
                        "Notes": log.get("notes", "—")[:30]
                    })
                
                st.dataframe(pd.DataFrame(log_data), use_container_width=True, hide_index=True)
            else:
                st.info("No production history yet. Produce batches in the 'Produce Batch' tab.")
            
            # Delete functionality
            st.markdown("---")
            st.subheader("🗑️ Delete Batch Type")
            delete_options = ["Select batch to delete..."] + [f"{batch['id']}: {batch['name']}" for batch in batches]
            delete_selection = st.selectbox("Select item to delete", delete_options, key="delete_batch")
            if delete_selection != "Select batch to delete...":
                batch_id_to_delete = int(delete_selection.split(":")[0])
                if st.button("🗑️ Delete Selected Batch", type="secondary", key="btn_delete_batch"):
                    batch_name = db["batches"].get(batch_id_to_delete)["name"]
                    db["batches"].delete(batch_id_to_delete)
                    st.success(f"✅ Deleted {batch_name}")
                    st.rerun()
        else:
            st.info("No batch types configured yet. Create a batch type above to get started.")
    

elif page == "📦 Inventory Tracking":
    st.header("📦 Non-Bonded Inventory and Depletions")
    
    db = get_db()
    
    # Add new inventory item
    with st.expander("➕ Add New Inventory Item"):
        with st.form("add_inventory_item"):
            col1, col2 = st.columns(2)
            with col1:
                item_name = st.text_input("Item Name (include units/case in name)")
                units_per_case = st.number_input("Units per Case", min_value=1, value=24)
            with col2:
                started = st.number_input("Starting Units", min_value=0, value=0)
                depleted = st.number_input("Depleted Units", min_value=0, value=0)
            
            if st.form_submit_button("Add Item"):
                if item_name:
                    added = 0
                    units_remaining = started - depleted + added
                    cases_remaining = int(units_remaining / units_per_case) if units_per_case > 0 else 0
                    
                    db["inventory_tracking"].insert({
                        "item_name": item_name,
                        "units_per_case": units_per_case,
                        "started": started,
                        "depleted": depleted,
                        "added": added,
                        "units_remaining": units_remaining,
                        "cases_remaining": cases_remaining
                    })
                    st.success(f"✅ Added {item_name}")
                    st.rerun()
    
    # Display inventory tracking with spreadsheet-like editing
    items = list(db["inventory_tracking"].rows)
    
    if items:
        st.subheader("Inventory Items - Click to Edit")
        st.caption("Edit **Started** or **Depleted** to track usage, or edit **Cases Remaining** to add/remove full cases (leftover units preserved).")
        
        # Prepare editable dataframe
        display_data = []
        for item in items:
            display_data.append({
                "ID": item["id"],
                "Item Name": item["item_name"],
                "Units per Case": item["units_per_case"],
                "Started": item["started"],
                "Depleted": item["depleted"],
                "Units Remaining": item["units_remaining"],
                "Cases Remaining": item["cases_remaining"]
            })
        
        df = pd.DataFrame(display_data)
        
        # Use data_editor for spreadsheet-like editing
        edited_df = st.data_editor(
            df,
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            key="edit_inventory_tracking",
            column_config={
                "ID": st.column_config.NumberColumn("ID", disabled=True),
                "Item Name": st.column_config.TextColumn("Item Name"),
                "Units per Case": st.column_config.NumberColumn("Units per Case", min_value=1, step=1, help="How many units make one case"),
                "Started": st.column_config.NumberColumn("Started", min_value=0, step=1, help="Initial inventory (units)"),
                "Depleted": st.column_config.NumberColumn("Depleted", step=1, help="Units used in production"),
                "Units Remaining": st.column_config.NumberColumn("Units Remaining", disabled=True, help="Auto-calculated based on Cases Remaining + leftover units"),
                "Cases Remaining": st.column_config.NumberColumn("Cases Remaining", min_value=0, step=1, help="Edit this to add/remove cases. Leftover units are preserved.")
            }
        )
        
        # Save changes button
        if st.button("💾 Save All Changes", type="primary", use_container_width=True):
            for idx, row in edited_df.iterrows():
                item_id = int(row["ID"])
                units_per_case = int(row["Units per Case"])
                started = int(row["Started"])
                depleted = int(row["Depleted"])
                new_cases_remaining = int(row["Cases Remaining"])
                
                # Get current data to detect what changed
                current_item = db["inventory_tracking"].get(item_id)
                current_started = current_item["started"]
                current_depleted = current_item["depleted"]
                current_cases = current_item["cases_remaining"]
                current_units_remaining = current_item["units_remaining"]
                current_added = current_item["added"]
                
                # Detect what changed and apply appropriate logic
                started_changed = started != current_started
                depleted_changed = depleted != current_depleted
                cases_changed = new_cases_remaining != current_cases
                
                if cases_changed and not (started_changed or depleted_changed):
                    # User edited Cases Remaining - preserve leftover units
                    leftover_units = current_units_remaining % units_per_case if units_per_case > 0 else 0
                    units_remaining = (new_cases_remaining * units_per_case) + leftover_units
                    # Back-calculate added: units_remaining = started - depleted + added
                    added = units_remaining - started + depleted
                    cases_remaining = new_cases_remaining
                else:
                    # User edited Started or Depleted - use standard formula
                    added = current_added  # Keep existing added value
                    units_remaining = started - depleted + added
                    cases_remaining = int(units_remaining / units_per_case) if units_per_case > 0 else 0
                
                db["inventory_tracking"].update(item_id, {
                    "item_name": row["Item Name"],
                    "units_per_case": units_per_case,
                    "started": started,
                    "depleted": depleted,
                    "added": added,
                    "units_remaining": units_remaining,
                    "cases_remaining": cases_remaining
                })
            
            st.success("✅ All changes saved!")
            st.rerun()
        
        # Delete row functionality
        st.subheader("Delete Items")
        delete_options = ["Select item to delete..."] + [f"{item['id']}: {item['item_name']}" for item in items]
        delete_selection = st.selectbox("Select item to delete", delete_options)
        
        if delete_selection != "Select item to delete...":
            item_id_to_delete = int(delete_selection.split(":")[0])
            if st.button("🗑️ Delete Selected Item", type="secondary"):
                item_name = db["inventory_tracking"].get(item_id_to_delete)["item_name"]
                db["inventory_tracking"].delete(item_id_to_delete)
                st.success(f"✅ Deleted {item_name}")
                st.rerun()
    else:
        st.info("No inventory items found in the database.")

elif page == "📋 Recipes":
    st.header("📋 Production Recipes")
    st.caption("Configure which inventory items and quantities are used when producing each finished good")
    st.info("💡 **New!** Configure separate recipes for Singles, Shipping (S), and In-store (I) packaging types.")
    
    db = get_db()
    
    # Get list of finished goods
    finished_goods = list(db["finished_goods"].rows)
    inventory_items = list(db["inventory_tracking"].rows)
    
    if not finished_goods:
        st.warning("No finished goods available. Please add finished goods first.")
    elif not inventory_items:
        st.warning("No inventory items available. Add items in Inventory Tracking first.")
    else:
        # Packaging types
        packaging_types = ["Singles", "Shipping (S)", "In-store (I)"]
        
        # Show all finished goods with their recipes
        for fg in finished_goods:
            fg_id = fg["id"]
            fg_name = fg["name"]
            
            # Get existing recipes for this finished good (all packaging types)
            all_recipes = {}
            if "production_recipes" in db.table_names():
                for recipe in db["production_recipes"].rows_where("finished_good_id = ?", [fg_id]):
                    pkg_type = recipe.get("packaging_type", "Singles")
                    if pkg_type not in all_recipes:
                        all_recipes[pkg_type] = {}
                    all_recipes[pkg_type][recipe["inventory_item_id"]] = recipe
            
            # Check which packaging types have recipes configured
            configured_types = [pkg for pkg in packaging_types if pkg in all_recipes and len(all_recipes[pkg]) > 0]
            missing_types = [pkg for pkg in packaging_types if pkg not in configured_types]
            
            with st.expander(f"📦 {fg_name} - Configure Recipes", expanded=len(configured_types) == 0):
                if missing_types:
                    st.warning(f"⚠️ Missing recipes for: {', '.join(missing_types)}")
                
                # Create tabs for each packaging type
                tabs = st.tabs(packaging_types)
                
                for idx, packaging_type in enumerate(packaging_types):
                    with tabs[idx]:
                        st.markdown(f"### {packaging_type} Packaging")
                        
                        # Get existing recipes for this packaging type
                        existing_recipes = all_recipes.get(packaging_type, {})
                        
                        if packaging_type == "Singles":
                            st.caption("🔹 Bottles, lids, labels (no boxes or trays)")
                        elif packaging_type == "Shipping (S)":
                            st.caption("🔹 4pack boxes, shrink film, shipping materials")
                        else:  # In-store (I)
                            st.caption("🔹 Case trays, in-store boxes, packaging")
                        
                        # Create a form for this packaging type
                        with st.form(f"recipe_form_{fg_id}_{packaging_type}"):
                            recipe_data = []
                            
                            # Display all inventory items with input fields
                            for inv_item in inventory_items:
                                inv_id = inv_item["id"]
                                inv_name = inv_item["item_name"]
                                units_per_case = inv_item.get("units_per_case", 1)
                                
                                # Get existing recipe values or defaults
                                existing_recipe = existing_recipes.get(inv_id)
                                default_qty = existing_recipe["qty_per_case"] if existing_recipe else 0.0
                                default_wastage = existing_recipe.get("wastage_factor", 0.0) if existing_recipe else 0.0
                                
                                # Single row with all info
                                col1, col2, col3 = st.columns([4, 2, 2])
                                with col1:
                                    st.write(f"**{inv_name}**")
                                    st.caption(f"Units per case: {units_per_case}")
                                with col2:
                                    qty = st.number_input(
                                        "Quantity per Case",
                                        min_value=0.0,
                                        value=default_qty,
                                        step=0.1,
                                        key=f"qty_{fg_id}_{packaging_type}_{inv_id}",
                                        help=f"How many {inv_name} used per case of {fg_name}"
                                    )
                                with col3:
                                    wastage = st.number_input(
                                        "Wastage %",
                                        min_value=0.0,
                                        max_value=100.0,
                                        value=default_wastage * 100,
                                        step=0.1,
                                        key=f"wastage_{fg_id}_{packaging_type}_{inv_id}",
                                        help=f"Expected wastage percentage"
                                    )
                                
                                if qty > 0:  # Only include items with quantity > 0
                                    recipe_data.append({
                                        "inventory_item_id": inv_id,
                                        "qty_per_case": qty,
                                        "wastage_factor": wastage / 100.0
                                    })
                            
                            col1, col2 = st.columns([1, 4])
                            with col1:
                                if st.form_submit_button("💾 Save Recipe", use_container_width=True):
                                    # Delete existing recipes for this finished good + packaging type
                                    if "production_recipes" in db.table_names():
                                        for recipe in db["production_recipes"].rows_where(
                                            "finished_good_id = ? AND packaging_type = ?", 
                                            [fg_id, packaging_type]
                                        ):
                                            db["production_recipes"].delete(recipe["id"])
                                    
                                    # Insert new recipes
                                    for recipe_item in recipe_data:
                                        db["production_recipes"].insert({
                                            "finished_good_id": fg_id,
                                            "inventory_item_id": recipe_item["inventory_item_id"],
                                            "qty_per_case": recipe_item["qty_per_case"],
                                            "wastage_factor": recipe_item["wastage_factor"],
                                            "packaging_type": packaging_type
                                        })
                                    
                                    st.success(f"✅ Recipe saved for {fg_name} - {packaging_type}")
                                    st.rerun()
                            
                            # Show current recipe summary
                            if existing_recipes:
                                st.divider()
                                st.caption(f"**Current recipe summary for {packaging_type}:**")
                                summary_data = []
                                for inv_id, recipe in existing_recipes.items():
                                    inv = db["inventory_tracking"].get(inv_id)
                                    if inv:
                                        summary_data.append({
                                            "Item": inv['item_name'],
                                            "Qty per Case": recipe['qty_per_case'],
                                            "Wastage": f"{recipe.get('wastage_factor', 0.0):.1%}",
                                            "Units": inv.get('units_per_case', 'N/A')
                                        })
                                if summary_data:
                                    st.dataframe(pd.DataFrame(summary_data), use_container_width=True, hide_index=True)

elif page == "⚙️ Production":
    st.header("⚙️ Production Management")
    
    db = get_db()
    
    # Get list of finished goods
    finished_goods = list(db["finished_goods"].rows)
    
    if not finished_goods:
        st.warning("No finished goods available. Please add finished goods first.")
    else:
        with st.form("production_form"):
            # Get unique cocktail names (without GLASS/PET suffix)
            cocktail_names = sorted(list(set([fg["name"].replace(" (GLASS)", "").replace(" (PET)", "") for fg in finished_goods])))
            
            # Production details - First row
            col1, col2, col3 = st.columns(3)
            with col1:
                selected_cocktail = st.selectbox("Select Cocktail", cocktail_names)
            with col2:
                bottle_type = st.selectbox("Bottle Type", ["GLASS", "PET"])
            with col3:
                production_date = st.date_input("Production Date", value=datetime.now().date())
            
            # Second row
            col4, col5 = st.columns(2)
            with col4:
                cases_produced = st.number_input("Number of Cases Produced", min_value=1, value=1, step=1)
            with col5:
                packaging_type = st.selectbox("Packaging Type", ["Shipping (S)", "In-store (I)", "Singles"])
            
            # Optional batch tracking
            col6, col7 = st.columns(2)
            with col6:
                batches = list(db["batches"].rows)
                batch_options = ["(No Batch)"] + [batch["name"] for batch in batches]
                selected_batch = st.selectbox("Source Batch (optional)", batch_options)
            with col7:
                production_notes = st.text_input("Notes (optional)", "")
            
            submitted = st.form_submit_button("Record Production")
            
            if submitted:
                # Construct the full product name
                selected_product = f"{selected_cocktail} ({bottle_type})"
                
                # Find the selected finished good
                selected_fg = next((fg for fg in finished_goods if fg["name"] == selected_product), None)
                
                if not selected_fg:
                    st.error(f"Product '{selected_product}' not found in database.")
                else:
                    fg_id = selected_fg["id"]
                    
                    # Update finished goods stock based on packaging type
                    if packaging_type == "Singles":
                        # Add to singles
                        new_singles = selected_fg["singles"] + (cases_produced * UNITS_PER_CASE)
                        new_bottled_s = selected_fg["bottled_s"]
                        new_bottled_i = selected_fg["bottled_i"]
                    elif packaging_type == "Shipping (S)":
                        new_singles = selected_fg["singles"]
                        new_bottled_s = selected_fg["bottled_s"] + cases_produced
                        new_bottled_i = selected_fg["bottled_i"]
                    else:  # In-store (I)
                        new_singles = selected_fg["singles"]
                        new_bottled_s = selected_fg["bottled_s"]
                        new_bottled_i = selected_fg["bottled_i"] + cases_produced
                    
                    # Recalculate totals via helper
                    update_finished_good(
                        db,
                        fg_id,
                        singles=new_singles,
                        bottled_s=new_bottled_s,
                        bottled_i=new_bottled_i,
                        abv=selected_fg.get("abv", 0.0)
                    )
                    
                    # Calculate production metrics for history
                    units_produced = cases_produced * UNITS_PER_CASE
                    abv = selected_fg.get("abv", 0.0)
                    total_gallons = cases_produced * GALLONS_PER_CASE
                    proof = abv * 2
                    proof_gallons_produced = calculate_proof_gallons(total_gallons, proof) if abv > 0 else 0.0
                    excise_tax_incurred = calculate_excise_tax(proof_gallons_produced) if proof_gallons_produced > 0 else 0.0
                    
                    batch_name = selected_batch if selected_batch != "(No Batch)" else ""
                    
                    # Record production history
                    db["production_history"].insert({
                        "production_date": production_date.strftime("%Y-%m-%d"),
                        "finished_good_id": fg_id,
                        "finished_good_name": selected_product,
                        "cases_produced": cases_produced,
                        "packaging_type": packaging_type,
                        "units_produced": units_produced,
                        "proof_gallons_produced": proof_gallons_produced,
                        "excise_tax_incurred": excise_tax_incurred,
                        "batch_name": batch_name,
                        "notes": production_notes
                    })
                    
                    # Get production recipes for this finished good + packaging type
                    production_recipes = []
                    if "production_recipes" in db.table_names():
                        production_recipes = list(db["production_recipes"].rows_where(
                            "finished_good_id = ? AND packaging_type = ?", 
                            [fg_id, packaging_type]
                        ))
                    
                    # Validate recipe exists
                    if not production_recipes:
                        st.warning(f"⚠️ No recipe configured for {selected_product} - {packaging_type}. Please configure the recipe in the Recipes page.")
                    
                    # Update inventory_tracking items based on production
                    inventory_updates_made = []
                    for recipe in production_recipes:
                        inventory_item_id = recipe["inventory_item_id"]
                        qty_per_case = recipe.get("qty_per_case", 1.0)
                        wastage_factor = recipe.get("wastage_factor", 0.0)
                        
                        # Get inventory item details
                        inventory_item = db["inventory_tracking"].get(inventory_item_id)
                        if not inventory_item:
                            continue
                        
                        # Calculate depletion: cases_produced * qty_per_case * (1 + wastage_factor)
                        depletion = cases_produced * qty_per_case * (1 + wastage_factor)
                        depletion_int = int(round(depletion))
                        
                        # Update depleted field (add to existing depletion)
                        new_depleted = inventory_item["depleted"] + depletion_int
                        
                        # Recalculate units_remaining and cases_remaining
                        new_units_remaining = inventory_item["started"] - new_depleted + inventory_item["added"]
                        new_cases_remaining = int(new_units_remaining / inventory_item["units_per_case"]) if inventory_item["units_per_case"] > 0 else 0
                        
                        # Update inventory item
                        db["inventory_tracking"].update(inventory_item_id, {
                            "depleted": new_depleted,
                            "units_remaining": new_units_remaining,
                            "cases_remaining": new_cases_remaining
                        })
                        
                        inventory_updates_made.append({
                            "item": inventory_item["item_name"],
                            "depletion": f"{depletion_int}",
                            "units_remaining": new_units_remaining,
                            "cases_remaining": new_cases_remaining
                        })
                
                # Also handle old raw_materials recipes (for backward compatibility)
                recipes = list(db["recipes"].rows_where("finished_good_id = ?", [fg_id]))
                raw_material_updates = []
                for recipe in recipes:
                    raw_material_id = recipe["raw_material_id"]
                    qty_per_case = recipe["qty_per_case"]
                    
                    # Get raw material details
                    raw_material = db["raw_materials"].get(raw_material_id)
                    if not raw_material:
                        continue
                    
                    wastage_factor = raw_material.get("wastage_factor", 0.0)
                    current_rm_stock = raw_material.get("current_stock", 0)
                    
                    # Calculate depletion
                    depletion = cases_produced * qty_per_case * (1 + wastage_factor)
                    new_rm_stock = max(0, current_rm_stock - int(depletion))
                    new_depleted = raw_material.get("depleted", 0) + int(depletion)
                    new_units_remaining = raw_material.get("started", 0) - new_depleted + raw_material.get("added", 0)
                    new_cases_remaining = int(new_units_remaining / raw_material.get("units_per_case", 24)) if raw_material.get("units_per_case", 24) > 0 else 0
                    
                    # Update raw material stock
                    db["raw_materials"].update(raw_material_id, {
                        "current_stock": new_rm_stock,
                        "depleted": new_depleted,
                        "units_remaining": new_units_remaining,
                        "cases_remaining": new_cases_remaining
                    })
                    
                    raw_material_updates.append({
                        "material": raw_material["name"],
                        "depletion": f"{depletion:.2f}",
                        "new_stock": new_rm_stock
                    })
                
                    updates_made = inventory_updates_made + raw_material_updates
                    
                    st.success(f"✅ Production recorded successfully!")
                    
                    # Display appropriate message based on packaging type
                    if packaging_type == "Singles":
                        st.info(f"**{selected_product}** - Added {cases_produced * UNITS_PER_CASE} singles. Total Singles: {new_singles}, Bottled (S): {new_bottled_s}, Bottled (I): {new_bottled_i}")
                    else:
                        st.info(f"**{selected_product}** - Added {cases_produced} case(s) to {packaging_type}. Singles: {new_singles}, Bottled (S): {new_bottled_s}, Bottled (I): {new_bottled_i}")
                    
                    if inventory_updates_made:
                        st.subheader("📦 Inventory Depletions:")
                        st.caption(f"Formula: Depletion = Cases Produced ({cases_produced}) × Qty per Case × (1 + Wastage %)")
                        inv_df = pd.DataFrame(inventory_updates_made)
                        st.dataframe(inv_df, use_container_width=True, hide_index=True)
                        st.caption("✅ Inventory items have been deducted. **Units Remaining** and **Cases Remaining** are automatically recalculated. Leftover units are preserved.")
                    
                    if raw_material_updates:
                        st.subheader("Raw Material Updates:")
                        rm_df = pd.DataFrame(raw_material_updates)
                        st.dataframe(rm_df, use_container_width=True, hide_index=True)
                    
                    if not inventory_updates_made and not raw_material_updates:
                        st.warning(f"⚠️ No production recipes configured for {selected_product} - {packaging_type}. Configure recipes in the Recipes page to automatically deduct inventory items.")
        
        # Display current finished goods stock
        st.subheader("Current Finished Goods Stock")
        fg_data = [{
            "Name": fg["name"],
            "Singles": fg["singles"],
            "Bottled (S)": fg["bottled_s"],
            "Bottled (I)": fg["bottled_i"],
            "All Cases": f"{(fg['singles'] + fg['bottled_s'] * UNITS_PER_CASE + fg['bottled_i'] * UNITS_PER_CASE) / UNITS_PER_CASE:.1f}",
            "ABV": f"{fg['abv']:.2f}%" if fg["abv"] > 0 else "N/A"
        } for fg in finished_goods]
        st.dataframe(pd.DataFrame(fg_data), use_container_width=True, hide_index=True)

elif page == "📈 Reports & Analytics":
    st.header("📈 Reports & Analytics")
    st.caption("Comprehensive business intelligence for investors, tax reporting, and operations")
    
    db = get_db()
    
    # Create tabs for different report types
    report_tab = st.tabs([
        "📊 Investor Dashboard",
        "💰 Monthly Sales",
        "🏛️ Tax & TTB Reports",
        "🏭 Production Reports",
        "📦 Inventory Snapshots"
    ])
    
    with report_tab[0]:  # Investor Dashboard
        st.subheader("📊 Investor Dashboard")
        
        # Date range for analysis
        col1, col2 = st.columns(2)
        with col1:
            period_options = ["Last 30 Days", "Last 90 Days", "Last 6 Months", "Last 12 Months", "Year to Date", "All Time"]
            selected_period = st.selectbox("Period", period_options, index=2)
        
        # Calculate date ranges
        from datetime import datetime, timedelta
        today = datetime.now()
        
        if selected_period == "Last 30 Days":
            start_date = (today - timedelta(days=30)).strftime("%Y-%m-%d")
        elif selected_period == "Last 90 Days":
            start_date = (today - timedelta(days=90)).strftime("%Y-%m-%d")
        elif selected_period == "Last 6 Months":
            start_date = (today - timedelta(days=180)).strftime("%Y-%m-%d")
        elif selected_period == "Last 12 Months":
            start_date = (today - timedelta(days=365)).strftime("%Y-%m-%d")
        elif selected_period == "Year to Date":
            start_date = f"{today.year}-01-01"
        else:  # All Time
            start_date = "2000-01-01"
        
        end_date = today.strftime("%Y-%m-%d")
        
        # Key Metrics
        st.markdown("### Key Performance Indicators")
        
        # Get shipped orders in period
        shipped_orders = list(db["orders"].rows_where(
            "status = 'Shipped' AND shipped_date >= ? AND shipped_date <= ?",
            [start_date, end_date]
        ))
        
        total_revenue = sum([o.get("total_revenue", 0.0) for o in shipped_orders])
        total_orders = len(shipped_orders)
        
        # Calculate cases sold
        total_cases_sold = 0
        for order in shipped_orders:
            order_items = list(db["order_items"].rows_where("order_id = ?", [order["id"]]))
            total_cases_sold += sum([item["quantity_cases"] for item in order_items])
        
        # Get production stats
        production_records = list(db["production_history"].rows_where(
            "production_date >= ? AND production_date <= ?",
            [start_date, end_date]
        ))
        
        total_cases_produced = sum([p["cases_produced"] for p in production_records])
        total_proof_gallons_produced = sum([p.get("proof_gallons_produced", 0.0) for p in production_records])
        
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Revenue",
                f"${total_revenue:,.2f}",
                help="Total revenue from shipped orders"
            )
        
        with col2:
            st.metric(
                "Cases Sold",
                f"{total_cases_sold:,}",
                help="Total cases shipped"
            )
        
        with col3:
            st.metric(
                "Orders Shipped",
                f"{total_orders}",
                help="Number of orders shipped"
            )
        
        with col4:
            avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
            st.metric(
                "Avg Order Value",
                f"${avg_order_value:,.2f}",
                help="Average revenue per order"
            )
        
        # Production metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Cases Produced",
                f"{total_cases_produced:,}",
                help="Total cases produced"
            )
        
        with col2:
            st.metric(
                "Proof Gallons Produced",
                f"{total_proof_gallons_produced:,.2f}",
                help="Total proof gallons produced"
            )
        
        with col3:
            # Current inventory
            finished_goods = list(db["finished_goods"].rows)
            current_stock = sum([fg.get("current_stock", 0) for fg in finished_goods])
            st.metric(
                "Current Inventory",
                f"{current_stock:,} cases",
                help="Current finished goods inventory"
            )
        
        with col4:
            # Days of inventory
            daily_sales = total_cases_sold / 30 if total_cases_sold > 0 else 1
            days_of_inventory = current_stock / daily_sales if daily_sales > 0 else 0
            st.metric(
                "Days of Inventory",
                f"{days_of_inventory:.0f} days",
                help="Days of inventory on hand"
            )
        
        # Revenue trend chart
        st.markdown("### Revenue Trend")
        if shipped_orders:
            # Group by month
            monthly_revenue = {}
            for order in shipped_orders:
                month = order.get("shipped_date", "")[:7]  # YYYY-MM
                if month:
                    if month not in monthly_revenue:
                        monthly_revenue[month] = 0
                    monthly_revenue[month] += order.get("total_revenue", 0.0)
            
            if monthly_revenue:
                chart_data = pd.DataFrame([
                    {"Month": month, "Revenue": revenue}
                    for month, revenue in sorted(monthly_revenue.items())
                ])
                st.line_chart(chart_data.set_index("Month"))
        else:
            st.info("No sales data available for the selected period")
        
        # Top products by revenue
        st.markdown("### Top Products by Revenue")
        product_revenue = {}
        for order in shipped_orders:
            order_items = list(db["order_items"].rows_where("order_id = ?", [order["id"]]))
            for item in order_items:
                product = item["product_name"]
                if product not in product_revenue:
                    product_revenue[product] = {"revenue": 0, "cases": 0}
                product_revenue[product]["revenue"] += item.get("line_total", 0.0)
                product_revenue[product]["cases"] += item["quantity_cases"]
        
        if product_revenue:
            product_data = []
            for product, data in product_revenue.items():
                product_data.append({
                    "Product": product,
                    "Revenue": f"${data['revenue']:,.2f}",
                    "Cases Sold": data['cases'],
                    "Avg Price/Case": f"${data['revenue']/data['cases']:.2f}" if data['cases'] > 0 else "$0.00"
                })
            
            product_df = pd.DataFrame(product_data)
            product_df = product_df.sort_values("Revenue", key=lambda x: x.str.replace('$', '').str.replace(',', '').astype(float), ascending=False)
            st.dataframe(product_df, use_container_width=True, hide_index=True)
        
        # Top customers
        st.markdown("### Top Customers")
        customer_revenue = {}
        for order in shipped_orders:
            customer = order["customer_name"]
            if customer not in customer_revenue:
                customer_revenue[customer] = {"revenue": 0, "orders": 0}
            customer_revenue[customer]["revenue"] += order.get("total_revenue", 0.0)
            customer_revenue[customer]["orders"] += 1
        
        if customer_revenue:
            customer_data = []
            for customer, data in customer_revenue.items():
                customer_data.append({
                    "Customer": customer,
                    "Revenue": f"${data['revenue']:,.2f}",
                    "Orders": data['orders'],
                    "Avg Order": f"${data['revenue']/data['orders']:.2f}"
                })
            
            customer_df = pd.DataFrame(customer_data)
            customer_df = customer_df.sort_values("Revenue", key=lambda x: x.str.replace('$', '').str.replace(',', '').astype(float), ascending=False)
            st.dataframe(customer_df.head(10), use_container_width=True, hide_index=True)
        
        # Export button
        if st.button("📥 Export Investor Report (CSV)"):
            report_data = {
                "Period": selected_period,
                "Start Date": start_date,
                "End Date": end_date,
                "Total Revenue": total_revenue,
                "Total Cases Sold": total_cases_sold,
                "Total Orders": total_orders,
                "Average Order Value": avg_order_value,
                "Cases Produced": total_cases_produced,
                "Proof Gallons Produced": total_proof_gallons_produced,
                "Current Inventory (cases)": current_stock
            }
            
            report_df = pd.DataFrame([report_data])
            csv = report_df.to_csv(index=False)
            st.download_button(
                label="Download Report",
                data=csv,
                file_name=f"investor_report_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
    
    with report_tab[1]:  # Monthly Sales
        st.subheader("💰 Monthly Sales Report")
        
        # Month/year selector
        col1, col2 = st.columns(2)
        with col1:
            report_year = st.selectbox("Year", list(range(datetime.now().year, datetime.now().year - 5, -1)))
        with col2:
            report_month = st.selectbox("Month", list(range(1, 13)), index=datetime.now().month - 1)
        
        # Calculate date range
        from calendar import monthrange
        start_date = f"{report_year}-{report_month:02d}-01"
        last_day = monthrange(report_year, report_month)[1]
        end_date = f"{report_year}-{report_month:02d}-{last_day}"
        
        st.markdown(f"### Sales Report: {start_date} to {end_date}")
        
        # Get shipped orders
        shipped_orders = list(db["orders"].rows_where(
            "status = 'Shipped' AND shipped_date >= ? AND shipped_date <= ?",
            [start_date, end_date]
        ))
        
        if shipped_orders:
            # Summary metrics
            total_revenue = sum([o.get("total_revenue", 0.0) for o in shipped_orders])
            total_orders = len(shipped_orders)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Revenue", f"${total_revenue:,.2f}")
            with col2:
                st.metric("Orders Shipped", total_orders)
            with col3:
                avg_order = total_revenue / total_orders if total_orders > 0 else 0
                st.metric("Avg Order Value", f"${avg_order:,.2f}")
            
            # Sales by product
            st.markdown("#### Sales by Product")
            product_sales = {}
            for order in shipped_orders:
                order_items = list(db["order_items"].rows_where("order_id = ?", [order["id"]]))
                for item in order_items:
                    product = item["product_name"]
                    if product not in product_sales:
                        product_sales[product] = {"cases": 0, "revenue": 0.0}
                    product_sales[product]["cases"] += item["quantity_cases"]
                    product_sales[product]["revenue"] += item.get("line_total", 0.0)
            
            sales_data = []
            for product, data in product_sales.items():
                sales_data.append({
                    "Product": product,
                    "Cases Sold": data["cases"],
                    "Revenue": f"${data['revenue']:,.2f}",
                    "Avg Price/Case": f"${data['revenue']/data['cases']:.2f}" if data['cases'] > 0 else "$0.00"
                })
            
            sales_df = pd.DataFrame(sales_data)
            st.dataframe(sales_df, use_container_width=True, hide_index=True)
            
            # Export
            if st.button("📥 Export Monthly Sales (CSV)", key="export_monthly_sales"):
                csv = sales_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"sales_report_{report_year}_{report_month:02d}.csv",
                    mime="text/csv"
                )
        else:
            st.info(f"No sales recorded for {report_year}-{report_month:02d}")
    
    with report_tab[2]:  # Tax & TTB Reports
        st.subheader("🏛️ Tax & TTB Compliance Reports")
        
        # Month/year selector
        col1, col2 = st.columns(2)
        with col1:
            tax_year = st.selectbox("Year", list(range(datetime.now().year, datetime.now().year - 5, -1)), key="tax_year")
        with col2:
            tax_month = st.selectbox("Month", list(range(1, 13)), index=datetime.now().month - 1, key="tax_month")
        
        from calendar import monthrange
        start_date_tax = f"{tax_year}-{tax_month:02d}-01"
        last_day_tax = monthrange(tax_year, tax_month)[1]
        end_date_tax = f"{tax_year}-{tax_month:02d}-{last_day_tax}"
        
        st.markdown(f"### Tax Report: {start_date_tax} to {end_date_tax}")
        
        # Get beginning inventory (from previous month snapshot or current)
        prev_month = tax_month - 1 if tax_month > 1 else 12
        prev_year = tax_year if tax_month > 1 else tax_year - 1
        prev_month_str = f"{prev_year}-{prev_month:02d}"
        
        snapshots = list(db["monthly_snapshots"].rows_where("snapshot_month = ?", [prev_month_str]))
        beginning_proof_gallons = snapshots[0].get("total_proof_gallons", 0.0) if snapshots else 0.0
        
        # Get production in period
        production_records = list(db["production_history"].rows_where(
            "production_date >= ? AND production_date <= ?",
            [start_date_tax, end_date_tax]
        ))
        
        proof_gallons_produced = sum([p.get("proof_gallons_produced", 0.0) for p in production_records])
        excise_tax_from_production = sum([p.get("excise_tax_incurred", 0.0) for p in production_records])
        
        # Get current inventory
        finished_goods = list(db["finished_goods"].rows)
        ending_proof_gallons = sum([fg.get("proof_gallons", 0.0) for fg in finished_goods])
        
        # Calculate removals (shipped)
        proof_gallons_removed = beginning_proof_gallons + proof_gallons_produced - ending_proof_gallons
        
        # Get bulk spirits inventory (beginning and ending)
        bulk_spirits_current = list(db["bulk_spirits"].rows)
        bulk_spirits_weight = sum([s.get("weight_lbs", 0.0) for s in bulk_spirits_current])
        bulk_spirits_pg = sum([s.get("proof_gallons", 0.0) for s in bulk_spirits_current])
        
        # Get batch production in period
        batch_production = list(db["batch_production_log"].rows_where(
            "production_date >= ? AND production_date <= ?",
            [start_date_tax, end_date_tax]
        ))
        batch_pg_produced = sum([b.get("proof_gallons", 0.0) for b in batch_production])
        
        # Display TTB report
        st.markdown("#### Excise Tax Summary")
        
        report_data = {
            "Beginning Inventory (Proof Gallons)": f"{beginning_proof_gallons:,.2f}",
            "Production (Proof Gallons)": f"{proof_gallons_produced:,.2f}",
            "Removals/Sales (Proof Gallons)": f"{proof_gallons_removed:,.2f}",
            "Ending Inventory (Proof Gallons)": f"{ending_proof_gallons:,.2f}",
            "Excise Tax Incurred": f"${excise_tax_from_production:,.2f}",
            "Excise Tax on Removals": f"${proof_gallons_removed * 13.50:,.2f}"
        }
        
        for label, value in report_data.items():
            st.text(f"{label}: {value}")
        
        # Bulk Spirits Inventory (Weight-Based)
        st.markdown("#### 🥃 Bulk Spirits Inventory (Weight-Based Tracking)")
        st.caption("Primary tracking by weight with auto-calculated proof gallons for TTB compliance")
        
        if bulk_spirits_current:
            bulk_data = []
            for spirit in bulk_spirits_current:
                bulk_data.append({
                    "Spirit": spirit["name"],
                    "Weight (lbs)": f"{spirit.get('weight_lbs', 0.0):,.2f}",
                    "Wine Gallons": f"{spirit.get('wine_gallons', 0.0):.2f}",
                    "ABV": f"{spirit.get('abv', 0.0):.2f}%",
                    "Proof Gallons": f"{spirit.get('proof_gallons', 0.0):.2f}"
                })
            
            bulk_df = pd.DataFrame(bulk_data)
            st.dataframe(bulk_df, use_container_width=True, hide_index=True)
            
            # Totals
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Weight", f"{bulk_spirits_weight:,.2f} lbs")
            with col2:
                total_wine_gal = sum([s.get("wine_gallons", 0.0) for s in bulk_spirits_current])
                st.metric("Total Wine Gallons", f"{total_wine_gal:,.2f}")
            with col3:
                st.metric("Total Proof Gallons", f"{bulk_spirits_pg:,.2f}")
        else:
            st.info("No bulk spirits in inventory")
        
        # Batch Production Detail
        if batch_production:
            st.markdown("#### 🔄 Batch Production (Weight-Based)")
            st.caption("Batches produced from bulk spirits during this period")
            
            batch_data = []
            for b in batch_production:
                batch_data.append({
                    "Date": b["production_date"],
                    "Batch": b["batch_name"],
                    "Weight (lbs)": f"{b['weight_produced_lbs']:.2f}",
                    "Gallons": f"{b['gallons_produced']:.2f}",
                    "ABV": f"{b['abv']:.2f}%",
                    "Proof Gallons": f"{b['proof_gallons']:.2f}"
                })
            
            st.dataframe(pd.DataFrame(batch_data), use_container_width=True, hide_index=True)
            
            st.info(f"💡 **Total batch production:** {sum([b['weight_produced_lbs'] for b in batch_production]):,.2f} lbs ({batch_pg_produced:.2f} proof gallons)")
        
        
        # Production detail
        st.markdown("#### Production Detail")
        if production_records:
            prod_data = []
            for p in production_records:
                prod_data.append({
                    "Date": p["production_date"],
                    "Product": p["finished_good_name"],
                    "Cases": p["cases_produced"],
                    "Proof Gallons": f"{p.get('proof_gallons_produced', 0.0):.2f}",
                    "Tax Incurred": f"${p.get('excise_tax_incurred', 0.0):.2f}"
                })
            
            prod_df = pd.DataFrame(prod_data)
            st.dataframe(prod_df, use_container_width=True, hide_index=True)
        else:
            st.info("No production recorded for this period")
        
        # Export TTB report
        if st.button("📥 Export TTB Report (CSV)", key="export_ttb"):
            ttb_data = pd.DataFrame([{
                "Report Period": f"{tax_year}-{tax_month:02d}",
                "Beginning Inventory (PG)": beginning_proof_gallons,
                "Production (PG)": proof_gallons_produced,
                "Removals (PG)": proof_gallons_removed,
                "Ending Inventory (PG)": ending_proof_gallons,
                "Excise Tax Incurred": excise_tax_from_production,
                "Excise Tax on Removals": proof_gallons_removed * 13.50
            }])
            
            csv = ttb_data.to_csv(index=False)
            st.download_button(
                label="Download TTB Report",
                data=csv,
                file_name=f"ttb_report_{tax_year}_{tax_month:02d}.csv",
                mime="text/csv"
            )
    
    with report_tab[3]:  # Production Reports
        st.subheader("🏭 Production Reports")
        
        # Date range
        col1, col2 = st.columns(2)
        with col1:
            prod_start = st.date_input("Start Date", value=datetime.now().date().replace(day=1), key="prod_start")
        with col2:
            prod_end = st.date_input("End Date", value=datetime.now().date(), key="prod_end")
        
        # Get production records
        production_records = list(db["production_history"].rows_where(
            "production_date >= ? AND production_date <= ?",
            [prod_start.strftime("%Y-%m-%d"), prod_end.strftime("%Y-%m-%d")]
        ))
        
        if production_records:
            # Summary
            total_cases = sum([p["cases_produced"] for p in production_records])
            total_pg = sum([p.get("proof_gallons_produced", 0.0) for p in production_records])
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Cases Produced", f"{total_cases:,}")
            with col2:
                st.metric("Total Proof Gallons", f"{total_pg:,.2f}")
            with col3:
                days = (prod_end - prod_start).days + 1
                st.metric("Avg Cases/Day", f"{total_cases/days:.1f}")
            
            # Production by product
            st.markdown("### Production by Product")
            product_prod = {}
            for p in production_records:
                product = p["finished_good_name"]
                if product not in product_prod:
                    product_prod[product] = {"cases": 0, "pg": 0.0}
                product_prod[product]["cases"] += p["cases_produced"]
                product_prod[product]["pg"] += p.get("proof_gallons_produced", 0.0)
            
            prod_data = []
            for product, data in product_prod.items():
                prod_data.append({
                    "Product": product,
                    "Cases Produced": data["cases"],
                    "Proof Gallons": f"{data['pg']:.2f}"
                })
            
            prod_df = pd.DataFrame(prod_data)
            st.dataframe(prod_df, use_container_width=True, hide_index=True)
            
            # Production timeline
            st.markdown("### Production Timeline")
            timeline_data = []
            for p in production_records:
                timeline_data.append({
                    "Date": p["production_date"],
                    "Product": p["finished_good_name"],
                    "Cases": p["cases_produced"],
                    "Type": p["packaging_type"],
                    "Batch": p.get("batch_name", "—"),
                    "Notes": p.get("notes", "—")
                })
            
            timeline_df = pd.DataFrame(timeline_data)
            st.dataframe(timeline_df, use_container_width=True, hide_index=True)
            
            # Export
            if st.button("📥 Export Production Report (CSV)", key="export_prod"):
                csv = timeline_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"production_report_{prod_start}_{prod_end}.csv",
                    mime="text/csv"
                )
        else:
            st.info("No production records found for the selected date range")
    
    with report_tab[4]:  # Inventory Snapshots
        st.subheader("📦 Inventory Snapshots")
        
        # Create snapshot button
        col1, col2 = st.columns([1, 3])
        with col1:
            if st.button("📸 Create Snapshot Now", type="primary"):
                snapshot_month = create_monthly_snapshot(db)
                st.success(f"✅ Snapshot created for {snapshot_month}!")
                st.rerun()
        
        # Show snapshot history
        snapshots = list(db["monthly_snapshots"].rows)
        
        if snapshots:
            snapshots_sorted = sorted(snapshots, key=lambda x: x["snapshot_date"], reverse=True)
            
            st.markdown("### Snapshot History")
            
            for snapshot in snapshots_sorted[:12]:  # Show last 12 snapshots
                with st.expander(f"📅 {snapshot['snapshot_month']} - {snapshot['total_finished_cases']} cases, {snapshot['total_proof_gallons']:.2f} PG"):
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric("Total Cases", f"{snapshot['total_finished_cases']:,}")
                    with col2:
                        st.metric("Total Proof Gallons", f"{snapshot['total_proof_gallons']:,.2f}")
                    with col3:
                        st.metric("Excise Tax Liability", f"${snapshot['total_excise_tax_liability']:,.2f}")
                    
                    # Show detailed inventory
                    import json
                    fg_data = json.loads(snapshot['finished_goods_json'])
                    
                    if fg_data:
                        display_data = []
                        for fg in fg_data:
                            display_data.append({
                                "Product": fg['name'],
                                "Stock (cases)": fg.get('current_stock', 0),
                                "Proof Gallons": f"{fg.get('proof_gallons', 0.0):.2f}",
                                "Tax Due": f"${fg.get('excise_tax_due', 0.0):.2f}"
                            })
                        
                        st.dataframe(pd.DataFrame(display_data), use_container_width=True, hide_index=True)
        else:
            st.info("No snapshots created yet. Click 'Create Snapshot Now' to save current inventory state.")

elif page == "🔍 Physical Counts & Waste":
    st.header("🔍 Physical Inventory Counts & Waste Tracking")
    st.caption("Track actual physical counts vs system counts to identify waste, shrinkage, and variances")
    
    db = get_db()
    
    # Create tabs for different count types
    tab1, tab2, tab3 = st.tabs(["📦 Finished Goods Count", "📋 Inventory Items Count", "📊 Waste Analysis"])
    
    with tab1:
        st.subheader("Physical Count - Finished Goods")
        
        # Count entry form
        with st.expander("➕ Record New Physical Count", expanded=True):
            with st.form("physical_count_fg"):
                count_date = st.date_input("Count Date", value=datetime.now().date())
                
                st.write("**Enter actual physical counts for each product:**")
                
                finished_goods = list(db["finished_goods"].rows)
                count_data = []
                
                for fg in finished_goods:
                    st.markdown(f"**{fg['name']}**")
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.caption(f"System: {fg['singles']} singles")
                        actual_singles = st.number_input(
                            "Actual Singles",
                            min_value=0,
                            value=fg['singles'],
                            key=f"count_singles_{fg['id']}",
                            label_visibility="collapsed"
                        )
                    
                    with col2:
                        st.caption(f"System: {fg['bottled_s']} Bottled (S)")
                        actual_bottled_s = st.number_input(
                            "Actual Bottled (S)",
                            min_value=0,
                            value=fg['bottled_s'],
                            key=f"count_bottled_s_{fg['id']}",
                            label_visibility="collapsed"
                        )
                    
                    with col3:
                        st.caption(f"System: {fg['bottled_i']} Bottled (I)")
                        actual_bottled_i = st.number_input(
                            "Actual Bottled (I)",
                            min_value=0,
                            value=fg['bottled_i'],
                            key=f"count_bottled_i_{fg['id']}",
                            label_visibility="collapsed"
                        )
                    
                    with col4:
                        notes = st.text_input(
                            "Notes",
                            key=f"count_notes_{fg['id']}",
                            placeholder="Reason for variance...",
                            label_visibility="collapsed"
                        )
                    
                    # Calculate variance
                    system_units = fg['singles'] + (fg['bottled_s'] * UNITS_PER_CASE) + (fg['bottled_i'] * UNITS_PER_CASE)
                    actual_units = actual_singles + (actual_bottled_s * UNITS_PER_CASE) + (actual_bottled_i * UNITS_PER_CASE)
                    variance_units = actual_units - system_units
                    variance_cases = variance_units / UNITS_PER_CASE
                    variance_percentage = (variance_units / system_units * 100) if system_units > 0 else 0
                    
                    count_data.append({
                        "fg_id": fg['id'],
                        "fg_name": fg['name'],
                        "system_singles": fg['singles'],
                        "actual_singles": actual_singles,
                        "system_bottled_s": fg['bottled_s'],
                        "actual_bottled_s": actual_bottled_s,
                        "system_bottled_i": fg['bottled_i'],
                        "actual_bottled_i": actual_bottled_i,
                        "variance_units": variance_units,
                        "variance_cases": variance_cases,
                        "variance_percentage": variance_percentage,
                        "notes": notes
                    })
                    
                    st.divider()
                
                col1, col2 = st.columns(2)
                with col1:
                    adjust_inventory = st.checkbox("Adjust system inventory to match physical count", value=False)
                with col2:
                    if st.form_submit_button("💾 Save Physical Count", type="primary", use_container_width=True):
                        # Save all counts
                        for count in count_data:
                            db["physical_inventory_counts"].insert({
                                "count_date": count_date.strftime("%Y-%m-%d"),
                                "finished_good_id": count["fg_id"],
                                "finished_good_name": count["fg_name"],
                                "system_singles": count["system_singles"],
                                "actual_singles": count["actual_singles"],
                                "system_bottled_s": count["system_bottled_s"],
                                "actual_bottled_s": count["actual_bottled_s"],
                                "system_bottled_i": count["system_bottled_i"],
                                "actual_bottled_i": count["actual_bottled_i"],
                                "variance_units": count["variance_units"],
                                "variance_cases": count["variance_cases"],
                                "variance_percentage": count["variance_percentage"],
                                "notes": count["notes"]
                            })
                            
                            # Optionally adjust inventory
                            if adjust_inventory:
                                fg = db["finished_goods"].get(count["fg_id"])
                                update_finished_good(
                                    db,
                                    count["fg_id"],
                                    singles=count["actual_singles"],
                                    bottled_s=count["actual_bottled_s"],
                                    bottled_i=count["actual_bottled_i"],
                                    abv=fg.get("abv", 0.0)
                                )
                        
                        if adjust_inventory:
                            st.success(f"✅ Physical count saved for {count_date.strftime('%Y-%m-%d')} and inventory adjusted!")
                        else:
                            st.success(f"✅ Physical count saved for {count_date.strftime('%Y-%m-%d')}!")
                        st.rerun()
        
        # Show count history
        st.subheader("Physical Count History")
        counts = list(db["physical_inventory_counts"].rows)
        if counts:
            # Get unique dates
            unique_dates = sorted(list(set([c['count_date'] for c in counts])), reverse=True)
            
            for date in unique_dates[:5]:  # Show last 5 counts
                date_counts = [c for c in counts if c['count_date'] == date]
                total_variance = sum([c['variance_cases'] for c in date_counts])
                
                with st.expander(f"📅 {date} - Total Variance: {total_variance:+.1f} cases"):
                    display_data = []
                    for c in date_counts:
                        if c['variance_units'] != 0:  # Only show items with variance
                            display_data.append({
                                "Product": c['finished_good_name'],
                                "System Units": c['system_singles'] + (c['system_bottled_s'] * UNITS_PER_CASE) + (c['system_bottled_i'] * UNITS_PER_CASE),
                                "Actual Units": c['actual_singles'] + (c['actual_bottled_s'] * UNITS_PER_CASE) + (c['actual_bottled_i'] * UNITS_PER_CASE),
                                "Variance": f"{c['variance_units']:+d}",
                                "Variance %": f"{c['variance_percentage']:+.2f}%",
                                "Notes": c['notes'] or "—"
                            })
                    
                    if display_data:
                        st.dataframe(pd.DataFrame(display_data), use_container_width=True, hide_index=True)
                    else:
                        st.info("No variances found - all counts matched system!")
        else:
            st.info("No physical counts recorded yet.")
    
    with tab2:
        st.subheader("Physical Count - Inventory Items")
        
        with st.expander("➕ Record Inventory Physical Count", expanded=True):
            with st.form("physical_count_inventory"):
                count_date_inv = st.date_input("Count Date", value=datetime.now().date(), key="inv_count_date")
                
                st.write("**Enter actual physical counts for inventory items:**")
                
                inventory_items = list(db["inventory_tracking"].rows)
                inv_count_data = []
                
                for item in inventory_items:
                    col1, col2, col3 = st.columns([3, 2, 2])
                    
                    with col1:
                        st.write(f"**{item['item_name']}**")
                        st.caption(f"System: {item['units_remaining']} units")
                    
                    with col2:
                        actual_units = st.number_input(
                            "Actual Units",
                            min_value=0,
                            value=item['units_remaining'],
                            key=f"inv_count_{item['id']}",
                            help=f"Enter actual unit count"
                        )
                    
                    with col3:
                        inv_notes = st.text_input(
                            "Notes",
                            key=f"inv_notes_{item['id']}",
                            placeholder="Variance reason...",
                            label_visibility="collapsed"
                        )
                    
                    variance = actual_units - item['units_remaining']
                    variance_pct = (variance / item['units_remaining'] * 100) if item['units_remaining'] > 0 else 0
                    
                    inv_count_data.append({
                        "item_id": item['id'],
                        "item_name": item['item_name'],
                        "system_units": item['units_remaining'],
                        "actual_units": actual_units,
                        "variance_units": variance,
                        "variance_percentage": variance_pct,
                        "notes": inv_notes
                    })
                
                col1, col2 = st.columns(2)
                with col1:
                    adjust_inv = st.checkbox("Adjust system inventory to match count", value=False, key="adjust_inv")
                with col2:
                    if st.form_submit_button("💾 Save Inventory Count", type="primary", use_container_width=True):
                        for count in inv_count_data:
                            db["inventory_physical_counts_raw"].insert({
                                "count_date": count_date_inv.strftime("%Y-%m-%d"),
                                "inventory_item_id": count["item_id"],
                                "inventory_item_name": count["item_name"],
                                "system_units": count["system_units"],
                                "actual_units": count["actual_units"],
                                "variance_units": count["variance_units"],
                                "variance_percentage": count["variance_percentage"],
                                "notes": count["notes"]
                            })
                            
                            if adjust_inv and count["variance_units"] != 0:
                                item = db["inventory_tracking"].get(count["item_id"])
                                # Adjust 'added' to bring units_remaining to actual count
                                new_added = count["actual_units"] - item["started"] + item["depleted"]
                                new_cases = int(count["actual_units"] / item["units_per_case"]) if item["units_per_case"] > 0 else 0
                                db["inventory_tracking"].update(count["item_id"], {
                                    "added": new_added,
                                    "units_remaining": count["actual_units"],
                                    "cases_remaining": new_cases
                                })
                        
                        if adjust_inv:
                            st.success(f"✅ Inventory count saved and adjusted!")
                        else:
                            st.success(f"✅ Inventory count saved!")
                        st.rerun()
        
        # Show inventory count history
        st.subheader("Inventory Count History")
        inv_counts = list(db["inventory_physical_counts_raw"].rows)
        if inv_counts:
            unique_dates = sorted(list(set([c['count_date'] for c in inv_counts])), reverse=True)
            
            for date in unique_dates[:5]:
                date_counts = [c for c in inv_counts if c['count_date'] == date]
                variance_items = [c for c in date_counts if c['variance_units'] != 0]
                
                with st.expander(f"📅 {date} - {len(variance_items)} items with variance"):
                    if variance_items:
                        display_data = [{
                            "Item": c['inventory_item_name'],
                            "System": c['system_units'],
                            "Actual": c['actual_units'],
                            "Variance": f"{c['variance_units']:+d}",
                            "Variance %": f"{c['variance_percentage']:+.2f}%",
                            "Notes": c['notes'] or "—"
                        } for c in variance_items]
                        st.dataframe(pd.DataFrame(display_data), use_container_width=True, hide_index=True)
                    else:
                        st.info("Perfect match - no variances!")
        else:
            st.info("No inventory counts recorded yet.")
    
    with tab3:
        st.subheader("📊 Waste & Variance Analysis")
        
        # Analysis period selector
        col1, col2 = st.columns(2)
        with col1:
            analysis_months = st.slider("Analysis Period (months)", 1, 12, 3)
        
        # Analyze finished goods waste
        st.markdown("### Finished Goods Waste")
        fg_counts = list(db["physical_inventory_counts"].rows)
        
        if fg_counts:
            # Calculate totals
            total_variance_units = sum([c['variance_units'] for c in fg_counts])
            total_variance_cases = sum([c['variance_cases'] for c in fg_counts])
            
            # Group by product
            product_waste = {}
            for count in fg_counts:
                name = count['finished_good_name']
                if name not in product_waste:
                    product_waste[name] = {"variance_units": 0, "count": 0}
                product_waste[name]["variance_units"] += count['variance_units']
                product_waste[name]["count"] += 1
            
            # Display metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Variance (Units)", f"{total_variance_units:+d}")
            with col2:
                st.metric("Total Variance (Cases)", f"{total_variance_cases:+.1f}")
            with col3:
                total_counts = len(set([c['count_date'] for c in fg_counts]))
                avg_variance = total_variance_cases / total_counts if total_counts > 0 else 0
                st.metric("Avg Variance per Count", f"{avg_variance:+.2f} cases")
            
            # Products with highest waste
            st.markdown("#### Products with Highest Variance")
            waste_summary = []
            for name, data in product_waste.items():
                avg_variance = data["variance_units"] / data["count"]
                waste_summary.append({
                    "Product": name,
                    "Total Variance": f"{data['variance_units']:+d}",
                    "Avg Variance": f"{avg_variance:+.1f}",
                    "Count Events": data["count"]
                })
            
            waste_df = pd.DataFrame(waste_summary)
            waste_df = waste_df.sort_values("Total Variance", key=lambda x: x.str.replace('+', '').astype(int))
            st.dataframe(waste_df, use_container_width=True, hide_index=True)
            
            # Waste trend chart
            st.markdown("#### Variance Trend Over Time")
            date_variance = {}
            for count in fg_counts:
                date = count['count_date']
                if date not in date_variance:
                    date_variance[date] = 0
                date_variance[date] += count['variance_cases']
            
            if date_variance:
                chart_data = pd.DataFrame([
                    {"Date": date, "Variance (Cases)": variance}
                    for date, variance in sorted(date_variance.items())
                ])
                st.line_chart(chart_data.set_index("Date"))
        else:
            st.info("No physical count data available for waste analysis. Record physical counts to see waste metrics.")
        
        # Export waste report
        if fg_counts:
            st.markdown("---")
            if st.button("📥 Export Waste Report (CSV)"):
                export_data = []
                for count in fg_counts:
                    export_data.append({
                        "Date": count['count_date'],
                        "Product": count['finished_good_name'],
                        "System_Singles": count['system_singles'],
                        "Actual_Singles": count['actual_singles'],
                        "System_Bottled_S": count['system_bottled_s'],
                        "Actual_Bottled_S": count['actual_bottled_s'],
                        "System_Bottled_I": count['system_bottled_i'],
                        "Actual_Bottled_I": count['actual_bottled_i'],
                        "Variance_Units": count['variance_units'],
                        "Variance_Cases": count['variance_cases'],
                        "Variance_Percentage": count['variance_percentage'],
                        "Notes": count['notes']
                    })
                
                export_df = pd.DataFrame(export_data)
                csv = export_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"waste_report_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )

elif page == "💼 CRM/Sales":
    st.header("💼 CRM / Sales Management")
    
    db = get_db()
    
    # Add new order with multiple items
    with st.expander("➕ Add New Order"):
        # Initialize session state for order items
        if "order_items" not in st.session_state:
            st.session_state.order_items = [{"product": "", "quantity": 1}]
        
        # Item management buttons (outside form)
        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("➕ Add Item", use_container_width=True):
                st.session_state.order_items.append({"product": "", "quantity": 1})
                st.rerun()
        
        # Display current items (outside form for removal buttons)
        if len(st.session_state.order_items) > 0:
            st.write("**Order Items:**")
            for i, item in enumerate(st.session_state.order_items):
                col1, col2, col3 = st.columns([3, 1, 1])
                with col1:
                    product_list = [""] + [fg["name"] for fg in db["finished_goods"].rows]
                    current_idx = 0
                    if item["product"] and item["product"] in [fg["name"] for fg in db["finished_goods"].rows]:
                        current_idx = product_list.index(item["product"])
                    
                    product_name = st.selectbox(
                        "Product",
                        product_list,
                        index=current_idx,
                        key=f"order_product_{i}",
                        label_visibility="collapsed"
                    )
                    st.session_state.order_items[i]["product"] = product_name
                with col2:
                    quantity = st.number_input(
                        "Cases",
                        min_value=1,
                        value=item["quantity"],
                        key=f"order_qty_{i}",
                        label_visibility="collapsed"
                    )
                    st.session_state.order_items[i]["quantity"] = quantity
                with col3:
                    if st.button("🗑️", key=f"remove_item_{i}", help="Remove", use_container_width=True):
                        st.session_state.order_items.pop(i)
                        st.rerun()
        
        # Order form
        with st.form("add_order"):
            col1, col2 = st.columns(2)
            with col1:
                customer_name = st.text_input("Customer Name")
            with col2:
                order_date = st.date_input("Order Date", value=datetime.now().date())
            
            status = st.selectbox("Status", ["Pending", "Paid", "Shipped", "Delivered"])
            
            if st.form_submit_button("💾 Create Order", type="primary", use_container_width=True):
                if customer_name and any(item["product"] for item in st.session_state.order_items):
                    # Calculate total revenue
                    total_revenue = 0.0
                    
                    # Create order
                    order_id = db["orders"].insert({
                        "customer_name": customer_name,
                        "order_date": order_date.strftime("%Y-%m-%d"),
                        "status": status,
                        "shipped_date": datetime.now().strftime("%Y-%m-%d") if status == "Shipped" else None,
                        "total_revenue": 0.0  # Will update after adding items
                    }).last_pk
                    
                    # Add order items
                    items_added = []
                    for item in st.session_state.order_items:
                        if item["product"]:
                            # Get current price from finished_goods
                            fg_list = list(db["finished_goods"].rows_where("name = ?", [item["product"]]))
                            unit_price = fg_list[0].get("price_per_case", 0.0) if fg_list else 0.0
                            line_total = item["quantity"] * unit_price
                            total_revenue += line_total
                            
                            db["order_items"].insert({
                                "order_id": order_id,
                                "product_name": item["product"],
                                "quantity_cases": item["quantity"],
                                "unit_price": unit_price,
                                "line_total": line_total
                            })
                            items_added.append(f"{item['quantity']} cases of {item['product']}")
                    
                    # Update order with total revenue
                    db["orders"].update(order_id, {"total_revenue": total_revenue})
                    
                    st.success(f"✅ Order #{order_id} created for {customer_name} (Total: ${total_revenue:.2f})")
                    st.info(f"Items: {', '.join(items_added)}")
                    st.session_state.order_items = [{"product": "", "quantity": 1}]
                    st.rerun()
                else:
                    st.warning("Please enter customer name and at least one product.")
    
    # Fetch all orders
    orders = list(db["orders"].rows)
    
    if orders:
        # Display orders table with status update capability
        st.subheader("All Orders")
        
        for order in orders:
            # Get order items
            order_items = []
            if "order_items" in db.table_names():
                order_items = list(db["order_items"].rows_where("order_id = ?", [order["id"]]))
            
            # For backward compatibility, check old structure
            if not order_items and order.get("product_name_ordered"):
                order_items = [{
                    "product_name": order["product_name_ordered"],
                    "quantity_cases": order.get("quantity_cases", 0)
                }]
            
            with st.expander(f"Order #{order['id']} - {order['customer_name']} ({order['status']}) - {len(order_items)} item(s)"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Customer:** {order['customer_name']}")
                    st.write(f"**Order Date:** {order['order_date']}")
                    st.write(f"**Status:** {order['status']}")
                    
                    if order_items:
                        st.write("**Order Items:**")
                        for item in order_items:
                            st.write(f"• {item['product_name']}: {item['quantity_cases']} cases")
                
                with col2:
                    # Status update dropdown
                    status_options = ["Pending", "Paid", "Shipped", "Delivered"]
                    current_status_idx = status_options.index(order['status']) if order['status'] in status_options else 0
                    new_status = st.selectbox(
                        f"Update Status (Order #{order['id']})",
                        status_options,
                        index=current_status_idx,
                        key=f"status_{order['id']}"
                    )
                    
                    if st.button(f"Update Status", key=f"btn_{order['id']}"):
                        old_status = order['status']
                        
                        # Update order status and shipped_date if applicable
                        update_data = {"status": new_status}
                        if old_status != "Shipped" and new_status == "Shipped":
                            update_data["shipped_date"] = datetime.now().strftime("%Y-%m-%d")
                        
                        db["orders"].update(order['id'], update_data)
                        
                        # If status changed to 'Shipped', decrease finished goods stock for all items
                        if old_status != "Shipped" and new_status == "Shipped":
                            stock_updates = []
                            for item in order_items:
                                product_name = item['product_name']
                                quantity_cases = item['quantity_cases']
                                
                                # Find the finished good
                                finished_goods = list(db["finished_goods"].rows_where("name = ?", [product_name]))
                                
                                if finished_goods:
                                    fg = finished_goods[0]
                                    # Decrease from bottled_s first, then bottled_i, then singles
                                    new_bottled_s = max(0, fg["bottled_s"] - quantity_cases)
                                    remaining = quantity_cases - (fg["bottled_s"] - new_bottled_s)
                                    new_bottled_i = max(0, fg["bottled_i"] - remaining)
                                    remaining = remaining - (fg["bottled_i"] - new_bottled_i)
                                    new_singles = max(0, fg["singles"] - remaining)
                                    
                                    # Update sold count
                                    new_sold = fg["sold"] + quantity_cases
                                    
                                    update_finished_good(
                                        db,
                                        fg["id"],
                                        singles=new_singles,
                                        bottled_s=new_bottled_s,
                                        bottled_i=new_bottled_i,
                                        abv=fg.get("abv", 0.0),
                                        extra_updates={"sold": new_sold}
                                    )
                                    
                                    stock_updates.append(f"{product_name}: {quantity_cases} cases")
                                else:
                                    stock_updates.append(f"{product_name}: ⚠️ Product not found")
                            
                            if stock_updates:
                                st.success(f"✅ Order status updated to 'Shipped'. Stock decreased for: {', '.join(stock_updates)}")
                        else:
                            st.success(f"✅ Order status updated to '{new_status}'")
                        
                        st.rerun()
        
        # Summary table
        st.subheader("Orders Summary")
        summary_data = []
        for o in orders:
            order_items = []
            if "order_items" in db.table_names():
                order_items = list(db["order_items"].rows_where("order_id = ?", [o["id"]]))
            
            # For backward compatibility
            if not order_items and o.get("product_name_ordered"):
                items_text = f"{o['product_name_ordered']} ({o.get('quantity_cases', 0)} cases)"
            elif order_items:
                items_text = ", ".join([f"{item['product_name']} ({item['quantity_cases']} cases)" for item in order_items])
            else:
                items_text = "No items"
            
            summary_data.append({
                "ID": o["id"],
                "Customer": o["customer_name"],
                "Items": items_text,
                "Order Date": o["order_date"],
                "Status": o["status"]
            })
        
        df = pd.DataFrame(summary_data)
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No orders found in the database.")
