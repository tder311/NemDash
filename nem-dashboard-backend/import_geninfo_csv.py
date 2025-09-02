#!/usr/bin/env python3
"""
Import generator information from the provided GenInfo.csv file
"""

import asyncio
import pandas as pd
import sqlite3
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app.database import NEMDatabase

async def import_geninfo_csv():
    """Import generator info from CSV file"""
    
    csv_path = './data/GenInfo.csv'
    db_path = './data/nem_dispatch.db'
    
    print("=== Importing Generator Info from CSV ===")
    
    # Read the CSV file
    try:
        df = pd.read_csv(csv_path, encoding='utf-8-sig')  # Handle BOM
        print(f"Loaded CSV with {len(df)} rows")
        print(f"Columns: {list(df.columns)}")
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return
    
    # Clean column names (remove extra spaces)
    df.columns = df.columns.str.strip()
    
    # Filter for existing plants with DUIDs
    df_valid = df[
        (df['DUID'].notna()) & 
        (df['DUID'] != '') & 
        (df['Asset Type'].str.contains('Existing', na=False))
    ].copy()
    
    print(f"Found {len(df_valid)} existing generators with DUIDs")
    
    # Create standardized generator info
    generators = []
    for _, row in df_valid.iterrows():
        duid = str(row['DUID']).strip()
        if not duid or duid == 'nan':
            continue
            
        # Map region (remove the '1' suffix)
        region = str(row['Region']).replace('1', '').strip()
        
        # Clean fuel type mapping
        fuel_type_raw = str(row['Fuel Type']).strip()
        if 'Solar' in fuel_type_raw:
            fuel_source = 'Solar'
        elif 'Wind' in fuel_type_raw:
            fuel_source = 'Wind'
        elif 'Water' in fuel_type_raw or 'Hydro' in fuel_type_raw:
            fuel_source = 'Hydro'
        elif 'Gas' in fuel_type_raw or 'Coal Mine Gas' in fuel_type_raw:
            fuel_source = 'Gas'
        elif 'Coal' in fuel_type_raw:
            fuel_source = 'Coal'
        elif 'Other' in fuel_type_raw and 'Battery' in str(row['Technology Type']):
            fuel_source = 'Battery'
        elif 'Diesel' in fuel_type_raw:
            fuel_source = 'Diesel'
        else:
            fuel_source = 'Other'
        
        # Clean technology type
        tech_type_raw = str(row['Technology Type']).strip()
        if 'Solar PV' in tech_type_raw:
            technology_type = 'Solar PV'
        elif 'Wind Turbine' in tech_type_raw:
            technology_type = 'Wind'
        elif 'Storage - Battery' in tech_type_raw:
            technology_type = 'Battery Storage'
        elif 'Hydro' in tech_type_raw:
            technology_type = 'Hydro'
        elif 'Gas Turbine' in tech_type_raw:
            technology_type = 'Gas Turbine'
        elif 'Steam Turbine' in tech_type_raw:
            if fuel_source == 'Coal':
                technology_type = 'Coal Steam'
            else:
                technology_type = 'Gas Steam'
        elif 'Reciprocating Engine' in tech_type_raw:
            technology_type = 'Reciprocating Engine'
        else:
            technology_type = tech_type_raw
        
        # Get capacity (try different columns)
        capacity = 0.0
        for cap_col in ['Nameplate Capacity (MW)', 'Aggregated Upper Nameplate Capacity (MW)', 'Upper Nameplate Capacity (MW)']:
            if cap_col in row and pd.notna(row[cap_col]):
                try:
                    cap_str = str(row[cap_col]).strip().replace(' - ', '-')
                    if '-' in cap_str:
                        # Handle range like "200.00 - 400.00"
                        cap_parts = cap_str.split('-')
                        capacity = float(cap_parts[-1].strip())
                    else:
                        capacity = float(cap_str)
                    break
                except:
                    continue
        
        if capacity == 0.0:
            capacity = 100.0  # Default
        
        # Clean site name
        station_name = str(row['Site Name']).strip()
        if not station_name or station_name == 'nan':
            station_name = duid
        
        generator_info = {
            'duid': duid,
            'station_name': station_name,
            'region': region,
            'fuel_source': fuel_source,
            'technology_type': technology_type,
            'capacity_mw': capacity
        }
        
        generators.append(generator_info)
    
    print(f"Processed {len(generators)} generator records")
    
    # Show sample of what we're importing
    print(f"\nSample generator records:")
    for gen in generators[:10]:
        print(f"  {gen['duid']:>12s} | {gen['region']:>3s} | {gen['fuel_source']:>8s} | {gen['capacity_mw']:>6.1f} MW | {gen['station_name']}")
    
    # Update database
    db = NEMDatabase(db_path)
    await db.initialize()
    
    await db.update_generator_info(generators)
    print(f"✅ Updated database with {len(generators)} generator records")
    
    # Show final statistics
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            fuel_source,
            COUNT(*) as count,
            ROUND(SUM(capacity_mw), 0) as total_capacity
        FROM generator_info 
        WHERE fuel_source != 'Unknown'
        GROUP BY fuel_source 
        ORDER BY total_capacity DESC
    """)
    
    print(f"\nUpdated Generator Summary by Fuel Source:")
    for fuel, count, capacity in cursor.fetchall():
        print(f"  {fuel:>12s}: {count:>3d} units, {capacity:>7.0f} MW")
    
    cursor.execute("""
        SELECT 
            region,
            COUNT(*) as count,
            ROUND(SUM(capacity_mw), 0) as total_capacity
        FROM generator_info 
        WHERE region != 'Unknown'
        GROUP BY region 
        ORDER BY total_capacity DESC
    """)
    
    print(f"\nUpdated Generator Summary by Region:")
    for region, count, capacity in cursor.fetchall():
        print(f"  {region:>8s}: {count:>3d} units, {capacity:>7.0f} MW")
    
    # Final quality check
    cursor.execute("SELECT COUNT(*) FROM generator_info WHERE fuel_source = 'Unknown'")
    unknown_fuel = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM generator_info WHERE region = 'Unknown'")
    unknown_region = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM generator_info")
    total_generators = cursor.fetchone()[0]
    
    print(f"\nFinal Classification Quality:")
    print(f"  Unknown fuel source: {unknown_fuel} units ({unknown_fuel/total_generators*100:.1f}%)")
    print(f"  Unknown region: {unknown_region} units ({unknown_region/total_generators*100:.1f}%)")
    print(f"  Successfully classified: {((total_generators-unknown_fuel)/total_generators*100):.1f}% fuel, {((total_generators-unknown_region)/total_generators*100):.1f}% region")
    
    conn.close()

if __name__ == "__main__":
    asyncio.run(import_geninfo_csv())