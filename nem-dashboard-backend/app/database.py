import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging
from typing import Optional, List, Dict, Any
import asyncio
import aiosqlite

logger = logging.getLogger(__name__)

class NEMDatabase:
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
    async def initialize(self):
        """Initialize the database with required tables"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.executescript("""
                CREATE TABLE IF NOT EXISTS dispatch_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    settlementdate DATETIME NOT NULL,
                    duid TEXT NOT NULL,
                    scadavalue REAL,
                    uigf REAL,
                    totalcleared REAL,
                    ramprate REAL,
                    availability REAL,
                    raise1sec REAL,
                    lower1sec REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(settlementdate, duid) ON CONFLICT REPLACE
                );
                
                CREATE TABLE IF NOT EXISTS generator_info (
                    duid TEXT PRIMARY KEY,
                    station_name TEXT,
                    region TEXT,
                    fuel_source TEXT,
                    technology_type TEXT,
                    capacity_mw REAL,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS price_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    settlementdate DATETIME NOT NULL,
                    region TEXT NOT NULL,
                    price REAL,
                    totaldemand REAL,
                    price_type TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(settlementdate, region, price_type) ON CONFLICT REPLACE
                );
                
                CREATE TABLE IF NOT EXISTS interconnector_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    settlementdate DATETIME NOT NULL,
                    interconnector TEXT NOT NULL,
                    meteredmwflow REAL,
                    mwflow REAL,
                    mwloss REAL,
                    marginalvalue REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(settlementdate, interconnector) ON CONFLICT REPLACE
                );
                
                CREATE INDEX IF NOT EXISTS idx_dispatch_settlement ON dispatch_data(settlementdate);
                CREATE INDEX IF NOT EXISTS idx_dispatch_duid ON dispatch_data(duid);
                CREATE INDEX IF NOT EXISTS idx_dispatch_settlement_duid ON dispatch_data(settlementdate, duid);
                
                CREATE INDEX IF NOT EXISTS idx_price_settlement ON price_data(settlementdate);
                CREATE INDEX IF NOT EXISTS idx_price_region ON price_data(region);
                CREATE INDEX IF NOT EXISTS idx_price_settlement_region ON price_data(settlementdate, region);
                
                CREATE INDEX IF NOT EXISTS idx_interconnector_settlement ON interconnector_data(settlementdate);
                CREATE INDEX IF NOT EXISTS idx_interconnector_name ON interconnector_data(interconnector);
            """)
            await db.commit()
    
    async def insert_dispatch_data(self, df: pd.DataFrame) -> int:
        """Insert dispatch data from DataFrame"""
        if df.empty:
            return 0
            
        async with aiosqlite.connect(self.db_path) as db:
            # Convert DataFrame to list of tuples
            records = []
            for _, row in df.iterrows():
                records.append((
                    row['settlementdate'].to_pydatetime() if hasattr(row['settlementdate'], 'to_pydatetime') else row['settlementdate'],
                    row['duid'],
                    row['scadavalue'],
                    row['uigf'],
                    row['totalcleared'],
                    row['ramprate'],
                    row['availability'],
                    row['raise1sec'],
                    row['lower1sec']
                ))
            
            await db.executemany("""
                INSERT OR REPLACE INTO dispatch_data 
                (settlementdate, duid, scadavalue, uigf, totalcleared, ramprate, availability, raise1sec, lower1sec)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, records)
            
            await db.commit()
            return len(records)
    
    async def insert_price_data(self, df: pd.DataFrame) -> int:
        """Insert price data from DataFrame"""
        if df.empty:
            return 0
            
        async with aiosqlite.connect(self.db_path) as db:
            records = []
            for _, row in df.iterrows():
                records.append((
                    row['settlementdate'].to_pydatetime() if hasattr(row['settlementdate'], 'to_pydatetime') else row['settlementdate'],
                    row['region'],
                    row['price'],
                    row['totaldemand'],
                    row['price_type']
                ))
            
            await db.executemany("""
                INSERT OR REPLACE INTO price_data 
                (settlementdate, region, price, totaldemand, price_type)
                VALUES (?, ?, ?, ?, ?)
            """, records)
            
            await db.commit()
            return len(records)
    
    async def insert_interconnector_data(self, df: pd.DataFrame) -> int:
        """Insert interconnector data from DataFrame"""
        if df.empty:
            return 0
            
        async with aiosqlite.connect(self.db_path) as db:
            records = []
            for _, row in df.iterrows():
                records.append((
                    row['settlementdate'].to_pydatetime() if hasattr(row['settlementdate'], 'to_pydatetime') else row['settlementdate'],
                    row['interconnector'],
                    row['meteredmwflow'],
                    row['mwflow'],
                    row['mwloss'],
                    row['marginalvalue']
                ))
            
            await db.executemany("""
                INSERT OR REPLACE INTO interconnector_data 
                (settlementdate, interconnector, meteredmwflow, mwflow, mwloss, marginalvalue)
                VALUES (?, ?, ?, ?, ?, ?)
            """, records)
            
            await db.commit()
            return len(records)
    
    async def get_latest_dispatch_data(self, limit: int = 1000) -> pd.DataFrame:
        """Get the most recent dispatch data"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            cursor = await db.execute("""
                SELECT * FROM dispatch_data 
                WHERE settlementdate = (SELECT MAX(settlementdate) FROM dispatch_data)
                ORDER BY duid
                LIMIT ?
            """, (limit,))
            
            rows = await cursor.fetchall()
            
            if not rows:
                return pd.DataFrame()
            
            # Convert to DataFrame
            data = [dict(row) for row in rows]
            df = pd.DataFrame(data)
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            
            return df
    
    async def get_dispatch_data_by_date_range(self, start_date: datetime, end_date: datetime, duid: Optional[str] = None) -> pd.DataFrame:
        """Get dispatch data for a date range"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            if duid:
                cursor = await db.execute("""
                    SELECT * FROM dispatch_data 
                    WHERE settlementdate BETWEEN ? AND ? AND duid = ?
                    ORDER BY settlementdate
                """, (start_date, end_date, duid))
            else:
                cursor = await db.execute("""
                    SELECT * FROM dispatch_data 
                    WHERE settlementdate BETWEEN ? AND ?
                    ORDER BY settlementdate
                """, (start_date, end_date))
            
            rows = await cursor.fetchall()
            
            if not rows:
                return pd.DataFrame()
            
            data = [dict(row) for row in rows]
            df = pd.DataFrame(data)
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            
            return df
    
    async def get_generation_by_fuel_type(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get aggregated generation data by fuel type"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            cursor = await db.execute("""
                SELECT 
                    d.settlementdate,
                    COALESCE(g.fuel_source, 'Unknown') as fuel_source,
                    SUM(d.scadavalue) as total_generation,
                    COUNT(*) as unit_count
                FROM dispatch_data d
                LEFT JOIN generator_info g ON d.duid = g.duid
                WHERE d.settlementdate BETWEEN ? AND ?
                GROUP BY d.settlementdate, g.fuel_source
                ORDER BY d.settlementdate, g.fuel_source
            """, (start_date, end_date))
            
            rows = await cursor.fetchall()
            
            if not rows:
                return pd.DataFrame()
            
            data = [dict(row) for row in rows]
            df = pd.DataFrame(data)
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            
            return df
    
    async def get_unique_duids(self) -> List[str]:
        """Get list of all unique DUIDs in the database"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT DISTINCT duid FROM dispatch_data ORDER BY duid")
            rows = await cursor.fetchall()
            return [row[0] for row in rows]
    
    async def update_generator_info(self, generator_data: List[Dict[str, Any]]):
        """Update generator information"""
        async with aiosqlite.connect(self.db_path) as db:
            records = []
            for gen in generator_data:
                records.append((
                    gen['duid'],
                    gen.get('station_name'),
                    gen.get('region'),
                    gen.get('fuel_source'),
                    gen.get('technology_type'),
                    gen.get('capacity_mw')
                ))
            
            await db.executemany("""
                INSERT OR REPLACE INTO generator_info 
                (duid, station_name, region, fuel_source, technology_type, capacity_mw)
                VALUES (?, ?, ?, ?, ?, ?)
            """, records)
            
            await db.commit()
    
    async def get_data_summary(self) -> Dict[str, Any]:
        """Get summary statistics about the data"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            # Get basic counts
            cursor = await db.execute("SELECT COUNT(*) as total_records FROM dispatch_data")
            total_records = (await cursor.fetchone())['total_records']
            
            cursor = await db.execute("SELECT COUNT(DISTINCT duid) as unique_duids FROM dispatch_data")
            unique_duids = (await cursor.fetchone())['unique_duids']
            
            cursor = await db.execute("SELECT MIN(settlementdate) as earliest, MAX(settlementdate) as latest FROM dispatch_data")
            date_range = await cursor.fetchone()
            
            cursor = await db.execute("""
                SELECT 
                    COALESCE(g.fuel_source, 'Unknown') as fuel_source,
                    COUNT(DISTINCT d.duid) as unit_count
                FROM dispatch_data d
                LEFT JOIN generator_info g ON d.duid = g.duid
                GROUP BY g.fuel_source
                ORDER BY unit_count DESC
            """)
            fuel_breakdown = await cursor.fetchall()
            
            return {
                'total_records': total_records,
                'unique_duids': unique_duids,
                'earliest_date': date_range['earliest'],
                'latest_date': date_range['latest'],
                'fuel_breakdown': [dict(row) for row in fuel_breakdown]
            }
    
    async def get_latest_prices(self, price_type: str = 'DISPATCH') -> pd.DataFrame:
        """Get the most recent price data"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            cursor = await db.execute("""
                SELECT * FROM price_data 
                WHERE price_type = ? AND settlementdate = (
                    SELECT MAX(settlementdate) FROM price_data WHERE price_type = ?
                )
                ORDER BY region
            """, (price_type, price_type))
            
            rows = await cursor.fetchall()
            
            if not rows:
                return pd.DataFrame()
            
            data = [dict(row) for row in rows]
            df = pd.DataFrame(data)
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            
            return df
    
    async def get_price_history(self, start_date: datetime, end_date: datetime, region: Optional[str] = None, price_type: str = 'DISPATCH') -> pd.DataFrame:
        """Get price data for a date range"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            if region:
                cursor = await db.execute("""
                    SELECT * FROM price_data 
                    WHERE price_type = ? AND region = ? AND settlementdate BETWEEN ? AND ?
                    ORDER BY settlementdate
                """, (price_type, region, start_date, end_date))
            else:
                cursor = await db.execute("""
                    SELECT * FROM price_data 
                    WHERE price_type = ? AND settlementdate BETWEEN ? AND ?
                    ORDER BY settlementdate
                """, (price_type, start_date, end_date))
            
            rows = await cursor.fetchall()
            
            if not rows:
                return pd.DataFrame()
            
            data = [dict(row) for row in rows]
            df = pd.DataFrame(data)
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            
            return df
    
    async def get_latest_interconnector_flows(self) -> pd.DataFrame:
        """Get the most recent interconnector flow data"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            cursor = await db.execute("""
                SELECT * FROM interconnector_data 
                WHERE settlementdate = (SELECT MAX(settlementdate) FROM interconnector_data)
                ORDER BY interconnector
            """)
            
            rows = await cursor.fetchall()
            
            if not rows:
                return pd.DataFrame()
            
            data = [dict(row) for row in rows]
            df = pd.DataFrame(data)
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            
            return df
    
    async def get_interconnector_history(self, start_date: datetime, end_date: datetime, interconnector: Optional[str] = None) -> pd.DataFrame:
        """Get interconnector flow data for a date range"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            if interconnector:
                cursor = await db.execute("""
                    SELECT * FROM interconnector_data 
                    WHERE interconnector = ? AND settlementdate BETWEEN ? AND ?
                    ORDER BY settlementdate
                """, (interconnector, start_date, end_date))
            else:
                cursor = await db.execute("""
                    SELECT * FROM interconnector_data 
                    WHERE settlementdate BETWEEN ? AND ?
                    ORDER BY settlementdate
                """, (start_date, end_date))
            
            rows = await cursor.fetchall()
            
            if not rows:
                return pd.DataFrame()
            
            data = [dict(row) for row in rows]
            df = pd.DataFrame(data)
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            
            return df
    
    async def get_generators_by_region_fuel(self, region: Optional[str] = None, fuel_source: Optional[str] = None) -> pd.DataFrame:
        """Get generators filtered by region and/or fuel source"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            # Base query for latest dispatch data with generator info
            base_query = """
                SELECT d.*, g.station_name, g.region, g.fuel_source, g.technology_type, g.capacity_mw
                FROM dispatch_data d
                LEFT JOIN generator_info g ON d.duid = g.duid
                WHERE d.settlementdate = (SELECT MAX(settlementdate) FROM dispatch_data)
            """
            
            params = []
            if region:
                base_query += " AND g.region = ?"
                params.append(region)
            
            if fuel_source:
                base_query += " AND g.fuel_source = ?"
                params.append(fuel_source)
            
            base_query += " ORDER BY d.scadavalue DESC"
            
            cursor = await db.execute(base_query, params)
            rows = await cursor.fetchall()
            
            if not rows:
                return pd.DataFrame()
            
            data = [dict(row) for row in rows]
            df = pd.DataFrame(data)
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            
            return df