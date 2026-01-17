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

    async def get_latest_price_timestamp(self, price_type: str = 'PUBLIC') -> Optional[datetime]:
        """Get the latest settlement timestamp for a given price type.

        Used to determine how far back to fetch DISPATCH prices when backfilling.
        """
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                SELECT MAX(settlementdate) as latest FROM price_data WHERE price_type = ?
            """, (price_type,))
            row = await cursor.fetchone()

            if row and row[0]:
                return datetime.fromisoformat(row[0])
            return None

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

    async def get_region_fuel_mix(self, region: str) -> pd.DataFrame:
        """Get current generation breakdown by fuel source for a specific region"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row

            cursor = await db.execute("""
                SELECT
                    COALESCE(g.fuel_source, 'Unknown') as fuel_source,
                    SUM(d.scadavalue) as generation_mw,
                    COUNT(*) as unit_count,
                    MAX(d.settlementdate) as settlementdate
                FROM dispatch_data d
                LEFT JOIN generator_info g ON d.duid = g.duid
                WHERE d.settlementdate = (SELECT MAX(settlementdate) FROM dispatch_data)
                AND g.region = ?
                GROUP BY g.fuel_source
                ORDER BY generation_mw DESC
            """, (region,))

            rows = await cursor.fetchall()

            if not rows:
                return pd.DataFrame()

            data = [dict(row) for row in rows]
            df = pd.DataFrame(data)

            # Calculate percentage
            total = df['generation_mw'].sum()
            df['percentage'] = (df['generation_mw'] / total * 100).round(1) if total > 0 else 0

            return df

    async def get_region_generation_history(self, region: str, hours: int = 24, aggregation_minutes: int = 30) -> pd.DataFrame:
        """Get historical generation by fuel source for a specific region"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row

            # Aggregate to reduce data points - group by time period
            cursor = await db.execute("""
                SELECT
                    datetime(
                        (strftime('%s', d.settlementdate) / (? * 60)) * (? * 60),
                        'unixepoch'
                    ) as period,
                    COALESCE(g.fuel_source, 'Unknown') as fuel_source,
                    AVG(d.scadavalue) as generation_mw,
                    COUNT(*) as sample_count
                FROM dispatch_data d
                LEFT JOIN generator_info g ON d.duid = g.duid
                WHERE g.region = ?
                AND d.settlementdate >= datetime('now', ? || ' hours')
                GROUP BY period, g.fuel_source
                ORDER BY period ASC, fuel_source
            """, (aggregation_minutes, aggregation_minutes, region, f'-{hours}'))

            rows = await cursor.fetchall()

            if not rows:
                return pd.DataFrame()

            data = [dict(row) for row in rows]
            df = pd.DataFrame(data)
            df['period'] = pd.to_datetime(df['period'])

            return df

    async def get_region_price_history(self, region: str, hours: int = 24, price_type: str = 'DISPATCH') -> pd.DataFrame:
        """Get price history for a specific region over the last N hours"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row

            cursor = await db.execute("""
                SELECT settlementdate, region, price, totaldemand, price_type
                FROM price_data
                WHERE region = ?
                AND price_type = ?
                AND settlementdate >= datetime('now', ? || ' hours')
                ORDER BY settlementdate ASC
            """, (region, price_type, f'-{hours}'))

            rows = await cursor.fetchall()

            if not rows:
                return pd.DataFrame()

            data = [dict(row) for row in rows]
            df = pd.DataFrame(data)
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])

            return df

    async def get_merged_price_history(self, region: str, hours: int = 24) -> pd.DataFrame:
        """Get price history merging PUBLIC and DISPATCH prices.

        Uses PUBLIC prices where available, fills gaps with DISPATCH prices
        for times after the latest PUBLIC timestamp.

        Args:
            region: NEM region (NSW, VIC, QLD, SA, TAS)
            hours: Number of hours of history to retrieve

        Returns:
            DataFrame with merged price data including 'source_type' column
        """
        # 1. Get PUBLIC prices
        public_df = await self.get_region_price_history(region, hours, 'PUBLIC')

        # 2. Get DISPATCH prices
        dispatch_df = await self.get_region_price_history(region, hours, 'DISPATCH')

        # Handle edge cases
        if public_df.empty and dispatch_df.empty:
            return pd.DataFrame()

        if public_df.empty:
            dispatch_df['source_type'] = 'DISPATCH'
            return dispatch_df.sort_values('settlementdate').reset_index(drop=True)

        if dispatch_df.empty:
            public_df['source_type'] = 'PUBLIC'
            return public_df.sort_values('settlementdate').reset_index(drop=True)

        # 3. Find latest PUBLIC timestamp
        latest_public = public_df['settlementdate'].max()

        # 4. Filter DISPATCH to only include times after latest PUBLIC
        dispatch_fill = dispatch_df[dispatch_df['settlementdate'] > latest_public].copy()

        # 5. Add source_type and merge
        public_df['source_type'] = 'PUBLIC'
        dispatch_fill['source_type'] = 'DISPATCH'

        merged = pd.concat([public_df, dispatch_fill], ignore_index=True)
        return merged.sort_values('settlementdate').reset_index(drop=True)

    async def get_region_summary(self, region: str) -> Dict[str, Any]:
        """Get summary statistics for a specific region"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row

            # Get latest price from TRADING
            cursor = await db.execute("""
                SELECT price, settlementdate
                FROM price_data
                WHERE region = ? AND price_type = 'TRADING'
                ORDER BY settlementdate DESC
                LIMIT 1
            """, (region,))
            price_row = await cursor.fetchone()

            # Get demand from DISPATCH (TRADING doesn't have REGIONSUM records)
            cursor = await db.execute("""
                SELECT totaldemand
                FROM price_data
                WHERE region = ? AND price_type = 'DISPATCH'
                ORDER BY settlementdate DESC
                LIMIT 1
            """, (region,))
            demand_row = await cursor.fetchone()

            # Get total generation for the region
            cursor = await db.execute("""
                SELECT SUM(d.scadavalue) as total_generation
                FROM dispatch_data d
                LEFT JOIN generator_info g ON d.duid = g.duid
                WHERE d.settlementdate = (SELECT MAX(settlementdate) FROM dispatch_data)
                AND g.region = ?
            """, (region,))
            gen_row = await cursor.fetchone()

            # Get generator count by fuel type
            cursor = await db.execute("""
                SELECT COUNT(DISTINCT d.duid) as generator_count
                FROM dispatch_data d
                LEFT JOIN generator_info g ON d.duid = g.duid
                WHERE d.settlementdate = (SELECT MAX(settlementdate) FROM dispatch_data)
                AND g.region = ?
            """, (region,))
            count_row = await cursor.fetchone()

            return {
                'region': region,
                'latest_price': price_row['price'] if price_row else None,
                'total_demand': demand_row['totaldemand'] if demand_row else None,
                'price_timestamp': price_row['settlementdate'] if price_row else None,
                'total_generation': gen_row['total_generation'] if gen_row else None,
                'generator_count': count_row['generator_count'] if count_row else 0
            }

    async def get_data_coverage(self, table: str = 'price_data') -> Dict[str, Any]:
        """Get data coverage information for backfill planning"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row

            cursor = await db.execute(f"""
                SELECT
                    MIN(settlementdate) as earliest_date,
                    MAX(settlementdate) as latest_date,
                    COUNT(*) as total_records,
                    COUNT(DISTINCT date(settlementdate)) as days_with_data
                FROM {table}
            """)
            row = await cursor.fetchone()

            return {
                'earliest_date': row['earliest_date'],
                'latest_date': row['latest_date'],
                'total_records': row['total_records'],
                'days_with_data': row['days_with_data']
            }

    async def get_missing_dates(self, start_date: datetime, end_date: datetime, price_type: str = 'PUBLIC') -> List[datetime]:
        """Find dates with no price data in the specified range"""
        async with aiosqlite.connect(self.db_path) as db:
            # Get dates that have data
            cursor = await db.execute("""
                SELECT DISTINCT date(settlementdate) as data_date
                FROM price_data
                WHERE price_type = ?
                AND date(settlementdate) BETWEEN date(?) AND date(?)
            """, (price_type, start_date, end_date))

            rows = await cursor.fetchall()
            existing_dates = {row[0] for row in rows}

            # Generate all dates in range
            missing = []
            current = start_date.date() if hasattr(start_date, 'date') else start_date
            end = end_date.date() if hasattr(end_date, 'date') else end_date

            from datetime import timedelta
            while current <= end:
                date_str = current.strftime('%Y-%m-%d')
                if date_str not in existing_dates:
                    missing.append(datetime.combine(current, datetime.min.time()))
                current += timedelta(days=1)

            return missing