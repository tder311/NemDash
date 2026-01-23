"""
PostgreSQL database layer for NEM Dashboard.

Configuration:
    DATABASE_URL=postgresql://user:pass@localhost:5432/nem_dashboard
"""

import pandas as pd
from datetime import datetime
import logging
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

import asyncpg

logger = logging.getLogger(__name__)


def calculate_aggregation_minutes(hours: int) -> int:
    """Calculate appropriate aggregation interval based on time range.

    Returns aggregation in minutes:
    - < 48h: 5 min (raw data)
    - 48h - 7d: 30 min
    - 7d - 30d: 60 min (hourly)
    - 30d - 90d: 1440 min (daily)
    - > 90d: 10080 min (weekly)
    """
    if hours < 48:
        return 5
    elif hours <= 168:  # 7 days
        return 30
    elif hours <= 720:  # 30 days
        return 60
    elif hours <= 2160:  # 90 days
        return 1440
    else:
        return 10080


@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    url: str
    pool_min: int = 5
    pool_max: int = 20


class NEMDatabase:
    """PostgreSQL database interface for NEM Dashboard."""

    def __init__(self, db_url: str, pool_min: int = 5, pool_max: int = 20):
        """Initialize database connection.

        Args:
            db_url: PostgreSQL connection URL (postgresql://user:pass@host:port/db)
            pool_min: Minimum pool connections
            pool_max: Maximum pool connections
        """
        self.config = DatabaseConfig(url=db_url, pool_min=pool_min, pool_max=pool_max)
        self._pool: Optional[asyncpg.Pool] = None
        logger.info("Initialized NEMDatabase for PostgreSQL")

    async def initialize(self):
        """Initialize the database connection pool and schema."""
        self._pool = await asyncpg.create_pool(
            self.config.url,
            min_size=self.config.pool_min,
            max_size=self.config.pool_max
        )

        async with self._pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS dispatch_data (
                    id BIGSERIAL PRIMARY KEY,
                    settlementdate TIMESTAMP NOT NULL,
                    duid TEXT NOT NULL,
                    scadavalue REAL,
                    uigf REAL,
                    totalcleared REAL,
                    ramprate REAL,
                    availability REAL,
                    raise1sec REAL,
                    lower1sec REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(settlementdate, duid)
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS generator_info (
                    duid TEXT PRIMARY KEY,
                    station_name TEXT,
                    region TEXT,
                    fuel_source TEXT,
                    technology_type TEXT,
                    capacity_mw REAL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS price_data (
                    id BIGSERIAL PRIMARY KEY,
                    settlementdate TIMESTAMP NOT NULL,
                    region TEXT NOT NULL,
                    price REAL,
                    totaldemand REAL,
                    price_type TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(settlementdate, region, price_type)
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS pdpasa_data (
                    id BIGSERIAL PRIMARY KEY,
                    run_datetime TIMESTAMP NOT NULL,
                    interval_datetime TIMESTAMP NOT NULL,
                    regionid TEXT NOT NULL,
                    demand10 REAL,
                    demand50 REAL,
                    demand90 REAL,
                    reservereq REAL,
                    capacityreq REAL,
                    aggregatecapacityavailable REAL,
                    aggregatepasaavailability REAL,
                    surplusreserve REAL,
                    lorcondition INTEGER,
                    calculatedlor1level REAL,
                    calculatedlor2level REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(run_datetime, interval_datetime, regionid)
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS stpasa_data (
                    id BIGSERIAL PRIMARY KEY,
                    run_datetime TIMESTAMP NOT NULL,
                    interval_datetime TIMESTAMP NOT NULL,
                    regionid TEXT NOT NULL,
                    demand10 REAL,
                    demand50 REAL,
                    demand90 REAL,
                    reservereq REAL,
                    capacityreq REAL,
                    aggregatecapacityavailable REAL,
                    aggregatepasaavailability REAL,
                    surplusreserve REAL,
                    lorcondition INTEGER,
                    calculatedlor1level REAL,
                    calculatedlor2level REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(run_datetime, interval_datetime, regionid)
                )
            """)


            # Create indexes
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_dispatch_settlement ON dispatch_data(settlementdate)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_dispatch_duid ON dispatch_data(duid)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_dispatch_settlement_duid ON dispatch_data(settlementdate, duid)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_price_settlement ON price_data(settlementdate)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_price_region ON price_data(region)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_price_settlement_region ON price_data(settlementdate, region)")
            # Optimized index for region price history queries (region first, then settlement)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_price_region_settlement ON price_data(region, settlementdate)")
            # Composite index for filtered queries by region and price_type
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_price_region_type_settlement ON price_data(region, price_type, settlementdate)")

            # PASA indexes
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_pdpasa_run ON pdpasa_data(run_datetime)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_pdpasa_region ON pdpasa_data(regionid)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_pdpasa_region_run ON pdpasa_data(regionid, run_datetime)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_stpasa_run ON stpasa_data(run_datetime)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_stpasa_region ON stpasa_data(regionid)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_stpasa_region_run ON stpasa_data(regionid, run_datetime)")

        logger.info("PostgreSQL database initialized")

    async def close(self):
        """Close database connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("PostgreSQL connection pool closed")

    # Data insertion methods
    async def insert_dispatch_data(self, df: pd.DataFrame) -> int:
        """Insert dispatch data from DataFrame."""
        if df.empty:
            return 0

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

        async with self._pool.acquire() as conn:
            await conn.executemany("""
                INSERT INTO dispatch_data
                (settlementdate, duid, scadavalue, uigf, totalcleared, ramprate, availability, raise1sec, lower1sec)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (settlementdate, duid) DO UPDATE SET
                    scadavalue = EXCLUDED.scadavalue,
                    uigf = EXCLUDED.uigf,
                    totalcleared = EXCLUDED.totalcleared,
                    ramprate = EXCLUDED.ramprate,
                    availability = EXCLUDED.availability,
                    raise1sec = EXCLUDED.raise1sec,
                    lower1sec = EXCLUDED.lower1sec
            """, records)

        return len(records)

    async def insert_price_data(self, df: pd.DataFrame) -> int:
        """Insert price data from DataFrame."""
        if df.empty:
            return 0

        records = []
        for _, row in df.iterrows():
            records.append((
                row['settlementdate'].to_pydatetime() if hasattr(row['settlementdate'], 'to_pydatetime') else row['settlementdate'],
                row['region'],
                row['price'],
                row['totaldemand'],
                row['price_type']
            ))

        async with self._pool.acquire() as conn:
            await conn.executemany("""
                INSERT INTO price_data
                (settlementdate, region, price, totaldemand, price_type)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (settlementdate, region, price_type) DO UPDATE SET
                    price = EXCLUDED.price,
                    totaldemand = EXCLUDED.totaldemand
            """, records)

        return len(records)

    async def update_generator_info(self, generator_data: List[Dict[str, Any]]):
        """Update generator information."""
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

        async with self._pool.acquire() as conn:
            await conn.executemany("""
                INSERT INTO generator_info
                (duid, station_name, region, fuel_source, technology_type, capacity_mw)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (duid) DO UPDATE SET
                    station_name = EXCLUDED.station_name,
                    region = EXCLUDED.region,
                    fuel_source = EXCLUDED.fuel_source,
                    technology_type = EXCLUDED.technology_type,
                    capacity_mw = EXCLUDED.capacity_mw,
                    updated_at = CURRENT_TIMESTAMP
            """, records)

    # Query methods
    async def get_latest_dispatch_data(self, limit: int = 1000) -> pd.DataFrame:
        """Get the most recent dispatch data."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM dispatch_data
                WHERE settlementdate = (SELECT MAX(settlementdate) FROM dispatch_data)
                ORDER BY duid
                LIMIT $1
            """, limit)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame([dict(row) for row in rows])
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        return df

    async def get_latest_dispatch_timestamp(self) -> Optional[datetime]:
        """Get the latest settlement timestamp from dispatch data."""
        async with self._pool.acquire() as conn:
            result = await conn.fetchval("SELECT MAX(settlementdate) FROM dispatch_data")
        return result

    async def get_earliest_dispatch_timestamp(self) -> Optional[datetime]:
        """Get the earliest settlement timestamp from dispatch data."""
        async with self._pool.acquire() as conn:
            result = await conn.fetchval("SELECT MIN(settlementdate) FROM dispatch_data")
        return result

    async def get_dispatch_dates_with_data(self, start_date: datetime, end_date: datetime, min_records: int = 100) -> set:
        """Get dates that have sufficient dispatch data."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT settlementdate::DATE as data_date, COUNT(*) as record_count
                FROM dispatch_data
                WHERE settlementdate::DATE BETWEEN $1::DATE AND $2::DATE
                GROUP BY settlementdate::DATE
                HAVING COUNT(*) >= $3
            """, start_date, end_date, min_records)

        return {str(row['data_date']) for row in rows}

    async def get_dispatch_data_by_date_range(self, start_date: datetime, end_date: datetime, duid: Optional[str] = None) -> pd.DataFrame:
        """Get dispatch data for a date range."""
        async with self._pool.acquire() as conn:
            if duid:
                rows = await conn.fetch("""
                    SELECT * FROM dispatch_data
                    WHERE settlementdate BETWEEN $1 AND $2 AND duid = $3
                    ORDER BY settlementdate
                """, start_date, end_date, duid)
            else:
                rows = await conn.fetch("""
                    SELECT * FROM dispatch_data
                    WHERE settlementdate BETWEEN $1 AND $2
                    ORDER BY settlementdate
                """, start_date, end_date)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame([dict(row) for row in rows])
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        return df

    async def get_generation_by_fuel_type(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get aggregated generation data by fuel type."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    d.settlementdate,
                    COALESCE(g.fuel_source, 'Unknown') as fuel_source,
                    SUM(d.scadavalue) as total_generation,
                    COUNT(*) as unit_count
                FROM dispatch_data d
                LEFT JOIN generator_info g ON d.duid = g.duid
                WHERE d.settlementdate BETWEEN $1 AND $2
                GROUP BY d.settlementdate, g.fuel_source
                ORDER BY d.settlementdate, g.fuel_source
            """, start_date, end_date)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame([dict(row) for row in rows])
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        return df

    async def get_unique_duids(self) -> List[str]:
        """Get list of all unique DUIDs in the database."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT DISTINCT duid FROM dispatch_data ORDER BY duid")
        return [row['duid'] for row in rows]

    async def get_data_summary(self) -> Dict[str, Any]:
        """Get summary statistics about the data."""
        async with self._pool.acquire() as conn:
            total_records = await conn.fetchval("SELECT COUNT(*) FROM dispatch_data")
            unique_duids = await conn.fetchval("SELECT COUNT(DISTINCT duid) FROM dispatch_data")

            date_range = await conn.fetchrow(
                "SELECT MIN(settlementdate) as earliest, MAX(settlementdate) as latest FROM dispatch_data"
            )

            fuel_breakdown = await conn.fetch("""
                SELECT
                    COALESCE(g.fuel_source, 'Unknown') as fuel_source,
                    COUNT(DISTINCT d.duid) as unit_count
                FROM dispatch_data d
                LEFT JOIN generator_info g ON d.duid = g.duid
                GROUP BY g.fuel_source
                ORDER BY unit_count DESC
            """)

        return {
            'total_records': total_records or 0,
            'unique_duids': unique_duids or 0,
            'earliest_date': str(date_range['earliest']) if date_range and date_range['earliest'] else None,
            'latest_date': str(date_range['latest']) if date_range and date_range['latest'] else None,
            'fuel_breakdown': [dict(row) for row in fuel_breakdown]
        }

    async def get_latest_prices(self, price_type: str = 'DISPATCH') -> pd.DataFrame:
        """Get the most recent price data."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM price_data
                WHERE price_type = $1 AND settlementdate = (
                    SELECT MAX(settlementdate) FROM price_data WHERE price_type = $2
                )
                ORDER BY region
            """, price_type, price_type)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame([dict(row) for row in rows])
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        return df

    async def get_latest_price_timestamp(self, price_type: str = 'PUBLIC') -> Optional[datetime]:
        """Get the latest settlement timestamp for a given price type."""
        async with self._pool.acquire() as conn:
            result = await conn.fetchval(
                "SELECT MAX(settlementdate) FROM price_data WHERE price_type = $1",
                price_type
            )
        return result

    async def get_price_history(self, start_date: datetime, end_date: datetime, region: Optional[str] = None, price_type: str = 'DISPATCH') -> pd.DataFrame:
        """Get price data for a date range."""
        async with self._pool.acquire() as conn:
            if region:
                rows = await conn.fetch("""
                    SELECT * FROM price_data
                    WHERE price_type = $1 AND region = $2 AND settlementdate BETWEEN $3 AND $4
                    ORDER BY settlementdate
                """, price_type, region, start_date, end_date)
            else:
                rows = await conn.fetch("""
                    SELECT * FROM price_data
                    WHERE price_type = $1 AND settlementdate BETWEEN $2 AND $3
                    ORDER BY settlementdate
                """, price_type, start_date, end_date)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame([dict(row) for row in rows])
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        return df

    async def get_generators_by_region_fuel(self, region: Optional[str] = None, fuel_source: Optional[str] = None) -> pd.DataFrame:
        """Get generators filtered by region and/or fuel source."""
        async with self._pool.acquire() as conn:
            base_query = """
                SELECT d.*, g.station_name, g.region, g.fuel_source, g.technology_type, g.capacity_mw
                FROM dispatch_data d
                LEFT JOIN generator_info g ON d.duid = g.duid
                WHERE d.settlementdate = (SELECT MAX(settlementdate) FROM dispatch_data)
            """

            params = []
            param_idx = 1

            if region:
                base_query += f" AND g.region = ${param_idx}"
                params.append(region)
                param_idx += 1

            if fuel_source:
                base_query += f" AND g.fuel_source = ${param_idx}"
                params.append(fuel_source)

            base_query += " ORDER BY d.scadavalue DESC"

            rows = await conn.fetch(base_query, *params)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame([dict(row) for row in rows])
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        return df

    async def get_region_fuel_mix(self, region: str) -> pd.DataFrame:
        """Get current generation breakdown by fuel source for a specific region."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    COALESCE(g.fuel_source, 'Unknown') as fuel_source,
                    SUM(d.scadavalue) as generation_mw,
                    COUNT(*) as unit_count,
                    MAX(d.settlementdate) as settlementdate
                FROM dispatch_data d
                LEFT JOIN generator_info g ON d.duid = g.duid
                WHERE d.settlementdate = (SELECT MAX(settlementdate) FROM dispatch_data)
                AND g.region = $1
                GROUP BY g.fuel_source
                ORDER BY SUM(d.scadavalue) DESC
            """, region)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame([dict(row) for row in rows])
        total = df['generation_mw'].sum()
        df['percentage'] = (df['generation_mw'] / total * 100).round(1) if total > 0 else 0
        return df

    async def get_region_generation_history(self, region: str, hours: int = 24, aggregation_minutes: Optional[int] = None) -> pd.DataFrame:
        """Get historical generation by fuel source for a specific region."""
        if aggregation_minutes is None:
            aggregation_minutes = calculate_aggregation_minutes(hours)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                WITH timestamp_totals AS (
                    SELECT
                        d.settlementdate,
                        COALESCE(g.fuel_source, 'Unknown') as fuel_source,
                        SUM(d.scadavalue) as total_mw
                    FROM dispatch_data d
                    INNER JOIN generator_info g ON d.duid = g.duid
                    WHERE g.region = $1
                    AND d.settlementdate >= (
                        (SELECT MAX(settlementdate) FROM dispatch_data) + ($2 || ' hours')::INTERVAL
                    )
                    GROUP BY d.settlementdate, g.fuel_source
                )
                SELECT
                    TO_TIMESTAMP(
                        (EXTRACT(EPOCH FROM settlementdate)::BIGINT / ($3 * 60)) * ($3 * 60)
                    ) as period,
                    fuel_source,
                    AVG(total_mw) as generation_mw,
                    COUNT(*) as sample_count
                FROM timestamp_totals
                GROUP BY 1, 2
                ORDER BY period ASC, fuel_source
            """, region, f'-{hours}', aggregation_minutes)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame([dict(row) for row in rows])
        df['period'] = pd.to_datetime(df['period'])
        return df

    async def get_region_price_history(self, region: str, hours: int = 24, price_type: str = 'DISPATCH') -> pd.DataFrame:
        """Get price history for a specific region over the last N hours."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT settlementdate, region, price, totaldemand, price_type
                FROM price_data
                WHERE region = $1
                AND price_type = $2
                AND settlementdate >= (SELECT MAX(settlementdate) FROM price_data WHERE region = $1 AND price_type = $2) + ($3 || ' hours')::INTERVAL
                ORDER BY settlementdate ASC
            """, region, price_type, f'-{hours}')

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame([dict(row) for row in rows])
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        return df

    async def get_merged_price_history(self, region: str, hours: int = 24) -> pd.DataFrame:
        """Get price history merging PUBLIC and DISPATCH prices."""
        public_df = await self.get_region_price_history(region, hours, 'PUBLIC')
        dispatch_df = await self.get_region_price_history(region, hours, 'DISPATCH')

        if public_df.empty and dispatch_df.empty:
            return pd.DataFrame()

        if public_df.empty:
            dispatch_df['source_type'] = 'DISPATCH'
            return dispatch_df.sort_values('settlementdate').reset_index(drop=True)

        if dispatch_df.empty:
            public_df['source_type'] = 'PUBLIC'
            return public_df.sort_values('settlementdate').reset_index(drop=True)

        public_df['source_type'] = 'PUBLIC'
        dispatch_df['source_type'] = 'DISPATCH'

        public_timestamps = set(public_df['settlementdate'])
        dispatch_fill = dispatch_df[~dispatch_df['settlementdate'].isin(public_timestamps)].copy()

        merged = pd.concat([public_df, dispatch_fill], ignore_index=True)
        return merged.sort_values('settlementdate').reset_index(drop=True)

    async def get_aggregated_price_history(self, region: str, hours: int = 24, aggregation_minutes: Optional[int] = None) -> pd.DataFrame:
        """Get price history with optional time-based aggregation."""
        if aggregation_minutes is None:
            aggregation_minutes = calculate_aggregation_minutes(hours)

        if aggregation_minutes <= 30:
            return await self.get_merged_price_history(region, hours)

        async with self._pool.acquire() as conn:
            # Use DISTINCT ON for efficient deduplication (O(n log n) vs O(n²) correlated subquery)
            rows = await conn.fetch("""
                WITH deduped AS (
                    SELECT DISTINCT ON (settlementdate)
                        settlementdate,
                        price,
                        totaldemand
                    FROM price_data
                    WHERE region = $1
                    AND settlementdate >= (
                        (SELECT MAX(settlementdate) FROM price_data WHERE region = $1) + ($2 || ' hours')::INTERVAL
                    )
                    ORDER BY settlementdate,
                        CASE WHEN price_type = 'PUBLIC' THEN 1 ELSE 2 END
                )
                SELECT
                    TO_TIMESTAMP(
                        (EXTRACT(EPOCH FROM settlementdate)::BIGINT / ($3 * 60)) * ($3 * 60)
                    ) as settlementdate,
                    AVG(price) as price,
                    AVG(totaldemand) as totaldemand,
                    COUNT(*) as sample_count
                FROM deduped
                GROUP BY 1
                ORDER BY settlementdate ASC
            """, region, f'-{hours}', aggregation_minutes)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame([dict(row) for row in rows])
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        df['source_type'] = 'AGGREGATED'
        return df

    async def get_region_summary(self, region: str) -> Dict[str, Any]:
        """Get summary statistics for a specific region."""
        async with self._pool.acquire() as conn:
            price_row = await conn.fetchrow("""
                SELECT price, settlementdate
                FROM price_data
                WHERE region = $1 AND price_type = 'TRADING'
                ORDER BY settlementdate DESC
                LIMIT 1
            """, region)

            demand_row = await conn.fetchrow("""
                SELECT totaldemand
                FROM price_data
                WHERE region = $1 AND price_type = 'DISPATCH'
                ORDER BY settlementdate DESC
                LIMIT 1
            """, region)

            gen_row = await conn.fetchrow("""
                SELECT SUM(d.scadavalue) as total_generation
                FROM dispatch_data d
                LEFT JOIN generator_info g ON d.duid = g.duid
                WHERE d.settlementdate = (SELECT MAX(settlementdate) FROM dispatch_data)
                AND g.region = $1
            """, region)

            count_row = await conn.fetchrow("""
                SELECT COUNT(DISTINCT d.duid) as generator_count
                FROM dispatch_data d
                LEFT JOIN generator_info g ON d.duid = g.duid
                WHERE d.settlementdate = (SELECT MAX(settlementdate) FROM dispatch_data)
                AND g.region = $1
            """, region)

        return {
            'region': region,
            'latest_price': price_row['price'] if price_row else None,
            'total_demand': demand_row['totaldemand'] if demand_row else None,
            'price_timestamp': str(price_row['settlementdate']) if price_row else None,
            'total_generation': gen_row['total_generation'] if gen_row else None,
            'generator_count': count_row['generator_count'] if count_row else 0
        }

    async def get_data_coverage(self, table: str = 'price_data') -> Dict[str, Any]:
        """Get data coverage information for backfill planning."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(f"""
                SELECT
                    MIN(settlementdate) as earliest_date,
                    MAX(settlementdate) as latest_date,
                    COUNT(*) as total_records,
                    COUNT(DISTINCT settlementdate::DATE) as days_with_data
                FROM {table}
            """)

        return {
            'earliest_date': str(row['earliest_date']) if row and row['earliest_date'] else None,
            'latest_date': str(row['latest_date']) if row and row['latest_date'] else None,
            'total_records': row['total_records'] if row else 0,
            'days_with_data': row['days_with_data'] if row else 0
        }

    async def get_missing_dates(self, start_date: datetime, end_date: datetime, price_type: str = 'PUBLIC') -> List[datetime]:
        """Find dates with no price data in the specified range."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT settlementdate::DATE as data_date
                FROM price_data
                WHERE price_type = $1
                AND settlementdate::DATE BETWEEN $2::DATE AND $3::DATE
            """, price_type, start_date, end_date)

        existing_dates = {str(row['data_date']) for row in rows}

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

    async def get_database_health(self, hours_back: int = 168) -> Dict[str, Any]:
        """Get comprehensive database health including gap detection.

        Args:
            hours_back: Number of hours to look back for gap detection (default 168 = 7 days)

        Returns:
            Dictionary with table statistics and detected gaps
        """
        tables = ['dispatch_data', 'price_data', 'generator_info']
        table_stats = []
        gaps_by_table = []

        async with self._pool.acquire() as conn:
            for table in tables:
                if table == 'generator_info':
                    # Static reference table - no time-series gaps
                    row = await conn.fetchrow("""
                        SELECT COUNT(*) as total_records,
                               MIN(updated_at) as earliest_date,
                               MAX(updated_at) as latest_date
                        FROM generator_info
                    """)
                    table_stats.append({
                        'table': table,
                        'total_records': row['total_records'] or 0,
                        'earliest_date': str(row['earliest_date']) if row['earliest_date'] else None,
                        'latest_date': str(row['latest_date']) if row['latest_date'] else None,
                        'days_with_data': None,
                        'expected_interval': None
                    })
                else:
                    # Time-series tables
                    row = await conn.fetchrow(f"""
                        SELECT COUNT(*) as total_records,
                               MIN(settlementdate) as earliest_date,
                               MAX(settlementdate) as latest_date,
                               COUNT(DISTINCT settlementdate::DATE) as days_with_data
                        FROM {table}
                    """)
                    table_stats.append({
                        'table': table,
                        'total_records': row['total_records'] or 0,
                        'earliest_date': str(row['earliest_date']) if row['earliest_date'] else None,
                        'latest_date': str(row['latest_date']) if row['latest_date'] else None,
                        'days_with_data': row['days_with_data'] or 0,
                        'expected_interval': 5
                    })

                    # Detect gaps using window function (efficient O(n log n) on indexed data)
                    gaps = await conn.fetch(f"""
                        WITH ordered_data AS (
                            SELECT DISTINCT settlementdate,
                                   LAG(settlementdate) OVER (ORDER BY settlementdate) as prev_date
                            FROM {table}
                            WHERE settlementdate >= NOW() - INTERVAL '{hours_back} hours'
                        ),
                        detected_gaps AS (
                            SELECT
                                prev_date as gap_start,
                                settlementdate as gap_end,
                                EXTRACT(EPOCH FROM (settlementdate - prev_date)) / 60 as gap_minutes
                            FROM ordered_data
                            WHERE prev_date IS NOT NULL
                            AND EXTRACT(EPOCH FROM (settlementdate - prev_date)) / 60 > 5
                        )
                        SELECT * FROM detected_gaps ORDER BY gap_start LIMIT 100
                    """)

                    gaps_by_table.append({
                        'table': table,
                        'gaps': [{
                            'gap_start': str(g['gap_start']),
                            'gap_end': str(g['gap_end']),
                            'missing_intervals': int(g['gap_minutes'] / 5) - 1,
                            'duration_minutes': int(g['gap_minutes'])
                        } for g in gaps],
                        'total_gaps': len(gaps)
                    })

        return {
            'tables': table_stats,
            'gaps': gaps_by_table,
            'checked_hours': hours_back,
            'checked_at': datetime.now().isoformat()
        }

    # PASA data methods
    async def insert_pdpasa_data(self, df: pd.DataFrame) -> int:
        """Insert PDPASA data from DataFrame."""
        if df.empty:
            return 0

        records = []
        for _, row in df.iterrows():
            records.append((
                row['run_datetime'].to_pydatetime() if hasattr(row['run_datetime'], 'to_pydatetime') else row['run_datetime'],
                row['interval_datetime'].to_pydatetime() if hasattr(row['interval_datetime'], 'to_pydatetime') else row['interval_datetime'],
                row['regionid'],
                row.get('demand10'),
                row.get('demand50'),
                row.get('demand90'),
                row.get('reservereq'),
                row.get('capacityreq'),
                row.get('aggregatecapacityavailable'),
                row.get('aggregatepasaavailability'),
                row.get('surplusreserve'),
                row.get('lorcondition'),
                row.get('calculatedlor1level'),
                row.get('calculatedlor2level')
            ))

        async with self._pool.acquire() as conn:
            await conn.executemany("""
                INSERT INTO pdpasa_data
                (run_datetime, interval_datetime, regionid, demand10, demand50, demand90,
                 reservereq, capacityreq, aggregatecapacityavailable, aggregatepasaavailability,
                 surplusreserve, lorcondition, calculatedlor1level, calculatedlor2level)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (run_datetime, interval_datetime, regionid) DO UPDATE SET
                    demand10 = EXCLUDED.demand10,
                    demand50 = EXCLUDED.demand50,
                    demand90 = EXCLUDED.demand90,
                    reservereq = EXCLUDED.reservereq,
                    capacityreq = EXCLUDED.capacityreq,
                    aggregatecapacityavailable = EXCLUDED.aggregatecapacityavailable,
                    aggregatepasaavailability = EXCLUDED.aggregatepasaavailability,
                    surplusreserve = EXCLUDED.surplusreserve,
                    lorcondition = EXCLUDED.lorcondition,
                    calculatedlor1level = EXCLUDED.calculatedlor1level,
                    calculatedlor2level = EXCLUDED.calculatedlor2level
            """, records)

        return len(records)

    async def insert_stpasa_data(self, df: pd.DataFrame) -> int:
        """Insert STPASA data from DataFrame."""
        if df.empty:
            return 0

        records = []
        for _, row in df.iterrows():
            records.append((
                row['run_datetime'].to_pydatetime() if hasattr(row['run_datetime'], 'to_pydatetime') else row['run_datetime'],
                row['interval_datetime'].to_pydatetime() if hasattr(row['interval_datetime'], 'to_pydatetime') else row['interval_datetime'],
                row['regionid'],
                row.get('demand10'),
                row.get('demand50'),
                row.get('demand90'),
                row.get('reservereq'),
                row.get('capacityreq'),
                row.get('aggregatecapacityavailable'),
                row.get('aggregatepasaavailability'),
                row.get('surplusreserve'),
                row.get('lorcondition'),
                row.get('calculatedlor1level'),
                row.get('calculatedlor2level')
            ))

        async with self._pool.acquire() as conn:
            await conn.executemany("""
                INSERT INTO stpasa_data
                (run_datetime, interval_datetime, regionid, demand10, demand50, demand90,
                 reservereq, capacityreq, aggregatecapacityavailable, aggregatepasaavailability,
                 surplusreserve, lorcondition, calculatedlor1level, calculatedlor2level)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (run_datetime, interval_datetime, regionid) DO UPDATE SET
                    demand10 = EXCLUDED.demand10,
                    demand50 = EXCLUDED.demand50,
                    demand90 = EXCLUDED.demand90,
                    reservereq = EXCLUDED.reservereq,
                    capacityreq = EXCLUDED.capacityreq,
                    aggregatecapacityavailable = EXCLUDED.aggregatecapacityavailable,
                    aggregatepasaavailability = EXCLUDED.aggregatepasaavailability,
                    surplusreserve = EXCLUDED.surplusreserve,
                    lorcondition = EXCLUDED.lorcondition,
                    calculatedlor1level = EXCLUDED.calculatedlor1level,
                    calculatedlor2level = EXCLUDED.calculatedlor2level
            """, records)

        return len(records)

    async def get_latest_pdpasa(self, region: str) -> List[Dict[str, Any]]:
        """Get the latest PDPASA run for a specific region."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM pdpasa_data
                WHERE regionid = $1
                AND run_datetime = (SELECT MAX(run_datetime) FROM pdpasa_data WHERE regionid = $1)
                ORDER BY interval_datetime ASC
            """, region)

        return [dict(row) for row in rows]

    async def get_latest_stpasa(self, region: str) -> List[Dict[str, Any]]:
        """Get the latest STPASA run for a specific region."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM stpasa_data
                WHERE regionid = $1
                AND run_datetime = (SELECT MAX(run_datetime) FROM stpasa_data WHERE regionid = $1)
                ORDER BY interval_datetime ASC
            """, region)

        return [dict(row) for row in rows]

    async def get_latest_pdpasa_run_datetime(self) -> Optional[datetime]:
        """Get the latest PDPASA run datetime."""
        async with self._pool.acquire() as conn:
            result = await conn.fetchval("SELECT MAX(run_datetime) FROM pdpasa_data")
        return result

    async def get_latest_stpasa_run_datetime(self) -> Optional[datetime]:
        """Get the latest STPASA run datetime."""
        async with self._pool.acquire() as conn:
            result = await conn.fetchval("SELECT MAX(run_datetime) FROM stpasa_data")
        return result
