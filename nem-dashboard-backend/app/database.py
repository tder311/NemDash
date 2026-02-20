"""
PostgreSQL database layer for NEM Dashboard.

Configuration:
    DATABASE_URL=postgresql://user:pass@localhost:5432/nem_dashboard
"""

import pandas as pd
from datetime import datetime, timezone, timedelta
import logging
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

import asyncpg

logger = logging.getLogger(__name__)

# NEM operates on Australian Eastern Standard Time (AEST, UTC+10) year-round
# It does NOT observe daylight saving time to avoid market complexity
AEST = timezone(timedelta(hours=10))


def to_aest_isoformat(dt):
    """Convert naive datetime (assumed AEST) to ISO string with timezone offset.

    NEM data is always in AEST (UTC+10). This function adds the timezone offset
    to ensure JavaScript/browsers correctly interpret the timestamps.

    Args:
        dt: A datetime object (naive, assumed to be AEST) or None

    Returns:
        ISO 8601 string with +10:00 offset, e.g., "2025-01-15T08:00:00+10:00"
        Returns None if input is None
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=AEST)
    return dt.isoformat()


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


            await conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_metrics (
                    id BIGSERIAL PRIMARY KEY,
                    metric_date DATE NOT NULL,
                    region TEXT NOT NULL,
                    capture_solar REAL,
                    capture_wind REAL,
                    capture_battery REAL,
                    capture_gas REAL,
                    capture_coal REAL,
                    capture_hydro REAL,
                    capture_price_solar REAL,
                    capture_price_wind REAL,
                    capture_price_battery REAL,
                    capture_price_gas REAL,
                    capture_price_coal REAL,
                    capture_price_hydro REAL,
                    baseload_price REAL,
                    tb2_spread REAL,
                    tb4_spread REAL,
                    tb8_spread REAL,
                    intervals_count INTEGER,
                    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(metric_date, region)
                )
            """)

            # Add hydro columns to existing daily_metrics tables
            for col in ['capture_hydro', 'capture_price_hydro']:
                await conn.execute(f"""
                    DO $$ BEGIN
                        ALTER TABLE daily_metrics ADD COLUMN {col} REAL;
                    EXCEPTION WHEN duplicate_column THEN NULL;
                    END $$
                """)

            # Price setter data from NEMDE archive
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS price_setter_data (
                    id BIGSERIAL PRIMARY KEY,
                    period_id TIMESTAMP NOT NULL,
                    region TEXT NOT NULL,
                    price REAL NOT NULL,
                    duid TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(period_id, region, duid)
                )
            """)

            # Add price setter columns to daily_metrics
            for col in [
                'ps_freq_solar', 'ps_freq_wind', 'ps_freq_battery',
                'ps_freq_gas', 'ps_freq_coal', 'ps_freq_hydro',
                'ps_price_solar', 'ps_price_wind', 'ps_price_battery',
                'ps_price_gas', 'ps_price_coal', 'ps_price_hydro',
            ]:
                await conn.execute(f"""
                    DO $$ BEGIN
                        ALTER TABLE daily_metrics ADD COLUMN {col} REAL;
                    EXCEPTION WHEN duplicate_column THEN NULL;
                    END $$
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

            # Daily metrics indexes
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_metrics_date ON daily_metrics(metric_date)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_metrics_region_date ON daily_metrics(region, metric_date)")

            # Price setter indexes
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_price_setter_period ON price_setter_data(period_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_price_setter_region_period ON price_setter_data(region, period_id)")

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

    async def insert_price_setter_data(self, df: pd.DataFrame) -> int:
        """Insert price setter data from DataFrame."""
        if df.empty:
            return 0

        records = []
        for _, row in df.iterrows():
            records.append((
                row['period_id'].to_pydatetime() if hasattr(row['period_id'], 'to_pydatetime') else row['period_id'],
                row['region'],
                row['price'],
                row['duid'],
            ))

        async with self._pool.acquire() as conn:
            await conn.executemany("""
                INSERT INTO price_setter_data (period_id, region, price, duid)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (period_id, region, duid) DO UPDATE SET
                    price = EXCLUDED.price
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

    async def get_dispatch_dates_with_data(self, start_date: datetime, end_date: datetime, min_intervals: int = 280) -> set:
        """Get dates that have sufficient dispatch data (complete 5-min interval coverage).

        A complete day has 288 5-minute intervals. Default threshold of 280 allows
        for minor gaps while catching days with significant missing data.
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT settlementdate::DATE as data_date,
                       COUNT(DISTINCT settlementdate) as interval_count
                FROM dispatch_data
                WHERE settlementdate::DATE BETWEEN $1::DATE AND $2::DATE
                GROUP BY settlementdate::DATE
                HAVING COUNT(DISTINCT settlementdate) >= $3
            """, start_date, end_date, min_intervals)

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
            # Use interval arithmetic instead of TO_TIMESTAMP to preserve naive timestamp type
            # TO_TIMESTAMP returns timestamptz (UTC) which causes timezone issues
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
                    settlementdate - (
                        (EXTRACT(EPOCH FROM settlementdate)::BIGINT % ($3 * 60)) * INTERVAL '1 second'
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
            # Use interval arithmetic instead of TO_TIMESTAMP to preserve naive timestamp type
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
                    settlementdate - (
                        (EXTRACT(EPOCH FROM settlementdate)::BIGINT % ($3 * 60)) * INTERVAL '1 second'
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

    async def get_aggregated_price_history_by_dates(self, region: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get price history with time-based aggregation for a specific date range."""
        # Calculate hours from date range for aggregation level
        delta = end_date - start_date
        hours = int(delta.total_seconds() / 3600)
        aggregation_minutes = calculate_aggregation_minutes(hours)

        if aggregation_minutes <= 30:
            # For short ranges, return merged data without aggregation
            # Use subquery to get demand from DISPATCH records when price record has NULL demand
            async with self._pool.acquire() as conn:
                rows = await conn.fetch("""
                    WITH price_source AS (
                        SELECT DISTINCT ON (settlementdate)
                            settlementdate,
                            price,
                            totaldemand,
                            price_type
                        FROM price_data
                        WHERE region = $1
                        AND settlementdate >= $2
                        AND settlementdate <= $3
                        ORDER BY settlementdate,
                            CASE WHEN price_type = 'PUBLIC' THEN 1 WHEN price_type = 'TRADING' THEN 2 ELSE 3 END
                    ),
                    demand_source AS (
                        SELECT DISTINCT ON (settlementdate)
                            settlementdate,
                            totaldemand as dispatch_demand
                        FROM price_data
                        WHERE region = $1
                        AND settlementdate >= $2
                        AND settlementdate <= $3
                        AND price_type = 'DISPATCH'
                        AND totaldemand IS NOT NULL
                        AND totaldemand > 0
                        ORDER BY settlementdate
                    )
                    SELECT
                        p.settlementdate,
                        p.price,
                        COALESCE(NULLIF(p.totaldemand, 0), d.dispatch_demand, p.totaldemand) as totaldemand
                    FROM price_source p
                    LEFT JOIN demand_source d ON p.settlementdate = d.settlementdate
                    ORDER BY p.settlementdate
                """, region, start_date, end_date)

            if not rows:
                return pd.DataFrame()

            df = pd.DataFrame([dict(row) for row in rows])
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            df['source_type'] = 'MERGED'
            return df

        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                WITH price_source AS (
                    SELECT DISTINCT ON (settlementdate)
                        settlementdate,
                        price,
                        totaldemand,
                        price_type
                    FROM price_data
                    WHERE region = $1
                    AND settlementdate >= $2
                    AND settlementdate <= $3
                    ORDER BY settlementdate,
                        CASE WHEN price_type = 'PUBLIC' THEN 1 WHEN price_type = 'TRADING' THEN 2 ELSE 3 END
                ),
                demand_source AS (
                    SELECT DISTINCT ON (settlementdate)
                        settlementdate,
                        totaldemand as dispatch_demand
                    FROM price_data
                    WHERE region = $1
                    AND settlementdate >= $2
                    AND settlementdate <= $3
                    AND price_type = 'DISPATCH'
                    AND totaldemand IS NOT NULL
                    AND totaldemand > 0
                    ORDER BY settlementdate
                ),
                merged AS (
                    SELECT
                        p.settlementdate,
                        p.price,
                        COALESCE(NULLIF(p.totaldemand, 0), d.dispatch_demand, p.totaldemand) as totaldemand
                    FROM price_source p
                    LEFT JOIN demand_source d ON p.settlementdate = d.settlementdate
                )
                SELECT
                    settlementdate - (
                        (EXTRACT(EPOCH FROM settlementdate)::BIGINT % ($4 * 60)) * INTERVAL '1 second'
                    ) as settlementdate,
                    AVG(price) as price,
                    AVG(totaldemand) as totaldemand,
                    COUNT(*) as sample_count
                FROM merged
                GROUP BY 1
                ORDER BY settlementdate ASC
            """, region, start_date, end_date, aggregation_minutes)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame([dict(row) for row in rows])
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        df['source_type'] = 'AGGREGATED'
        return df

    async def get_region_generation_history_by_dates(self, region: str, start_date: datetime, end_date: datetime, aggregation_minutes: int) -> pd.DataFrame:
        """Get historical generation by fuel source for a specific date range."""
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
                    AND d.settlementdate >= $2
                    AND d.settlementdate <= $3
                    GROUP BY d.settlementdate, g.fuel_source
                )
                SELECT
                    settlementdate - (
                        (EXTRACT(EPOCH FROM settlementdate)::BIGINT % ($4 * 60)) * INTERVAL '1 second'
                    ) as period,
                    fuel_source,
                    AVG(total_mw) as generation_mw,
                    COUNT(*) as sample_count
                FROM timestamp_totals
                GROUP BY 1, 2
                ORDER BY period ASC, fuel_source
            """, region, start_date, end_date, aggregation_minutes)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame([dict(row) for row in rows])
        df['period'] = pd.to_datetime(df['period'])
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

    async def get_region_data_range(self, region: str) -> Dict[str, Any]:
        """Get the available date range for a specific region's price data.

        Args:
            region: Region code (NSW, VIC, QLD, SA, TAS)

        Returns:
            Dict with earliest_date and latest_date for the region
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT
                    MIN(settlementdate) as earliest_date,
                    MAX(settlementdate) as latest_date
                FROM price_data
                WHERE region = $1
            """, region)

        return {
            'earliest_date': to_aest_isoformat(row['earliest_date']) if row and row['earliest_date'] else None,
            'latest_date': to_aest_isoformat(row['latest_date']) if row and row['latest_date'] else None
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
        tables = ['dispatch_data', 'price_data', 'generator_info', 'daily_metrics', 'price_setter_data']
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
                elif table == 'price_setter_data':
                    row = await conn.fetchrow("""
                        SELECT COUNT(*) as total_records,
                               MIN(period_id) as earliest_date,
                               MAX(period_id) as latest_date,
                               COUNT(DISTINCT period_id::DATE) as days_with_data
                        FROM price_setter_data
                    """)
                    table_stats.append({
                        'table': table,
                        'total_records': row['total_records'] or 0,
                        'earliest_date': str(row['earliest_date']) if row['earliest_date'] else None,
                        'latest_date': str(row['latest_date']) if row['latest_date'] else None,
                        'days_with_data': row['days_with_data'] or 0,
                        'expected_interval': 5
                    })
                elif table == 'daily_metrics':
                    # Daily metrics - date-based, not 5-min intervals
                    row = await conn.fetchrow("""
                        SELECT COUNT(*) as total_records,
                               MIN(metric_date) as earliest_date,
                               MAX(metric_date) as latest_date,
                               COUNT(DISTINCT metric_date) as days_with_data,
                               COUNT(DISTINCT region) as region_count
                        FROM daily_metrics
                    """)
                    table_stats.append({
                        'table': table,
                        'total_records': row['total_records'] or 0,
                        'earliest_date': str(row['earliest_date']) if row['earliest_date'] else None,
                        'latest_date': str(row['latest_date']) if row['latest_date'] else None,
                        'days_with_data': row['days_with_data'] or 0,
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

    # Export methods for CSV downloads
    async def get_unique_fuel_sources(self) -> List[str]:
        """Get list of all unique fuel sources for filter dropdown."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT DISTINCT fuel_source FROM generator_info WHERE fuel_source IS NOT NULL ORDER BY fuel_source"
            )
        return [row['fuel_source'] for row in rows]

    async def export_price_data(self, start_date: datetime, end_date: datetime,
                                 regions: Optional[List[str]] = None) -> pd.DataFrame:
        """Export price data for CSV download.

        Args:
            start_date: Start of date range
            end_date: End of date range
            regions: Optional list of regions to filter (None = all regions)

        Returns:
            DataFrame with price data
        """
        async with self._pool.acquire() as conn:
            if regions:
                rows = await conn.fetch("""
                    SELECT settlementdate, region, price, totaldemand, price_type
                    FROM price_data
                    WHERE settlementdate BETWEEN $1 AND $2
                    AND region = ANY($3)
                    ORDER BY settlementdate, region
                """, start_date, end_date, regions)
            else:
                rows = await conn.fetch("""
                    SELECT settlementdate, region, price, totaldemand, price_type
                    FROM price_data
                    WHERE settlementdate BETWEEN $1 AND $2
                    ORDER BY settlementdate, region
                """, start_date, end_date)

        if not rows:
            return pd.DataFrame(columns=['settlementdate', 'region', 'price', 'totaldemand', 'price_type'])

        df = pd.DataFrame([dict(row) for row in rows])
        df['settlementdate'] = pd.to_datetime(df['settlementdate']).dt.strftime('%Y-%m-%d %H:%M:%S')
        return df

    async def export_generation_data(self, start_date: datetime, end_date: datetime,
                                      regions: Optional[List[str]] = None,
                                      fuel_sources: Optional[List[str]] = None) -> pd.DataFrame:
        """Export generation data for CSV download.

        Args:
            start_date: Start of date range
            end_date: End of date range
            regions: Optional list of regions to filter
            fuel_sources: Optional list of fuel sources to filter

        Returns:
            DataFrame with generation data including generator metadata
        """
        async with self._pool.acquire() as conn:
            base_query = """
                SELECT
                    d.settlementdate,
                    d.duid,
                    g.station_name,
                    g.region,
                    g.fuel_source,
                    g.technology_type,
                    d.scadavalue as generation_mw,
                    d.totalcleared,
                    d.availability
                FROM dispatch_data d
                LEFT JOIN generator_info g ON d.duid = g.duid
                WHERE d.settlementdate BETWEEN $1 AND $2
            """
            params = [start_date, end_date]
            param_idx = 3

            if regions:
                base_query += f" AND g.region = ANY(${param_idx})"
                params.append(regions)
                param_idx += 1

            if fuel_sources:
                base_query += f" AND g.fuel_source = ANY(${param_idx})"
                params.append(fuel_sources)

            base_query += " ORDER BY d.settlementdate, g.region, g.fuel_source, d.duid"

            rows = await conn.fetch(base_query, *params)

        if not rows:
            return pd.DataFrame(columns=['settlementdate', 'duid', 'station_name', 'region',
                                         'fuel_source', 'technology_type', 'generation_mw',
                                         'totalcleared', 'availability'])

        df = pd.DataFrame([dict(row) for row in rows])
        df['settlementdate'] = pd.to_datetime(df['settlementdate']).dt.strftime('%Y-%m-%d %H:%M:%S')
        return df

    async def export_latest_pasa_data(self, pasa_type: str,
                                       regions: Optional[List[str]] = None) -> pd.DataFrame:
        """Export the latest PASA forecast data for CSV download.

        Args:
            pasa_type: 'pdpasa' or 'stpasa'
            regions: Optional list of regions to filter (e.g., ['NSW', 'VIC'])

        Returns:
            DataFrame with the latest PASA forecast data
        """
        table = 'pdpasa_data' if pasa_type == 'pdpasa' else 'stpasa_data'

        # Convert simple region names to PASA format if needed
        pasa_regions = None
        if regions:
            pasa_regions = []
            for r in regions:
                if not r.endswith('1'):
                    pasa_regions.append(r + '1')
                else:
                    pasa_regions.append(r)

        async with self._pool.acquire() as conn:
            if pasa_regions:
                rows = await conn.fetch(f"""
                    SELECT run_datetime, interval_datetime, regionid,
                           demand10, demand50, demand90,
                           reservereq, capacityreq,
                           aggregatecapacityavailable, aggregatepasaavailability,
                           surplusreserve, lorcondition,
                           calculatedlor1level, calculatedlor2level
                    FROM {table}
                    WHERE run_datetime = (SELECT MAX(run_datetime) FROM {table})
                    AND regionid = ANY($1)
                    ORDER BY interval_datetime, regionid
                """, pasa_regions)
            else:
                rows = await conn.fetch(f"""
                    SELECT run_datetime, interval_datetime, regionid,
                           demand10, demand50, demand90,
                           reservereq, capacityreq,
                           aggregatecapacityavailable, aggregatepasaavailability,
                           surplusreserve, lorcondition,
                           calculatedlor1level, calculatedlor2level
                    FROM {table}
                    WHERE run_datetime = (SELECT MAX(run_datetime) FROM {table})
                    ORDER BY interval_datetime, regionid
                """)

        if not rows:
            return pd.DataFrame(columns=['run_datetime', 'interval_datetime', 'regionid',
                                         'demand10', 'demand50', 'demand90',
                                         'reservereq', 'capacityreq',
                                         'aggregatecapacityavailable', 'aggregatepasaavailability',
                                         'surplusreserve', 'lorcondition',
                                         'calculatedlor1level', 'calculatedlor2level'])

        df = pd.DataFrame([dict(row) for row in rows])
        df['run_datetime'] = pd.to_datetime(df['run_datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df['interval_datetime'] = pd.to_datetime(df['interval_datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')
        return df

    async def export_daily_metrics(self, start_date: datetime, end_date: datetime,
                                    regions: Optional[List[str]] = None) -> pd.DataFrame:
        """Export daily metrics data for CSV download."""
        async with self._pool.acquire() as conn:
            base_query = """
                SELECT
                    metric_date, region, baseload_price,
                    capture_solar, capture_wind, capture_battery, capture_gas, capture_coal, capture_hydro,
                    capture_price_solar, capture_price_wind, capture_price_battery,
                    capture_price_gas, capture_price_coal, capture_price_hydro,
                    tb2_spread, tb4_spread, tb8_spread,
                    intervals_count,
                    ps_freq_solar, ps_freq_wind, ps_freq_battery,
                    ps_freq_gas, ps_freq_coal, ps_freq_hydro,
                    ps_price_solar, ps_price_wind, ps_price_battery,
                    ps_price_gas, ps_price_coal, ps_price_hydro
                FROM daily_metrics
                WHERE metric_date BETWEEN $1::DATE AND $2::DATE
            """
            params = [start_date, end_date]

            if regions:
                base_query += " AND region = ANY($3)"
                params.append(regions)

            base_query += " ORDER BY metric_date, region"
            rows = await conn.fetch(base_query, *params)

        if not rows:
            return pd.DataFrame(columns=['metric_date', 'region', 'baseload_price',
                                         'capture_solar', 'capture_wind', 'capture_battery',
                                         'capture_gas', 'capture_coal', 'capture_hydro',
                                         'capture_price_solar', 'capture_price_wind', 'capture_price_battery',
                                         'capture_price_gas', 'capture_price_coal', 'capture_price_hydro',
                                         'tb2_spread', 'tb4_spread', 'tb8_spread', 'intervals_count',
                                         'ps_freq_solar', 'ps_freq_wind', 'ps_freq_battery',
                                         'ps_freq_gas', 'ps_freq_coal', 'ps_freq_hydro',
                                         'ps_price_solar', 'ps_price_wind', 'ps_price_battery',
                                         'ps_price_gas', 'ps_price_coal', 'ps_price_hydro'])

        return pd.DataFrame([dict(row) for row in rows])

    async def get_export_data_ranges(self) -> Dict[str, Any]:
        """Get available data ranges for all exportable data types."""
        async with self._pool.acquire() as conn:
            price_range = await conn.fetchrow("""
                SELECT MIN(settlementdate) as earliest, MAX(settlementdate) as latest
                FROM price_data
            """)

            generation_range = await conn.fetchrow("""
                SELECT MIN(settlementdate) as earliest, MAX(settlementdate) as latest
                FROM dispatch_data
            """)

            pdpasa_range = await conn.fetchrow("""
                SELECT MIN(interval_datetime) as earliest, MAX(interval_datetime) as latest
                FROM pdpasa_data
            """)

            stpasa_range = await conn.fetchrow("""
                SELECT MIN(interval_datetime) as earliest, MAX(interval_datetime) as latest
                FROM stpasa_data
            """)

            metrics_range = await conn.fetchrow("""
                SELECT MIN(metric_date) as earliest, MAX(metric_date) as latest
                FROM daily_metrics
            """)

        return {
            'prices': {
                'earliest_date': to_aest_isoformat(price_range['earliest']) if price_range and price_range['earliest'] else None,
                'latest_date': to_aest_isoformat(price_range['latest']) if price_range and price_range['latest'] else None
            },
            'generation': {
                'earliest_date': to_aest_isoformat(generation_range['earliest']) if generation_range and generation_range['earliest'] else None,
                'latest_date': to_aest_isoformat(generation_range['latest']) if generation_range and generation_range['latest'] else None
            },
            'pasa': {
                'pdpasa': {
                    'earliest_date': to_aest_isoformat(pdpasa_range['earliest']) if pdpasa_range and pdpasa_range['earliest'] else None,
                    'latest_date': to_aest_isoformat(pdpasa_range['latest']) if pdpasa_range and pdpasa_range['latest'] else None
                },
                'stpasa': {
                    'earliest_date': to_aest_isoformat(stpasa_range['earliest']) if stpasa_range and stpasa_range['earliest'] else None,
                    'latest_date': to_aest_isoformat(stpasa_range['latest']) if stpasa_range and stpasa_range['latest'] else None
                }
            },
            'metrics': {
                'earliest_date': str(metrics_range['earliest']) if metrics_range and metrics_range['earliest'] else None,
                'latest_date': str(metrics_range['latest']) if metrics_range and metrics_range['latest'] else None
            }
        }

    # ---- Daily Metrics (Capture Rates & TB Spreads) ----

    async def calculate_daily_metrics(self, region: str, metric_date) -> bool:
        """Calculate and upsert capture rates and TB spreads for a region and date.

        Uses merged prices (PUBLIC preferred, DISPATCH as fallback) to maximise
        historical coverage.

        Returns True if metrics were calculated, False if skipped (incomplete data).
        """
        # CTE that deduplicates prices: prefer PUBLIC over DISPATCH per interval
        merged_prices_cte = """
            merged_prices AS (
                SELECT DISTINCT ON (settlementdate)
                    settlementdate, region, price, price_type
                FROM price_data
                WHERE region = $1
                  AND settlementdate::DATE = $2::DATE
                ORDER BY settlementdate, CASE WHEN price_type = 'PUBLIC' THEN 0 ELSE 1 END
            )
        """

        async with self._pool.acquire() as conn:
            # Baseload price and interval count
            baseload_row = await conn.fetchrow(f"""
                WITH {merged_prices_cte}
                SELECT AVG(price) AS baseload_price, COUNT(*) AS intervals_count
                FROM merged_prices
            """, region, metric_date)

            if not baseload_row or (baseload_row['intervals_count'] or 0) < 240:
                return False

            baseload_price = float(baseload_row['baseload_price'])
            intervals_count = baseload_row['intervals_count']

            # Capture prices via single-pass conditional aggregation
            capture_row = await conn.fetchrow(f"""
                WITH {merged_prices_cte},
                gen AS (
                    SELECT
                        d.settlementdate,
                        COALESCE(g.fuel_source, 'Unknown') AS fuel_source,
                        GREATEST(d.scadavalue, 0) AS generation_mw,
                        p.price
                    FROM dispatch_data d
                    INNER JOIN generator_info g ON d.duid = g.duid
                    INNER JOIN merged_prices p
                        ON p.settlementdate = d.settlementdate
                    WHERE g.region = $1
                      AND d.settlementdate::DATE = $2::DATE
                )
                SELECT
                    CASE WHEN SUM(CASE WHEN fuel_source='Solar'   THEN generation_mw END) > 0
                         THEN SUM(CASE WHEN fuel_source='Solar'   THEN generation_mw * price END) /
                              SUM(CASE WHEN fuel_source='Solar'   THEN generation_mw END)
                    END AS cp_solar,
                    CASE WHEN SUM(CASE WHEN fuel_source='Wind'    THEN generation_mw END) > 0
                         THEN SUM(CASE WHEN fuel_source='Wind'    THEN generation_mw * price END) /
                              SUM(CASE WHEN fuel_source='Wind'    THEN generation_mw END)
                    END AS cp_wind,
                    CASE WHEN SUM(CASE WHEN fuel_source='Battery' THEN generation_mw END) > 0
                         THEN SUM(CASE WHEN fuel_source='Battery' THEN generation_mw * price END) /
                              SUM(CASE WHEN fuel_source='Battery' THEN generation_mw END)
                    END AS cp_battery,
                    CASE WHEN SUM(CASE WHEN fuel_source='Gas'     THEN generation_mw END) > 0
                         THEN SUM(CASE WHEN fuel_source='Gas'     THEN generation_mw * price END) /
                              SUM(CASE WHEN fuel_source='Gas'     THEN generation_mw END)
                    END AS cp_gas,
                    CASE WHEN SUM(CASE WHEN fuel_source='Coal'    THEN generation_mw END) > 0
                         THEN SUM(CASE WHEN fuel_source='Coal'    THEN generation_mw * price END) /
                              SUM(CASE WHEN fuel_source='Coal'    THEN generation_mw END)
                    END AS cp_coal,
                    CASE WHEN SUM(CASE WHEN fuel_source='Hydro'   THEN generation_mw END) > 0
                         THEN SUM(CASE WHEN fuel_source='Hydro'   THEN generation_mw * price END) /
                              SUM(CASE WHEN fuel_source='Hydro'   THEN generation_mw END)
                    END AS cp_hydro
                FROM gen
            """, region, metric_date)

            # TB spreads
            tb_row = await conn.fetchrow(f"""
                WITH {merged_prices_cte},
                ranked AS (
                    SELECT
                        price,
                        ROW_NUMBER() OVER (ORDER BY price DESC) AS rank_desc,
                        ROW_NUMBER() OVER (ORDER BY price ASC)  AS rank_asc
                    FROM merged_prices
                )
                SELECT
                    AVG(CASE WHEN rank_desc <= 24 THEN price END) -
                    AVG(CASE WHEN rank_asc  <= 24 THEN price END) AS tb2_spread,
                    AVG(CASE WHEN rank_desc <= 48 THEN price END) -
                    AVG(CASE WHEN rank_asc  <= 48 THEN price END) AS tb4_spread,
                    AVG(CASE WHEN rank_desc <= 96 THEN price END) -
                    AVG(CASE WHEN rank_asc  <= 96 THEN price END) AS tb8_spread
                FROM ranked
            """, region, metric_date)

            # Compute capture ratios
            def safe_ratio(cp, bp):
                if cp is not None and bp and bp != 0:
                    return float(cp) / bp
                return None

            cp_solar = capture_row['cp_solar'] if capture_row else None
            cp_wind = capture_row['cp_wind'] if capture_row else None
            cp_battery = capture_row['cp_battery'] if capture_row else None
            cp_gas = capture_row['cp_gas'] if capture_row else None
            cp_coal = capture_row['cp_coal'] if capture_row else None
            cp_hydro = capture_row['cp_hydro'] if capture_row else None

            await conn.execute("""
                INSERT INTO daily_metrics
                (metric_date, region,
                 capture_solar, capture_wind, capture_battery, capture_gas, capture_coal, capture_hydro,
                 capture_price_solar, capture_price_wind, capture_price_battery,
                 capture_price_gas, capture_price_coal, capture_price_hydro,
                 baseload_price, tb2_spread, tb4_spread, tb8_spread,
                 intervals_count, calculated_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,NOW())
                ON CONFLICT (metric_date, region) DO UPDATE SET
                    capture_solar          = EXCLUDED.capture_solar,
                    capture_wind           = EXCLUDED.capture_wind,
                    capture_battery        = EXCLUDED.capture_battery,
                    capture_gas            = EXCLUDED.capture_gas,
                    capture_coal           = EXCLUDED.capture_coal,
                    capture_hydro          = EXCLUDED.capture_hydro,
                    capture_price_solar    = EXCLUDED.capture_price_solar,
                    capture_price_wind     = EXCLUDED.capture_price_wind,
                    capture_price_battery  = EXCLUDED.capture_price_battery,
                    capture_price_gas      = EXCLUDED.capture_price_gas,
                    capture_price_coal     = EXCLUDED.capture_price_coal,
                    capture_price_hydro    = EXCLUDED.capture_price_hydro,
                    baseload_price         = EXCLUDED.baseload_price,
                    tb2_spread             = EXCLUDED.tb2_spread,
                    tb4_spread             = EXCLUDED.tb4_spread,
                    tb8_spread             = EXCLUDED.tb8_spread,
                    intervals_count        = EXCLUDED.intervals_count,
                    calculated_at          = NOW()
            """,
                metric_date, region,
                safe_ratio(cp_solar, baseload_price),
                safe_ratio(cp_wind, baseload_price),
                safe_ratio(cp_battery, baseload_price),
                safe_ratio(cp_gas, baseload_price),
                safe_ratio(cp_coal, baseload_price),
                safe_ratio(cp_hydro, baseload_price),
                float(cp_solar) if cp_solar is not None else None,
                float(cp_wind) if cp_wind is not None else None,
                float(cp_battery) if cp_battery is not None else None,
                float(cp_gas) if cp_gas is not None else None,
                float(cp_coal) if cp_coal is not None else None,
                float(cp_hydro) if cp_hydro is not None else None,
                baseload_price,
                float(tb_row['tb2_spread']) if tb_row and tb_row['tb2_spread'] is not None else None,
                float(tb_row['tb4_spread']) if tb_row and tb_row['tb4_spread'] is not None else None,
                float(tb_row['tb8_spread']) if tb_row and tb_row['tb8_spread'] is not None else None,
                intervals_count
            )

        logger.info(f"Calculated daily metrics for {region} {metric_date}: "
                     f"baseload=${baseload_price:.2f}, intervals={intervals_count}")
        return True

    async def calculate_daily_price_setter_metrics(self, region: str, metric_date) -> bool:
        """Calculate price setter frequency and average price per fuel type.

        Updates ps_freq_* and ps_price_* columns in daily_metrics.
        Requires price_setter_data to be populated and a daily_metrics row to exist.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("""
                WITH ps_fuel AS (
                    SELECT
                        ps.period_id,
                        COALESCE(g.fuel_source, 'Unknown') AS fuel_source,
                        ps.price
                    FROM price_setter_data ps
                    INNER JOIN generator_info g ON ps.duid = g.duid
                    WHERE ps.region = $1
                      AND ps.period_id::DATE = $2::DATE
                )
                SELECT
                    COUNT(DISTINCT period_id) AS total_intervals,
                    COUNT(DISTINCT CASE WHEN fuel_source = 'Solar'   THEN period_id END)::REAL
                        / NULLIF(COUNT(DISTINCT period_id), 0) AS ps_freq_solar,
                    COUNT(DISTINCT CASE WHEN fuel_source = 'Wind'    THEN period_id END)::REAL
                        / NULLIF(COUNT(DISTINCT period_id), 0) AS ps_freq_wind,
                    COUNT(DISTINCT CASE WHEN fuel_source = 'Battery' THEN period_id END)::REAL
                        / NULLIF(COUNT(DISTINCT period_id), 0) AS ps_freq_battery,
                    COUNT(DISTINCT CASE WHEN fuel_source = 'Gas'     THEN period_id END)::REAL
                        / NULLIF(COUNT(DISTINCT period_id), 0) AS ps_freq_gas,
                    COUNT(DISTINCT CASE WHEN fuel_source = 'Coal'    THEN period_id END)::REAL
                        / NULLIF(COUNT(DISTINCT period_id), 0) AS ps_freq_coal,
                    COUNT(DISTINCT CASE WHEN fuel_source = 'Hydro'   THEN period_id END)::REAL
                        / NULLIF(COUNT(DISTINCT period_id), 0) AS ps_freq_hydro,
                    AVG(CASE WHEN fuel_source = 'Solar'   THEN price END) AS ps_price_solar,
                    AVG(CASE WHEN fuel_source = 'Wind'    THEN price END) AS ps_price_wind,
                    AVG(CASE WHEN fuel_source = 'Battery' THEN price END) AS ps_price_battery,
                    AVG(CASE WHEN fuel_source = 'Gas'     THEN price END) AS ps_price_gas,
                    AVG(CASE WHEN fuel_source = 'Coal'    THEN price END) AS ps_price_coal,
                    AVG(CASE WHEN fuel_source = 'Hydro'   THEN price END) AS ps_price_hydro
                FROM ps_fuel
            """, region, metric_date)

            if not row or not row['total_intervals']:
                return False

            def safe_float(v):
                return float(v) if v is not None else None

            updated = await conn.execute("""
                UPDATE daily_metrics SET
                    ps_freq_solar = $3, ps_freq_wind = $4, ps_freq_battery = $5,
                    ps_freq_gas = $6, ps_freq_coal = $7, ps_freq_hydro = $8,
                    ps_price_solar = $9, ps_price_wind = $10, ps_price_battery = $11,
                    ps_price_gas = $12, ps_price_coal = $13, ps_price_hydro = $14,
                    calculated_at = NOW()
                WHERE metric_date = $1 AND region = $2
            """,
                metric_date, region,
                safe_float(row['ps_freq_solar']),
                safe_float(row['ps_freq_wind']),
                safe_float(row['ps_freq_battery']),
                safe_float(row['ps_freq_gas']),
                safe_float(row['ps_freq_coal']),
                safe_float(row['ps_freq_hydro']),
                safe_float(row['ps_price_solar']),
                safe_float(row['ps_price_wind']),
                safe_float(row['ps_price_battery']),
                safe_float(row['ps_price_gas']),
                safe_float(row['ps_price_coal']),
                safe_float(row['ps_price_hydro']),
            )

            if updated == 'UPDATE 0':
                logger.warning(f"No daily_metrics row to update for {region} {metric_date}")
                return False

        logger.info(f"Calculated price setter metrics for {region} {metric_date}")
        return True

    async def get_daily_metrics(self, region: str, start_date, end_date) -> List[Dict[str, Any]]:
        """Fetch precalculated daily metrics for a region and date range."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    metric_date,
                    region,
                    capture_solar, capture_wind, capture_battery, capture_gas, capture_coal, capture_hydro,
                    capture_price_solar, capture_price_wind, capture_price_battery,
                    capture_price_gas, capture_price_coal, capture_price_hydro,
                    baseload_price,
                    tb2_spread, tb4_spread, tb8_spread,
                    intervals_count,
                    ps_freq_solar, ps_freq_wind, ps_freq_battery,
                    ps_freq_gas, ps_freq_coal, ps_freq_hydro,
                    ps_price_solar, ps_price_wind, ps_price_battery,
                    ps_price_gas, ps_price_coal, ps_price_hydro
                FROM daily_metrics
                WHERE region = $1
                  AND metric_date >= $2::DATE
                  AND metric_date <= $3::DATE
                ORDER BY metric_date ASC
            """, region, start_date, end_date)

        return [
            {**dict(r), 'metric_date': r['metric_date'].isoformat()}
            for r in rows
        ]

    async def get_metrics_summary(self, region: str, start_date, end_date) -> Dict[str, Any]:
        """Get averaged metrics for a region over a date range."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT
                    AVG(capture_solar) AS capture_solar,
                    AVG(capture_wind) AS capture_wind,
                    AVG(capture_battery) AS capture_battery,
                    AVG(capture_gas) AS capture_gas,
                    AVG(capture_coal) AS capture_coal,
                    AVG(capture_hydro) AS capture_hydro,
                    AVG(capture_price_solar) AS capture_price_solar,
                    AVG(capture_price_wind) AS capture_price_wind,
                    AVG(capture_price_battery) AS capture_price_battery,
                    AVG(capture_price_gas) AS capture_price_gas,
                    AVG(capture_price_coal) AS capture_price_coal,
                    AVG(capture_price_hydro) AS capture_price_hydro,
                    AVG(baseload_price) AS baseload_price,
                    AVG(tb2_spread) AS tb2_spread,
                    AVG(tb4_spread) AS tb4_spread,
                    AVG(tb8_spread) AS tb8_spread,
                    AVG(ps_freq_solar) AS ps_freq_solar,
                    AVG(ps_freq_wind) AS ps_freq_wind,
                    AVG(ps_freq_battery) AS ps_freq_battery,
                    AVG(ps_freq_gas) AS ps_freq_gas,
                    AVG(ps_freq_coal) AS ps_freq_coal,
                    AVG(ps_freq_hydro) AS ps_freq_hydro,
                    AVG(ps_price_solar) AS ps_price_solar,
                    AVG(ps_price_wind) AS ps_price_wind,
                    AVG(ps_price_battery) AS ps_price_battery,
                    AVG(ps_price_gas) AS ps_price_gas,
                    AVG(ps_price_coal) AS ps_price_coal,
                    AVG(ps_price_hydro) AS ps_price_hydro,
                    COUNT(*) AS days_count
                FROM daily_metrics
                WHERE region = $1
                  AND metric_date >= $2::DATE
                  AND metric_date <= $3::DATE
            """, region, start_date, end_date)

        if not row or row['days_count'] == 0:
            return None

        return {k: (float(v) if v is not None else None) for k, v in dict(row).items()}

    async def get_earliest_metrics_date(self):
        """Get the earliest date where both dispatch and price data overlap."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT GREATEST(
                    (SELECT MIN(settlementdate)::DATE FROM dispatch_data),
                    (SELECT MIN(settlementdate)::DATE FROM price_data)
                ) AS earliest_date
            """)
        if row and row['earliest_date']:
            return row['earliest_date']
        return None
