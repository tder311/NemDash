#!/usr/bin/env python3
"""
Migrate NEM Dashboard data from SQLite to PostgreSQL.

This script copies all data from an existing SQLite database to a new PostgreSQL database.
It handles schema creation and batch data transfer for optimal performance.

Usage:
    # Set environment variables
    export SQLITE_PATH=./data/nem_dispatch.db
    export POSTGRES_URL=postgresql://postgres:localdev@localhost:5432/nem_dashboard

    # Run migration
    python scripts/migrate_to_postgres.py

    # Or with command line arguments
    python scripts/migrate_to_postgres.py --sqlite ./data/nem_dispatch.db --postgres postgresql://...
"""

import asyncio
import argparse
import os
import sys
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import aiosqlite

try:
    import asyncpg
except ImportError:
    print("ERROR: asyncpg is not installed. Install it with: pip install asyncpg")
    sys.exit(1)


BATCH_SIZE = 10000  # Number of records to transfer at a time


async def get_table_count(source_conn, table: str) -> int:
    """Get the number of rows in a SQLite table."""
    cursor = await source_conn.execute(f"SELECT COUNT(*) FROM {table}")
    row = await cursor.fetchone()
    return row[0] if row else 0


async def migrate_table(
    source_conn: aiosqlite.Connection,
    target_pool: asyncpg.Pool,
    table: str,
    columns: list[str],
    progress_callback=None
) -> int:
    """Migrate a single table from SQLite to PostgreSQL."""
    total_migrated = 0
    offset = 0

    # Get column list for SQL
    col_list = ', '.join(columns)

    # PostgreSQL parameter placeholders
    pg_placeholders = ', '.join(f'${i+1}' for i in range(len(columns)))

    # Build PostgreSQL INSERT with ON CONFLICT
    if table == 'dispatch_data':
        conflict_cols = 'settlementdate, duid'
        update_cols = ', '.join(f'{c} = EXCLUDED.{c}' for c in columns if c not in ['id', 'settlementdate', 'duid', 'created_at'])
    elif table == 'price_data':
        conflict_cols = 'settlementdate, region, price_type'
        update_cols = ', '.join(f'{c} = EXCLUDED.{c}' for c in columns if c not in ['id', 'settlementdate', 'region', 'price_type', 'created_at'])
    elif table == 'interconnector_data':
        conflict_cols = 'settlementdate, interconnector'
        update_cols = ', '.join(f'{c} = EXCLUDED.{c}' for c in columns if c not in ['id', 'settlementdate', 'interconnector', 'created_at'])
    elif table == 'generator_info':
        conflict_cols = 'duid'
        update_cols = ', '.join(f'{c} = EXCLUDED.{c}' for c in columns if c not in ['duid', 'updated_at'])
    else:
        conflict_cols = None
        update_cols = None

    if conflict_cols and update_cols:
        pg_insert = f"""
            INSERT INTO {table} ({col_list})
            VALUES ({pg_placeholders})
            ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_cols}
        """
    else:
        pg_insert = f"INSERT INTO {table} ({col_list}) VALUES ({pg_placeholders})"

    while True:
        # Fetch batch from SQLite
        cursor = await source_conn.execute(
            f"SELECT {col_list} FROM {table} LIMIT {BATCH_SIZE} OFFSET {offset}"
        )
        rows = await cursor.fetchall()

        if not rows:
            break

        # Insert batch into PostgreSQL
        async with target_pool.acquire() as conn:
            await conn.executemany(pg_insert, rows)

        total_migrated += len(rows)
        offset += len(rows)

        if progress_callback:
            progress_callback(table, total_migrated)

        # Check if we got less than batch size (last batch)
        if len(rows) < BATCH_SIZE:
            break

    return total_migrated


async def create_postgres_schema(pool: asyncpg.Pool):
    """Create the database schema in PostgreSQL.

    Schema matches NEMDatabase.initialize() in app/database.py
    """
    async with pool.acquire() as conn:
        # Create tables
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
            CREATE TABLE IF NOT EXISTS interconnector_data (
                id BIGSERIAL PRIMARY KEY,
                settlementdate TIMESTAMP NOT NULL,
                interconnector TEXT NOT NULL,
                meteredmwflow REAL,
                mwflow REAL,
                mwloss REAL,
                marginalvalue REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(settlementdate, interconnector)
            )
        """)

        # Create indexes
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_dispatch_settlement ON dispatch_data(settlementdate)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_dispatch_duid ON dispatch_data(duid)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_dispatch_settlement_duid ON dispatch_data(settlementdate, duid)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_price_settlement ON price_data(settlementdate)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_price_region ON price_data(region)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_price_settlement_region ON price_data(settlementdate, region)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_interconnector_settlement ON interconnector_data(settlementdate)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_interconnector_name ON interconnector_data(interconnector)")

    print("PostgreSQL schema created")


async def migrate(sqlite_path: str, postgres_url: str, verbose: bool = True):
    """Main migration function."""
    print(f"\n{'='*60}")
    print("NEM Dashboard SQLite to PostgreSQL Migration")
    print(f"{'='*60}\n")

    print(f"Source: {sqlite_path}")
    print(f"Target: {postgres_url.split('@')[1] if '@' in postgres_url else postgres_url}")
    print()

    # Connect to SQLite
    if not os.path.exists(sqlite_path):
        print(f"ERROR: SQLite database not found: {sqlite_path}")
        sys.exit(1)

    source_conn = await aiosqlite.connect(sqlite_path)

    # Connect to PostgreSQL
    try:
        target_pool = await asyncpg.create_pool(postgres_url, min_size=2, max_size=10)
    except Exception as e:
        print(f"ERROR: Could not connect to PostgreSQL: {e}")
        print("\nMake sure PostgreSQL is running:")
        print("  docker-compose up -d")
        sys.exit(1)

    try:
        # Create schema
        print("Creating PostgreSQL schema...")
        await create_postgres_schema(target_pool)

        # Define tables and their columns to migrate
        tables = {
            'generator_info': [
                'duid', 'station_name', 'region', 'fuel_source',
                'technology_type', 'capacity_mw', 'updated_at'
            ],
            'dispatch_data': [
                'settlementdate', 'duid', 'scadavalue', 'uigf',
                'totalcleared', 'ramprate', 'availability',
                'raise1sec', 'lower1sec', 'created_at'
            ],
            'price_data': [
                'settlementdate', 'region', 'price', 'totaldemand',
                'price_type', 'created_at'
            ],
            'interconnector_data': [
                'settlementdate', 'interconnector', 'meteredmwflow',
                'mwflow', 'mwloss', 'marginalvalue', 'created_at'
            ],
        }

        # Get source counts
        print("\nSource database statistics:")
        source_counts = {}
        for table in tables:
            count = await get_table_count(source_conn, table)
            source_counts[table] = count
            print(f"  {table}: {count:,} records")

        total_records = sum(source_counts.values())
        print(f"  Total: {total_records:,} records\n")

        if total_records == 0:
            print("Source database is empty. Nothing to migrate.")
            return

        # Migrate each table
        start_time = datetime.now()
        migrated_counts = {}

        def progress(table, count):
            if verbose:
                pct = (count / source_counts[table] * 100) if source_counts[table] > 0 else 100
                print(f"  {table}: {count:,} / {source_counts[table]:,} ({pct:.1f}%)", end='\r')

        for table, columns in tables.items():
            print(f"\nMigrating {table}...")
            count = await migrate_table(source_conn, target_pool, table, columns, progress)
            migrated_counts[table] = count
            print(f"  {table}: {count:,} records migrated" + " " * 20)

        # Calculate duration
        duration = (datetime.now() - start_time).total_seconds()

        # Verify migration
        print("\n" + "="*60)
        print("Migration verification:")
        print("="*60)

        all_match = True
        async with target_pool.acquire() as conn:
            for table in tables:
                pg_count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                source = source_counts[table]
                match = "OK" if pg_count == source else "MISMATCH"
                if pg_count != source:
                    all_match = False
                print(f"  {table}: SQLite={source:,}, PostgreSQL={pg_count:,} [{match}]")

        print()
        total_migrated = sum(migrated_counts.values())
        print(f"Total records migrated: {total_migrated:,}")
        print(f"Migration duration: {duration:.1f} seconds")
        print(f"Average rate: {total_migrated/duration:,.0f} records/second")
        print()

        if all_match:
            print("Migration completed successfully!")
            print("\nNext steps:")
            print("  1. Update your .env file:")
            print(f"     DATABASE_URL={postgres_url}")
            print("  2. Restart the backend:")
            print("     cd nem-dashboard-backend && python run.py")
        else:
            print("WARNING: Record counts do not match. Please investigate.")

    finally:
        await source_conn.close()
        await target_pool.close()


def main():
    parser = argparse.ArgumentParser(
        description="Migrate NEM Dashboard from SQLite to PostgreSQL"
    )
    parser.add_argument(
        '--sqlite', '-s',
        default=os.getenv('SQLITE_PATH', './data/nem_dispatch.db'),
        help='Path to SQLite database (default: ./data/nem_dispatch.db)'
    )
    parser.add_argument(
        '--postgres', '-p',
        default=os.getenv('POSTGRES_URL', 'postgresql://postgres:localdev@localhost:5432/nem_dashboard'),
        help='PostgreSQL connection URL'
    )
    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Suppress progress output'
    )

    args = parser.parse_args()

    asyncio.run(migrate(args.sqlite, args.postgres, verbose=not args.quiet))


if __name__ == '__main__':
    main()
