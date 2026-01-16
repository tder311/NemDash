# NEM Dashboard Database Schema

Complete database schema documentation for the NEM Dashboard SQLite database.

## Overview

The NEM Dashboard uses SQLite with async access via `aiosqlite`. The database stores:
- Generator dispatch (SCADA) data
- Regional electricity prices
- Interconnector power flows
- Generator metadata

**Default Location**: `nem-dashboard-backend/data/nem_dispatch.db`

## Tables

### dispatch_data

Stores generator SCADA (Supervisory Control and Data Acquisition) values at 5-minute intervals.

```sql
CREATE TABLE dispatch_data (
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

-- Indexes for query performance
CREATE INDEX idx_dispatch_settlementdate ON dispatch_data(settlementdate);
CREATE INDEX idx_dispatch_duid ON dispatch_data(duid);
CREATE INDEX idx_dispatch_date_duid ON dispatch_data(settlementdate, duid);
```

#### Columns

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INTEGER | No | Auto-increment primary key |
| `settlementdate` | DATETIME | No | 5-minute dispatch interval timestamp |
| `duid` | TEXT | No | Dispatchable Unit Identifier (generator ID) |
| `scadavalue` | REAL | Yes | Actual MW output from SCADA reading |
| `uigf` | REAL | Yes | Unconstrained Intermittent Generation Forecast (MW) |
| `totalcleared` | REAL | Yes | Dispatch target MW |
| `ramprate` | REAL | Yes | Ramp rate in MW/min |
| `availability` | REAL | Yes | Available capacity in MW |
| `raise1sec` | REAL | Yes | 1-second raise FCAS contribution (MW) |
| `lower1sec` | REAL | Yes | 1-second lower FCAS contribution (MW) |
| `created_at` | DATETIME | No | Record insertion timestamp |

#### Unique Constraint

- **Key**: `(settlementdate, duid)`
- **Behavior**: `ON CONFLICT REPLACE` - Updates existing record if duplicate

#### Sample Data

```
settlementdate       | duid    | scadavalue | totalcleared | availability
---------------------|---------|------------|--------------|-------------
2025-01-15 10:30:00 | BASTYAN | 82.5       | 80.0         | 82.0
2025-01-15 10:30:00 | AGLSOM  | 125.3      | 125.0        | 160.0
2025-01-15 10:30:00 | BARRON1 | 45.2       | 45.0         | 66.0
```

---

### price_data

Stores regional electricity prices at different intervals (5-min dispatch, 30-min trading, daily public).

```sql
CREATE TABLE price_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    settlementdate DATETIME NOT NULL,
    region TEXT NOT NULL,
    price REAL,
    totaldemand REAL,
    price_type TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(settlementdate, region, price_type) ON CONFLICT REPLACE
);

-- Indexes for query performance
CREATE INDEX idx_price_settlementdate ON price_data(settlementdate);
CREATE INDEX idx_price_region ON price_data(region);
CREATE INDEX idx_price_date_region ON price_data(settlementdate, region);
```

#### Columns

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INTEGER | No | Auto-increment primary key |
| `settlementdate` | DATETIME | No | Price interval timestamp |
| `region` | TEXT | No | NEM region code (NSW, VIC, QLD, SA, TAS) |
| `price` | REAL | Yes | Regional Reference Price ($/MWh) |
| `totaldemand` | REAL | Yes | Total regional demand (MW) |
| `price_type` | TEXT | No | Price interval type |
| `created_at` | DATETIME | No | Record insertion timestamp |

#### Price Types

| Type | Interval | Description |
|------|----------|-------------|
| `DISPATCH` | 5 minutes | Real-time dispatch prices |
| `TRADING` | 30 minutes | Trading interval prices (standard market interval) |
| `PUBLIC` | Daily | Historical public price archives |

#### Unique Constraint

- **Key**: `(settlementdate, region, price_type)`
- **Behavior**: `ON CONFLICT REPLACE` - Updates existing record if duplicate

#### Sample Data

```
settlementdate       | region | price  | totaldemand | price_type
---------------------|--------|--------|-------------|------------
2025-01-15 10:30:00 | NSW    | 85.50  | 8500.0      | DISPATCH
2025-01-15 10:30:00 | VIC    | 78.25  | 5200.0      | DISPATCH
2025-01-15 10:30:00 | QLD    | 92.10  | 6800.0      | TRADING
2025-01-15 00:00:00 | SA     | 65.00  | 1500.0      | PUBLIC
```

---

### interconnector_data

Stores power flows between NEM regions via interconnectors.

```sql
CREATE TABLE interconnector_data (
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

-- Indexes for query performance
CREATE INDEX idx_interconnector_settlementdate ON interconnector_data(settlementdate);
CREATE INDEX idx_interconnector_id ON interconnector_data(interconnector);
```

#### Columns

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INTEGER | No | Auto-increment primary key |
| `settlementdate` | DATETIME | No | Flow interval timestamp |
| `interconnector` | TEXT | No | Interconnector identifier |
| `meteredmwflow` | REAL | Yes | Actual metered power flow (MW) |
| `mwflow` | REAL | Yes | Dispatched flow target (MW) |
| `mwloss` | REAL | Yes | Transmission losses (MW) |
| `marginalvalue` | REAL | Yes | Marginal value of flow ($/MWh) |
| `created_at` | DATETIME | No | Record insertion timestamp |

#### NEM Interconnectors

| ID | From | To | Description |
|----|------|-----|-------------|
| `NSW1-QLD1` | NSW | QLD | Queensland-NSW Interconnector (QNI) |
| `VIC1-NSW1` | VIC | NSW | Victoria-NSW Interconnector |
| `VIC1-SA1` | VIC | SA | Heywood Interconnector |
| `T-V-MNSP1` | TAS | VIC | Basslink (Market Network Service Provider) |
| `N-Q-MNSP1` | NSW | QLD | Terranora Interconnector |
| `V-SA` | VIC | SA | Murraylink |

#### Unique Constraint

- **Key**: `(settlementdate, interconnector)`
- **Behavior**: `ON CONFLICT REPLACE` - Updates existing record if duplicate

#### Sample Data

```
settlementdate       | interconnector | meteredmwflow | mwflow  | mwloss
---------------------|----------------|---------------|---------|-------
2025-01-15 10:30:00 | NSW1-QLD1      | 450.5         | 448.0   | 2.5
2025-01-15 10:30:00 | VIC1-SA1       | -125.3        | -125.0  | 0.3
2025-01-15 10:30:00 | T-V-MNSP1      | 200.0         | 198.5   | 1.5
```

**Note**: Positive flow indicates power moving in the direction From → To. Negative flow indicates reverse direction.

---

### generator_info

Stores static generator metadata (fuel source, region, capacity).

```sql
CREATE TABLE generator_info (
    duid TEXT PRIMARY KEY,
    station_name TEXT,
    region TEXT,
    fuel_source TEXT,
    technology_type TEXT,
    capacity_mw REAL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

#### Columns

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `duid` | TEXT | No | Dispatchable Unit Identifier (primary key) |
| `station_name` | TEXT | Yes | Generator station name |
| `region` | TEXT | Yes | NEM region code |
| `fuel_source` | TEXT | Yes | Primary fuel source |
| `technology_type` | TEXT | Yes | Generation technology type |
| `capacity_mw` | REAL | Yes | Registered capacity in MW |
| `updated_at` | DATETIME | No | Last update timestamp |

#### Fuel Sources

| Fuel Source | Examples |
|-------------|----------|
| `Coal` | Black coal, brown coal thermal plants |
| `Gas` | CCGT, OCGT, gas turbines |
| `Wind` | Wind farms |
| `Solar` | Solar PV, solar thermal |
| `Hydro` | Hydro gravity, pumped storage |
| `Battery` | Grid-scale batteries |
| `Diesel` | Diesel generators |
| `Other` | Other/unknown sources |

#### Sample Data

```
duid     | station_name       | region | fuel_source | technology_type | capacity_mw
---------|-------------------|--------|-------------|-----------------|------------
BASTYAN  | Bastyan           | TAS    | Hydro       | Hydro - Gravity | 82.0
AGLSOM   | AGL Somerton      | VIC    | Gas         | OCGT            | 160.0
BARRON1  | Barron Gorge      | QLD    | Hydro       | Hydro - Gravity | 66.0
ARWF1    | Ararat Wind Farm  | VIC    | Wind        | Wind - Onshore  | 240.0
```

---

## Entity Relationships

```
                          ┌─────────────────┐
                          │  generator_info │
                          │                 │
                          │  duid (PK)      │
                          │  station_name   │
                          │  region         │
                          │  fuel_source    │
                          │  capacity_mw    │
                          └────────┬────────┘
                                   │
                                   │ duid (logical FK)
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────┐
│                       dispatch_data                          │
│                                                             │
│  id (PK)                                                    │
│  settlementdate + duid (UNIQUE)                             │
│  scadavalue, uigf, totalcleared, ramprate, availability     │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                         price_data                           │
│                                                             │
│  id (PK)                                                    │
│  settlementdate + region + price_type (UNIQUE)              │
│  price, totaldemand                                         │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                    interconnector_data                       │
│                                                             │
│  id (PK)                                                    │
│  settlementdate + interconnector (UNIQUE)                   │
│  meteredmwflow, mwflow, mwloss, marginalvalue              │
└─────────────────────────────────────────────────────────────┘
```

**Note**: The relationship between `dispatch_data` and `generator_info` is logical (not enforced with foreign key constraint). This allows dispatch data to be inserted even if generator metadata hasn't been loaded yet.

---

## Query Patterns

### Latest Dispatch Data

```sql
SELECT d.*, g.station_name, g.region, g.fuel_source, g.capacity_mw
FROM dispatch_data d
LEFT JOIN generator_info g ON d.duid = g.duid
WHERE d.settlementdate = (SELECT MAX(settlementdate) FROM dispatch_data)
ORDER BY d.scadavalue DESC
LIMIT 100;
```

### Generation by Fuel Type

```sql
SELECT g.fuel_source, SUM(d.scadavalue) as total_generation
FROM dispatch_data d
JOIN generator_info g ON d.duid = g.duid
WHERE d.settlementdate BETWEEN ? AND ?
GROUP BY g.fuel_source
ORDER BY total_generation DESC;
```

### Price History for Region

```sql
SELECT settlementdate, price, totaldemand
FROM price_data
WHERE region = ?
  AND price_type = ?
  AND settlementdate BETWEEN ? AND ?
ORDER BY settlementdate;
```

### Latest Interconnector Flows

```sql
SELECT *
FROM interconnector_data
WHERE settlementdate = (SELECT MAX(settlementdate) FROM interconnector_data)
ORDER BY ABS(meteredmwflow) DESC;
```

### Database Summary

```sql
SELECT
    (SELECT COUNT(*) FROM dispatch_data) as total_dispatch,
    (SELECT COUNT(DISTINCT duid) FROM dispatch_data) as unique_duids,
    (SELECT MIN(settlementdate) FROM dispatch_data) as earliest_date,
    (SELECT MAX(settlementdate) FROM dispatch_data) as latest_date,
    (SELECT COUNT(*) FROM price_data) as total_prices,
    (SELECT COUNT(*) FROM interconnector_data) as total_interconnector;
```

---

## Data Growth

### Estimated Record Counts

| Table | Records/Day | Records/Month | Records/Year |
|-------|-------------|---------------|--------------|
| dispatch_data | ~130,000 | ~4M | ~48M |
| price_data | ~4,320 | ~130K | ~1.6M |
| interconnector_data | ~1,728 | ~52K | ~630K |

**Calculations:**
- Dispatch: ~450 generators × 288 intervals/day = 129,600 records/day
- Prices: 5 regions × 3 types × 288 intervals = 4,320 records/day
- Interconnectors: 6 interconnectors × 288 intervals = 1,728 records/day

### Storage Estimates

- SQLite overhead: ~100 bytes per record average
- Daily growth: ~15 MB
- Monthly growth: ~450 MB
- Yearly growth: ~5.5 GB

**Recommendation**: Implement data retention policy for production deployments.

---

## Maintenance

### Vacuum Database

Reclaim space after deletions:

```sql
VACUUM;
```

### Analyze for Query Optimization

Update statistics for query planner:

```sql
ANALYZE;
```

### Check Integrity

Verify database integrity:

```sql
PRAGMA integrity_check;
```

### Backup

```bash
# Simple file copy (when database not in use)
cp nem_dispatch.db nem_dispatch_backup.db

# Or use SQLite backup API
sqlite3 nem_dispatch.db ".backup 'nem_dispatch_backup.db'"
```

---

## Migration to PostgreSQL

For production with concurrent access, migrate to PostgreSQL:

### Schema Changes

1. Replace `INTEGER PRIMARY KEY AUTOINCREMENT` with `SERIAL`
2. Replace `DATETIME` with `TIMESTAMP`
3. Replace `ON CONFLICT REPLACE` with `ON CONFLICT ... DO UPDATE`
4. Add proper foreign key constraints

### Example PostgreSQL Schema

```sql
CREATE TABLE dispatch_data (
    id SERIAL PRIMARY KEY,
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
);

-- Use INSERT ... ON CONFLICT DO UPDATE for upserts
INSERT INTO dispatch_data (settlementdate, duid, scadavalue, ...)
VALUES (?, ?, ?, ...)
ON CONFLICT (settlementdate, duid)
DO UPDATE SET scadavalue = EXCLUDED.scadavalue, ...;
```

### Code Changes

- Replace `aiosqlite` with `asyncpg`
- Update connection string handling
- Modify parameterized query syntax (`?` → `$1, $2, ...`)
