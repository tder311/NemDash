# NEM Dashboard API Reference

Complete API documentation for the NEM Dashboard backend.

**Base URL**: `http://localhost:8000`

## Table of Contents

- [Health & Status](#health--status)
- [Dispatch Data](#dispatch-data)
- [Price Data](#price-data)
- [Interconnector Data](#interconnector-data)
- [Generator Data](#generator-data)
- [Analysis Endpoints](#analysis-endpoints)
- [Data Ingestion](#data-ingestion)
- [Response Schemas](#response-schemas)
- [Error Handling](#error-handling)

---

## Health & Status

### GET /
Root endpoint - health check.

**Response**
```json
{
  "message": "NEM Dispatch Dashboard API",
  "version": "1.0.0"
}
```

### GET /health
Detailed health check.

**Response**
```json
{
  "status": "healthy",
  "database": "connected"
}
```

---

## Dispatch Data

### GET /api/dispatch/latest
Get the latest dispatch SCADA data.

**Query Parameters**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `limit` | integer | No | 1000 | Number of records (1-5000) |

**Response**
```json
{
  "data": [
    {
      "id": 1,
      "settlementdate": "2025-01-15T10:30:00",
      "duid": "BASTYAN",
      "scadavalue": 82.5,
      "uigf": 82.0,
      "totalcleared": 80.0,
      "ramprate": 5.0,
      "availability": 82.0,
      "raise1sec": 0.0,
      "lower1sec": 0.0,
      "created_at": "2025-01-15T10:30:05"
    }
  ],
  "count": 1,
  "message": "Latest dispatch data retrieved successfully"
}
```

**Example**
```bash
curl "http://localhost:8000/api/dispatch/latest?limit=100"
```

---

### GET /api/dispatch/range
Get dispatch data for a date range.

**Query Parameters**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `start_date` | string (ISO 8601) | Yes | - | Start date |
| `end_date` | string (ISO 8601) | Yes | - | End date |
| `duid` | string | No | - | Filter by generator DUID |

**Response**
```json
{
  "data": [...],
  "count": 288,
  "message": "Dispatch data for date range retrieved successfully"
}
```

**Example**
```bash
curl "http://localhost:8000/api/dispatch/range?start_date=2025-01-15T00:00:00&end_date=2025-01-15T23:59:59&duid=BASTYAN"
```

---

## Price Data

### GET /api/prices/latest
Get the latest price data by type.

**Query Parameters**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `price_type` | string | No | DISPATCH | Price type: `DISPATCH`, `TRADING`, or `PUBLIC` |

**Price Types**
| Type | Interval | Description |
|------|----------|-------------|
| `DISPATCH` | 5 minutes | Real-time dispatch prices |
| `TRADING` | 30 minutes | Trading interval prices |
| `PUBLIC` | Daily | Historical public prices |

**Response**
```json
{
  "data": [
    {
      "id": 1,
      "settlementdate": "2025-01-15T10:30:00",
      "region": "NSW",
      "price": 85.50,
      "totaldemand": 8500.0,
      "price_type": "DISPATCH",
      "created_at": "2025-01-15T10:30:05"
    },
    {
      "id": 2,
      "settlementdate": "2025-01-15T10:30:00",
      "region": "VIC",
      "price": 78.25,
      "totaldemand": 5200.0,
      "price_type": "DISPATCH",
      "created_at": "2025-01-15T10:30:05"
    }
  ],
  "count": 5,
  "message": "Latest DISPATCH prices retrieved successfully"
}
```

**Example**
```bash
curl "http://localhost:8000/api/prices/latest?price_type=TRADING"
```

---

### GET /api/prices/history
Get historical price data.

**Query Parameters**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `start_date` | string (ISO 8601) | Yes | - | Start date |
| `end_date` | string (ISO 8601) | Yes | - | End date |
| `region` | string | No | - | Filter by region code |
| `price_type` | string | No | - | Filter by price type |

**Response**
```json
{
  "data": [...],
  "count": 1440,
  "message": "Price history retrieved successfully"
}
```

**Example**
```bash
curl "http://localhost:8000/api/prices/history?start_date=2025-01-14T10:00:00&end_date=2025-01-15T10:00:00&region=NSW&price_type=PUBLIC"
```

---

## Interconnector Data

### GET /api/interconnectors/latest
Get the latest interconnector flow data.

**Response**
```json
{
  "data": [
    {
      "id": 1,
      "settlementdate": "2025-01-15T10:30:00",
      "interconnector": "NSW1-QLD1",
      "meteredmwflow": 450.5,
      "mwflow": 448.0,
      "mwloss": 2.5,
      "marginalvalue": 5.25,
      "created_at": "2025-01-15T10:30:05"
    }
  ],
  "count": 6,
  "message": "Latest interconnector data retrieved successfully"
}
```

**NEM Interconnectors**
| Interconnector | From | To | Description |
|----------------|------|-----|-------------|
| `NSW1-QLD1` | NSW | QLD | Queensland-NSW Interconnector |
| `VIC1-NSW1` | VIC | NSW | Victoria-NSW Interconnector |
| `VIC1-SA1` | VIC | SA | Heywood Interconnector |
| `T-V-MNSP1` | TAS | VIC | Basslink (Tasmania-Victoria) |
| `N-Q-MNSP1` | NSW | QLD | Terranora Interconnector |
| `V-SA` | VIC | SA | Murraylink |

**Example**
```bash
curl "http://localhost:8000/api/interconnectors/latest"
```

---

### GET /api/interconnectors/history
Get historical interconnector data.

**Query Parameters**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `start_date` | string (ISO 8601) | Yes | - | Start date |
| `end_date` | string (ISO 8601) | Yes | - | End date |
| `interconnector` | string | No | - | Filter by interconnector ID |

**Response**
```json
{
  "data": [...],
  "count": 288,
  "message": "Interconnector history retrieved successfully"
}
```

**Example**
```bash
curl "http://localhost:8000/api/interconnectors/history?start_date=2025-01-15T00:00:00&end_date=2025-01-15T23:59:59&interconnector=NSW1-QLD1"
```

---

## Generator Data

### GET /api/generators/filter
Get generators with latest SCADA data, filtered by region and/or fuel source.

**Query Parameters**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `region` | string | No | - | Filter by region: `NSW`, `VIC`, `QLD`, `SA`, `TAS` |
| `fuel_source` | string | No | - | Filter by fuel source |

**Fuel Sources**
- `Solar`
- `Wind`
- `Hydro`
- `Gas`
- `Coal`
- `Battery`
- `Diesel`
- `Other`

**Response**
```json
{
  "data": [
    {
      "duid": "BASTYAN",
      "station_name": "Bastyan",
      "region": "TAS",
      "fuel_source": "Hydro",
      "technology_type": "Hydro - Gravity",
      "capacity_mw": 82.0,
      "scadavalue": 78.5,
      "settlementdate": "2025-01-15T10:30:00"
    }
  ],
  "count": 1,
  "message": "Filtered generators retrieved successfully"
}
```

**Example**
```bash
curl "http://localhost:8000/api/generators/filter?region=TAS&fuel_source=Hydro"
```

---

### GET /api/duids
Get list of all unique generator DUIDs in the database.

**Response**
```json
{
  "duids": ["ADPCC1", "AGLHAL", "AGLSOM", "..."],
  "count": 450,
  "message": "DUID list retrieved successfully"
}
```

**Example**
```bash
curl "http://localhost:8000/api/duids"
```

---

## Analysis Endpoints

### GET /api/generation/by-fuel
Get aggregated generation by fuel type for a date range.

**Query Parameters**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `start_date` | string (ISO 8601) | Yes | - | Start date |
| `end_date` | string (ISO 8601) | Yes | - | End date |

**Response**
```json
{
  "data": [
    {
      "fuel_source": "Coal",
      "total_generation": 125000.5
    },
    {
      "fuel_source": "Wind",
      "total_generation": 45000.2
    },
    {
      "fuel_source": "Solar",
      "total_generation": 38000.8
    }
  ],
  "count": 8,
  "message": "Generation by fuel type retrieved successfully"
}
```

**Example**
```bash
curl "http://localhost:8000/api/generation/by-fuel?start_date=2025-01-15T00:00:00&end_date=2025-01-15T23:59:59"
```

---

### GET /api/summary
Get database summary statistics.

**Response**
```json
{
  "total_records": 150000,
  "unique_duids": 450,
  "earliest_date": "2025-01-01T00:00:00",
  "latest_date": "2025-01-15T10:30:00",
  "fuel_breakdown": [
    {
      "fuel_source": "Coal",
      "generator_count": 25,
      "total_capacity_mw": 15000.0
    },
    {
      "fuel_source": "Wind",
      "generator_count": 85,
      "total_capacity_mw": 8500.0
    }
  ]
}
```

**Example**
```bash
curl "http://localhost:8000/api/summary"
```

---

## Data Ingestion

These endpoints trigger manual data ingestion. The system also runs automatic ingestion every 5 minutes.

### POST /api/ingest/current
Manually trigger ingestion of current data from NEMWEB.

**Response**
```json
{
  "message": "Current data ingestion completed successfully",
  "dispatch_records": 450,
  "price_records": 10,
  "interconnector_records": 6
}
```

**Example**
```bash
curl -X POST "http://localhost:8000/api/ingest/current"
```

---

### POST /api/ingest/historical
Ingest historical dispatch data for a date range.

**Query Parameters**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `start_date` | string (YYYY-MM-DD) | Yes | - | Start date |
| `end_date` | string (YYYY-MM-DD) | Yes | - | End date |

**Response**
```json
{
  "message": "Historical data ingestion completed",
  "days_processed": 7,
  "total_records": 50400
}
```

**Example**
```bash
curl -X POST "http://localhost:8000/api/ingest/historical?start_date=2025-01-01&end_date=2025-01-07"
```

---

### POST /api/ingest/historical-prices
Ingest historical public prices for a date range.

**Query Parameters**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `start_date` | string (YYYY-MM-DD) | Yes | - | Start date |
| `end_date` | string (YYYY-MM-DD) | Yes | - | End date |

**Response**
```json
{
  "message": "Historical price ingestion completed",
  "days_processed": 7,
  "total_records": 35
}
```

**Example**
```bash
curl -X POST "http://localhost:8000/api/ingest/historical-prices?start_date=2025-01-01&end_date=2025-01-07"
```

---

## Response Schemas

### Standard Response Wrapper

All list endpoints return data in a consistent wrapper:

```json
{
  "data": [...],
  "count": 100,
  "message": "Success message"
}
```

### DispatchRecord

```json
{
  "id": 1,
  "settlementdate": "2025-01-15T10:30:00",
  "duid": "string",
  "scadavalue": 0.0,
  "uigf": 0.0,
  "totalcleared": 0.0,
  "ramprate": 0.0,
  "availability": 0.0,
  "raise1sec": 0.0,
  "lower1sec": 0.0,
  "created_at": "2025-01-15T10:30:05"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `id` | integer | Database record ID |
| `settlementdate` | datetime | 5-minute dispatch interval |
| `duid` | string | Dispatchable Unit Identifier |
| `scadavalue` | float | Actual MW output (SCADA reading) |
| `uigf` | float | Unconstrained Intermittent Generation Forecast |
| `totalcleared` | float | Dispatch target MW |
| `ramprate` | float | Ramp rate MW/min |
| `availability` | float | Available capacity MW |
| `raise1sec` | float | 1-second raise FCAS MW |
| `lower1sec` | float | 1-second lower FCAS MW |
| `created_at` | datetime | Record creation timestamp |

### PriceRecord

```json
{
  "id": 1,
  "settlementdate": "2025-01-15T10:30:00",
  "region": "NSW",
  "price": 85.50,
  "totaldemand": 8500.0,
  "price_type": "DISPATCH",
  "created_at": "2025-01-15T10:30:05"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `id` | integer | Database record ID |
| `settlementdate` | datetime | Price interval timestamp |
| `region` | string | NEM region code |
| `price` | float | Regional Reference Price ($/MWh) |
| `totaldemand` | float | Total regional demand (MW) |
| `price_type` | string | `DISPATCH`, `TRADING`, or `PUBLIC` |
| `created_at` | datetime | Record creation timestamp |

### InterconnectorRecord

```json
{
  "id": 1,
  "settlementdate": "2025-01-15T10:30:00",
  "interconnector": "NSW1-QLD1",
  "meteredmwflow": 450.5,
  "mwflow": 448.0,
  "mwloss": 2.5,
  "marginalvalue": 5.25,
  "created_at": "2025-01-15T10:30:05"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `id` | integer | Database record ID |
| `settlementdate` | datetime | Flow interval timestamp |
| `interconnector` | string | Interconnector ID |
| `meteredmwflow` | float | Actual metered flow (MW) |
| `mwflow` | float | Dispatched flow target (MW) |
| `mwloss` | float | Transmission losses (MW) |
| `marginalvalue` | float | Marginal value ($/MWh) |
| `created_at` | datetime | Record creation timestamp |

### GeneratorInfo

```json
{
  "duid": "BASTYAN",
  "station_name": "Bastyan",
  "region": "TAS",
  "fuel_source": "Hydro",
  "technology_type": "Hydro - Gravity",
  "capacity_mw": 82.0
}
```

| Field | Type | Description |
|-------|------|-------------|
| `duid` | string | Dispatchable Unit Identifier |
| `station_name` | string | Generator station name |
| `region` | string | NEM region code |
| `fuel_source` | string | Primary fuel source |
| `technology_type` | string | Generation technology |
| `capacity_mw` | float | Registered capacity (MW) |

---

## Error Handling

### Error Response Format

```json
{
  "detail": "Error message describing the issue"
}
```

### HTTP Status Codes

| Code | Description |
|------|-------------|
| `200` | Success |
| `400` | Bad Request - Invalid parameters |
| `404` | Not Found - Resource doesn't exist |
| `422` | Validation Error - Invalid input format |
| `500` | Internal Server Error |

### Common Errors

**Invalid date format**
```json
{
  "detail": "Invalid date format. Use ISO 8601 format (YYYY-MM-DDTHH:MM:SS)"
}
```

**Invalid limit parameter**
```json
{
  "detail": "Limit must be between 1 and 5000"
}
```

**No data found**
```json
{
  "detail": "No data found for the specified criteria"
}
```

---

## Rate Limiting

The API does not implement rate limiting, but NEMWEB (the upstream data source) may throttle requests. The automatic data ingestion respects a 5-minute interval to avoid overloading NEMWEB.

For historical data ingestion, a 1-second delay is added between daily requests.

---

## CORS Configuration

The API allows requests from:
- `http://localhost:3000` (React development server)
- `http://localhost:8050` (Alternative development port)

To add additional origins, modify the `allow_origins` list in `app/main.py`.
