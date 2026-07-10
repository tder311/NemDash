from pydantic import BaseModel
from typing import List, Optional, Dict, Any

class DispatchRecord(BaseModel):
    id: Optional[int] = None
    settlementdate: str
    duid: str
    scadavalue: Optional[float] = None
    uigf: Optional[float] = None
    totalcleared: Optional[float] = None
    ramprate: Optional[float] = None
    availability: Optional[float] = None
    raise1sec: Optional[float] = None
    lower1sec: Optional[float] = None
    created_at: Optional[str] = None

class DispatchDataResponse(BaseModel):
    data: List[Dict[str, Any]]
    count: int
    message: str

class GenerationByFuelRecord(BaseModel):
    settlementdate: str
    fuel_source: str
    total_generation: float
    unit_count: int

class GenerationByFuelResponse(BaseModel):
    data: List[Dict[str, Any]]
    count: int
    message: str

class DUIDListResponse(BaseModel):
    duids: List[str]
    count: int
    message: str

class FuelBreakdown(BaseModel):
    fuel_source: str
    unit_count: int

class DataSummaryResponse(BaseModel):
    total_records: int
    unique_duids: int
    earliest_date: Optional[str]
    latest_date: Optional[str]
    fuel_breakdown: List[Dict[str, Any]]

class GeneratorInfo(BaseModel):
    duid: str
    station_name: Optional[str] = None
    region: Optional[str] = None
    fuel_source: Optional[str] = None
    technology_type: Optional[str] = None
    capacity_mw: Optional[float] = None
    updated_at: Optional[str] = None

class PriceRecord(BaseModel):
    id: Optional[int] = None
    settlementdate: str
    region: str
    price: float
    totaldemand: Optional[float] = None
    price_type: str
    created_at: Optional[str] = None

class PriceDataResponse(BaseModel):
    data: List[Dict[str, Any]]
    count: int
    message: str

class InterconnectorRecord(BaseModel):
    id: Optional[int] = None
    settlementdate: str
    interconnector: str
    meteredmwflow: Optional[float] = None
    mwflow: Optional[float] = None
    mwloss: Optional[float] = None
    marginalvalue: Optional[float] = None
    created_at: Optional[str] = None

class InterconnectorDataResponse(BaseModel):
    data: List[Dict[str, Any]]
    count: int
    message: str


class FuelMixRecord(BaseModel):
    fuel_source: str
    generation_mw: float
    percentage: float
    unit_count: int


class RegionFuelMixResponse(BaseModel):
    region: str
    settlementdate: Optional[str] = None
    total_generation: float
    fuel_mix: List[FuelMixRecord]
    message: str


class RegionPriceHistoryResponse(BaseModel):
    region: str
    data: List[Dict[str, Any]]
    count: int
    hours: int
    price_type: str
    aggregation_minutes: Optional[int] = None
    message: str


class RegionGenerationHistoryResponse(BaseModel):
    region: str
    data: List[Dict[str, Any]]
    count: int
    hours: int
    aggregation_minutes: int
    message: str


class RegionSummaryResponse(BaseModel):
    region: str
    latest_price: Optional[float] = None
    total_demand: Optional[float] = None
    price_timestamp: Optional[str] = None
    total_generation: Optional[float] = None
    generator_count: int
    message: str


class DataCoverageResponse(BaseModel):
    table: str
    earliest_date: Optional[str] = None
    latest_date: Optional[str] = None
    total_records: int
    days_with_data: int
    message: str


class GapInfo(BaseModel):
    gap_start: str
    gap_end: str
    missing_intervals: int
    duration_minutes: int


class TableGaps(BaseModel):
    table: str
    gaps: List[GapInfo]
    total_gaps: int


class TableStats(BaseModel):
    table: str
    total_records: int
    earliest_date: Optional[str] = None
    latest_date: Optional[str] = None
    days_with_data: Optional[int] = None
    expected_interval: Optional[int] = None


class DatabaseHealthResponse(BaseModel):
    tables: List[TableStats]
    gaps: List[TableGaps]
    checked_hours: int
    checked_at: str


class PASARegionRecord(BaseModel):
    id: Optional[int] = None
    run_datetime: str
    interval_datetime: str
    regionid: str
    demand10: Optional[float] = None
    demand50: Optional[float] = None
    demand90: Optional[float] = None
    reservereq: Optional[float] = None
    capacityreq: Optional[float] = None
    aggregatecapacityavailable: Optional[float] = None
    aggregatepasaavailability: Optional[float] = None
    surplusreserve: Optional[float] = None
    lorcondition: Optional[int] = None
    calculatedlor1level: Optional[float] = None
    calculatedlor2level: Optional[float] = None
    created_at: Optional[str] = None


class PASADataResponse(BaseModel):
    data: List[Dict[str, Any]]
    run_datetime: Optional[str] = None
    region: str
    count: int
    message: str


class RegionDataRangeResponse(BaseModel):
    region: str
    earliest_date: Optional[str] = None
    latest_date: Optional[str] = None
    message: str


class DailyMetricsResponse(BaseModel):
    region: str
    data: List[Dict[str, Any]]
    count: int
    start_date: str
    end_date: str
    message: str


class MetricsSummaryResponse(BaseModel):
    region: str
    periods: Dict[str, Any]
    message: str


class BidBandResponse(BaseModel):
    duid: str
    date: str
    data: List[Dict[str, Any]]
    count: int
    price_bands: List[Optional[float]]
    message: str


class DUIDSearchResponse(BaseModel):
    results: List[Dict[str, Any]]
    count: int
    message: str


class PriceForecastResponse(BaseModel):
    region: str
    data: List[Dict[str, Any]]  # [{interval_datetime, predicted_price, p10, p90}, ...]
    count: int
    horizon_intervals: int
    model_trained_at: Optional[str] = None
    message: str


class ForecastAccuracyStats(BaseModel):
    n: int
    mae: Optional[float] = None
    coverage_n: int
    p10_p90_coverage: Optional[float] = None
    spike_n: int
    spike_recall: Optional[float] = None


class ForecastAccuracyBucket(ForecastAccuracyStats):
    lead_bucket_hours: float


class ForecastAccuracyResponse(BaseModel):
    region: str
    days: int
    buckets: List[ForecastAccuracyBucket]
    overall: ForecastAccuracyStats
    message: str


class InterconnectorInterval(BaseModel):
    interval_datetime: str
    mwflow: Optional[float] = None
    exportlimit: Optional[float] = None
    importlimit: Optional[float] = None
    marginalvalue: Optional[float] = None


class NetworkInterconnectorsResponse(BaseModel):
    run_datetime: Optional[str] = None
    data: Dict[str, List[InterconnectorInterval]]


class ConstraintInterval(BaseModel):
    interval_datetime: str
    marginalvalue: Optional[float] = None
    rhs: Optional[float] = None
    violationdegree: Optional[float] = None


class ConstraintSummary(BaseModel):
    constraintid: str
    category: str
    regions: List[str]
    kind: Optional[str] = None
    intervals: List[ConstraintInterval]


class NetworkConstraintsResponse(BaseModel):
    run_datetime: Optional[str] = None
    constraints: List[ConstraintSummary]