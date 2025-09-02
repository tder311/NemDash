from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime

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