"""
Unit tests for Pydantic models
"""
import pytest
from pydantic import ValidationError

from app.models import (
    DispatchRecord,
    DispatchDataResponse,
    GenerationByFuelRecord,
    GenerationByFuelResponse,
    DUIDListResponse,
    FuelBreakdown,
    DataSummaryResponse,
    GeneratorInfo,
    PriceRecord,
    PriceDataResponse,
    InterconnectorRecord,
    InterconnectorDataResponse,
    FuelMixRecord,
    RegionFuelMixResponse,
    RegionPriceHistoryResponse,
    RegionSummaryResponse,
    DataCoverageResponse,
    GapInfo,
    TableGaps,
    TableStats,
    DatabaseHealthResponse,
)


class TestDispatchRecord:
    """Tests for DispatchRecord model"""

    def test_dispatch_record_with_all_fields(self):
        """Test DispatchRecord with all optional fields populated"""
        record = DispatchRecord(
            settlementdate="2025-01-15T10:30:00",
            duid="BAYSW1",
            scadavalue=350.5,
            uigf=400.0,
            totalcleared=350.0,
            ramprate=5.0,
            availability=400.0,
            raise1sec=10.0,
            lower1sec=10.0,
        )
        assert record.duid == "BAYSW1"
        assert record.scadavalue == 350.5
        assert record.settlementdate == "2025-01-15T10:30:00"

    def test_dispatch_record_minimal(self):
        """Test DispatchRecord with only required fields"""
        record = DispatchRecord(settlementdate="2025-01-15T10:30:00", duid="TEST")
        assert record.duid == "TEST"
        assert record.scadavalue is None
        assert record.uigf is None

    def test_dispatch_record_optional_id(self):
        """Test that id field is optional"""
        record = DispatchRecord(settlementdate="2025-01-15T10:30:00", duid="TEST")
        assert record.id is None

        record_with_id = DispatchRecord(
            id=123,
            settlementdate="2025-01-15T10:30:00",
            duid="TEST"
        )
        assert record_with_id.id == 123


class TestDispatchDataResponse:
    """Tests for DispatchDataResponse model"""

    def test_dispatch_data_response_valid(self):
        """Test valid DispatchDataResponse"""
        response = DispatchDataResponse(
            data=[{"duid": "TEST", "scadavalue": 100.0}],
            count=1,
            message="Success"
        )
        assert response.count == 1
        assert len(response.data) == 1

    def test_dispatch_data_response_empty(self):
        """Test empty data response"""
        response = DispatchDataResponse(
            data=[],
            count=0,
            message="No data found"
        )
        assert response.count == 0
        assert response.data == []


class TestPriceRecord:
    """Tests for PriceRecord model"""

    def test_price_record_validation(self):
        """Test PriceRecord with valid data"""
        record = PriceRecord(
            settlementdate="2025-01-15T10:30:00",
            region="NSW",
            price=85.50,
            price_type="DISPATCH"
        )
        assert record.region == "NSW"
        assert record.price == 85.50
        assert record.price_type == "DISPATCH"

    def test_price_record_negative_price(self):
        """Test that negative prices are allowed (valid in NEM during oversupply)"""
        record = PriceRecord(
            settlementdate="2025-01-15T10:30:00",
            region="SA",
            price=-50.00,
            price_type="DISPATCH"
        )
        assert record.price == -50.00

    def test_price_record_with_demand(self):
        """Test PriceRecord with totaldemand"""
        record = PriceRecord(
            settlementdate="2025-01-15T10:30:00",
            region="NSW",
            price=85.50,
            totaldemand=7500.0,
            price_type="TRADING"
        )
        assert record.totaldemand == 7500.0

    def test_price_record_demand_optional(self):
        """Test that totaldemand is optional"""
        record = PriceRecord(
            settlementdate="2025-01-15T10:30:00",
            region="NSW",
            price=85.50,
            price_type="DISPATCH"
        )
        assert record.totaldemand is None


class TestInterconnectorRecord:
    """Tests for InterconnectorRecord model"""

    def test_interconnector_record_valid(self):
        """Test valid InterconnectorRecord"""
        record = InterconnectorRecord(
            settlementdate="2025-01-15T10:30:00",
            interconnector="NSW1-QLD1",
            meteredmwflow=350.5,
            mwflow=355.0,
            mwloss=4.5,
            marginalvalue=12.30
        )
        assert record.interconnector == "NSW1-QLD1"
        assert record.meteredmwflow == 350.5

    def test_interconnector_negative_flow(self):
        """Test interconnector with negative flow (reverse direction)"""
        record = InterconnectorRecord(
            settlementdate="2025-01-15T10:30:00",
            interconnector="VIC1-SA1",
            meteredmwflow=-150.0,
            mwflow=-148.0,
            mwloss=2.0,
            marginalvalue=8.50
        )
        assert record.meteredmwflow < 0


class TestGeneratorInfo:
    """Tests for GeneratorInfo model"""

    def test_generator_info_valid(self):
        """Test valid GeneratorInfo"""
        gen = GeneratorInfo(
            duid="BAYSW1",
            station_name="Bayswater",
            region="NSW",
            fuel_source="Coal",
            technology_type="Steam",
            capacity_mw=660.0
        )
        assert gen.duid == "BAYSW1"
        assert gen.fuel_source == "Coal"

    def test_generator_info_minimal(self):
        """Test GeneratorInfo with only required field"""
        gen = GeneratorInfo(duid="TEST")
        assert gen.duid == "TEST"
        assert gen.station_name is None
        assert gen.region is None


class TestFuelMixRecord:
    """Tests for FuelMixRecord model"""

    def test_fuel_mix_record_valid(self):
        """Test valid FuelMixRecord"""
        record = FuelMixRecord(
            fuel_source="Coal",
            generation_mw=4500.0,
            percentage=45.0,
            unit_count=12
        )
        assert record.fuel_source == "Coal"
        assert record.percentage == 45.0

    def test_fuel_mix_zero_values(self):
        """Test FuelMixRecord with zero values"""
        record = FuelMixRecord(
            fuel_source="Solar",
            generation_mw=0.0,
            percentage=0.0,
            unit_count=0
        )
        assert record.generation_mw == 0.0


class TestRegionFuelMixResponse:
    """Tests for RegionFuelMixResponse model"""

    def test_region_fuel_mix_response_valid(self):
        """Test valid RegionFuelMixResponse"""
        response = RegionFuelMixResponse(
            region="NSW",
            total_generation=10000.0,
            fuel_mix=[
                FuelMixRecord(
                    fuel_source="Coal",
                    generation_mw=6000.0,
                    percentage=60.0,
                    unit_count=10
                ),
                FuelMixRecord(
                    fuel_source="Solar",
                    generation_mw=4000.0,
                    percentage=40.0,
                    unit_count=50
                ),
            ],
            message="Success"
        )
        assert response.region == "NSW"
        assert len(response.fuel_mix) == 2


class TestRegionSummaryResponse:
    """Tests for RegionSummaryResponse model"""

    def test_region_summary_valid(self):
        """Test valid RegionSummaryResponse"""
        response = RegionSummaryResponse(
            region="NSW",
            latest_price=85.50,
            total_demand=7500.0,
            price_timestamp="2025-01-15T10:30:00",
            total_generation=7200.0,
            generator_count=57,
            message="Success"
        )
        assert response.region == "NSW"
        assert response.generator_count == 57

    def test_region_summary_nullable_fields(self):
        """Test RegionSummaryResponse with null values"""
        response = RegionSummaryResponse(
            region="NSW",
            generator_count=0,
            message="No data"
        )
        assert response.latest_price is None
        assert response.total_demand is None


class TestDataCoverageResponse:
    """Tests for DataCoverageResponse model"""

    def test_data_coverage_valid(self):
        """Test valid DataCoverageResponse"""
        response = DataCoverageResponse(
            table="price_data",
            earliest_date="2025-01-01T00:00:00",
            latest_date="2025-01-15T10:30:00",
            total_records=10000,
            days_with_data=15,
            message="Success"
        )
        assert response.table == "price_data"
        assert response.days_with_data == 15

    def test_data_coverage_empty(self):
        """Test DataCoverageResponse with no data"""
        response = DataCoverageResponse(
            table="price_data",
            total_records=0,
            days_with_data=0,
            message="No data"
        )
        assert response.earliest_date is None
        assert response.total_records == 0


class TestGenerationByFuelRecord:
    """Tests for GenerationByFuelRecord model"""

    def test_generation_by_fuel_valid(self):
        """Test valid GenerationByFuelRecord"""
        record = GenerationByFuelRecord(
            settlementdate="2025-01-15T10:30:00",
            fuel_source="Coal",
            total_generation=5000.0,
            unit_count=15
        )
        assert record.fuel_source == "Coal"
        assert record.total_generation == 5000.0


class TestDUIDListResponse:
    """Tests for DUIDListResponse model"""

    def test_duid_list_valid(self):
        """Test valid DUIDListResponse"""
        response = DUIDListResponse(
            duids=["BAYSW1", "AGLHAL", "ARWF1"],
            count=3,
            message="Success"
        )
        assert len(response.duids) == 3
        assert "BAYSW1" in response.duids

    def test_duid_list_empty(self):
        """Test empty DUIDListResponse"""
        response = DUIDListResponse(
            duids=[],
            count=0,
            message="No DUIDs found"
        )
        assert response.count == 0


class TestRegionPriceHistoryResponse:
    """Tests for RegionPriceHistoryResponse model"""

    def test_region_price_history_valid(self):
        """Test valid RegionPriceHistoryResponse"""
        response = RegionPriceHistoryResponse(
            region="NSW",
            data=[
                {"settlementdate": "2025-01-15T10:00:00", "price": 80.0},
                {"settlementdate": "2025-01-15T10:30:00", "price": 85.0},
            ],
            count=2,
            hours=24,
            price_type="DISPATCH",
            message="Success"
        )
        assert response.region == "NSW"
        assert response.hours == 24
        assert response.price_type == "DISPATCH"


class TestGapInfo:
    """Tests for GapInfo model"""

    def test_gap_info_valid(self):
        """Test valid GapInfo"""
        gap = GapInfo(
            gap_start="2025-01-15T10:15:00",
            gap_end="2025-01-15T10:45:00",
            missing_intervals=5,
            duration_minutes=30
        )
        assert gap.gap_start == "2025-01-15T10:15:00"
        assert gap.gap_end == "2025-01-15T10:45:00"
        assert gap.missing_intervals == 5
        assert gap.duration_minutes == 30

    def test_gap_info_single_interval(self):
        """Test GapInfo for a single missing interval"""
        gap = GapInfo(
            gap_start="2025-01-15T10:00:00",
            gap_end="2025-01-15T10:10:00",
            missing_intervals=1,
            duration_minutes=10
        )
        assert gap.missing_intervals == 1


class TestTableGaps:
    """Tests for TableGaps model"""

    def test_table_gaps_valid(self):
        """Test valid TableGaps with gaps"""
        gaps = TableGaps(
            table="dispatch_data",
            gaps=[
                GapInfo(
                    gap_start="2025-01-15T10:15:00",
                    gap_end="2025-01-15T10:45:00",
                    missing_intervals=5,
                    duration_minutes=30
                )
            ],
            total_gaps=1
        )
        assert gaps.table == "dispatch_data"
        assert gaps.total_gaps == 1
        assert len(gaps.gaps) == 1

    def test_table_gaps_empty(self):
        """Test TableGaps with no gaps"""
        gaps = TableGaps(
            table="price_data",
            gaps=[],
            total_gaps=0
        )
        assert gaps.total_gaps == 0
        assert len(gaps.gaps) == 0


class TestTableStats:
    """Tests for TableStats model"""

    def test_table_stats_valid(self):
        """Test valid TableStats"""
        stats = TableStats(
            table="dispatch_data",
            total_records=1000000,
            earliest_date="2025-01-01T00:00:00",
            latest_date="2025-01-15T10:30:00",
            days_with_data=15,
            expected_interval=5
        )
        assert stats.table == "dispatch_data"
        assert stats.total_records == 1000000
        assert stats.expected_interval == 5

    def test_table_stats_static_table(self):
        """Test TableStats for static reference table (no intervals)"""
        stats = TableStats(
            table="generator_info",
            total_records=456,
            earliest_date="2025-01-01T00:00:00",
            latest_date="2025-01-15T00:00:00"
        )
        assert stats.table == "generator_info"
        assert stats.days_with_data is None
        assert stats.expected_interval is None

    def test_table_stats_empty(self):
        """Test TableStats for empty table"""
        stats = TableStats(
            table="dispatch_data",
            total_records=0
        )
        assert stats.total_records == 0
        assert stats.earliest_date is None
        assert stats.latest_date is None


class TestDatabaseHealthResponse:
    """Tests for DatabaseHealthResponse model"""

    def test_database_health_response_valid(self):
        """Test valid DatabaseHealthResponse"""
        response = DatabaseHealthResponse(
            tables=[
                TableStats(
                    table="dispatch_data",
                    total_records=1000000,
                    earliest_date="2025-01-01T00:00:00",
                    latest_date="2025-01-15T10:30:00",
                    days_with_data=15,
                    expected_interval=5
                ),
                TableStats(
                    table="price_data",
                    total_records=50000,
                    earliest_date="2025-01-01T00:00:00",
                    latest_date="2025-01-15T10:30:00",
                    days_with_data=15,
                    expected_interval=5
                ),
            ],
            gaps=[
                TableGaps(table="dispatch_data", gaps=[], total_gaps=0),
                TableGaps(table="price_data", gaps=[], total_gaps=0),
            ],
            checked_hours=168,
            checked_at="2025-01-15T10:30:00"
        )
        assert len(response.tables) == 2
        assert len(response.gaps) == 2
        assert response.checked_hours == 168

    def test_database_health_response_with_gaps(self):
        """Test DatabaseHealthResponse with detected gaps"""
        response = DatabaseHealthResponse(
            tables=[
                TableStats(
                    table="dispatch_data",
                    total_records=500000,
                    days_with_data=7,
                    expected_interval=5
                )
            ],
            gaps=[
                TableGaps(
                    table="dispatch_data",
                    gaps=[
                        GapInfo(
                            gap_start="2025-01-10T03:00:00",
                            gap_end="2025-01-10T03:30:00",
                            missing_intervals=5,
                            duration_minutes=30
                        )
                    ],
                    total_gaps=1
                )
            ],
            checked_hours=168,
            checked_at="2025-01-15T10:30:00"
        )
        assert response.gaps[0].total_gaps == 1
        assert response.gaps[0].gaps[0].missing_intervals == 5
