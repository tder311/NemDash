"""
Unit tests for NEMPriceSetterClient
"""
import pytest
import httpx
import zipfile
import io
from datetime import datetime, date

from app.nem_price_setter_client import NEMPriceSetterClient, REGION_MAPPING, INCREASE_THRESHOLD, BAND_PRICE_GAP_THRESHOLD


# ============================================================================
# Sample XML fixtures
# ============================================================================

SAMPLE_PRICE_SETTER_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<SolutionAnalysis>
  <PriceSetting Market="Energy" DispatchedMarket="ENOF" RegionID="NSW1"
    PeriodID="2025-01-15T10:30:00+10:00" Unit="BAYSW1" Price="85.50"
    Increase="0.61" RRNBandPrice="85.50" BandNo="4" />
  <PriceSetting Market="Energy" DispatchedMarket="ENOF" RegionID="VIC1"
    PeriodID="2025-01-15T10:30:00+10:00" Unit="LOYS1" Price="72.30"
    Increase="0.39" RRNBandPrice="72.30" BandNo="3" />
  <PriceSetting Market="Energy" DispatchedMarket="ENOF" RegionID="QLD1"
    PeriodID="2025-01-15T10:30:00+10:00" Unit="GSTONE5,ENOF,2,GSTONE6,ENOF,2" Price="65.00"
    Increase="0.71" RRNBandPrice="65.00" BandNo="2" />
  <PriceSetting Market="FCAS" DispatchedMarket="R6SE" RegionID="NSW1"
    PeriodID="2025-01-15T10:30:00+10:00" Unit="FCAS_UNIT" Price="10.00"
    Increase="-1" RRNBandPrice="10.00" BandNo="1" />
  <PriceSetting Market="Energy" DispatchedMarket="ENOF" RegionID="UNKNOWN_REGION"
    PeriodID="2025-01-15T10:30:00+10:00" Unit="MYSTERY1" Price="50.00"
    Increase="0.50" RRNBandPrice="50.00" BandNo="1" />
</SolutionAnalysis>
"""

SAMPLE_PRICE_SETTER_XML_EMPTY_UNIT = b"""<?xml version="1.0" encoding="UTF-8"?>
<SolutionAnalysis>
  <PriceSetting Market="Energy" DispatchedMarket="ENOF" RegionID="NSW1"
    PeriodID="2025-01-15T10:30:00+10:00" Unit="" Price="85.50" />
</SolutionAnalysis>
"""

SAMPLE_PRICE_SETTER_XML_BAD_PRICE = b"""<?xml version="1.0" encoding="UTF-8"?>
<SolutionAnalysis>
  <PriceSetting Market="Energy" DispatchedMarket="ENOF" RegionID="NSW1"
    PeriodID="2025-01-15T10:30:00+10:00" Unit="BAYSW1" Price="not_a_number" />
</SolutionAnalysis>
"""

SAMPLE_PRICE_SETTER_XML_NO_ENERGY = b"""<?xml version="1.0" encoding="UTF-8"?>
<SolutionAnalysis>
  <PriceSetting Market="FCAS" DispatchedMarket="R6SE" RegionID="NSW1"
    PeriodID="2025-01-15T10:30:00+10:00" Unit="FCAS_UNIT" Price="10.00" />
</SolutionAnalysis>
"""

SAMPLE_PRICE_SETTER_XML_MULTI_REGION = b"""<?xml version="1.0" encoding="UTF-8"?>
<SolutionAnalysis>
  <PriceSetting Market="Energy" DispatchedMarket="ENOF" RegionID="NSW1"
    PeriodID="2025-01-15T10:30:00+10:00" Unit="BAYSW1" Price="85.50"
    Increase="0.61" RRNBandPrice="85.50" BandNo="4" />
  <PriceSetting Market="Energy" DispatchedMarket="ENOF" RegionID="SA1"
    PeriodID="2025-01-15T10:30:00+10:00" Unit="TORRB1" Price="120.00"
    Increase="0.50" RRNBandPrice="120.00" BandNo="5" />
  <PriceSetting Market="Energy" DispatchedMarket="ENOF" RegionID="TAS1"
    PeriodID="2025-01-15T10:30:00+10:00" Unit="GORDON1" Price="45.00"
    Increase="1.0" RRNBandPrice="45.00" BandNo="2" />
</SolutionAnalysis>
"""

SAMPLE_PRICE_SETTER_XML_WITH_CONSTRAINT = b"""<?xml version="1.0" encoding="UTF-8"?>
<SolutionAnalysis>
  <PriceSetting Market="Energy" DispatchedMarket="ENOF" RegionID="NSW1"
    PeriodID="2025-01-15T10:30:00+10:00" Unit="BAYSW1" Price="20300.00"
    Increase="0.707" RRNBandPrice="20300" BandNo="10" />
  <PriceSetting Market="Energy" DispatchedMarket="ENOF" RegionID="NSW1"
    PeriodID="2025-01-15T10:30:00+10:00" Unit="SOLARSF1" Price="20300.00"
    Increase="-0.0009" RRNBandPrice="-1000" BandNo="1" />
</SolutionAnalysis>
"""


def create_price_setter_zip(*xml_entries):
    """Create a ZIP file containing XML files."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for i, xml_content in enumerate(xml_entries):
            zf.writestr(f"NemPriceSetter_20250115_{i:03d}.xml", xml_content)
    return buf.getvalue()


def create_price_setter_zip_no_xml():
    """Create a ZIP file with no XML files."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("readme.txt", "No XML files here")
    return buf.getvalue()


# ============================================================================
# Tests
# ============================================================================

class TestNEMPriceSetterClientInit:
    """Tests for NEMPriceSetterClient initialization"""

    def test_init_default_url(self):
        """Test default base URL"""
        client = NEMPriceSetterClient()
        assert client.base_url == "https://www.nemweb.com.au"

    def test_init_custom_url(self):
        """Test custom base URL"""
        client = NEMPriceSetterClient("https://custom.url.com")
        assert client.base_url == "https://custom.url.com"

    def test_init_strips_trailing_slash(self):
        """Test that trailing slash is stripped"""
        client = NEMPriceSetterClient("https://example.com/")
        assert client.base_url == "https://example.com"


class TestRegionMapping:
    """Tests for REGION_MAPPING constant"""

    def test_all_nem_regions_mapped(self):
        """Test that all 5 NEM regions are mapped"""
        assert len(REGION_MAPPING) == 5
        assert REGION_MAPPING['NSW1'] == 'NSW'
        assert REGION_MAPPING['VIC1'] == 'VIC'
        assert REGION_MAPPING['QLD1'] == 'QLD'
        assert REGION_MAPPING['SA1'] == 'SA'
        assert REGION_MAPPING['TAS1'] == 'TAS'


class TestParsePriceSetterXml:
    """Tests for _parse_price_setter_xml method"""

    @pytest.fixture
    def client(self):
        return NEMPriceSetterClient()

    def test_parse_energy_records(self, client):
        """Test parsing extracts energy market ENOF records"""
        records = client._parse_price_setter_xml(SAMPLE_PRICE_SETTER_XML)
        assert len(records) == 3
        regions = [r['region'] for r in records]
        assert 'NSW' in regions
        assert 'VIC' in regions
        assert 'QLD' in regions

    def test_parse_filters_fcas(self, client):
        """Test that FCAS records are filtered out"""
        records = client._parse_price_setter_xml(SAMPLE_PRICE_SETTER_XML)
        duids = [r['duid'] for r in records]
        assert 'FCAS_UNIT' not in duids

    def test_parse_filters_unknown_region(self, client):
        """Test that unknown regions are filtered out"""
        records = client._parse_price_setter_xml(SAMPLE_PRICE_SETTER_XML)
        duids = [r['duid'] for r in records]
        assert 'MYSTERY1' not in duids

    def test_parse_extracts_first_duid_from_multi_unit(self, client):
        """Test that comma-separated Unit field extracts first DUID"""
        records = client._parse_price_setter_xml(SAMPLE_PRICE_SETTER_XML)
        qld_record = [r for r in records if r['region'] == 'QLD'][0]
        assert qld_record['duid'] == 'GSTONE5'

    def test_parse_extracts_price(self, client):
        """Test that price is parsed correctly"""
        records = client._parse_price_setter_xml(SAMPLE_PRICE_SETTER_XML)
        nsw_record = [r for r in records if r['region'] == 'NSW'][0]
        assert nsw_record['price'] == 85.50

    def test_parse_extracts_period_id(self, client):
        """Test that period_id is preserved from XML"""
        records = client._parse_price_setter_xml(SAMPLE_PRICE_SETTER_XML)
        assert records[0]['period_id'] == '2025-01-15T10:30:00+10:00'

    def test_parse_empty_unit_skipped(self, client):
        """Test that records with empty Unit field are skipped"""
        records = client._parse_price_setter_xml(SAMPLE_PRICE_SETTER_XML_EMPTY_UNIT)
        assert len(records) == 0

    def test_parse_bad_price_skipped(self, client):
        """Test that records with non-numeric Price are skipped"""
        records = client._parse_price_setter_xml(SAMPLE_PRICE_SETTER_XML_BAD_PRICE)
        assert len(records) == 0

    def test_parse_no_energy_records(self, client):
        """Test parsing XML with no energy records returns empty list"""
        records = client._parse_price_setter_xml(SAMPLE_PRICE_SETTER_XML_NO_ENERGY)
        assert len(records) == 0

    def test_parse_all_regions(self, client):
        """Test parsing XML with NSW, SA, TAS regions"""
        records = client._parse_price_setter_xml(SAMPLE_PRICE_SETTER_XML_MULTI_REGION)
        assert len(records) == 3
        regions = {r['region'] for r in records}
        assert regions == {'NSW', 'SA', 'TAS'}

    def test_parse_extracts_increase(self, client):
        """Test that Increase coefficient is extracted"""
        records = client._parse_price_setter_xml(SAMPLE_PRICE_SETTER_XML)
        nsw_record = [r for r in records if r['region'] == 'NSW'][0]
        assert nsw_record['increase'] == 0.61

    def test_parse_extracts_band_price(self, client):
        """Test that RRNBandPrice is extracted as band_price"""
        records = client._parse_price_setter_xml(SAMPLE_PRICE_SETTER_XML)
        nsw_record = [r for r in records if r['region'] == 'NSW'][0]
        assert nsw_record['band_price'] == 85.50

    def test_parse_extracts_band_no(self, client):
        """Test that BandNo is extracted"""
        records = client._parse_price_setter_xml(SAMPLE_PRICE_SETTER_XML)
        nsw_record = [r for r in records if r['region'] == 'NSW'][0]
        assert nsw_record['band_no'] == 4

    def test_parse_includes_constraint_artifacts(self, client):
        """Test that constraint artifacts are parsed (filtering happens at query time)"""
        records = client._parse_price_setter_xml(SAMPLE_PRICE_SETTER_XML_WITH_CONSTRAINT)
        assert len(records) == 2
        constraint = [r for r in records if r['duid'] == 'SOLARSF1'][0]
        assert abs(constraint['increase']) < INCREASE_THRESHOLD

    def test_band_price_gap_threshold_constant(self):
        """Test that BAND_PRICE_GAP_THRESHOLD is set for filtering constraint artifacts"""
        assert BAND_PRICE_GAP_THRESHOLD == 200

    def test_parse_missing_increase_defaults_to_zero(self, client):
        """Test that missing Increase attribute defaults to 0.0"""
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
        <SolutionAnalysis>
          <PriceSetting Market="Energy" DispatchedMarket="ENOF" RegionID="NSW1"
            PeriodID="2025-01-15T10:30:00+10:00" Unit="BAYSW1" Price="85.50" />
        </SolutionAnalysis>
        """
        records = client._parse_price_setter_xml(xml)
        assert records[0]['increase'] == 0.0


class TestParsePriceSetterZip:
    """Tests for _parse_price_setter_zip method"""

    @pytest.fixture
    def client(self):
        return NEMPriceSetterClient()

    def test_parse_zip_with_xml(self, client):
        """Test parsing a ZIP containing valid XML files"""
        zip_content = create_price_setter_zip(
            SAMPLE_PRICE_SETTER_XML,
            SAMPLE_PRICE_SETTER_XML_MULTI_REGION,
        )
        df = client._parse_price_setter_zip(zip_content)

        assert df is not None
        assert len(df) > 0
        assert 'period_id' in df.columns
        assert 'region' in df.columns
        assert 'price' in df.columns
        assert 'duid' in df.columns
        assert 'increase' in df.columns
        assert 'band_price' in df.columns
        assert 'band_no' in df.columns

    def test_parse_zip_converts_timezone(self, client):
        """Test that period_id is converted from UTC to naive AEST"""
        import pandas as pd
        zip_content = create_price_setter_zip(SAMPLE_PRICE_SETTER_XML)
        df = client._parse_price_setter_zip(zip_content)

        assert df is not None
        # Should be timezone-naive after conversion
        assert df['period_id'].dt.tz is None

    def test_parse_zip_deduplicates(self, client):
        """Test that duplicate records are removed"""
        # Same XML twice should produce duplicates that get deduped
        zip_content = create_price_setter_zip(
            SAMPLE_PRICE_SETTER_XML,
            SAMPLE_PRICE_SETTER_XML,
        )
        df = client._parse_price_setter_zip(zip_content)

        assert df is not None
        dupes = df.duplicated(subset=['period_id', 'region', 'duid'])
        assert not dupes.any()

    def test_parse_zip_no_xml_files(self, client):
        """Test parsing a ZIP with no XML files returns None"""
        zip_content = create_price_setter_zip_no_xml()
        df = client._parse_price_setter_zip(zip_content)
        assert df is None

    def test_parse_zip_all_empty_xml(self, client):
        """Test parsing ZIP where XML files have no energy records returns None"""
        zip_content = create_price_setter_zip(SAMPLE_PRICE_SETTER_XML_NO_ENERGY)
        df = client._parse_price_setter_zip(zip_content)
        assert df is None

    def test_parse_invalid_zip(self, client):
        """Test parsing invalid ZIP content returns None"""
        df = client._parse_price_setter_zip(b'not a zip file')
        assert df is None

    def test_parse_zip_handles_bad_xml_gracefully(self, client):
        """Test that a bad XML file doesn't break parsing of valid ones"""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("good.xml", SAMPLE_PRICE_SETTER_XML)
            zf.writestr("bad.xml", b"<not valid xml<><>")
        zip_content = buf.getvalue()

        df = client._parse_price_setter_zip(zip_content)
        assert df is not None
        assert len(df) > 0


class TestGetDailyPriceSetter:
    """Tests for get_daily_price_setter async method"""

    @pytest.fixture
    def client(self):
        return NEMPriceSetterClient("https://test.nemweb.com.au")

    @pytest.mark.asyncio
    async def test_get_daily_success(self, client, httpx_mock):
        """Test successful fetch and parse"""
        zip_content = create_price_setter_zip(SAMPLE_PRICE_SETTER_XML)

        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Data_Archive/Wholesale_Electricity/NEMDE/2025/NEMDE_2025_01/NEMDE_Market_Data/NEMDE_Files/NemPriceSetter_20250115_xml.zip",
            content=zip_content,
        )

        df = await client.get_daily_price_setter(datetime(2025, 1, 15))

        assert df is not None
        assert len(df) > 0
        assert 'region' in df.columns

    @pytest.mark.asyncio
    async def test_get_daily_with_date_object(self, client, httpx_mock):
        """Test that date objects (not just datetime) are accepted"""
        zip_content = create_price_setter_zip(SAMPLE_PRICE_SETTER_XML)

        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Data_Archive/Wholesale_Electricity/NEMDE/2025/NEMDE_2025_01/NEMDE_Market_Data/NEMDE_Files/NemPriceSetter_20250115_xml.zip",
            content=zip_content,
        )

        df = await client.get_daily_price_setter(date(2025, 1, 15))

        assert df is not None

    @pytest.mark.asyncio
    async def test_get_daily_404(self, client, httpx_mock):
        """Test 404 returns None"""
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Data_Archive/Wholesale_Electricity/NEMDE/2025/NEMDE_2025_01/NEMDE_Market_Data/NEMDE_Files/NemPriceSetter_20250115_xml.zip",
            status_code=404,
        )

        df = await client.get_daily_price_setter(datetime(2025, 1, 15))
        assert df is None

    @pytest.mark.asyncio
    async def test_get_daily_server_error(self, client, httpx_mock):
        """Test HTTP 500 returns None"""
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Data_Archive/Wholesale_Electricity/NEMDE/2025/NEMDE_2025_01/NEMDE_Market_Data/NEMDE_Files/NemPriceSetter_20250115_xml.zip",
            status_code=500,
        )

        df = await client.get_daily_price_setter(datetime(2025, 1, 15))
        assert df is None

    @pytest.mark.asyncio
    async def test_get_daily_network_error(self, client, httpx_mock):
        """Test network error returns None"""
        httpx_mock.add_exception(httpx.ConnectError("Connection refused"))

        df = await client.get_daily_price_setter(datetime(2025, 1, 15))
        assert df is None

    @pytest.mark.asyncio
    async def test_get_daily_url_format(self, client, httpx_mock):
        """Test URL is correctly formatted for different months"""
        zip_content = create_price_setter_zip(SAMPLE_PRICE_SETTER_XML)

        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Data_Archive/Wholesale_Electricity/NEMDE/2025/NEMDE_2025_12/NEMDE_Market_Data/NEMDE_Files/NemPriceSetter_20251231_xml.zip",
            content=zip_content,
        )

        df = await client.get_daily_price_setter(datetime(2025, 12, 31))
        assert df is not None
