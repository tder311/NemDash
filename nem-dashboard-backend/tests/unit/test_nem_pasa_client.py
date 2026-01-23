"""
Unit tests for NEMPASAClient
"""
import pytest
import httpx

from app.nem_pasa_client import NEMPASAClient
from tests.fixtures.sample_pasa_csv import (
    SAMPLE_PDPASA_CSV,
    SAMPLE_STPASA_CSV,
    SAMPLE_PASA_NO_RECORDS,
    SAMPLE_PDPASA_LOR3_CSV,
    SAMPLE_PDPASA_DIR,
    SAMPLE_STPASA_DIR,
    SAMPLE_PASA_EMPTY_DIR,
    create_pasa_zip,
)


class TestNEMPASAClientInit:
    """Tests for NEMPASAClient initialization"""

    def test_init_default_url(self):
        """Test default base URL"""
        client = NEMPASAClient()
        assert client.base_url == "https://www.nemweb.com.au"

    def test_init_custom_url(self):
        """Test custom base URL"""
        client = NEMPASAClient("https://custom.url.com")
        assert client.base_url == "https://custom.url.com"

    def test_init_strips_trailing_slash(self):
        """Test that trailing slash is stripped"""
        client = NEMPASAClient("https://example.com/")
        assert client.base_url == "https://example.com"


class TestGetLorDescription:
    """Tests for get_lor_description static method"""

    def test_lor0_description(self):
        """Test LOR 0 description"""
        assert NEMPASAClient.get_lor_description(0) == "No LOR"

    def test_lor1_description(self):
        """Test LOR 1 description"""
        result = NEMPASAClient.get_lor_description(1)
        assert "LOR1" in result
        assert "Low Reserve" in result

    def test_lor2_description(self):
        """Test LOR 2 description"""
        result = NEMPASAClient.get_lor_description(2)
        assert "LOR2" in result
        assert "Lack of Reserve" in result

    def test_lor3_description(self):
        """Test LOR 3 description"""
        result = NEMPASAClient.get_lor_description(3)
        assert "LOR3" in result
        assert "Load Shedding" in result

    def test_unknown_lor_level(self):
        """Test unknown LOR level returns unknown description"""
        result = NEMPASAClient.get_lor_description(99)
        assert "Unknown" in result
        assert "99" in result

    def test_lor_description_with_float(self):
        """Test LOR description handles float input by converting to int"""
        assert NEMPASAClient.get_lor_description(1.0) == NEMPASAClient.get_lor_description(1)


class TestParsePasaZip:
    """Tests for _parse_pasa_zip method"""

    @pytest.fixture
    def client(self):
        return NEMPASAClient()

    def test_parse_pdpasa_zip(self, client):
        """Test parsing PDPASA ZIP content"""
        zip_content = create_pasa_zip(SAMPLE_PDPASA_CSV, 'PDPASA')
        df = client._parse_pasa_zip(zip_content, 'PDPASA')

        assert df is not None
        assert len(df) > 0
        assert 'interval_datetime' in df.columns
        assert 'regionid' in df.columns
        assert 'demand50' in df.columns
        assert 'lorcondition' in df.columns

    def test_parse_stpasa_zip(self, client):
        """Test parsing STPASA ZIP content"""
        zip_content = create_pasa_zip(SAMPLE_STPASA_CSV, 'STPASA')
        df = client._parse_pasa_zip(zip_content, 'STPASA')

        assert df is not None
        assert len(df) > 0
        assert 'interval_datetime' in df.columns
        assert 'regionid' in df.columns

    def test_parse_pasa_numeric_conversion(self, client):
        """Test that numeric columns are converted properly"""
        zip_content = create_pasa_zip(SAMPLE_PDPASA_CSV, 'PDPASA')
        df = client._parse_pasa_zip(zip_content, 'PDPASA')

        assert df is not None
        # Numeric columns should be numeric types
        assert df['demand50'].dtype in ['float64', 'int64']
        assert df['lorcondition'].dtype in ['float64', 'int64']

    def test_parse_pasa_datetime_conversion(self, client):
        """Test that datetime columns are converted properly"""
        zip_content = create_pasa_zip(SAMPLE_PDPASA_CSV, 'PDPASA')
        df = client._parse_pasa_zip(zip_content, 'PDPASA')

        assert df is not None
        assert df['interval_datetime'].dtype == 'datetime64[ns]'
        assert df['run_datetime'].dtype == 'datetime64[ns]'

    def test_parse_pasa_no_records(self, client):
        """Test parsing ZIP with no REGIONSOLUTION records returns None"""
        zip_content = create_pasa_zip(SAMPLE_PASA_NO_RECORDS, 'PDPASA')
        df = client._parse_pasa_zip(zip_content, 'PDPASA')

        assert df is None

    def test_parse_pasa_lor3_condition(self, client):
        """Test parsing PASA with LOR3 condition"""
        zip_content = create_pasa_zip(SAMPLE_PDPASA_LOR3_CSV, 'PDPASA')
        df = client._parse_pasa_zip(zip_content, 'PDPASA')

        assert df is not None
        assert len(df) == 1
        assert df.iloc[0]['lorcondition'] == 3
        assert df.iloc[0]['surplusreserve'] == -700

    def test_parse_invalid_zip(self, client):
        """Test parsing invalid ZIP content returns None"""
        df = client._parse_pasa_zip(b'not a zip file', 'PDPASA')
        assert df is None

    def test_parse_pasa_deduplicates_records(self, client):
        """Test that duplicate records are removed"""
        zip_content = create_pasa_zip(SAMPLE_PDPASA_CSV, 'PDPASA')
        df = client._parse_pasa_zip(zip_content, 'PDPASA')

        assert df is not None
        # Should have unique interval_datetime + regionid combinations
        duplicates = df.duplicated(subset=['interval_datetime', 'regionid'])
        assert not duplicates.any()

    def test_parse_pasa_sorted_by_interval(self, client):
        """Test that output is sorted by interval_datetime"""
        zip_content = create_pasa_zip(SAMPLE_PDPASA_CSV, 'PDPASA')
        df = client._parse_pasa_zip(zip_content, 'PDPASA')

        assert df is not None
        # Data should be sorted
        assert df['interval_datetime'].is_monotonic_increasing


class TestGetLatestPdpasa:
    """Tests for get_latest_pdpasa async method"""

    @pytest.fixture
    def client(self):
        return NEMPASAClient("https://test.nemweb.com.au")

    @pytest.mark.asyncio
    async def test_get_latest_pdpasa_success(self, client, httpx_mock):
        """Test successful PDPASA fetch"""
        # Mock directory listing
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/PDPASA/",
            html=SAMPLE_PDPASA_DIR
        )

        # Mock ZIP file download
        zip_content = create_pasa_zip(SAMPLE_PDPASA_CSV, 'PDPASA')
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/PDPASA/PUBLIC_PDPASA_202501151000_00000005.zip",
            content=zip_content
        )

        df = await client.get_latest_pdpasa()

        assert df is not None
        assert len(df) > 0
        assert 'regionid' in df.columns

    @pytest.mark.asyncio
    async def test_get_latest_pdpasa_empty_directory(self, client, httpx_mock):
        """Test PDPASA fetch with empty directory"""
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/PDPASA/",
            html=SAMPLE_PASA_EMPTY_DIR
        )

        df = await client.get_latest_pdpasa()

        assert df is None

    @pytest.mark.asyncio
    async def test_get_latest_pdpasa_network_error(self, client, httpx_mock):
        """Test PDPASA fetch handles network errors"""
        httpx_mock.add_exception(httpx.ConnectError("Connection refused"))

        df = await client.get_latest_pdpasa()

        assert df is None

    @pytest.mark.asyncio
    async def test_get_latest_pdpasa_server_error(self, client, httpx_mock):
        """Test PDPASA fetch handles HTTP 500 errors"""
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/PDPASA/",
            status_code=500
        )

        df = await client.get_latest_pdpasa()

        assert df is None

    @pytest.mark.asyncio
    async def test_get_latest_pdpasa_file_not_found(self, client, httpx_mock):
        """Test PDPASA fetch handles file not found"""
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/PDPASA/",
            html=SAMPLE_PDPASA_DIR
        )
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/PDPASA/PUBLIC_PDPASA_202501151000_00000005.zip",
            status_code=404
        )

        df = await client.get_latest_pdpasa()

        assert df is None


class TestGetLatestStpasa:
    """Tests for get_latest_stpasa async method"""

    @pytest.fixture
    def client(self):
        return NEMPASAClient("https://test.nemweb.com.au")

    @pytest.mark.asyncio
    async def test_get_latest_stpasa_success(self, client, httpx_mock):
        """Test successful STPASA fetch"""
        # Mock directory listing
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/Short_Term_PASA_Reports/",
            html=SAMPLE_STPASA_DIR
        )

        # Mock ZIP file download
        zip_content = create_pasa_zip(SAMPLE_STPASA_CSV, 'STPASA')
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/Short_Term_PASA_Reports/PUBLIC_STPASA_202501151200_00000002.zip",
            content=zip_content
        )

        df = await client.get_latest_stpasa()

        assert df is not None
        assert len(df) > 0
        assert 'regionid' in df.columns

    @pytest.mark.asyncio
    async def test_get_latest_stpasa_empty_directory(self, client, httpx_mock):
        """Test STPASA fetch with empty directory"""
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/Short_Term_PASA_Reports/",
            html=SAMPLE_PASA_EMPTY_DIR
        )

        df = await client.get_latest_stpasa()

        assert df is None

    @pytest.mark.asyncio
    async def test_get_latest_stpasa_network_error(self, client, httpx_mock):
        """Test STPASA fetch handles network errors"""
        httpx_mock.add_exception(httpx.ConnectError("Connection refused"))

        df = await client.get_latest_stpasa()

        assert df is None

    @pytest.mark.asyncio
    async def test_get_latest_stpasa_server_error(self, client, httpx_mock):
        """Test STPASA fetch handles HTTP 500 errors"""
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/Short_Term_PASA_Reports/",
            status_code=500
        )

        df = await client.get_latest_stpasa()

        assert df is None

    @pytest.mark.asyncio
    async def test_get_latest_stpasa_file_not_found(self, client, httpx_mock):
        """Test STPASA fetch handles file not found"""
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/Short_Term_PASA_Reports/",
            html=SAMPLE_STPASA_DIR
        )
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/Short_Term_PASA_Reports/PUBLIC_STPASA_202501151200_00000002.zip",
            status_code=404
        )

        df = await client.get_latest_stpasa()

        assert df is None


class TestParsePasaRegions:
    """Tests for region handling in PASA parsing"""

    @pytest.fixture
    def client(self):
        return NEMPASAClient()

    def test_parse_pdpasa_all_regions(self, client):
        """Test that all NEM regions are present in parsed data"""
        zip_content = create_pasa_zip(SAMPLE_PDPASA_CSV, 'PDPASA')
        df = client._parse_pasa_zip(zip_content, 'PDPASA')

        assert df is not None
        regions = set(df['regionid'].unique())
        expected_regions = {'NSW1', 'VIC1', 'QLD1', 'SA1', 'TAS1'}
        assert regions == expected_regions

    def test_parse_pdpasa_multiple_intervals(self, client):
        """Test that multiple intervals are parsed correctly"""
        zip_content = create_pasa_zip(SAMPLE_PDPASA_CSV, 'PDPASA')
        df = client._parse_pasa_zip(zip_content, 'PDPASA')

        assert df is not None
        # Sample data has 2 intervals with 5 regions each = 10 records
        assert len(df) == 10
        assert df['interval_datetime'].nunique() == 2
