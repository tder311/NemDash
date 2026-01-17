"""
Unit tests for NEMPriceClient
"""
import pytest

from app.nem_price_client import NEMPriceClient, REGION_MAPPING
from tests.fixtures.sample_price_csv import (
    SAMPLE_DISPATCH_PRICE_CSV,
    SAMPLE_DISPATCH_PRICE_CSV_V5,
    SAMPLE_TRADING_PRICE_CSV,
    SAMPLE_PUBLIC_PRICE_CSV,
    SAMPLE_PRICE_NO_RECORDS,
    SAMPLE_DISPATCH_PRICE_DIR,
    SAMPLE_TRADING_DIR,
    SAMPLE_IRSR_DIR,
    SAMPLE_IRSR_DIR_ALT,
    create_price_zip,
)
from tests.fixtures.sample_interconnector_csv import (
    SAMPLE_INTERCONNECTOR_CSV,
    SAMPLE_INTERCONNECTOR_NEGATIVE,
    SAMPLE_INTERCONNECTOR_NO_RECORDS,
    SAMPLE_INTERCONNECTOR_MALFORMED,
    create_interconnector_zip,
)


class TestRegionMapping:
    """Tests for REGION_MAPPING constant"""

    def test_region_mapping_nsw(self):
        """Test NSW mapping"""
        assert REGION_MAPPING['NSW1'] == 'NSW'
        assert REGION_MAPPING['1'] == 'NSW'

    def test_region_mapping_all_regions(self):
        """Test all region mappings"""
        assert REGION_MAPPING['VIC1'] == 'VIC'
        assert REGION_MAPPING['QLD1'] == 'QLD'
        assert REGION_MAPPING['SA1'] == 'SA'
        assert REGION_MAPPING['TAS1'] == 'TAS'

    def test_region_mapping_numeric(self):
        """Test numeric region codes"""
        assert REGION_MAPPING['1'] == 'NSW'
        assert REGION_MAPPING['2'] == 'VIC'
        assert REGION_MAPPING['3'] == 'QLD'
        assert REGION_MAPPING['4'] == 'SA'
        assert REGION_MAPPING['5'] == 'TAS'


class TestNEMPriceClientInit:
    """Tests for NEMPriceClient initialization"""

    def test_init_default_url(self):
        """Test default base URL"""
        client = NEMPriceClient()
        assert client.base_url == "https://www.nemweb.com.au"

    def test_init_custom_url(self):
        """Test custom base URL"""
        client = NEMPriceClient("https://custom.url.com")
        assert client.base_url == "https://custom.url.com"

    def test_init_strips_trailing_slash(self):
        """Test that trailing slash is stripped"""
        client = NEMPriceClient("https://example.com/")
        assert client.base_url == "https://example.com"


class TestSafeFloat:
    """Tests for _safe_float utility method"""

    @pytest.fixture
    def client(self):
        return NEMPriceClient()

    def test_safe_float_valid(self, client):
        """Test valid float conversion"""
        assert client._safe_float("123.45") == 123.45

    def test_safe_float_quoted(self, client):
        """Test quoted value handling"""
        assert client._safe_float('"123.45"') == 123.45

    def test_safe_float_empty_quoted(self, client):
        """Test empty quoted string"""
        assert client._safe_float('""') == 0.0

    def test_safe_float_empty(self, client):
        """Test empty string"""
        assert client._safe_float("") == 0.0

    def test_safe_float_whitespace(self, client):
        """Test whitespace"""
        assert client._safe_float("   ") == 0.0

    def test_safe_float_invalid(self, client):
        """Test invalid value"""
        assert client._safe_float("invalid") == 0.0

    def test_safe_float_none(self, client):
        """Test None"""
        assert client._safe_float(None) == 0.0

    def test_safe_float_negative(self, client):
        """Test negative value"""
        assert client._safe_float("-50.25") == -50.25


class TestParseLatestFiles:
    """Tests for _parse_latest_*_file methods"""

    @pytest.fixture
    def client(self):
        return NEMPriceClient()

    def test_parse_latest_dispatch_price_file(self, client):
        """Test parsing dispatch price directory"""
        result = client._parse_latest_dispatch_price_file(SAMPLE_DISPATCH_PRICE_DIR)
        assert result is not None
        assert "DISPATCHIS" in result
        # Should return the latest (sorted by timestamp)
        assert "202501151030" in result

    def test_parse_latest_dispatch_price_file_empty(self, client):
        """Test empty directory"""
        result = client._parse_latest_dispatch_price_file("<html></html>")
        assert result is None

    def test_parse_latest_trading_file(self, client):
        """Test parsing trading directory"""
        result = client._parse_latest_trading_file(SAMPLE_TRADING_DIR)
        assert result is not None
        assert "TRADINGIS" in result

    def test_parse_latest_trading_file_empty(self, client):
        """Test empty trading directory"""
        result = client._parse_latest_trading_file("<html></html>")
        assert result is None

    def test_parse_latest_irsr_file(self, client):
        """Test parsing IRSR directory"""
        result = client._parse_latest_irsr_file(SAMPLE_IRSR_DIR)
        assert result is not None
        assert "IRSR" in result

    def test_parse_latest_irsr_file_alt_pattern(self, client):
        """Test IRSR with alternative naming pattern"""
        result = client._parse_latest_irsr_file(SAMPLE_IRSR_DIR_ALT)
        assert result is not None
        assert "DISPATCH_IRSR" in result

    def test_parse_latest_irsr_file_empty(self, client):
        """Test empty IRSR directory"""
        result = client._parse_latest_irsr_file("<html></html>")
        assert result is None


class TestParsePriceCsv:
    """Tests for _parse_price_csv method"""

    @pytest.fixture
    def client(self):
        return NEMPriceClient()

    def test_parse_price_csv_dispatch_format(self, client):
        """Test parsing DISPATCH price format (version 3)"""
        df = client._parse_price_csv(SAMPLE_DISPATCH_PRICE_CSV, 'DISPATCH')

        assert df is not None
        assert len(df) == 5  # 5 regions
        assert 'NSW' in df['region'].values
        assert df.loc[df['region'] == 'NSW', 'price'].values[0] == 85.50
        assert df['price_type'].iloc[0] == 'DISPATCH'

    def test_parse_price_csv_dispatch_format_v5(self, client):
        """Test parsing DISPATCH price format (version 5 with INTERVENTION column)"""
        df = client._parse_price_csv(SAMPLE_DISPATCH_PRICE_CSV_V5, 'DISPATCH')

        assert df is not None
        assert len(df) == 5  # 5 regions
        assert 'NSW' in df['region'].values
        # Version 5 has INTERVENTION column at index 8, RRP at index 9
        # Verify prices are correctly extracted (not zeros)
        assert df.loc[df['region'] == 'NSW', 'price'].values[0] == 85.50
        assert df.loc[df['region'] == 'VIC', 'price'].values[0] == 72.30
        assert df.loc[df['region'] == 'SA', 'price'].values[0] == 95.20
        assert df['price_type'].iloc[0] == 'DISPATCH'

    def test_parse_price_csv_trading_format(self, client):
        """Test parsing TRADING price format"""
        df = client._parse_price_csv(SAMPLE_TRADING_PRICE_CSV, 'TRADING')

        assert df is not None
        assert 'NSW' in df['region'].values
        assert 'SA' in df['region'].values
        assert df['price_type'].iloc[0] == 'TRADING'

    def test_parse_price_csv_public_format(self, client):
        """Test parsing PUBLIC (DREGION) price format"""
        df = client._parse_price_csv(SAMPLE_PUBLIC_PRICE_CSV, 'PUBLIC')

        assert df is not None
        assert len(df) == 5
        # PUBLIC format includes demand directly
        assert df.loc[df['region'] == 'NSW', 'totaldemand'].values[0] == 7500.0

    def test_parse_price_csv_region_mapping(self, client):
        """Test that region codes are correctly mapped"""
        df = client._parse_price_csv(SAMPLE_DISPATCH_PRICE_CSV, 'DISPATCH')

        assert df is not None
        # Should be 'NSW', not 'NSW1'
        assert 'NSW1' not in df['region'].values
        assert 'NSW' in df['region'].values
        assert 'TAS' in df['region'].values

    def test_parse_price_csv_negative_price(self, client):
        """Test handling of negative prices"""
        df = client._parse_price_csv(SAMPLE_TRADING_PRICE_CSV, 'TRADING')

        assert df is not None
        # SA has negative price in sample
        sa_price = df.loc[df['region'] == 'SA', 'price'].values[0]
        assert sa_price == -50.25

    def test_parse_price_csv_no_records(self, client):
        """Test CSV with no price records"""
        df = client._parse_price_csv(SAMPLE_PRICE_NO_RECORDS, 'DISPATCH')
        assert df is None

    def test_parse_price_csv_datetime_conversion(self, client):
        """Test datetime conversion"""
        df = client._parse_price_csv(SAMPLE_DISPATCH_PRICE_CSV, 'DISPATCH')

        assert df is not None
        assert str(df['settlementdate'].dtype).startswith('datetime')

    def test_parse_price_csv_demand_from_regionsum(self, client):
        """Test that demand is extracted from REGIONSUM records"""
        df = client._parse_price_csv(SAMPLE_DISPATCH_PRICE_CSV, 'DISPATCH')

        assert df is not None
        # totaldemand column should exist
        assert 'totaldemand' in df.columns
        # Demand should be populated (values exist)
        nsw_row = df.loc[df['region'] == 'NSW']
        assert len(nsw_row) > 0


class TestParseInterconnectorCsv:
    """Tests for _parse_interconnector_csv method"""

    @pytest.fixture
    def client(self):
        return NEMPriceClient()

    def test_parse_interconnector_csv_valid(self, client):
        """Test parsing valid interconnector data"""
        df = client._parse_interconnector_csv(SAMPLE_INTERCONNECTOR_CSV)

        assert df is not None
        assert len(df) == 5
        assert 'NSW1-QLD1' in df['interconnector'].values

    def test_parse_interconnector_csv_columns(self, client):
        """Test all expected columns are present"""
        df = client._parse_interconnector_csv(SAMPLE_INTERCONNECTOR_CSV)

        expected_cols = [
            'settlementdate', 'interconnector',
            'meteredmwflow', 'mwflow', 'mwloss', 'marginalvalue'
        ]
        for col in expected_cols:
            assert col in df.columns

    def test_parse_interconnector_csv_negative_flow(self, client):
        """Test handling of negative flows (reverse direction)"""
        df = client._parse_interconnector_csv(SAMPLE_INTERCONNECTOR_NEGATIVE)

        assert df is not None
        assert df.loc[0, 'meteredmwflow'] < 0

    def test_parse_interconnector_csv_no_records(self, client):
        """Test CSV with no interconnector records"""
        df = client._parse_interconnector_csv(SAMPLE_INTERCONNECTOR_NO_RECORDS)
        assert df is None

    def test_parse_interconnector_csv_malformed(self, client):
        """Test malformed lines are skipped"""
        df = client._parse_interconnector_csv(SAMPLE_INTERCONNECTOR_MALFORMED)

        assert df is not None
        # Should have 2 valid records
        assert len(df) == 2


class TestParseZipFiles:
    """Tests for ZIP parsing methods"""

    @pytest.fixture
    def client(self):
        return NEMPriceClient()

    def test_parse_dispatch_price_zip(self, client):
        """Test dispatch price ZIP parsing"""
        zip_content = create_price_zip(SAMPLE_DISPATCH_PRICE_CSV, 'DISPATCH')
        df = client._parse_dispatch_price_zip(zip_content)

        assert df is not None
        assert len(df) == 5

    def test_parse_trading_price_zip(self, client):
        """Test trading price ZIP parsing"""
        zip_content = create_price_zip(SAMPLE_TRADING_PRICE_CSV, 'TRADING')
        df = client._parse_trading_price_zip(zip_content)

        assert df is not None

    def test_parse_public_prices_zip(self, client):
        """Test public prices ZIP parsing"""
        zip_content = create_price_zip(SAMPLE_PUBLIC_PRICE_CSV, 'PUBLIC')
        df = client._parse_public_prices_zip(zip_content)

        assert df is not None

    def test_parse_irsr_zip(self, client):
        """Test IRSR (interconnector) ZIP parsing"""
        zip_content = create_interconnector_zip()
        df = client._parse_irsr_zip(zip_content)

        assert df is not None

    def test_parse_zip_invalid_content(self, client):
        """Test invalid ZIP content"""
        df = client._parse_dispatch_price_zip(b'not a zip')
        assert df is None

    def test_parse_zip_no_csv(self, client):
        """Test ZIP with no CSV files"""
        import io
        import zipfile

        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w') as zf:
            zf.writestr('readme.txt', 'no csv here')

        df = client._parse_dispatch_price_zip(buffer.getvalue())
        assert df is None


class TestAsyncMethods:
    """Tests for async HTTP methods"""

    @pytest.fixture
    def client(self):
        return NEMPriceClient()

    @pytest.mark.asyncio
    async def test_get_current_dispatch_prices_success(self, client, httpx_mock):
        """Test successful dispatch price fetch"""
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/",
            html=SAMPLE_DISPATCH_PRICE_DIR
        )
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202501151030_0000000123456790.zip",
            content=create_price_zip(SAMPLE_DISPATCH_PRICE_CSV, 'DISPATCH')
        )

        df = await client.get_current_dispatch_prices()
        assert df is not None

    @pytest.mark.asyncio
    async def test_get_trading_prices_success(self, client, httpx_mock):
        """Test successful trading price fetch"""
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/TradingIS_Reports/",
            html=SAMPLE_TRADING_DIR
        )
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/TradingIS_Reports/PUBLIC_TRADINGIS_202501151030_0000000123456790.zip",
            content=create_price_zip(SAMPLE_TRADING_PRICE_CSV, 'TRADING')
        )

        df = await client.get_trading_prices()
        assert df is not None

    @pytest.mark.asyncio
    async def test_get_interconnector_flows_success(self, client, httpx_mock):
        """Test successful interconnector flow fetch"""
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/Dispatch_IRSR/",
            html=SAMPLE_IRSR_DIR
        )
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/Dispatch_IRSR/PUBLIC_IRSR_202501151030_0000000123456789.zip",
            content=create_interconnector_zip()
        )

        df = await client.get_interconnector_flows()
        assert df is not None

    @pytest.mark.asyncio
    async def test_get_current_dispatch_prices_no_file(self, client, httpx_mock):
        """Test when no file found"""
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/",
            html="<html>empty</html>"
        )

        df = await client.get_current_dispatch_prices()
        assert df is None

    @pytest.mark.asyncio
    async def test_get_daily_prices_success(self, client, httpx_mock):
        """Test successful daily price fetch with market day boundary handling.

        get_daily_prices now fetches TWO files to get complete calendar day data:
        - Previous day's file (for 00:00-04:00 of target date)
        - Target day's file (for 04:05-23:55 of target date)
        """
        from datetime import datetime

        test_date = datetime(2025, 1, 15)

        # Mock directory listing with both files (prev day and target day)
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/Public_Prices/",
            html='<a href="PUBLIC_PRICES_202501140000_00000000000001.zip">file1</a>'
                 '<a href="PUBLIC_PRICES_202501150000_00000000000001.zip">file2</a>'
        )
        # Mock both file downloads
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/Public_Prices/PUBLIC_PRICES_202501140000_00000000000001.zip",
            content=create_price_zip(SAMPLE_PUBLIC_PRICE_CSV, 'PUBLIC')
        )
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/Public_Prices/PUBLIC_PRICES_202501150000_00000000000001.zip",
            content=create_price_zip(SAMPLE_PUBLIC_PRICE_CSV, 'PUBLIC')
        )

        df = await client.get_daily_prices(test_date)
        assert df is not None

    @pytest.mark.asyncio
    async def test_network_error_handling(self, client, httpx_mock):
        """Test network error handling"""
        import httpx
        httpx_mock.add_exception(httpx.ConnectError("Connection refused"))

        df = await client.get_current_dispatch_prices()
        assert df is None


class TestGetAllCurrentDispatchPrices:
    """Tests for get_all_current_dispatch_prices method (backfill from Current directory)"""

    @pytest.fixture
    def client(self):
        return NEMPriceClient()

    @pytest.mark.asyncio
    async def test_returns_dataframe_with_expected_columns(self, client, httpx_mock):
        """Should return DataFrame with settlementdate, region, price, totaldemand, price_type"""
        from tests.fixtures.sample_price_csv import (
            SAMPLE_DISPATCH_PRICE_DIR_MULTI,
            create_dispatch_price_csv_for_time,
            create_price_zip,
        )

        # Mock directory listing
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/",
            html=SAMPLE_DISPATCH_PRICE_DIR_MULTI
        )

        # Mock each file download (5 files in the directory)
        timestamps = [
            ("2025/01/15 04:00:00", "202501150400_0000000123456780"),
            ("2025/01/15 04:05:00", "202501150405_0000000123456781"),
            ("2025/01/15 04:10:00", "202501150410_0000000123456782"),
            ("2025/01/15 04:15:00", "202501150415_0000000123456783"),
            ("2025/01/15 04:20:00", "202501150420_0000000123456784"),
        ]

        for ts, file_suffix in timestamps:
            csv_content = create_dispatch_price_csv_for_time(ts)
            httpx_mock.add_response(
                url=f"https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_{file_suffix}.zip",
                content=create_price_zip(csv_content, 'DISPATCH')
            )

        df = await client.get_all_current_dispatch_prices()

        assert df is not None
        assert 'settlementdate' in df.columns
        assert 'region' in df.columns
        assert 'price' in df.columns
        assert 'totaldemand' in df.columns
        assert 'price_type' in df.columns

    @pytest.mark.asyncio
    async def test_fetches_all_files_from_directory(self, client, httpx_mock):
        """Should fetch and parse all available ZIP files"""
        from tests.fixtures.sample_price_csv import (
            SAMPLE_DISPATCH_PRICE_DIR_MULTI,
            create_dispatch_price_csv_for_time,
            create_price_zip,
        )

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/",
            html=SAMPLE_DISPATCH_PRICE_DIR_MULTI
        )

        timestamps = [
            ("2025/01/15 04:00:00", "202501150400_0000000123456780"),
            ("2025/01/15 04:05:00", "202501150405_0000000123456781"),
            ("2025/01/15 04:10:00", "202501150410_0000000123456782"),
            ("2025/01/15 04:15:00", "202501150415_0000000123456783"),
            ("2025/01/15 04:20:00", "202501150420_0000000123456784"),
        ]

        for ts, file_suffix in timestamps:
            csv_content = create_dispatch_price_csv_for_time(ts)
            httpx_mock.add_response(
                url=f"https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_{file_suffix}.zip",
                content=create_price_zip(csv_content, 'DISPATCH')
            )

        df = await client.get_all_current_dispatch_prices()

        assert df is not None
        # Should have data for 5 timestamps * 5 regions = 25 records
        assert len(df) == 25
        # Should have 5 unique timestamps
        assert df['settlementdate'].nunique() == 5

    @pytest.mark.asyncio
    async def test_deduplicates_by_settlementdate_and_region(self, client, httpx_mock):
        """Should not have duplicate (settlementdate, region) combinations"""
        from tests.fixtures.sample_price_csv import (
            SAMPLE_DISPATCH_PRICE_DIR_MULTI,
            create_dispatch_price_csv_for_time,
            create_price_zip,
        )

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/",
            html=SAMPLE_DISPATCH_PRICE_DIR_MULTI
        )

        timestamps = [
            ("2025/01/15 04:00:00", "202501150400_0000000123456780"),
            ("2025/01/15 04:05:00", "202501150405_0000000123456781"),
            ("2025/01/15 04:10:00", "202501150410_0000000123456782"),
            ("2025/01/15 04:15:00", "202501150415_0000000123456783"),
            ("2025/01/15 04:20:00", "202501150420_0000000123456784"),
        ]

        for ts, file_suffix in timestamps:
            csv_content = create_dispatch_price_csv_for_time(ts)
            httpx_mock.add_response(
                url=f"https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_{file_suffix}.zip",
                content=create_price_zip(csv_content, 'DISPATCH')
            )

        df = await client.get_all_current_dispatch_prices()

        assert df is not None
        # Check no duplicates
        duplicates = df.duplicated(subset=['settlementdate', 'region'])
        assert duplicates.sum() == 0

    @pytest.mark.asyncio
    async def test_handles_empty_directory(self, client, httpx_mock):
        """Should return None if no files found"""
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/",
            html="<html><body>No files</body></html>"
        )

        df = await client.get_all_current_dispatch_prices()
        assert df is None

    @pytest.mark.asyncio
    async def test_handles_network_error(self, client, httpx_mock):
        """Should return None on network error"""
        import httpx
        httpx_mock.add_exception(httpx.ConnectError("Connection refused"))

        df = await client.get_all_current_dispatch_prices()
        assert df is None

    @pytest.mark.asyncio
    async def test_continues_on_individual_file_error(self, client, httpx_mock):
        """Should continue processing even if one file fails to download"""
        from tests.fixtures.sample_price_csv import (
            create_dispatch_price_csv_for_time,
            create_price_zip,
        )

        # Directory with 3 files
        dir_html = '''<html><body>
<a href="PUBLIC_DISPATCHIS_202501150400_0000000123456780.zip">file1</a>
<a href="PUBLIC_DISPATCHIS_202501150405_0000000123456781.zip">file2</a>
<a href="PUBLIC_DISPATCHIS_202501150410_0000000123456782.zip">file3</a>
</body></html>'''

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/",
            html=dir_html
        )

        # First file succeeds
        csv1 = create_dispatch_price_csv_for_time("2025/01/15 04:00:00")
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202501150400_0000000123456780.zip",
            content=create_price_zip(csv1, 'DISPATCH')
        )

        # Second file fails (404)
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202501150405_0000000123456781.zip",
            status_code=404
        )

        # Third file succeeds
        csv3 = create_dispatch_price_csv_for_time("2025/01/15 04:10:00")
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202501150410_0000000123456782.zip",
            content=create_price_zip(csv3, 'DISPATCH')
        )

        df = await client.get_all_current_dispatch_prices()

        # Should still return data from 2 successful files
        assert df is not None
        assert len(df) == 10  # 2 files * 5 regions

    @pytest.mark.asyncio
    async def test_filters_files_by_since_parameter(self, client, httpx_mock):
        """Should only fetch files with timestamps after 'since' parameter"""
        from datetime import datetime
        from tests.fixtures.sample_price_csv import (
            create_dispatch_price_csv_for_time,
            create_price_zip,
        )

        # Directory with 5 files at different times
        dir_html = '''<html><body>
<a href="PUBLIC_DISPATCHIS_202501150400_0000000123456780.zip">file1</a>
<a href="PUBLIC_DISPATCHIS_202501150405_0000000123456781.zip">file2</a>
<a href="PUBLIC_DISPATCHIS_202501150410_0000000123456782.zip">file3</a>
<a href="PUBLIC_DISPATCHIS_202501150415_0000000123456783.zip">file4</a>
<a href="PUBLIC_DISPATCHIS_202501150420_0000000123456784.zip">file5</a>
</body></html>'''

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/",
            html=dir_html
        )

        # Only mock files that should be fetched (after 04:10)
        timestamps = [
            ("2025/01/15 04:15:00", "202501150415_0000000123456783"),
            ("2025/01/15 04:20:00", "202501150420_0000000123456784"),
        ]

        for ts, file_suffix in timestamps:
            csv_content = create_dispatch_price_csv_for_time(ts)
            httpx_mock.add_response(
                url=f"https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_{file_suffix}.zip",
                content=create_price_zip(csv_content, 'DISPATCH')
            )

        # Request with since=04:10, should only get files at 04:15 and 04:20
        since = datetime(2025, 1, 15, 4, 10)
        df = await client.get_all_current_dispatch_prices(since=since)

        assert df is not None
        # Should have 2 timestamps * 5 regions = 10 records
        assert len(df) == 10
        # All timestamps should be after since
        assert all(df['settlementdate'] > since)

    @pytest.mark.asyncio
    async def test_since_returns_none_when_no_newer_files(self, client, httpx_mock):
        """Should return None when no files are newer than 'since'"""
        from datetime import datetime

        # Directory with files only from earlier times
        dir_html = '''<html><body>
<a href="PUBLIC_DISPATCHIS_202501150400_0000000123456780.zip">file1</a>
<a href="PUBLIC_DISPATCHIS_202501150405_0000000123456781.zip">file2</a>
</body></html>'''

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/",
            html=dir_html
        )

        # Request with since in the future
        since = datetime(2025, 1, 15, 10, 0)  # 10:00, after all files
        df = await client.get_all_current_dispatch_prices(since=since)

        assert df is None
