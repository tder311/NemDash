"""
Unit tests for NEMPriceClient
"""
import pytest

from app.nem_price_client import NEMPriceClient, REGION_MAPPING, _fetch_zip_with_retry
from tests.fixtures.sample_price_csv import (
    SAMPLE_DISPATCH_PRICE_CSV,
    SAMPLE_DISPATCH_PRICE_CSV_V5,
    SAMPLE_TRADING_PRICE_CSV,
    SAMPLE_PUBLIC_PRICE_CSV,
    SAMPLE_PRICE_NO_RECORDS,
    SAMPLE_DISPATCH_PRICE_DIR,
    SAMPLE_TRADING_DIR,
    create_price_zip,
    create_public_prices_archive_zip,
    create_trading_price_csv_for_time,
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


class TestFetchZipWithRetry:
    """Tests for the module-level _fetch_zip_with_retry helper"""

    @pytest.mark.asyncio
    async def test_succeeds_after_retryable_429(self, httpx_mock):
        import httpx

        httpx_mock.add_response(url="https://x.test/file.zip", status_code=429)
        httpx_mock.add_response(url="https://x.test/file.zip", content=b"zipbytes")

        async with httpx.AsyncClient() as client:
            content = await _fetch_zip_with_retry(client, "https://x.test/file.zip", "file.zip", initial_backoff=0.001)

        assert content == b"zipbytes"

    @pytest.mark.asyncio
    async def test_gives_up_after_max_attempts_403(self, httpx_mock):
        import httpx

        for _ in range(3):
            httpx_mock.add_response(url="https://x.test/file.zip", status_code=403)

        async with httpx.AsyncClient() as client:
            content = await _fetch_zip_with_retry(
                client, "https://x.test/file.zip", "file.zip", max_attempts=3, initial_backoff=0.001
            )

        assert content is None

    @pytest.mark.asyncio
    async def test_non_retryable_status_stops_immediately(self, httpx_mock):
        import httpx

        httpx_mock.add_response(url="https://x.test/file.zip", status_code=400)

        async with httpx.AsyncClient() as client:
            content = await _fetch_zip_with_retry(client, "https://x.test/file.zip", "file.zip")

        assert content is None

    @pytest.mark.asyncio
    async def test_generic_exception_returns_none(self, httpx_mock):
        import httpx

        httpx_mock.add_exception(httpx.ConnectError("refused"))

        async with httpx.AsyncClient() as client:
            content = await _fetch_zip_with_retry(client, "https://x.test/file.zip", "file.zip")

        assert content is None


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


# ============================================================================
# Additional Coverage Tests
# ============================================================================


class TestHTTPErrorHandling:
    """Tests for HTTP error handling in various methods."""

    @pytest.fixture
    def client(self):
        return NEMPriceClient()

    @pytest.mark.asyncio
    async def test_get_current_dispatch_prices_500_error(self, client, httpx_mock):
        """Test handling of 500 server error for dispatch prices."""
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/",
            status_code=500
        )
        result = await client.get_current_dispatch_prices()
        assert result is None

    @pytest.mark.asyncio
    async def test_get_trading_prices_500_error(self, client, httpx_mock):
        """Test handling of 500 server error for trading prices."""
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/TradingIS_Reports/",
            status_code=500
        )
        result = await client.get_trading_prices()
        assert result is None

    @pytest.mark.asyncio
    async def test_get_trading_prices_no_file_found(self, client, httpx_mock):
        """Test when no trading file found in directory."""
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/TradingIS_Reports/",
            html="<html>empty directory</html>"
        )
        result = await client.get_trading_prices()
        assert result is None


class TestMonthlyArchivePrices:
    """Tests for get_monthly_archive_prices method."""

    @pytest.fixture
    def client(self):
        return NEMPriceClient()

    @pytest.mark.asyncio
    async def test_get_monthly_archive_prices_404(self, client, httpx_mock):
        """Test 404 handling for monthly archive."""
        # URL format: PUBLIC_PRICES_{year}{month:02d}01.zip
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Archive/Public_Prices/PUBLIC_PRICES_20250101.zip",
            status_code=404
        )
        result = await client.get_monthly_archive_prices(2025, 1)
        assert result is None

    @pytest.mark.asyncio
    async def test_get_monthly_archive_prices_500(self, client, httpx_mock):
        """Test 500 error handling for monthly archive."""
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Archive/Public_Prices/PUBLIC_PRICES_20250101.zip",
            status_code=500
        )
        result = await client.get_monthly_archive_prices(2025, 1)
        assert result is None


class TestParseArchiveMonthlyZip:
    """Tests for _parse_archive_monthly_zip method."""

    @pytest.fixture
    def client(self):
        return NEMPriceClient()

    def test_parse_archive_with_invalid_zip(self, client):
        """Test handling of invalid ZIP content."""
        from datetime import datetime
        result = client._parse_archive_monthly_zip(
            b'not a valid zip file',
            datetime(2025, 1, 15).date(),
            datetime(2025, 1, 14).date()
        )
        assert result == []

    def test_parse_archive_with_no_matching_files(self, client):
        """Test when archive has no matching daily files."""
        import io
        import zipfile
        from datetime import datetime

        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w') as zf:
            zf.writestr('OTHER_FILE_20250101.zip', b'dummy content')

        result = client._parse_archive_monthly_zip(
            buffer.getvalue(),
            datetime(2025, 1, 15).date(),
            datetime(2025, 1, 14).date()
        )
        assert result == []


class TestFilterToTargetDate:
    """Tests for _filter_to_target_date method."""

    @pytest.fixture
    def client(self):
        return NEMPriceClient()

    def test_filter_empty_list_raises_value_error(self, client):
        """Test that empty dataframes list raises ValueError."""
        from datetime import datetime
        import pandas as pd
        # pd.concat([]) raises ValueError: No objects to concatenate
        with pytest.raises(ValueError, match="No objects to concatenate"):
            client._filter_to_target_date([], datetime(2025, 1, 15).date())

    def test_filter_removes_duplicates_keeps_last(self, client):
        """Test that duplicate (timestamp, region) entries are deduplicated, keeping last."""
        import pandas as pd
        from datetime import datetime

        df1 = pd.DataFrame([{
            'settlementdate': pd.Timestamp('2025-01-15 10:30:00'),
            'region': 'NSW',
            'price': 85.50,
            'totaldemand': 7500.0,
            'price_type': 'PUBLIC'
        }])

        df2 = pd.DataFrame([{
            'settlementdate': pd.Timestamp('2025-01-15 10:30:00'),
            'region': 'NSW',
            'price': 90.00,  # Different price - should be kept as it's last
            'totaldemand': 7600.0,
            'price_type': 'PUBLIC'
        }])

        result = client._filter_to_target_date([df1, df2], datetime(2025, 1, 15).date())

        assert result is not None
        assert len(result) == 1  # Deduplicated
        # Should keep the last occurrence (90.00)
        assert result.iloc[0]['price'] == 90.00

    def test_filter_with_dataframes_containing_no_matching_dates(self, client):
        """Test filtering when no records match the target date."""
        import pandas as pd
        from datetime import datetime

        df1 = pd.DataFrame([{
            'settlementdate': pd.Timestamp('2025-01-14 10:30:00'),  # Wrong date
            'region': 'NSW',
            'price': 85.50,
            'totaldemand': 7500.0,
            'price_type': 'PUBLIC'
        }])

        result = client._filter_to_target_date([df1], datetime(2025, 1, 15).date())

        # Should return empty or None when no data matches target date
        assert result is None or len(result) == 0


class TestPriceCsvEdgeCases:
    """Tests for edge cases in _parse_price_csv."""

    @pytest.fixture
    def client(self):
        return NEMPriceClient()

    def test_parse_dispatch_with_malformed_line(self, client):
        """Test handling of malformed dispatch price line."""
        # A line with too few fields should be skipped
        csv_content = b'''C,NEMP.WORLD,,DISPATCH,PRICE,1
I,DISPATCH,PRICE,3,SETTLEMENTDATE,RUNNO,REGIONID
D,DISPATCH,PRICE,3,"2025/01/15 10:30:00"
D,DISPATCH,PRICE,3,"2025/01/15 10:30:00",1,NSW1,0,85.50,0,0,0
C,END OF REPORT,,,
'''
        result = client._parse_price_csv(csv_content, 'DISPATCH')
        # May return data for the valid line or None/empty depending on implementation
        # The important thing is it doesn't crash
        assert result is None or isinstance(result, type(None)) or hasattr(result, 'empty')

    def test_parse_empty_csv_content(self, client):
        """Test parsing empty CSV content."""
        result = client._parse_price_csv(b'', 'DISPATCH')
        assert result is None

    def test_parse_csv_with_only_header(self, client):
        """Test parsing CSV with only header, no data."""
        csv_content = b'''C,NEMP.WORLD,,DISPATCH,PRICE,1
I,DISPATCH,PRICE,3,SETTLEMENTDATE,RUNNO,REGIONID
C,END OF REPORT,,,
'''
        result = client._parse_price_csv(csv_content, 'DISPATCH')
        assert result is None


class TestGetAllCurrentTradingPrices:
    """Tests for get_all_current_trading_prices method."""

    @pytest.fixture
    def client(self):
        return NEMPriceClient()

    @pytest.mark.asyncio
    async def test_returns_none_when_directory_empty(self, client, httpx_mock):
        """Should return None if no trading files found."""
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/TradingIS_Reports/",
            html="<html><body>No files</body></html>"
        )

        df = await client.get_all_current_trading_prices()
        assert df is None

    @pytest.mark.asyncio
    async def test_handles_network_error(self, client, httpx_mock):
        """Should return None on network error."""
        import httpx
        httpx_mock.add_exception(httpx.ConnectError("Connection refused"))

        df = await client.get_all_current_trading_prices()
        assert df is None

    @pytest.mark.asyncio
    async def test_fetches_all_trading_files_from_directory(self, client, httpx_mock):
        """Should fetch and parse all available trading ZIP files"""
        dir_html = '''<html><body>
<a href="PUBLIC_TRADINGIS_202501150400_0000000123456780.zip">file1</a>
<a href="PUBLIC_TRADINGIS_202501150405_0000000123456781.zip">file2</a>
</body></html>'''

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/TradingIS_Reports/",
            html=dir_html
        )
        for suffix in ["202501150400_0000000123456780", "202501150405_0000000123456781"]:
            csv_content = create_trading_price_csv_for_time("2025/01/15 04:00:00")
            httpx_mock.add_response(
                url=f"https://www.nemweb.com.au/Reports/Current/TradingIS_Reports/PUBLIC_TRADINGIS_{suffix}.zip",
                content=create_price_zip(csv_content, 'TRADING')
            )

        df = await client.get_all_current_trading_prices(request_delay=0)

        assert df is not None
        assert 'settlementdate' in df.columns
        assert 'region' in df.columns
        # Should be deduped by (settlementdate, region): both files share the same timestamp
        assert df.duplicated(subset=['settlementdate', 'region']).sum() == 0

    @pytest.mark.asyncio
    async def test_filters_trading_files_by_since_parameter(self, client, httpx_mock):
        """Should only fetch trading files newer than 'since'"""
        from datetime import datetime

        dir_html = '''<html><body>
<a href="PUBLIC_TRADINGIS_202501150400_0000000123456780.zip">file1</a>
<a href="PUBLIC_TRADINGIS_202501150430_0000000123456781.zip">file2</a>
</body></html>'''

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/TradingIS_Reports/",
            html=dir_html
        )
        csv_content = create_trading_price_csv_for_time("2025/01/15 04:30:00")
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/TradingIS_Reports/PUBLIC_TRADINGIS_202501150430_0000000123456781.zip",
            content=create_price_zip(csv_content, 'TRADING')
        )

        since = datetime(2025, 1, 15, 4, 10)
        df = await client.get_all_current_trading_prices(since=since, request_delay=0)

        assert df is not None
        assert all(df['settlementdate'] > since)

    @pytest.mark.asyncio
    async def test_trading_since_returns_none_when_no_newer_files(self, client, httpx_mock):
        """Should return None when no trading files are newer than 'since'"""
        from datetime import datetime

        dir_html = '''<html><body>
<a href="PUBLIC_TRADINGIS_202501150400_0000000123456780.zip">file1</a>
</body></html>'''

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/TradingIS_Reports/",
            html=dir_html
        )

        since = datetime(2025, 1, 15, 12, 0)
        df = await client.get_all_current_trading_prices(since=since, request_delay=0)
        assert df is None

    @pytest.mark.asyncio
    async def test_trading_all_files_fail_returns_none(self, client, httpx_mock):
        """Should return None when every matched trading file fails to download"""
        dir_html = '''<html><body>
<a href="PUBLIC_TRADINGIS_202501150400_0000000123456780.zip">file1</a>
</body></html>'''

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/TradingIS_Reports/",
            html=dir_html
        )
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/TradingIS_Reports/PUBLIC_TRADINGIS_202501150400_0000000123456780.zip",
            status_code=404
        )

        df = await client.get_all_current_trading_prices(request_delay=0)
        assert df is None

    @pytest.mark.asyncio
    async def test_trading_skips_files_with_invalid_timestamp(self, client, httpx_mock):
        """Should skip filenames whose timestamp segment fails to parse"""
        dir_html = '''<html><body>
<a href="PUBLIC_TRADINGIS_202513991020_0000000123456789.zip">bad</a>
<a href="PUBLIC_TRADINGIS_202501150400_0000000123456781.zip">good</a>
</body></html>'''

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/TradingIS_Reports/",
            html=dir_html
        )
        csv_content = create_trading_price_csv_for_time("2025/01/15 04:00:00")
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/TradingIS_Reports/PUBLIC_TRADINGIS_202501150400_0000000123456781.zip",
            content=create_price_zip(csv_content, 'TRADING')
        )

        df = await client.get_all_current_trading_prices(request_delay=0)
        assert df is not None


class TestDispatchPricesEdgeCases:
    """Additional coverage for get_all_current_dispatch_prices branches."""

    @pytest.fixture
    def client(self):
        return NEMPriceClient()

    @pytest.mark.asyncio
    async def test_all_dispatch_files_fail_returns_none(self, client, httpx_mock):
        """Should return None when every matched dispatch file fails to download"""
        dir_html = '''<html><body>
<a href="PUBLIC_DISPATCHIS_202501150400_0000000123456780.zip">file1</a>
</body></html>'''

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/",
            html=dir_html
        )
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202501150400_0000000123456780.zip",
            status_code=404
        )

        df = await client.get_all_current_dispatch_prices(request_delay=0)
        assert df is None

    @pytest.mark.asyncio
    async def test_dispatch_skips_files_with_invalid_timestamp(self, client, httpx_mock):
        """Should skip filenames whose timestamp segment fails to parse"""
        from tests.fixtures.sample_price_csv import create_dispatch_price_csv_for_time

        dir_html = '''<html><body>
<a href="PUBLIC_DISPATCHIS_202513991020_0000000123456789.zip">bad</a>
<a href="PUBLIC_DISPATCHIS_202501150400_0000000123456781.zip">good</a>
</body></html>'''

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/",
            html=dir_html
        )
        csv_content = create_dispatch_price_csv_for_time("2025/01/15 04:00:00")
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202501150400_0000000123456781.zip",
            content=create_price_zip(csv_content, 'DISPATCH')
        )

        df = await client.get_all_current_dispatch_prices(request_delay=0)
        assert df is not None

    @pytest.mark.asyncio
    async def test_dispatch_dedupes_duplicate_filename_in_directory_html(self, client, httpx_mock):
        """A filename appearing twice (href + link text) should only be fetched once"""
        from tests.fixtures.sample_price_csv import create_dispatch_price_csv_for_time

        dir_html = ('<a href="PUBLIC_DISPATCHIS_202501150400_0000000123456780.zip">'
                    'PUBLIC_DISPATCHIS_202501150400_0000000123456780.zip</a>')

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/",
            html=dir_html
        )
        csv_content = create_dispatch_price_csv_for_time("2025/01/15 04:00:00")
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202501150400_0000000123456780.zip",
            content=create_price_zip(csv_content, 'DISPATCH')
        )

        df = await client.get_all_current_dispatch_prices(request_delay=0)
        assert df is not None
        assert len(df) == 5  # fetched once, not twice


class TestGetDailyPricesFallback:
    """Tests for get_daily_prices Current-then-Archive fallback behaviour."""

    @pytest.fixture
    def client(self):
        return NEMPriceClient()

    @pytest.mark.asyncio
    async def test_falls_back_to_archive_when_current_has_no_files(self, client, httpx_mock):
        """When Current has no matching files, should fall back to the monthly Archive."""
        from datetime import datetime

        test_date = datetime(2025, 1, 15)

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/Public_Prices/",
            html="<html><body>no matching files</body></html>"
        )
        archive_zip = create_public_prices_archive_zip([
            'PUBLIC_PRICES_202501140000_00000000000001.zip',
            'PUBLIC_PRICES_202501150000_00000000000001.zip',
        ])
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Archive/Public_Prices/PUBLIC_PRICES_20250101.zip",
            content=archive_zip
        )

        df = await client.get_daily_prices(test_date)
        assert df is not None
        assert not df.empty

    @pytest.mark.asyncio
    async def test_returns_none_when_current_and_archive_both_empty(self, client, httpx_mock):
        """Should return None when neither Current nor Archive have data."""
        from datetime import datetime

        test_date = datetime(2025, 1, 15)

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/Public_Prices/",
            html="<html><body>no matching files</body></html>"
        )
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Archive/Public_Prices/PUBLIC_PRICES_20250101.zip",
            status_code=404
        )

        df = await client.get_daily_prices(test_date)
        assert df is None

    @pytest.mark.asyncio
    async def test_current_error_falls_through_to_archive(self, client, httpx_mock):
        """A network error hitting Current should not prevent trying the Archive."""
        import httpx
        from datetime import datetime

        test_date = datetime(2025, 1, 15)

        httpx_mock.add_exception(
            httpx.ConnectError("refused"),
            url="https://www.nemweb.com.au/Reports/Current/Public_Prices/",
        )
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Archive/Public_Prices/PUBLIC_PRICES_20250101.zip",
            status_code=404
        )

        df = await client.get_daily_prices(test_date)
        assert df is None

    @pytest.mark.asyncio
    async def test_current_only_has_target_day_file_not_previous_day(self, client, httpx_mock):
        """When only the target day's file is listed, the previous-day fetch should be skipped, not fail."""
        from datetime import datetime

        test_date = datetime(2025, 1, 15)

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/Public_Prices/",
            html='<a href="PUBLIC_PRICES_202501150000_00000000000001.zip">file</a>'
        )
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Current/Public_Prices/PUBLIC_PRICES_202501150000_00000000000001.zip",
            content=create_price_zip(SAMPLE_PUBLIC_PRICE_CSV, 'PUBLIC')
        )

        df = await client.get_daily_prices(test_date)
        assert df is not None


class TestGetMonthlyArchivePricesSuccess:
    """Tests for the get_monthly_archive_prices success path and generic errors."""

    @pytest.fixture
    def client(self):
        return NEMPriceClient()

    @pytest.mark.asyncio
    async def test_returns_combined_dataframe(self, client, httpx_mock):
        archive_zip = create_public_prices_archive_zip([
            'PUBLIC_PRICES_202501010000_00000000000001.zip',
            'PUBLIC_PRICES_202501020000_00000000000001.zip',
        ])
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Archive/Public_Prices/PUBLIC_PRICES_20250101.zip",
            content=archive_zip
        )

        df = await client.get_monthly_archive_prices(2025, 1)
        assert df is not None
        assert df.duplicated(subset=['settlementdate', 'region']).sum() == 0

    @pytest.mark.asyncio
    async def test_generic_network_error_returns_none(self, client, httpx_mock):
        import httpx
        httpx_mock.add_exception(httpx.ConnectError("refused"))

        df = await client.get_monthly_archive_prices(2025, 1)
        assert df is None


class TestParseArchiveMonthlyZipSkipsAndParses:
    """Additional _parse_archive_monthly_zip coverage: non-zip entries + successful parse."""

    @pytest.fixture
    def client(self):
        return NEMPriceClient()

    def test_skips_non_zip_entries_and_parses_matching_zip(self, client):
        import io
        import zipfile
        from datetime import datetime

        outer_buffer = io.BytesIO()
        with zipfile.ZipFile(outer_buffer, 'w') as zf:
            zf.writestr('readme.txt', 'not a zip')
            zf.writestr(
                'PUBLIC_PRICES_202501150000_00000000000001.zip',
                create_price_zip(SAMPLE_PUBLIC_PRICE_CSV, 'PUBLIC'),
            )

        result = client._parse_archive_monthly_zip(
            outer_buffer.getvalue(),
            datetime(2025, 1, 15).date(),
            datetime(2025, 1, 14).date(),
        )
        assert len(result) == 1


class TestParseZipEdgeCases:
    """Coverage for the no-CSV and exception branches of the ZIP parsers."""

    @pytest.fixture
    def client(self):
        return NEMPriceClient()

    def test_parse_trading_zip_no_csv(self, client):
        import io
        import zipfile

        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w') as zf:
            zf.writestr('readme.txt', 'no csv here')

        df = client._parse_trading_price_zip(buffer.getvalue())
        assert df is None

    def test_parse_trading_zip_invalid_content(self, client):
        df = client._parse_trading_price_zip(b'not a zip')
        assert df is None

    def test_parse_public_prices_zip_no_csv(self, client):
        import io
        import zipfile

        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w') as zf:
            zf.writestr('readme.txt', 'no csv here')

        df = client._parse_public_prices_zip(buffer.getvalue())
        assert df is None

    def test_parse_public_prices_zip_invalid_content(self, client):
        df = client._parse_public_prices_zip(b'not a zip')
        assert df is None


class TestParsePriceCsvErrorBranches:
    """Coverage for malformed-line and decode-error branches of _parse_price_csv."""

    @pytest.fixture
    def client(self):
        return NEMPriceClient()

    def test_dispatch_line_with_non_numeric_version_is_skipped(self, client):
        """A DISPATCH,PRICE line with a corrupt version field should be skipped, not raise."""
        csv_content = b'''C,NEMP.WORLD,,DISPATCH,PRICE,1
I,DISPATCH,PRICE,3,SETTLEMENTDATE,RUNNO,REGIONID,DISPATCHINTERVAL,RRP,EEP,ROP,APCFLAG
D,DISPATCH,PRICE,BAD,"2025/01/15 10:30:00",1,NSW1,0,85.50,0,0,0
D,DISPATCH,PRICE,3,"2025/01/15 10:30:00",1,VIC1,0,72.30,0,0,0
C,END OF REPORT,,,
'''
        df = client._parse_price_csv(csv_content, 'DISPATCH')
        assert df is not None
        assert len(df) == 1
        assert df.iloc[0]['region'] == 'VIC'

    def test_invalid_utf8_content_returns_none(self, client):
        df = client._parse_price_csv(b'\xff\xfe\x00invalid', 'DISPATCH')
        assert df is None
