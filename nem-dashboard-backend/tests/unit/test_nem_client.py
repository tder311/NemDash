"""
Unit tests for NEMDispatchClient
"""
import pytest
import zipfile
import io

from app.nem_client import NEMDispatchClient
from tests.fixtures.sample_dispatch_csv import (
    SAMPLE_DISPATCH_CSV,
    SAMPLE_DISPATCH_EMPTY_VALUES,
    SAMPLE_DISPATCH_INVALID_FLOAT,
    SAMPLE_DISPATCH_NO_RECORDS,
    SAMPLE_DISPATCH_MALFORMED,
    SAMPLE_DISPATCH_NEGATIVE,
    SAMPLE_DIRECTORY_HTML,
    SAMPLE_DIRECTORY_HTML_EMPTY,
    SAMPLE_DIRECTORY_HTML_SINGLE,
    create_sample_dispatch_zip,
    create_empty_zip,
    create_nested_archive_zip,
)


class TestNEMDispatchClientInit:
    """Tests for NEMDispatchClient initialization"""

    def test_init_default_url(self):
        """Test default base URL"""
        client = NEMDispatchClient()
        assert client.base_url == "https://www.nemweb.com.au"

    def test_init_custom_url(self):
        """Test custom base URL"""
        client = NEMDispatchClient("https://custom.url.com")
        assert client.base_url == "https://custom.url.com"

    def test_init_strips_trailing_slash(self):
        """Test that trailing slash is stripped from base URL"""
        client = NEMDispatchClient("https://example.com/")
        assert client.base_url == "https://example.com"


class TestSafeFloat:
    """Tests for _safe_float utility method"""

    @pytest.fixture
    def client(self):
        return NEMDispatchClient()

    def test_safe_float_valid_number(self, client):
        """Test valid float string conversion"""
        assert client._safe_float("123.45") == 123.45

    def test_safe_float_integer_string(self, client):
        """Test integer string conversion"""
        assert client._safe_float("100") == 100.0

    def test_safe_float_empty_string(self, client):
        """Test empty string returns 0.0"""
        assert client._safe_float("") == 0.0

    def test_safe_float_whitespace(self, client):
        """Test whitespace-only string returns 0.0"""
        assert client._safe_float("   ") == 0.0

    def test_safe_float_invalid_string(self, client):
        """Test non-numeric string returns 0.0"""
        assert client._safe_float("invalid") == 0.0

    def test_safe_float_none(self, client):
        """Test None input returns 0.0"""
        assert client._safe_float(None) == 0.0

    def test_safe_float_negative(self, client):
        """Test negative numbers are preserved"""
        assert client._safe_float("-50.25") == -50.25

    def test_safe_float_scientific_notation(self, client):
        """Test scientific notation"""
        assert client._safe_float("1.5e2") == 150.0

    def test_safe_float_zero(self, client):
        """Test zero value"""
        assert client._safe_float("0") == 0.0
        assert client._safe_float("0.0") == 0.0


class TestParseLatestDispatchFile:
    """Tests for _parse_latest_dispatch_file method"""

    @pytest.fixture
    def client(self):
        return NEMDispatchClient()

    def test_parse_latest_dispatch_file_finds_latest(self, client):
        """Test that the latest file is selected from HTML listing"""
        result = client._parse_latest_dispatch_file(SAMPLE_DIRECTORY_HTML)
        # Should pick the file with highest timestamp (202501151030)
        assert "202501151030" in result

    def test_parse_latest_dispatch_file_no_match(self, client):
        """Test empty HTML returns None"""
        result = client._parse_latest_dispatch_file(SAMPLE_DIRECTORY_HTML_EMPTY)
        assert result is None

    def test_parse_latest_dispatch_file_single_file(self, client):
        """Test single file is returned"""
        result = client._parse_latest_dispatch_file(SAMPLE_DIRECTORY_HTML_SINGLE)
        assert result is not None
        assert "DISPATCHSCADA" in result

    def test_parse_latest_dispatch_file_wrong_pattern(self, client):
        """Test HTML with non-matching files returns None"""
        html = '<a href="OTHER_FILE_202501151030.zip">other</a>'
        result = client._parse_latest_dispatch_file(html)
        assert result is None


class TestParseDispatchCsv:
    """Tests for _parse_dispatch_csv method"""

    @pytest.fixture
    def client(self):
        return NEMDispatchClient()

    def test_parse_dispatch_csv_valid_data(self, client):
        """Test parsing valid NEM dispatch CSV format"""
        df = client._parse_dispatch_csv(SAMPLE_DISPATCH_CSV)

        assert df is not None
        assert len(df) == 5
        assert "BAYSW1" in df['duid'].values
        assert df.loc[df['duid'] == 'BAYSW1', 'scadavalue'].values[0] == 350.5

    def test_parse_dispatch_csv_columns(self, client):
        """Test that all expected columns are present"""
        df = client._parse_dispatch_csv(SAMPLE_DISPATCH_CSV)

        expected_columns = [
            'settlementdate', 'duid', 'scadavalue',
            'uigf', 'totalcleared', 'ramprate',
            'availability', 'raise1sec', 'lower1sec'
        ]
        for col in expected_columns:
            assert col in df.columns

    def test_parse_dispatch_csv_datetime_conversion(self, client):
        """Test that settlementdate is converted to datetime"""
        df = client._parse_dispatch_csv(SAMPLE_DISPATCH_CSV)

        assert df is not None
        # Check datetime type
        assert str(df['settlementdate'].dtype).startswith('datetime')

    def test_parse_dispatch_csv_no_dispatch_records(self, client):
        """Test CSV with no DISPATCH,UNIT_SCADA records returns None"""
        df = client._parse_dispatch_csv(SAMPLE_DISPATCH_NO_RECORDS)
        assert df is None

    def test_parse_dispatch_csv_empty_values(self, client):
        """Test handling of empty SCADA values"""
        df = client._parse_dispatch_csv(SAMPLE_DISPATCH_EMPTY_VALUES)

        if df is not None:
            # Empty values should be converted to 0.0
            assert all(df['scadavalue'] == 0.0)

    def test_parse_dispatch_csv_invalid_float(self, client):
        """Test handling of invalid float values"""
        df = client._parse_dispatch_csv(SAMPLE_DISPATCH_INVALID_FLOAT)

        if df is not None:
            # Invalid float should be 0.0
            assert df.loc[0, 'scadavalue'] == 0.0

    def test_parse_dispatch_csv_malformed_lines(self, client):
        """Test that malformed lines are skipped"""
        df = client._parse_dispatch_csv(SAMPLE_DISPATCH_MALFORMED)

        assert df is not None
        # Should have 2 valid records (malformed line skipped)
        assert len(df) == 2
        assert "GOOD1" in df['duid'].values
        assert "GOOD2" in df['duid'].values

    def test_parse_dispatch_csv_negative_values(self, client):
        """Test handling of negative SCADA values (valid for batteries)"""
        df = client._parse_dispatch_csv(SAMPLE_DISPATCH_NEGATIVE)

        assert df is not None
        assert df.loc[0, 'scadavalue'] == -50.5

    def test_parse_dispatch_csv_empty_content(self, client):
        """Test empty content returns None"""
        df = client._parse_dispatch_csv(b"")
        assert df is None

    def test_parse_dispatch_csv_invalid_utf8(self, client):
        """Test bytes that fail UTF-8 decoding are handled and return None"""
        df = client._parse_dispatch_csv(b'\xff\xfe\x00invalid')
        assert df is None


class TestParseDispatchZip:
    """Tests for _parse_dispatch_zip method"""

    @pytest.fixture
    def client(self):
        return NEMDispatchClient()

    def test_parse_dispatch_zip_valid(self, client):
        """Test parsing valid ZIP file with CSV inside"""
        zip_content = create_sample_dispatch_zip()
        df = client._parse_dispatch_zip(zip_content)

        assert df is not None
        assert len(df) == 5

    def test_parse_dispatch_zip_empty_zip(self, client):
        """Test handling of ZIP with no CSV files"""
        zip_content = create_empty_zip()
        df = client._parse_dispatch_zip(zip_content)
        assert df is None

    def test_parse_dispatch_zip_invalid_content(self, client):
        """Test handling of non-ZIP content"""
        df = client._parse_dispatch_zip(b'not a zip file')
        assert df is None

    def test_parse_dispatch_zip_corrupted(self, client):
        """Test handling of corrupted ZIP"""
        # Create a partial/corrupted ZIP
        df = client._parse_dispatch_zip(b'PK\x03\x04corrupted')
        assert df is None

    def test_parse_dispatch_zip_multiple_csvs(self, client):
        """Test ZIP with multiple CSV files (uses first one)"""
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w') as zf:
            zf.writestr('first.CSV', SAMPLE_DISPATCH_CSV)
            zf.writestr('second.CSV', b'other content')

        df = client._parse_dispatch_zip(buffer.getvalue())
        # Should parse the first CSV successfully
        assert df is not None


class TestAsyncMethods:
    """Tests for async HTTP methods (with mocking)"""

    @pytest.fixture
    def client(self):
        return NEMDispatchClient()

    @pytest.mark.asyncio
    async def test_get_current_dispatch_data_success(self, client, httpx_mock):
        """Test successful fetch of current dispatch data"""
        # Mock directory listing
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/",
            html=SAMPLE_DIRECTORY_HTML
        )

        # Mock ZIP file download
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_202501151030_0000000123456791.zip",
            content=create_sample_dispatch_zip()
        )

        df = await client.get_current_dispatch_data()
        assert df is not None
        assert len(df) == 5

    @pytest.mark.asyncio
    async def test_get_current_dispatch_data_no_file(self, client, httpx_mock):
        """Test when no dispatch file is found in directory"""
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/",
            html="<html>empty</html>"
        )

        df = await client.get_current_dispatch_data()
        assert df is None

    @pytest.mark.asyncio
    async def test_get_current_dispatch_data_network_error(self, client, httpx_mock):
        """Test handling of network errors"""
        import httpx
        httpx_mock.add_exception(httpx.ConnectError("Connection refused"))

        df = await client.get_current_dispatch_data()
        assert df is None

    @pytest.mark.asyncio
    async def test_get_historical_dispatch_data_success(self, client, httpx_mock):
        """Test successful fetch of historical dispatch data from nested ZIP archive"""
        from datetime import datetime

        test_date = datetime(2025, 1, 15)

        # NEMWEB archives are nested ZIPs (outer ZIP contains inner ZIPs with CSVs)
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Archive/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_20250115.zip",
            content=create_nested_archive_zip()
        )

        df = await client.get_historical_dispatch_data(test_date)
        assert df is not None
        # Should have records from the nested archive (2 intervals * 5 records each, deduped)
        assert len(df) >= 5

    @pytest.mark.asyncio
    async def test_get_historical_dispatch_data_not_found(self, client, httpx_mock):
        """Test handling of 404 for historical data"""
        from datetime import datetime
        import httpx

        test_date = datetime(2025, 1, 15)

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Archive/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_20250115.zip",
            status_code=404
        )

        df = await client.get_historical_dispatch_data(test_date)
        assert df is None

    @pytest.mark.asyncio
    async def test_get_historical_dispatch_data_no_inner_zips(self, client, httpx_mock):
        """Test archive with no nested ZIP files returns None"""
        from datetime import datetime

        test_date = datetime(2025, 1, 15)

        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w') as zf:
            zf.writestr('readme.txt', 'no zips here')

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Archive/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_20250115.zip",
            content=buffer.getvalue()
        )

        df = await client.get_historical_dispatch_data(test_date)
        assert df is None

    @pytest.mark.asyncio
    async def test_get_historical_dispatch_data_skips_corrupted_inner_zip(self, client, httpx_mock):
        """Test that a corrupted inner ZIP is skipped but valid ones are still processed"""
        from datetime import datetime

        test_date = datetime(2025, 1, 15)

        outer_buffer = io.BytesIO()
        with zipfile.ZipFile(outer_buffer, 'w', zipfile.ZIP_DEFLATED) as outer_zf:
            # Corrupted inner "zip" - not actually a valid ZIP
            outer_zf.writestr('PUBLIC_DISPATCHSCADA_202501151030_0000000123456789.zip', b'not a zip')

            # Valid inner zip with CSV
            inner_buffer = io.BytesIO()
            with zipfile.ZipFile(inner_buffer, 'w', zipfile.ZIP_DEFLATED) as inner_zf:
                inner_zf.writestr('PUBLIC_DISPATCHSCADA_202501151035.CSV', SAMPLE_DISPATCH_CSV)
            outer_zf.writestr('PUBLIC_DISPATCHSCADA_202501151035_0000000123456790.zip', inner_buffer.getvalue())

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Archive/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_20250115.zip",
            content=outer_buffer.getvalue()
        )

        df = await client.get_historical_dispatch_data(test_date)
        assert df is not None
        assert len(df) == 5

    @pytest.mark.asyncio
    async def test_get_historical_dispatch_data_no_valid_records(self, client, httpx_mock):
        """Test archive whose inner ZIPs contain no usable dispatch data returns None"""
        from datetime import datetime

        test_date = datetime(2025, 1, 15)

        outer_buffer = io.BytesIO()
        with zipfile.ZipFile(outer_buffer, 'w', zipfile.ZIP_DEFLATED) as outer_zf:
            inner_buffer = io.BytesIO()
            with zipfile.ZipFile(inner_buffer, 'w', zipfile.ZIP_DEFLATED) as inner_zf:
                inner_zf.writestr('readme.txt', 'no csv in here')
            outer_zf.writestr('PUBLIC_DISPATCHSCADA_202501151030_0000000123456789.zip', inner_buffer.getvalue())

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/Reports/Archive/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_20250115.zip",
            content=outer_buffer.getvalue()
        )

        df = await client.get_historical_dispatch_data(test_date)
        assert df is None


class TestGetAllCurrentDispatchData:
    """Tests for get_all_current_dispatch_data method (backfill from Current directory)"""

    @pytest.fixture
    def client(self):
        return NEMDispatchClient()

    @pytest.mark.asyncio
    async def test_fetches_all_files_from_directory(self, client, httpx_mock):
        """Should fetch and parse all available ZIP files"""
        dir_html = '''<html><body>
<a href="PUBLIC_DISPATCHSCADA_202501151020_0000000123456789.zip">file1</a>
<a href="PUBLIC_DISPATCHSCADA_202501151025_0000000123456790.zip">file2</a>
</body></html>'''

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/",
            html=dir_html
        )

        for suffix in ["202501151020_0000000123456789", "202501151025_0000000123456790"]:
            httpx_mock.add_response(
                url=f"https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_{suffix}.zip",
                content=create_sample_dispatch_zip()
            )

        df = await client.get_all_current_dispatch_data(request_delay=0)

        assert df is not None
        assert len(df) == 5

    @pytest.mark.asyncio
    async def test_handles_empty_directory(self, client, httpx_mock):
        """Should return None if no files found in directory"""
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/",
            html="<html><body>No files</body></html>"
        )

        df = await client.get_all_current_dispatch_data(request_delay=0)
        assert df is None

    @pytest.mark.asyncio
    async def test_filters_files_by_since_parameter(self, client, httpx_mock):
        """Should only fetch files with timestamps after 'since' parameter"""
        from datetime import datetime

        dir_html = '''<html><body>
<a href="PUBLIC_DISPATCHSCADA_202501151020_0000000123456789.zip">file1</a>
<a href="PUBLIC_DISPATCHSCADA_202501151025_0000000123456790.zip">file2</a>
<a href="PUBLIC_DISPATCHSCADA_202501151030_0000000123456791.zip">file3</a>
</body></html>'''

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/",
            html=dir_html
        )

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_202501151030_0000000123456791.zip",
            content=create_sample_dispatch_zip()
        )

        since = datetime(2025, 1, 15, 10, 25)
        df = await client.get_all_current_dispatch_data(since=since, request_delay=0)

        assert df is not None
        assert len(df) == 5

    @pytest.mark.asyncio
    async def test_since_returns_none_when_no_newer_files(self, client, httpx_mock):
        """Should return None when no files are newer than 'since'"""
        from datetime import datetime

        dir_html = '''<html><body>
<a href="PUBLIC_DISPATCHSCADA_202501151020_0000000123456789.zip">file1</a>
</body></html>'''

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/",
            html=dir_html
        )

        since = datetime(2025, 1, 15, 12, 0)
        df = await client.get_all_current_dispatch_data(since=since, request_delay=0)
        assert df is None

    @pytest.mark.asyncio
    async def test_continues_on_individual_file_error(self, client, httpx_mock):
        """Should continue processing even if one file fails to download"""
        dir_html = '''<html><body>
<a href="PUBLIC_DISPATCHSCADA_202501151020_0000000123456789.zip">file1</a>
<a href="PUBLIC_DISPATCHSCADA_202501151025_0000000123456790.zip">file2</a>
</body></html>'''

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/",
            html=dir_html
        )

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_202501151020_0000000123456789.zip",
            status_code=404
        )
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_202501151025_0000000123456790.zip",
            content=create_sample_dispatch_zip()
        )

        df = await client.get_all_current_dispatch_data(request_delay=0)

        assert df is not None
        assert len(df) == 5

    @pytest.mark.asyncio
    async def test_network_error_returns_none(self, client, httpx_mock):
        """Should return None on network error"""
        import httpx
        httpx_mock.add_exception(httpx.ConnectError("Connection refused"))

        df = await client.get_all_current_dispatch_data(request_delay=0)
        assert df is None

    @pytest.mark.asyncio
    async def test_all_files_fail_returns_none(self, client, httpx_mock):
        """Should return None when every matched file fails to download"""
        dir_html = '''<html><body>
<a href="PUBLIC_DISPATCHSCADA_202501151020_0000000123456789.zip">file1</a>
<a href="PUBLIC_DISPATCHSCADA_202501151025_0000000123456790.zip">file2</a>
</body></html>'''

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/",
            html=dir_html
        )
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_202501151020_0000000123456789.zip",
            status_code=404
        )
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_202501151025_0000000123456790.zip",
            status_code=404
        )

        df = await client.get_all_current_dispatch_data(request_delay=0)
        assert df is None

    @pytest.mark.asyncio
    async def test_skips_files_with_invalid_timestamp(self, client, httpx_mock):
        """Should skip filenames whose timestamp segment fails to parse"""
        dir_html = '''<html><body>
<a href="PUBLIC_DISPATCHSCADA_202513991020_0000000123456789.zip">bad</a>
<a href="PUBLIC_DISPATCHSCADA_202501151025_0000000123456790.zip">good</a>
</body></html>'''

        httpx_mock.add_response(
            url="https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/",
            html=dir_html
        )
        httpx_mock.add_response(
            url="https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_202501151025_0000000123456790.zip",
            content=create_sample_dispatch_zip()
        )

        df = await client.get_all_current_dispatch_data(request_delay=0)
        assert df is not None
        assert len(df) == 5
