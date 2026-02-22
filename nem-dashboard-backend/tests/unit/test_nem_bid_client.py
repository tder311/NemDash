"""
Unit tests for NEMBidClient
"""
import pytest
import httpx
import zipfile
import io
from datetime import datetime, date

from app.nem_bid_client import NEMBidClient


# ============================================================================
# Sample CSV fixtures (mimicking real NEMWEB Bidmove_Complete CSV structure)
# ============================================================================

SAMPLE_BID_CSV = (
    'I,BID,BIDDAYOFFER_D,1,SETTLEMENTDATE,DUID,BIDTYPE,PRICEBAND1,PRICEBAND2,PRICEBAND3,'
    'PRICEBAND4,PRICEBAND5,PRICEBAND6,PRICEBAND7,PRICEBAND8,PRICEBAND9,PRICEBAND10,'
    'MINIMUMLOAD,T1,T2,T3,T4,OFFERDATE\n'
    'D,BID,BIDDAYOFFER_D,1,"2026/02/21 00:00:00","BAYSW1","ENERGY",-987.00,0.00,30.00,'
    '50.00,100.00,300.00,1000.00,5000.00,10000.00,15000.00,'
    '200.00,3.00,3.00,3.00,3.00,"2026/02/20 12:00:00"\n'
    'D,BID,BIDDAYOFFER_D,1,"2026/02/21 00:00:00","BAYSW1","FCAS",-100.00,0.00,10.00,'
    '20.00,50.00,100.00,200.00,500.00,1000.00,2000.00,'
    '0.00,1.00,1.00,1.00,1.00,"2026/02/20 12:00:00"\n'
    'D,BID,BIDDAYOFFER_D,1,"2026/02/21 00:00:00","LOYS1","ENERGY",-500.00,0.00,25.00,'
    '45.00,85.00,250.00,800.00,4000.00,9000.00,14000.00,'
    '150.00,2.00,2.00,2.00,2.00,"2026/02/20 11:00:00"\n'
    'I,BID,BIDPEROFFER_D,1,INTERVAL_DATETIME,SETTLEMENTDATE,DUID,BIDTYPE,BANDAVAIL1,BANDAVAIL2,BANDAVAIL3,'
    'BANDAVAIL4,BANDAVAIL5,BANDAVAIL6,BANDAVAIL7,BANDAVAIL8,BANDAVAIL9,BANDAVAIL10,'
    'MAXAVAIL,FIXEDLOAD,ROCUP,ROCDOWN,PASAAVAILABILITY,OFFERDATE\n'
    'D,BID,BIDPEROFFER_D,1,"2026/02/21 00:05:00","2026/02/21 00:05:00","BAYSW1","ENERGY",100.0,50.0,200.0,'
    '0.0,0.0,0.0,0.0,0.0,0.0,0.0,'
    '660.0,0.0,5.0,5.0,660.0,"2026/02/20 12:00:00"\n'
    'D,BID,BIDPEROFFER_D,1,"2026/02/21 00:10:00","2026/02/21 00:10:00","BAYSW1","ENERGY",120.0,40.0,180.0,'
    '10.0,0.0,0.0,0.0,0.0,0.0,0.0,'
    '660.0,0.0,5.0,5.0,660.0,"2026/02/20 12:00:00"\n'
    'D,BID,BIDPEROFFER_D,1,"2026/02/21 00:05:00","2026/02/21 00:05:00","BAYSW1","FCAS",10.0,5.0,0.0,'
    '0.0,0.0,0.0,0.0,0.0,0.0,0.0,'
    '20.0,0.0,1.0,1.0,20.0,"2026/02/20 12:00:00"\n'
)

SAMPLE_BID_CSV_NO_ENERGY = (
    'I,BID,BIDDAYOFFER_D,1,SETTLEMENTDATE,DUID,BIDTYPE,PRICEBAND1,PRICEBAND2\n'
    'D,BID,BIDDAYOFFER_D,1,"2026/02/21 00:00:00","UNIT1","FCAS",-100.00,0.00\n'
    'I,BID,BIDPEROFFER_D,1,INTERVAL_DATETIME,SETTLEMENTDATE,DUID,BIDTYPE,BANDAVAIL1,BANDAVAIL2\n'
    'D,BID,BIDPEROFFER_D,1,"2026/02/21 00:05:00","2026/02/21 00:05:00","UNIT1","FCAS",10.0,5.0\n'
)

SAMPLE_BID_CSV_MISSING_DUID = (
    'I,BID,BIDDAYOFFER_D,1,SETTLEMENTDATE,DUID,BIDTYPE,PRICEBAND1\n'
    'D,BID,BIDDAYOFFER_D,1,"2026/02/21 00:00:00","","ENERGY",-100.00\n'
)

SAMPLE_BID_CSV_MISSING_DATE = (
    'I,BID,BIDDAYOFFER_D,1,SETTLEMENTDATE,DUID,BIDTYPE,PRICEBAND1\n'
    'D,BID,BIDDAYOFFER_D,1,"","BAYSW1","ENERGY",-100.00\n'
)

# CSV using BIDS table group (some NEMWEB versions use this)
SAMPLE_BID_CSV_BIDS_GROUP = (
    'I,BIDS,BIDDAYOFFER_D,1,SETTLEMENTDATE,DUID,BIDTYPE,PRICEBAND1,OFFERDATE\n'
    'D,BIDS,BIDDAYOFFER_D,1,"2026/02/21 00:00:00","UNIT1","ENERGY",-500.00,"2026/02/20 12:00:00"\n'
    'I,BIDS,BIDPEROFFER_D,1,INTERVAL_DATETIME,SETTLEMENTDATE,DUID,BIDTYPE,BANDAVAIL1,MAXAVAIL,OFFERDATE\n'
    'D,BIDS,BIDPEROFFER_D,1,"2026/02/21 00:05:00","2026/02/21 00:05:00","UNIT1","ENERGY",100.0,500.0,"2026/02/20 12:00:00"\n'
)


# ============================================================================
# Helper functions
# ============================================================================

def create_bid_zip(csv_content):
    """Create a ZIP file containing a single CSV file."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        if isinstance(csv_content, str):
            csv_content = csv_content.encode('utf-8')
        zf.writestr("PUBLIC_BIDMOVE_COMPLETE_20260221.CSV", csv_content)
    return buf.getvalue()


def create_nested_bid_zip(csv_content, inner_date_str="20260221"):
    """Create a nested ZIP (archive format): outer ZIP containing inner ZIP containing CSV."""
    # Create inner ZIP with the CSV
    inner_buf = io.BytesIO()
    with zipfile.ZipFile(inner_buf, 'w', zipfile.ZIP_DEFLATED) as inner_zf:
        if isinstance(csv_content, str):
            csv_content = csv_content.encode('utf-8')
        inner_zf.writestr("PUBLIC_BIDMOVE_COMPLETE.CSV", csv_content)

    # Create outer ZIP containing inner ZIP
    outer_buf = io.BytesIO()
    with zipfile.ZipFile(outer_buf, 'w', zipfile.ZIP_DEFLATED) as outer_zf:
        outer_zf.writestr(
            f"PUBLIC_BIDMOVE_COMPLETE_{inner_date_str}_0000000504578183.zip",
            inner_buf.getvalue()
        )
    return outer_buf.getvalue()


def create_empty_zip():
    """Create a ZIP file with no CSV or ZIP files."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("readme.txt", "No data here")
    return buf.getvalue()


CURRENT_DIR_HTML = """
<html><body>
<a href="PUBLIC_BIDMOVE_COMPLETE_20260221_0000000504578183.zip">file1</a>
<a href="PUBLIC_BIDMOVE_COMPLETE_20260221_0000000504578184.zip">file2</a>
<a href="PUBLIC_BIDMOVE_COMPLETE_20260220_0000000504578180.zip">wrong date</a>
</body></html>
"""

ARCHIVE_DIR_HTML = """
<html><body>
<a href="PUBLIC_BIDMOVE_COMPLETE_20260201.zip">feb archive</a>
<a href="PUBLIC_BIDMOVE_COMPLETE_20260101.zip">jan archive</a>
</body></html>
"""


# ============================================================================
# Tests
# ============================================================================

class TestNEMBidClientInit:
    """Tests for NEMBidClient initialization"""

    def test_init_default_url(self):
        client = NEMBidClient()
        assert client.base_url == "https://www.nemweb.com.au"

    def test_init_custom_url(self):
        client = NEMBidClient("https://custom.url.com")
        assert client.base_url == "https://custom.url.com"

    def test_init_strips_trailing_slash(self):
        client = NEMBidClient("https://example.com/")
        assert client.base_url == "https://example.com"


class TestGetField:
    """Tests for _get_field and _get_float helper methods"""

    @pytest.fixture
    def client(self):
        return NEMBidClient()

    def test_get_field_valid(self, client):
        parts = ['"D"', '"BID"', '"BIDDAYOFFER_D"', '"1"', '"BAYSW1"']
        columns = {'DUID': 4}
        assert client._get_field(parts, columns, 'DUID') == 'BAYSW1'

    def test_get_field_missing_column(self, client):
        parts = ['"D"', '"BID"']
        columns = {'DUID': 4}
        assert client._get_field(parts, columns, 'DUID') is None

    def test_get_field_unknown_column(self, client):
        parts = ['"D"', '"BID"']
        columns = {}
        assert client._get_field(parts, columns, 'NONEXISTENT') is None

    def test_get_field_empty_value(self, client):
        parts = ['D', 'BID', '', '']
        columns = {'DUID': 2}
        assert client._get_field(parts, columns, 'DUID') is None

    def test_get_float_valid(self, client):
        parts = ['D', 'BID', '123.45']
        columns = {'PRICE': 2}
        assert client._get_float(parts, columns, 'PRICE') == 123.45

    def test_get_float_invalid(self, client):
        parts = ['D', 'BID', 'not_a_number']
        columns = {'PRICE': 2}
        assert client._get_float(parts, columns, 'PRICE') is None

    def test_get_float_missing(self, client):
        parts = ['D', 'BID']
        columns = {'PRICE': 5}
        assert client._get_float(parts, columns, 'PRICE') is None


class TestParseBidCsv:
    """Tests for _parse_bid_csv method"""

    @pytest.fixture
    def client(self):
        return NEMBidClient()

    def test_parse_energy_records(self, client):
        day_records, per_records = client._parse_bid_csv(
            SAMPLE_BID_CSV.encode('utf-8'), date(2026, 2, 21)
        )
        # Should have 2 ENERGY day offers (BAYSW1 + LOYS1), FCAS filtered out
        assert len(day_records) == 2
        duids = [r['duid'] for r in day_records]
        assert 'BAYSW1' in duids
        assert 'LOYS1' in duids

    def test_parse_filters_fcas_day(self, client):
        day_records, _ = client._parse_bid_csv(
            SAMPLE_BID_CSV.encode('utf-8'), date(2026, 2, 21)
        )
        bidtypes = [r.get('duid') for r in day_records]
        # FCAS BAYSW1 should be filtered; only 2 ENERGY records
        assert len(day_records) == 2

    def test_parse_per_offer_records(self, client):
        _, per_records = client._parse_bid_csv(
            SAMPLE_BID_CSV.encode('utf-8'), date(2026, 2, 21)
        )
        # Should have 2 ENERGY per-offer records, FCAS filtered out
        assert len(per_records) == 2
        assert all(r['duid'] == 'BAYSW1' for r in per_records)

    def test_parse_price_bands(self, client):
        day_records, _ = client._parse_bid_csv(
            SAMPLE_BID_CSV.encode('utf-8'), date(2026, 2, 21)
        )
        baysw = [r for r in day_records if r['duid'] == 'BAYSW1'][0]
        assert baysw['priceband1'] == -987.00
        assert baysw['priceband3'] == 30.00
        assert baysw['priceband10'] == 15000.00
        assert baysw['minimumload'] == 200.00

    def test_parse_band_availability(self, client):
        _, per_records = client._parse_bid_csv(
            SAMPLE_BID_CSV.encode('utf-8'), date(2026, 2, 21)
        )
        first = per_records[0]
        assert first['bandavail1'] == 100.0
        assert first['bandavail2'] == 50.0
        assert first['bandavail3'] == 200.0
        assert first['maxavail'] == 660.0
        assert first['rocup'] == 5.0
        assert first['rocdown'] == 5.0

    def test_parse_timestamps(self, client):
        day_records, per_records = client._parse_bid_csv(
            SAMPLE_BID_CSV.encode('utf-8'), date(2026, 2, 21)
        )
        assert day_records[0]['settlementdate'] is not None
        assert day_records[0]['offerdate'] is not None
        assert per_records[0]['settlementdate'] is not None

    def test_parse_no_energy_records(self, client):
        day_records, per_records = client._parse_bid_csv(
            SAMPLE_BID_CSV_NO_ENERGY.encode('utf-8'), date(2026, 2, 21)
        )
        assert len(day_records) == 0
        assert len(per_records) == 0

    def test_parse_missing_duid_skipped(self, client):
        day_records, _ = client._parse_bid_csv(
            SAMPLE_BID_CSV_MISSING_DUID.encode('utf-8'), date(2026, 2, 21)
        )
        assert len(day_records) == 0

    def test_parse_missing_date_skipped(self, client):
        day_records, _ = client._parse_bid_csv(
            SAMPLE_BID_CSV_MISSING_DATE.encode('utf-8'), date(2026, 2, 21)
        )
        assert len(day_records) == 0

    def test_parse_bids_table_group(self, client):
        """Test that BIDS table group is also accepted (some NEMWEB versions)."""
        day_records, per_records = client._parse_bid_csv(
            SAMPLE_BID_CSV_BIDS_GROUP.encode('utf-8'), date(2026, 2, 21)
        )
        assert len(day_records) == 1
        assert len(per_records) == 1

    def test_parse_empty_csv(self, client):
        day_records, per_records = client._parse_bid_csv(b'', date(2026, 2, 21))
        assert len(day_records) == 0
        assert len(per_records) == 0

    def test_parse_short_lines_skipped(self, client):
        csv = 'A,B\nC\n'
        day_records, per_records = client._parse_bid_csv(csv.encode('utf-8'), date(2026, 2, 21))
        assert len(day_records) == 0
        assert len(per_records) == 0

    def test_parse_ramp_rates(self, client):
        day_records, _ = client._parse_bid_csv(
            SAMPLE_BID_CSV.encode('utf-8'), date(2026, 2, 21)
        )
        baysw = [r for r in day_records if r['duid'] == 'BAYSW1'][0]
        assert baysw['t1'] == 3.00
        assert baysw['t2'] == 3.00


class TestParseBidmoveZip:
    """Tests for _parse_bidmove_zip method"""

    @pytest.fixture
    def client(self):
        return NEMBidClient()

    def test_parse_zip_with_csv(self, client):
        zip_content = create_bid_zip(SAMPLE_BID_CSV)
        result = client._parse_bidmove_zip(zip_content, date(2026, 2, 21))

        assert result is not None
        day_df, per_df = result
        assert len(day_df) == 2
        assert len(per_df) == 2
        assert 'duid' in day_df.columns
        assert 'priceband1' in day_df.columns
        assert 'bandavail1' in per_df.columns

    def test_parse_nested_zip(self, client):
        zip_content = create_nested_bid_zip(SAMPLE_BID_CSV, "20260221")
        result = client._parse_bidmove_zip(zip_content, date(2026, 2, 21))

        assert result is not None
        day_df, per_df = result
        assert len(day_df) == 2

    def test_parse_nested_zip_wrong_date(self, client):
        """Nested ZIP for wrong date should return None."""
        zip_content = create_nested_bid_zip(SAMPLE_BID_CSV, "20260220")
        result = client._parse_bidmove_zip(zip_content, date(2026, 2, 21))
        assert result is None

    def test_parse_empty_zip(self, client):
        zip_content = create_empty_zip()
        result = client._parse_bidmove_zip(zip_content, date(2026, 2, 21))
        assert result is None

    def test_parse_invalid_zip(self, client):
        result = client._parse_bidmove_zip(b'not a zip file', date(2026, 2, 21))
        assert result is None

    def test_parse_zip_no_energy_returns_none(self, client):
        zip_content = create_bid_zip(SAMPLE_BID_CSV_NO_ENERGY)
        result = client._parse_bidmove_zip(zip_content, date(2026, 2, 21))
        assert result is None


class TestFetchFromCurrent:
    """Tests for _fetch_from_current async method"""

    @pytest.fixture
    def client(self):
        return NEMBidClient("https://test.nemweb.com.au")

    @pytest.mark.asyncio
    async def test_fetch_current_success(self, client, httpx_mock):
        """Test successful fetch from Current directory."""
        zip_content = create_bid_zip(SAMPLE_BID_CSV)

        # First request: directory listing
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/Bidmove_Complete/",
            text=CURRENT_DIR_HTML,
        )
        # Second request: file download (latest file)
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/Bidmove_Complete/PUBLIC_BIDMOVE_COMPLETE_20260221_0000000504578184.zip",
            content=zip_content,
        )

        result = await client._fetch_from_current(date(2026, 2, 21))

        assert result is not None
        day_df, per_df = result
        assert len(day_df) > 0

    @pytest.mark.asyncio
    async def test_fetch_current_no_matching_files(self, client, httpx_mock):
        """Test when directory listing has no matching files for the date."""
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/Bidmove_Complete/",
            text="<html><body>no files here</body></html>",
        )

        result = await client._fetch_from_current(date(2026, 2, 21))
        assert result is None

    @pytest.mark.asyncio
    async def test_fetch_current_404(self, client, httpx_mock):
        """Test 404 on directory listing returns None."""
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/Bidmove_Complete/",
            status_code=404,
        )

        result = await client._fetch_from_current(date(2026, 2, 21))
        assert result is None

    @pytest.mark.asyncio
    async def test_fetch_current_500(self, client, httpx_mock):
        """Test HTTP 500 returns None."""
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/Bidmove_Complete/",
            status_code=500,
        )

        result = await client._fetch_from_current(date(2026, 2, 21))
        assert result is None

    @pytest.mark.asyncio
    async def test_fetch_current_network_error(self, client, httpx_mock):
        """Test network error returns None."""
        httpx_mock.add_exception(httpx.ConnectError("Connection refused"))

        result = await client._fetch_from_current(date(2026, 2, 21))
        assert result is None


class TestFetchFromArchive:
    """Tests for _fetch_from_archive async method"""

    @pytest.fixture
    def client(self):
        return NEMBidClient("https://test.nemweb.com.au")

    @pytest.mark.asyncio
    async def test_fetch_archive_success(self, client, httpx_mock):
        """Test successful fetch from Archive directory."""
        zip_content = create_bid_zip(SAMPLE_BID_CSV)

        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Archive/Bidmove_Complete/",
            text=ARCHIVE_DIR_HTML,
        )
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Archive/Bidmove_Complete/PUBLIC_BIDMOVE_COMPLETE_20260201.zip",
            content=zip_content,
        )

        result = await client._fetch_from_archive(date(2026, 2, 21))

        assert result is not None
        day_df, _ = result
        assert len(day_df) > 0

    @pytest.mark.asyncio
    async def test_fetch_archive_no_matching_month(self, client, httpx_mock):
        """Test when archive has no matching month file."""
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Archive/Bidmove_Complete/",
            text="<html><body>no files</body></html>",
        )

        result = await client._fetch_from_archive(date(2026, 3, 15))
        assert result is None

    @pytest.mark.asyncio
    async def test_fetch_archive_404(self, client, httpx_mock):
        """Test 404 returns None."""
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Archive/Bidmove_Complete/",
            status_code=404,
        )

        result = await client._fetch_from_archive(date(2026, 2, 21))
        assert result is None

    @pytest.mark.asyncio
    async def test_fetch_archive_500(self, client, httpx_mock):
        """Test HTTP 500 returns None."""
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Archive/Bidmove_Complete/",
            status_code=500,
        )

        result = await client._fetch_from_archive(date(2026, 2, 21))
        assert result is None

    @pytest.mark.asyncio
    async def test_fetch_archive_network_error(self, client, httpx_mock):
        """Test network error returns None."""
        httpx_mock.add_exception(httpx.ConnectError("Connection refused"))

        result = await client._fetch_from_archive(date(2026, 2, 21))
        assert result is None


class TestGetDailyBids:
    """Tests for get_daily_bids top-level method"""

    @pytest.fixture
    def client(self):
        return NEMBidClient("https://test.nemweb.com.au")

    @pytest.mark.asyncio
    async def test_get_daily_bids_from_current(self, client, httpx_mock):
        """Test that Current directory is tried first."""
        zip_content = create_bid_zip(SAMPLE_BID_CSV)

        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/Bidmove_Complete/",
            text=CURRENT_DIR_HTML,
        )
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/Bidmove_Complete/PUBLIC_BIDMOVE_COMPLETE_20260221_0000000504578184.zip",
            content=zip_content,
        )

        result = await client.get_daily_bids(datetime(2026, 2, 21))

        assert result is not None
        day_df, per_df = result
        assert len(day_df) == 2

    @pytest.mark.asyncio
    async def test_get_daily_bids_falls_back_to_archive(self, client, httpx_mock):
        """Test fallback to Archive when Current has no data."""
        zip_content = create_bid_zip(SAMPLE_BID_CSV)

        # Current directory: no matching files
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/Bidmove_Complete/",
            text="<html><body>no files</body></html>",
        )
        # Archive directory: has data
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Archive/Bidmove_Complete/",
            text=ARCHIVE_DIR_HTML,
        )
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Archive/Bidmove_Complete/PUBLIC_BIDMOVE_COMPLETE_20260201.zip",
            content=zip_content,
        )

        result = await client.get_daily_bids(datetime(2026, 2, 21))

        assert result is not None

    @pytest.mark.asyncio
    async def test_get_daily_bids_both_fail(self, client, httpx_mock):
        """Test returns None when both Current and Archive fail."""
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/Bidmove_Complete/",
            text="<html><body>no files</body></html>",
        )
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Archive/Bidmove_Complete/",
            text="<html><body>no files</body></html>",
        )

        result = await client.get_daily_bids(datetime(2026, 2, 21))
        assert result is None

    @pytest.mark.asyncio
    async def test_get_daily_bids_accepts_date_object(self, client, httpx_mock):
        """Test that date objects (not just datetime) are accepted."""
        zip_content = create_bid_zip(SAMPLE_BID_CSV)

        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/Bidmove_Complete/",
            text=CURRENT_DIR_HTML,
        )
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/Bidmove_Complete/PUBLIC_BIDMOVE_COMPLETE_20260221_0000000504578184.zip",
            content=zip_content,
        )

        result = await client.get_daily_bids(date(2026, 2, 21))
        assert result is not None
