"""Unit tests for NEMPredispatchClient (PD7Day price/interconnector/constraint parsing)."""

import zipfile
import io

import pandas as pd
import pytest

from app.nem_predispatch_client import NEMPredispatchClient

# Real header lines verified against a downloaded PUBLIC_PD7DAY_*.zip (2026-07-09 run).
SAMPLE_PD7DAY_CSV = (
    'C,NEMP.WORLD,PD7DAY,AEMO,PUBLIC,2026/07/09,17:40:03,1,1,1\n'
    'I,PD7DAY,PRICESOLUTION,1,RUN_DATETIME,INTERVENTION,INTERVAL_DATETIME,REGIONID,RRP,LASTCHANGED\n'
    'D,PD7DAY,PRICESOLUTION,1,"2026/07/09 18:00:00",0,"2026/07/09 18:00:00",NSW1,65.12,'
    '"2026/07/09 17:39:24"\n'
    'D,PD7DAY,PRICESOLUTION,1,"2026/07/09 18:00:00",0,"2026/07/09 18:30:00",NSW1,70.55,'
    '"2026/07/09 17:39:24"\n'
    # Intervention row for the same run/interval should be dropped.
    'D,PD7DAY,PRICESOLUTION,1,"2026/07/09 18:00:00",1,"2026/07/09 18:00:00",NSW1,999.0,'
    '"2026/07/09 17:39:24"\n'
    'I,PD7DAY,INTERCONNECTORSOLUTION,1,RUN_DATETIME,INTERVENTION,INTERVAL_DATETIME,INTERCONNECTORID,'
    'MWFLOW,EXPORTLIMIT,IMPORTLIMIT,MARGINALVALUE,LASTCHANGED\n'
    'D,PD7DAY,INTERCONNECTORSOLUTION,1,"2026/07/09 18:00:00",0,"2026/07/09 18:00:00",NSW1-QLD1,'
    '-757.02649,376.54952,-1088.91164,0,"2026/07/09 17:39:24"\n'
    'D,PD7DAY,INTERCONNECTORSOLUTION,1,"2026/07/09 18:00:00",0,"2026/07/09 18:30:00",NSW1-QLD1,'
    '-700.0,380.0,-1080.0,12.5,"2026/07/09 17:39:24"\n'
    # Intervention row should be dropped.
    'D,PD7DAY,INTERCONNECTORSOLUTION,1,"2026/07/09 18:00:00",1,"2026/07/09 18:00:00",NSW1-QLD1,'
    '999.0,999.0,999.0,999.0,"2026/07/09 17:39:24"\n'
    'I,PD7DAY,CONSTRAINTSOLUTION,1,RUN_DATETIME,INTERVENTION,INTERVAL_DATETIME,CONSTRAINTID,RHS,'
    'MARGINALVALUE,VIOLATIONDEGREE,LASTCHANGED\n'
    'D,PD7DAY,CONSTRAINTSOLUTION,1,"2026/07/09 18:00:00",0,"2026/07/09 18:00:00",C_BINDING,150,'
    '25.5,0,"2026/07/09 17:39:24"\n'
    'D,PD7DAY,CONSTRAINTSOLUTION,1,"2026/07/09 18:00:00",0,"2026/07/09 18:00:00",C_SLACK,80,0,0,'
    '"2026/07/09 17:39:24"\n'
    'D,PD7DAY,CONSTRAINTSOLUTION,1,"2026/07/09 18:00:00",0,"2026/07/09 18:00:00",C_VIOLATED,10,0,'
    '5,"2026/07/09 17:39:24"\n'
    # Intervention row should be dropped.
    'D,PD7DAY,CONSTRAINTSOLUTION,1,"2026/07/09 18:00:00",1,"2026/07/09 18:00:00",C_BINDING,150,'
    '999.0,0,"2026/07/09 17:39:24"\n'
    'C,"END OF REPORT",9\n'
)


def make_pd7day_zip(text: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("PUBLIC_PD7DAY_20260709174003_0000000526620246.CSV", text)
    return buf.getvalue()


class TestNEMPredispatchClientInit:
    def test_init_default_url(self):
        client = NEMPredispatchClient()
        assert client.base_url == "https://www.nemweb.com.au"

    def test_init_strips_trailing_slash(self):
        client = NEMPredispatchClient("https://example.com/")
        assert client.base_url == "https://example.com"


class TestParsePriceCsv:
    @pytest.fixture
    def client(self):
        return NEMPredispatchClient()

    def test_parses_expected_rows(self, client):
        df = client._parse_csv(SAMPLE_PD7DAY_CSV)
        assert len(df) == 2
        assert list(df.columns) == ["run_datetime", "interval_datetime", "regionid", "rrp"]

    def test_drops_intervention_rows(self, client):
        df = client._parse_csv(SAMPLE_PD7DAY_CSV)
        assert 999.0 not in df["rrp"].tolist()

    def test_no_table_returns_none(self, client):
        assert client._parse_csv("C,header only\n") is None


class TestParseInterconnectorCsv:
    @pytest.fixture
    def client(self):
        return NEMPredispatchClient()

    def test_parses_expected_rows_and_columns(self, client):
        df = client._parse_interconnector_csv(SAMPLE_PD7DAY_CSV)
        assert len(df) == 2
        assert list(df.columns) == [
            "run_datetime", "interval_datetime", "interconnectorid",
            "mwflow", "exportlimit", "importlimit", "marginalvalue",
        ]
        assert set(df["interconnectorid"]) == {"NSW1-QLD1"}

    def test_drops_intervention_rows(self, client):
        df = client._parse_interconnector_csv(SAMPLE_PD7DAY_CSV)
        assert 999.0 not in df["mwflow"].tolist()

    def test_parses_numeric_values(self, client):
        df = client._parse_interconnector_csv(SAMPLE_PD7DAY_CSV)
        row = df[df["interval_datetime"] == pd.Timestamp("2026-07-09 18:30:00")].iloc[0]
        assert row["mwflow"] == pytest.approx(-700.0)
        assert row["exportlimit"] == pytest.approx(380.0)
        assert row["importlimit"] == pytest.approx(-1080.0)
        assert row["marginalvalue"] == pytest.approx(12.5)

    def test_no_table_returns_none(self, client):
        assert client._parse_interconnector_csv("C,header only\n") is None


class TestParseConstraintCsv:
    @pytest.fixture
    def client(self):
        return NEMPredispatchClient()

    def test_parses_all_rows_unfiltered_by_binding(self, client):
        """Binding-only filtering is applied at the DB insert layer, not here."""
        df = client._parse_constraint_csv(SAMPLE_PD7DAY_CSV)
        assert len(df) == 3
        assert set(df["constraintid"]) == {"C_BINDING", "C_SLACK", "C_VIOLATED"}
        assert list(df.columns) == [
            "run_datetime", "interval_datetime", "constraintid",
            "rhs", "marginalvalue", "violationdegree",
        ]

    def test_drops_intervention_rows(self, client):
        df = client._parse_constraint_csv(SAMPLE_PD7DAY_CSV)
        assert 999.0 not in df["marginalvalue"].tolist()

    def test_parses_numeric_values(self, client):
        df = client._parse_constraint_csv(SAMPLE_PD7DAY_CSV)
        row = df[df["constraintid"] == "C_VIOLATED"].iloc[0]
        assert row["rhs"] == pytest.approx(10.0)
        assert row["marginalvalue"] == pytest.approx(0.0)
        assert row["violationdegree"] == pytest.approx(5.0)

    def test_no_table_returns_none(self, client):
        assert client._parse_constraint_csv("C,header only\n") is None


class TestGetLatestPredispatchAll:
    @pytest.fixture
    def client(self):
        return NEMPredispatchClient("https://test.nemweb.com.au")

    @pytest.mark.asyncio
    async def test_single_download_populates_all_three_frames(self, client, httpx_mock):
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/PD7Day/",
            html='<A HREF="PUBLIC_PD7DAY_20260709174003_0000000526620246.zip">x</A>',
        )
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/PD7Day/"
            "PUBLIC_PD7DAY_20260709174003_0000000526620246.zip",
            content=make_pd7day_zip(SAMPLE_PD7DAY_CSV),
        )

        result = await client.get_latest_predispatch_all()

        assert result is not None
        assert len(result["prices"]) == 2
        assert len(result["interconnector"]) == 2
        assert len(result["constraint"]) == 3
        # Exactly one directory listing + one zip download -- no extra requests.
        assert len(httpx_mock.get_requests()) == 2

    @pytest.mark.asyncio
    async def test_empty_directory_returns_none(self, client, httpx_mock):
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/PD7Day/",
            html="<html>nothing here</html>",
        )
        result = await client.get_latest_predispatch_all()
        assert result is None

    @pytest.mark.asyncio
    async def test_network_error_returns_none(self, client, httpx_mock):
        import httpx as httpx_module
        httpx_mock.add_exception(httpx_module.ConnectError("Connection refused"))
        result = await client.get_latest_predispatch_all()
        assert result is None


class TestGetLatestPredispatch:
    @pytest.fixture
    def client(self):
        return NEMPredispatchClient("https://test.nemweb.com.au")

    @pytest.mark.asyncio
    async def test_returns_price_dataframe_only(self, client, httpx_mock):
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/PD7Day/",
            html='<A HREF="PUBLIC_PD7DAY_20260709174003_0000000526620246.zip">x</A>',
        )
        httpx_mock.add_response(
            url="https://test.nemweb.com.au/Reports/Current/PD7Day/"
            "PUBLIC_PD7DAY_20260709174003_0000000526620246.zip",
            content=make_pd7day_zip(SAMPLE_PD7DAY_CSV),
        )

        df = await client.get_latest_predispatch()

        assert df is not None
        assert len(df) == 2
        assert "rrp" in df.columns
