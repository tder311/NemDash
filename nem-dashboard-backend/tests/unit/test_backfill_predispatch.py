"""Unit tests for the PredispatchIS weekly-archive backfill (pure parts only, no network/DB)."""

import pandas as pd
import pytest

from scripts.backfill_predispatch import (
    build_run_constraint_dataframe,
    build_run_dataframe,
    build_run_interconnector_dataframe,
    list_week_files,
    member_run_datetime,
    parse_predispatch_constraint_csv,
    parse_predispatch_csv,
    parse_predispatch_interconnector_csv,
    resolve_table_keys,
    select_members,
    week_url,
)

INDEX_HTML = (
    '<A HREF="/Reports/ARCHIVE/PredispatchIS_Reports/PUBLIC_PREDISPATCHIS_20250629_20250705.zip">x</A>'
    '<A HREF="/Reports/ARCHIVE/PredispatchIS_Reports/PUBLIC_PREDISPATCHIS_20250622_20250628.zip">x</A>'
    '<A HREF="/Reports/ARCHIVE/PredispatchIS_Reports/PUBLIC_PREDISPATCHIS_20250622_20250628.zip">dup</A>'
    '<A HREF="/Reports/ARCHIVE">parent</A>'
)


class TestListWeekFiles:
    def test_extracts_sorted_unique_filenames(self):
        files = list_week_files(INDEX_HTML)
        assert files == [
            "PUBLIC_PREDISPATCHIS_20250622_20250628.zip",
            "PUBLIC_PREDISPATCHIS_20250629_20250705.zip",
        ]

    def test_no_matches_returns_empty(self):
        assert list_week_files("<html>nothing here</html>") == []


class TestWeekUrl:
    def test_builds_archive_url(self):
        url = week_url("PUBLIC_PREDISPATCHIS_20250622_20250628.zip")
        assert url == (
            "https://www.nemweb.com.au/Reports/Archive/PredispatchIS_Reports/"
            "PUBLIC_PREDISPATCHIS_20250622_20250628.zip"
        )


class TestMemberRunDatetime:
    def test_parses_inner_filename_token(self):
        # Real member name from PUBLIC_PREDISPATCHIS_20250622_20250628.zip.
        name = "PUBLIC_PREDISPATCHIS_202506220030_20250622000300.zip"
        assert member_run_datetime(name) == pd.Timestamp("2025-06-22 00:30:00")

    def test_midnight_rollover_member(self):
        name = "PUBLIC_PREDISPATCHIS_202506290000_20250628233300.zip"
        assert member_run_datetime(name) == pd.Timestamp("2025-06-29 00:00:00")


def _synthetic_week_members():
    """A full week of half-hourly member names, mirroring the real archive layout."""
    runs = pd.date_range("2025-06-22 00:30", "2025-06-29 00:00", freq="30min")
    return [f"PUBLIC_PREDISPATCHIS_{ts:%Y%m%d%H%M}_{ts:%Y%m%d%H%M}00.zip" for ts in runs]


class TestSelectMembers:
    def test_keeps_four_runs_per_full_day(self):
        members = _synthetic_week_members()
        selected = select_members(members)
        by_day = pd.Series([member_run_datetime(m) for m in selected]).dt.normalize()
        counts = by_day.value_counts()
        # Seven full days at 4 runs each; the trailing midnight-only day gets 1.
        assert (counts == 4).sum() == 7
        assert 28 <= len(selected) <= 29

    def test_selects_runs_nearest_target_times(self):
        members = _synthetic_week_members()
        selected = select_members(members, target_times=("04:30", "10:30", "16:30", "22:30"))
        full_days = [m for m in selected if member_run_datetime(m).date().day != 29]
        times = {member_run_datetime(m).strftime("%H:%M") for m in full_days}
        assert times == {"04:30", "10:30", "16:30", "22:30"}

    def test_nearest_available_when_exact_time_missing(self):
        members = [
            "PUBLIC_PREDISPATCHIS_202506220400_20250622033300.zip",
            "PUBLIC_PREDISPATCHIS_202506221045_20250622101500.zip",
        ]
        selected = select_members(members)
        assert sorted(selected) == sorted(members)

    def test_returns_members_in_run_order(self):
        members = list(reversed(_synthetic_week_members()))
        selected = select_members(members)
        runs = [member_run_datetime(m) for m in selected]
        assert runs == sorted(runs)


SAMPLE_CSV = (
    'C,NEMP.WORLD,PREDISPATCHIS,AEMO,PUBLIC,2025/06/22,00:03:00,1,1,1\n'
    'I,PREDISPATCH,CASE_SOLUTION,1,PREDISPATCHSEQNO,RUNNO,SOLUTIONSTATUS\n'
    'D,PREDISPATCH,CASE_SOLUTION,1,2025062141,1,0\n'
    'I,PREDISPATCH,REGION_PRICES,2,PREDISPATCHSEQNO,RUNNO,REGIONID,PERIODID,INTERVENTION,RRP,EEP,'
    'LASTCHANGED,DATETIME\n'
    'D,PREDISPATCH,REGION_PRICES,2,2025062141,1,NSW1,01,0,56.06,0,'
    '"2025/06/22 00:03:00","2025/06/22 00:30:00"\n'
    'D,PREDISPATCH,REGION_PRICES,2,2025062141,1,NSW1,02,0,58.11,0,'
    '"2025/06/22 00:03:00","2025/06/22 01:00:00"\n'
    'D,PREDISPATCH,REGION_PRICES,2,2025062141,1,VIC1,01,0,-2.33,0,'
    '"2025/06/22 00:03:00","2025/06/22 00:30:00"\n'
    # Intervention row for the same run/region should be dropped.
    'D,PREDISPATCH,REGION_PRICES,2,2025062141,1,NSW1,01,1,999.0,0,'
    '"2025/06/22 00:03:00","2025/06/22 00:30:00"\n'
    # Non-NEM region should be dropped.
    'D,PREDISPATCH,REGION_PRICES,2,2025062141,1,NEM1,01,0,10.0,0,'
    '"2025/06/22 00:03:00","2025/06/22 00:30:00"\n'
    'C,"END OF REPORT",9\n'
)


class TestParsePredispatchCsv:
    def test_parses_expected_rows(self):
        df = parse_predispatch_csv(SAMPLE_CSV)
        assert len(df) == 3
        assert set(df["regionid"]) == {"NSW1", "VIC1"}

    def test_keeps_multiple_intervals_per_run(self):
        df = parse_predispatch_csv(SAMPLE_CSV)
        nsw = df[df["regionid"] == "NSW1"]
        assert list(nsw["interval_datetime"]) == [
            pd.Timestamp("2025-06-22 00:30:00"),
            pd.Timestamp("2025-06-22 01:00:00"),
        ]

    def test_drops_intervention_rows(self):
        df = parse_predispatch_csv(SAMPLE_CSV)
        assert 999.0 not in df["rrp"].tolist()

    def test_drops_non_nem_regions(self):
        df = parse_predispatch_csv(SAMPLE_CSV)
        assert "NEM1" not in set(df["regionid"])

    def test_parses_values(self):
        df = parse_predispatch_csv(SAMPLE_CSV)
        row = df[df["interval_datetime"] == pd.Timestamp("2025-06-22 00:30:00")]
        assert row[row["regionid"] == "NSW1"]["rrp"].iloc[0] == pytest.approx(56.06)
        assert row[row["regionid"] == "VIC1"]["rrp"].iloc[0] == pytest.approx(-2.33)

    def test_no_price_table_returns_empty(self):
        df = parse_predispatch_csv("C,header only\n")
        assert df.empty
        assert list(df.columns) == ["interval_datetime", "regionid", "rrp"]


class TestBuildRunDataframe:
    def test_attaches_run_datetime(self):
        run_dt = pd.Timestamp("2025-06-22 00:30:00")
        df = build_run_dataframe(SAMPLE_CSV, run_dt)
        assert list(df.columns) == ["run_datetime", "interval_datetime", "regionid", "rrp"]
        assert (df["run_datetime"] == run_dt).all()
        assert len(df) == 3

    def test_empty_csv_returns_empty_frame(self):
        df = build_run_dataframe("C,header only\n", pd.Timestamp("2025-06-22 00:30:00"))
        assert df.empty
        assert list(df.columns) == ["run_datetime", "interval_datetime", "regionid", "rrp"]


# Real table/column names verified against a downloaded PUBLIC_PREDISPATCHIS_*.zip inner file
# (run 202607092200, 2026-07-09).
SAMPLE_INTERCONNECTOR_CSV = (
    'C,NEMP.WORLD,PREDISPATCHIS,AEMO,PUBLIC,2026/07/09,21:31:37,1,1,1\n'
    'I,PREDISPATCH,INTERCONNECTOR_SOLN,3,PREDISPATCHSEQNO,RUNNO,INTERCONNECTORID,PERIODID,'
    'INTERVENTION,METEREDMWFLOW,MWFLOW,MWLOSSES,MARGINALVALUE,VIOLATIONDEGREE,LASTCHANGED,'
    'DATETIME,EXPORTLIMIT,IMPORTLIMIT,MARGINALLOSS,EXPORTGENCONID,IMPORTGENCONID,'
    'FCASEXPORTLIMIT,FCASIMPORTLIMIT,LOCAL_PRICE_ADJUSTMENT_EXPORT,LOCALLY_CONSTRAINED_EXPORT,'
    'LOCAL_PRICE_ADJUSTMENT_IMPORT,LOCALLY_CONSTRAINED_IMPORT\n'
    'D,PREDISPATCH,INTERCONNECTOR_SOLN,3,2026070936,1,NSW1-QLD1,01,0,-918.09375,-757.02649,'
    '33.7397,0,0,"2026/07/09 21:31:37","2026/07/09 22:00:00",335.40733,-915.48736,0.89248,'
    'N>>NIL_33_34,N>>LDTM_964_81_OPEN,2204,-2478,0,0,0,0\n'
    'D,PREDISPATCH,INTERCONNECTOR_SOLN,3,2026070936,1,NSW1-QLD1,02,0,-900.0,-700.0,30.0,12.5,0,'
    '"2026/07/09 21:31:37","2026/07/09 22:30:00",335.40733,-915.48736,0.89248,'
    'N>>NIL_33_34,N>>LDTM_964_81_OPEN,2204,-2478,0,0,0,0\n'
    # Intervention row for the same run/interconnector should be dropped.
    'D,PREDISPATCH,INTERCONNECTOR_SOLN,3,2026070936,1,NSW1-QLD1,01,1,999,999,999,999,0,'
    '"2026/07/09 21:31:37","2026/07/09 22:00:00",999,999,0.89248,'
    'N>>NIL_33_34,N>>LDTM_964_81_OPEN,2204,-2478,0,0,0,0\n'
    'C,"END OF REPORT",9\n'
)

SAMPLE_CONSTRAINT_CSV = (
    'C,NEMP.WORLD,PREDISPATCHIS,AEMO,PUBLIC,2026/07/09,21:31:37,1,1,1\n'
    'I,PREDISPATCH,CONSTRAINT_SOLUTION,5,PREDISPATCHSEQNO,RUNNO,CONSTRAINTID,PERIODID,'
    'INTERVENTION,RHS,MARGINALVALUE,VIOLATIONDEGREE,LASTCHANGED,DATETIME,DUID,'
    'GENCONID_EFFECTIVEDATE,GENCONID_VERSIONNO,LHS\n'
    'D,PREDISPATCH,CONSTRAINT_SOLUTION,5,2026070936,1,C_BINDING,2026070936,0,150,25.5,0,'
    '"2026/07/09 21:31:37","2026/07/09 22:00:00",,"2026/06/02 00:00:00",1,55.33532\n'
    'D,PREDISPATCH,CONSTRAINT_SOLUTION,5,2026070936,1,C_SLACK,2026070936,0,80,0,0,'
    '"2026/07/09 21:31:37","2026/07/09 22:00:00",,"2026/06/02 00:00:00",1,10.0\n'
    'D,PREDISPATCH,CONSTRAINT_SOLUTION,5,2026070936,1,C_VIOLATED,2026070936,0,10,0,5,'
    '"2026/07/09 21:31:37","2026/07/09 22:00:00",,"2026/06/02 00:00:00",1,15.0\n'
    # Intervention row should be dropped.
    'D,PREDISPATCH,CONSTRAINT_SOLUTION,5,2026070936,1,C_BINDING,2026070936,1,150,999,0,'
    '"2026/07/09 21:31:37","2026/07/09 22:00:00",,"2026/06/02 00:00:00",1,55.33532\n'
    'C,"END OF REPORT",9\n'
)


class TestParsePredispatchInterconnectorCsv:
    def test_parses_expected_rows(self):
        df = parse_predispatch_interconnector_csv(SAMPLE_INTERCONNECTOR_CSV)
        assert len(df) == 2
        assert list(df.columns) == [
            "interval_datetime", "interconnectorid", "mwflow", "exportlimit",
            "importlimit", "marginalvalue",
        ]

    def test_drops_intervention_rows(self):
        df = parse_predispatch_interconnector_csv(SAMPLE_INTERCONNECTOR_CSV)
        assert 999 not in df["mwflow"].tolist()

    def test_parses_values(self):
        df = parse_predispatch_interconnector_csv(SAMPLE_INTERCONNECTOR_CSV)
        row = df[df["interval_datetime"] == pd.Timestamp("2026-07-09 22:30:00")].iloc[0]
        assert row["mwflow"] == pytest.approx(-700.0)
        assert row["marginalvalue"] == pytest.approx(12.5)

    def test_no_table_returns_empty(self):
        df = parse_predispatch_interconnector_csv("C,header only\n")
        assert df.empty
        assert list(df.columns) == [
            "interval_datetime", "interconnectorid", "mwflow", "exportlimit",
            "importlimit", "marginalvalue",
        ]


class TestParsePredispatchConstraintCsv:
    def test_parses_all_rows_unfiltered_by_binding(self):
        """Binding-only filtering happens downstream (build_run_constraint_dataframe)."""
        df = parse_predispatch_constraint_csv(SAMPLE_CONSTRAINT_CSV)
        assert len(df) == 3
        assert set(df["constraintid"]) == {"C_BINDING", "C_SLACK", "C_VIOLATED"}

    def test_drops_intervention_rows(self):
        df = parse_predispatch_constraint_csv(SAMPLE_CONSTRAINT_CSV)
        assert 999 not in df["marginalvalue"].tolist()

    def test_no_table_returns_empty(self):
        df = parse_predispatch_constraint_csv("C,header only\n")
        assert df.empty
        assert list(df.columns) == [
            "interval_datetime", "constraintid", "rhs", "marginalvalue", "violationdegree",
        ]


class TestBuildRunInterconnectorDataframe:
    def test_attaches_run_datetime(self):
        run_dt = pd.Timestamp("2026-07-09 22:00:00")
        df = build_run_interconnector_dataframe(SAMPLE_INTERCONNECTOR_CSV, run_dt)
        assert (df["run_datetime"] == run_dt).all()
        assert len(df) == 2

    def test_empty_csv_returns_empty_frame(self):
        df = build_run_interconnector_dataframe("C,header only\n", pd.Timestamp("2026-07-09 22:00:00"))
        assert df.empty


class TestBuildRunConstraintDataframe:
    def test_keeps_binding_or_violated_rows_only(self):
        run_dt = pd.Timestamp("2026-07-09 22:00:00")
        df = build_run_constraint_dataframe(SAMPLE_CONSTRAINT_CSV, run_dt)
        assert set(df["constraintid"]) == {"C_BINDING", "C_VIOLATED"}
        assert (df["run_datetime"] == run_dt).all()

    def test_all_slack_returns_empty_frame(self):
        slack_only = SAMPLE_CONSTRAINT_CSV.replace(
            'D,PREDISPATCH,CONSTRAINT_SOLUTION,5,2026070936,1,C_BINDING,2026070936,0,150,25.5,0,',
            'D,PREDISPATCH,CONSTRAINT_SOLUTION,5,2026070936,1,C_BINDING,2026070936,0,150,0,0,',
        ).replace(
            'D,PREDISPATCH,CONSTRAINT_SOLUTION,5,2026070936,1,C_VIOLATED,2026070936,0,10,0,5,',
            'D,PREDISPATCH,CONSTRAINT_SOLUTION,5,2026070936,1,C_VIOLATED,2026070936,0,10,0,0,',
        )
        df = build_run_constraint_dataframe(slack_only, pd.Timestamp("2026-07-09 22:00:00"))
        assert df.empty


class TestResolveTableKeys:
    def test_prices_only(self):
        assert resolve_table_keys("prices") == ["prices"]

    def test_network_expands_to_interconnector_and_constraint(self):
        assert resolve_table_keys("network") == ["interconnector", "constraint"]

    def test_default_combines_both_groups(self):
        assert resolve_table_keys("prices,network") == ["prices", "interconnector", "constraint"]

    def test_strips_whitespace(self):
        assert resolve_table_keys(" prices , network ") == ["prices", "interconnector", "constraint"]
