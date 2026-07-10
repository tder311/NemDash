"""Unit tests for the MMSDM constraint-equation-terms ingestion (pure parts only, no network/DB)."""

import pandas as pd
import pytest

from app.database import SENTINEL_MMSDM_VERSION
from scripts.ingest_constraint_equations import (
    archive_url,
    build_constraint_equation_terms,
    latest_version_only,
    parse_available_months,
    parse_dudetailsummary_csv,
    parse_spd_connection_point_csv,
    parse_spd_interconnector_csv,
    parse_spd_region_csv,
)

# Real header lines verified against a downloaded MMSDM_2026_05 PUBLIC_ARCHIVE#*#FILE01#*.CSV.
SAMPLE_DUDETAILSUMMARY_CSV = (
    'C,SETP.WORLD,DVD_DUDETAILSUMMARY,AEMO,PUBLIC,2026/06/09,11:19:22,1,MONTHLY_ARCHIVE,1\n'
    'I,PARTICIPANT_REGISTRATION,DUDETAILSUMMARY,7,DUID,START_DATE,END_DATE,DISPATCHTYPE,'
    'CONNECTIONPOINTID,REGIONID,STATIONID,PARTICIPANTID,LASTCHANGED,TRANSMISSIONLOSSFACTOR\n'
    # Expired generation of NBAYSW pointing at a DIFFERENT DUID -- the current row must win.
    'D,PARTICIPANT_REGISTRATION,DUDETAILSUMMARY,7,OLDBAYSW,"2020/07/01 00:00:00","2021/07/01 00:00:00",'
    'GENERATOR,NBAYSW,NSW1,BAYSWATER,AGL,"2026/06/08 10:42:26",0.99\n'
    'D,PARTICIPANT_REGISTRATION,DUDETAILSUMMARY,7,BAYSW1,"2021/07/01 00:00:00","2999/12/31 00:00:00",'
    'GENERATOR,NBAYSW,NSW1,BAYSWATER,AGL,"2026/06/08 10:42:26",0.985\n'
    'D,PARTICIPANT_REGISTRATION,DUDETAILSUMMARY,7,ARWF1,"2015/01/01 00:00:00","2999/12/31 00:00:00",'
    'GENERATOR,NARWF,VIC1,ARARAT,PACIFICHYDRO,"2026/06/08 10:42:26",1.0\n'
    # Fully retired connection point (every row expired) -- must be absent from the mapping.
    'D,PARTICIPANT_REGISTRATION,DUDETAILSUMMARY,7,RETIRED1,"2010/01/01 00:00:00","2018/07/01 00:00:00",'
    'GENERATOR,NRETIRED,NSW1,OLDPLANT,GONECO,"2026/06/08 10:42:26",0.95\n'
    'C,"END OF REPORT",9\n'
)

SAMPLE_SPD_CPC_CSV = (
    'C,SETP.WORLD,DVD_SPDCONNECTIONPOINTCONSTRAINT,AEMO,PUBLIC,2026/06/09,12:05:43,1,MONTHLY_ARCHIVE,1\n'
    'I,SPDCPC,NULL,2,CONNECTIONPOINTID,EFFECTIVEDATE,VERSIONNO,GENCONID,FACTOR,LASTCHANGED,BIDTYPE\n'
    'D,SPDCPC,NULL,2,NBAYSW,"2026/05/01 00:00:00",1,C_BINDING,1,"2026/05/01 02:06:52",ENERGY\n'
    # Superseded version for the same constraint -- must be dropped by latest_version_only.
    'D,SPDCPC,NULL,2,NBAYSW,"2026/04/01 00:00:00",3,C_BINDING,0.5,"2026/04/01 02:06:52",ENERGY\n'
    # FCAS bidtype row for the same connection point -- must be dropped (ENERGY-only).
    'D,SPDCPC,NULL,2,NBAYSW,"2026/05/01 00:00:00",1,C_BINDING,1,"2026/05/01 02:06:52",RAISEREG\n'
    # Connection point with no current DUDETAILSUMMARY mapping -- must be dropped downstream.
    'D,SPDCPC,NULL,2,NUNMAPPED,"2026/05/01 00:00:00",1,C_BINDING,-1,"2026/05/01 02:06:52",ENERGY\n'
    'C,"END OF REPORT",9\n'
)

SAMPLE_SPD_ICC_CSV = (
    'C,SETP.WORLD,DVD_SPDINTERCONNECTORCONSTRAINT,AEMO,PUBLIC,2026/06/09,12:05:48,1,MONTHLY_ARCHIVE,1\n'
    'I,SPDICC,NULL,1,INTERCONNECTORID,EFFECTIVEDATE,VERSIONNO,GENCONID,FACTOR,LASTCHANGED\n'
    'D,SPDICC,NULL,1,NSW1-QLD1,"2026/05/01 00:00:00",1,C_BINDING,-1,"2026/05/01 02:06:52"\n'
    'C,"END OF REPORT",9\n'
)

SAMPLE_SPD_RC_CSV = (
    'C,SETP.WORLD,DVD_SPDREGIONCONSTRAINT,AEMO,PUBLIC,2026/06/09,12:05:49,1,MONTHLY_ARCHIVE,1\n'
    'I,SPDRC,NULL,2,REGIONID,EFFECTIVEDATE,VERSIONNO,GENCONID,FACTOR,LASTCHANGED,BIDTYPE\n'
    'D,SPDRC,NULL,2,NSW1,"2026/05/01 00:00:00",1,C_REGION,1,"2026/05/01 07:06:52",ENERGY\n'
    'C,"END OF REPORT",9\n'
)


class TestArchiveUrl:
    def test_builds_expected_url_with_percent_encoded_hash(self):
        url = archive_url(2026, 5, "SPDCONNECTIONPOINTCONSTRAINT")
        assert url == (
            "https://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2026/MMSDM_2026_05/"
            "MMSDM_Historical_Data_SQLLoader/DATA/"
            "PUBLIC_ARCHIVE%23SPDCONNECTIONPOINTCONSTRAINT%23FILE01%23202605010000.zip"
        )

    def test_pads_single_digit_month(self):
        url = archive_url(2026, 1, "DUDETAILSUMMARY")
        assert "MMSDM_2026_01" in url
        assert "202601010000" in url


class TestParseAvailableMonths:
    def test_extracts_year_month_pairs(self):
        html = '<A HREF="MMSDM_2026_03/">x</A><A HREF="MMSDM_2026_04/">x</A>'
        assert parse_available_months(html) == [(2026, 3), (2026, 4)]

    def test_dedupes_and_sorts(self):
        html = '<A HREF="MMSDM_2026_04/">x</A><A HREF="MMSDM_2026_03/">x</A><A HREF="MMSDM_2026_03/">x</A>'
        assert parse_available_months(html) == [(2026, 3), (2026, 4)]

    def test_no_matches_returns_empty(self):
        assert parse_available_months("<html>nothing</html>") == []


class TestParseDudetailsummaryCsv:
    def test_current_generation_wins_over_expired_different_duid(self):
        df = parse_dudetailsummary_csv(SAMPLE_DUDETAILSUMMARY_CSV)
        assert set(df.columns) == {"connectionpointid", "duid"}
        row = df[df["connectionpointid"] == "NBAYSW"].iloc[0]
        # NBAYSW's expired generation maps to OLDBAYSW; only the current row's DUID is valid.
        assert row["duid"] == "BAYSW1"
        assert "OLDBAYSW" not in df["duid"].tolist()

    def test_fully_retired_connection_point_is_dropped(self):
        df = parse_dudetailsummary_csv(SAMPLE_DUDETAILSUMMARY_CSV)
        assert "NRETIRED" not in df["connectionpointid"].tolist()
        assert "RETIRED1" not in df["duid"].tolist()

    def test_one_row_per_connection_point(self):
        df = parse_dudetailsummary_csv(SAMPLE_DUDETAILSUMMARY_CSV)
        assert len(df) == df["connectionpointid"].nunique()
        assert set(df["connectionpointid"]) == {"NBAYSW", "NARWF"}


class TestParseSpdConnectionPointCsv:
    def test_keeps_only_energy_bidtype(self):
        df = parse_spd_connection_point_csv(SAMPLE_SPD_CPC_CSV)
        assert "RAISEREG" not in df.get("bidtype", pd.Series(dtype=object)).tolist()
        assert set(df["connectionpointid"]) == {"NBAYSW", "NUNMAPPED"}

    def test_columns(self):
        df = parse_spd_connection_point_csv(SAMPLE_SPD_CPC_CSV)
        assert set(df.columns) == {"connectionpointid", "constraintid", "factor", "effectivedate", "versionno"}


class TestParseSpdInterconnectorCsv:
    def test_parses_rows(self):
        df = parse_spd_interconnector_csv(SAMPLE_SPD_ICC_CSV)
        assert len(df) == 1
        assert df.iloc[0]["interconnectorid"] == "NSW1-QLD1"
        assert df.iloc[0]["factor"] == pytest.approx(-1.0)


class TestParseSpdRegionCsv:
    def test_parses_energy_rows(self):
        df = parse_spd_region_csv(SAMPLE_SPD_RC_CSV)
        assert len(df) == 1
        assert df.iloc[0]["regionid"] == "NSW1"
        assert df.iloc[0]["constraintid"] == "C_REGION"


class TestLatestVersionOnly:
    def test_keeps_max_effectivedate_and_versionno_per_constraintid(self):
        df = pd.DataFrame([
            {"constraintid": "C1", "effectivedate": pd.Timestamp("2026-05-01"), "versionno": 1, "factor": 1.0},
            {"constraintid": "C1", "effectivedate": pd.Timestamp("2026-04-01"), "versionno": 3, "factor": 0.5},
            {"constraintid": "C2", "effectivedate": pd.Timestamp("2026-05-01"), "versionno": 2, "factor": 2.0},
            {"constraintid": "C2", "effectivedate": pd.Timestamp("2026-05-01"), "versionno": 1, "factor": 9.0},
        ])
        out = latest_version_only(df)
        assert len(out) == 2
        c1 = out[out["constraintid"] == "C1"].iloc[0]
        assert c1["versionno"] == 1
        c2 = out[out["constraintid"] == "C2"].iloc[0]
        assert c2["versionno"] == 2

    def test_empty_frame_returns_empty(self):
        df = pd.DataFrame(columns=["constraintid", "effectivedate", "versionno", "factor"])
        assert latest_version_only(df).empty


class TestBuildConstraintEquationTerms:
    def test_full_pipeline_maps_duids_drops_unmapped_and_dedupes_versions(self):
        cpc_df = parse_spd_connection_point_csv(SAMPLE_SPD_CPC_CSV)
        icc_df = parse_spd_interconnector_csv(SAMPLE_SPD_ICC_CSV)
        rc_df = parse_spd_region_csv(SAMPLE_SPD_RC_CSV)
        dud_df = parse_dudetailsummary_csv(SAMPLE_DUDETAILSUMMARY_CSV)

        terms = build_constraint_equation_terms(cpc_df, icc_df, rc_df, dud_df)

        assert list(terms.columns) == ["constraintid", "version", "term_type", "term_id", "factor"]
        c_binding = terms[terms["constraintid"] == "C_BINDING"]
        # NUNMAPPED has no DUDETAILSUMMARY entry so is dropped; only the latest (v1) BAYSW1 term remains.
        assert set(c_binding["term_type"]) == {"duid", "interconnector"}
        duid_row = c_binding[c_binding["term_type"] == "duid"].iloc[0]
        assert duid_row["term_id"] == "BAYSW1"
        assert duid_row["factor"] == pytest.approx(1.0)
        # This source has no effective_date, so output rows always take the sentinel version --
        # v2 (factor 0.5) was already dropped by latest_version_only above.
        assert duid_row["version"] == SENTINEL_MMSDM_VERSION

        c_region = terms[terms["constraintid"] == "C_REGION"]
        assert len(c_region) == 1
        assert c_region.iloc[0]["term_type"] == "region"
        assert c_region.iloc[0]["term_id"] == "NSW1"

    def test_no_duplicate_constraint_term_type_term_id(self):
        cpc_df = parse_spd_connection_point_csv(SAMPLE_SPD_CPC_CSV)
        icc_df = parse_spd_interconnector_csv(SAMPLE_SPD_ICC_CSV)
        rc_df = parse_spd_region_csv(SAMPLE_SPD_RC_CSV)
        dud_df = parse_dudetailsummary_csv(SAMPLE_DUDETAILSUMMARY_CSV)

        terms = build_constraint_equation_terms(cpc_df, icc_df, rc_df, dud_df)
        assert not terms.duplicated(subset=["constraintid", "term_type", "term_id"]).any()
