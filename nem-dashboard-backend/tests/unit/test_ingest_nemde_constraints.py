"""Unit tests for the NEMDE constraint-equation-terms ingestion (pure parts only, no network/DB)."""

import io

import pandas as pd
import pytest

from scripts.ingest_nemde_constraints import (
    day_zip_url,
    dedupe_versions,
    parse_generic_constraint,
    parse_member_xml,
    select_sample_indices,
)

# Attribute scheme verified against a real downloaded 2026-05-15 NEMDE case file
# (NemSpdOutputs_20260515_loaded.zip, NEMSPDOutputs_2026051510100.loaded member).
SAMPLE_GENERIC_CONSTRAINT_XML = (
    '<GenericConstraintCollection>'
    '<GenericConstraint ConstraintID="#BBATTERY1_E1" Version="20241004000000_1" '
    'EffectiveDate="2024-10-04T00:00:00+10:00" VersionNo="1" Type="LE" '
    'ViolationPrice="7308000" RHS="50" Force_SCADA="False">'
    '<LHSFactorCollection>'
    '<TraderFactor Factor="1" TradeType="BDOF" TraderID="BBATTERY1" />'
    '</LHSFactorCollection>'
    '</GenericConstraint>'
    '<GenericConstraint ConstraintID="DATASNAP_DFS_LS" Version="20180413000000_1" '
    'EffectiveDate="2018-04-13T00:00:00+10:00" VersionNo="1" Type="LE" '
    'ViolationPrice="81200" RHS="100" Force_SCADA="False">'
    '<LHSFactorCollection>'
    '<InterconnectorFactor Factor="1" InterconnectorID="N-Q-MNSP1" />'
    '</LHSFactorCollection>'
    '</GenericConstraint>'
    '<GenericConstraint ConstraintID="D_I+BIP_ML2_L1" Version="20250217000000_1" '
    'EffectiveDate="2025-02-17T00:00:00+10:00" VersionNo="1" Type="GE" '
    'ViolationPrice="182700" RHS="75" Force_SCADA="False">'
    '<LHSFactorCollection>'
    '<RegionFactor Factor="1" TradeType="L1SE" RegionID="NSW1" />'
    '<RegionFactor Factor="1" TradeType="L1SE" RegionID="QLD1" />'
    '</LHSFactorCollection>'
    '</GenericConstraint>'
    '</GenericConstraintCollection>'
)


class TestDayZipUrl:
    def test_builds_expected_url(self):
        url = day_zip_url(pd.Timestamp("2026-05-15"))
        assert url == (
            "https://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/NEMDE/2026/"
            "NEMDE_2026_05/NEMDE_Market_Data/NEMDE_Files/NemSpdOutputs_20260515_loaded.zip"
        )


class TestSelectSampleIndices:
    def test_default_twelve_samples_evenly_spaced_across_288(self):
        indices = select_sample_indices(288, 12)
        assert len(indices) == 12
        assert indices[0] == 0
        assert indices == sorted(indices)
        # Roughly every 2 hours (24 five-minute members apart).
        gaps = [b - a for a, b in zip(indices, indices[1:])]
        assert all(gap == 24 for gap in gaps)

    def test_samples_per_day_exceeding_members_returns_all(self):
        assert select_sample_indices(5, 12) == [0, 1, 2, 3, 4]


class TestParseGenericConstraint:
    def test_extracts_all_three_factor_types(self):
        import xml.etree.ElementTree as ET

        root = ET.fromstring(SAMPLE_GENERIC_CONSTRAINT_XML)
        constraints = root.findall("GenericConstraint")

        duid_rows = parse_generic_constraint(constraints[0])
        assert duid_rows == [{
            "constraintid": "#BBATTERY1_E1", "version": 1,
            "effective_date": pd.Timestamp("2024-10-04").date(),
            "term_type": "duid", "term_id": "BBATTERY1", "factor": 1.0,
        }]

        ic_rows = parse_generic_constraint(constraints[1])
        assert ic_rows == [{
            "constraintid": "DATASNAP_DFS_LS", "version": 1,
            "effective_date": pd.Timestamp("2018-04-13").date(),
            "term_type": "interconnector", "term_id": "N-Q-MNSP1", "factor": 1.0,
        }]

        region_rows = parse_generic_constraint(constraints[2])
        assert len(region_rows) == 2
        assert {r["term_id"] for r in region_rows} == {"NSW1", "QLD1"}
        assert all(r["term_type"] == "region" for r in region_rows)
        assert all(r["version"] == 1 for r in region_rows)

    def test_no_lhs_factor_collection_returns_empty(self):
        import xml.etree.ElementTree as ET

        elem = ET.fromstring(
            '<GenericConstraint ConstraintID="C1" VersionNo="1" '
            'EffectiveDate="2024-01-01T00:00:00+10:00" />'
        )
        assert parse_generic_constraint(elem) == []


class TestParseMemberXml:
    def test_full_document_parses_all_constraints(self):
        fileobj = io.BytesIO(SAMPLE_GENERIC_CONSTRAINT_XML.encode("utf-8"))
        df = parse_member_xml(fileobj)

        assert len(df) == 4  # 1 duid + 1 interconnector + 2 region
        assert set(df["constraintid"]) == {"#BBATTERY1_E1", "DATASNAP_DFS_LS", "D_I+BIP_ML2_L1"}
        assert set(df["term_type"]) == {"duid", "interconnector", "region"}
        assert list(df.columns) == [
            "constraintid", "version", "effective_date", "term_type", "term_id", "factor",
        ]


class TestDedupeVersions:
    def test_collapses_repeat_sightings_of_the_same_version(self):
        rows = pd.DataFrame([
            {"constraintid": "C1", "version": 1, "effective_date": pd.Timestamp("2024-01-01").date(),
             "term_type": "duid", "term_id": "A", "factor": 1.0},
            {"constraintid": "C1", "version": 1, "effective_date": pd.Timestamp("2024-01-01").date(),
             "term_type": "duid", "term_id": "A", "factor": 1.0},
        ])
        out = dedupe_versions(rows)
        assert len(out) == 1

    def test_keeps_different_versions_of_the_same_constraint(self):
        rows = pd.DataFrame([
            {"constraintid": "C1", "version": 1, "effective_date": pd.Timestamp("2024-01-01").date(),
             "term_type": "duid", "term_id": "A", "factor": 1.0},
            {"constraintid": "C1", "version": 2, "effective_date": pd.Timestamp("2025-01-01").date(),
             "term_type": "duid", "term_id": "A", "factor": 0.5},
        ])
        out = dedupe_versions(rows)
        assert len(out) == 2

    def test_empty_input_returns_empty(self):
        empty = pd.DataFrame(columns=["constraintid", "version", "effective_date", "term_type", "term_id", "factor"])
        assert dedupe_versions(empty).empty
