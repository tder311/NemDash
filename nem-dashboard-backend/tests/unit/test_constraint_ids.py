"""Unit tests for the AEMO constraint ID parser (pure, no DB/network)."""

from app.constraint_ids import parse_constraint_id


class TestFCASConstraints:
    def test_tas_island_token(self):
        result = parse_constraint_id("F_T+RREG_0050")
        assert result["category"] == "fcas"
        assert result["regions"] == ["TAS1"]
        assert result["kind"] == "fcas"

    def test_mainland_token(self):
        result = parse_constraint_id("F_MAIN+NIL_MG_R1")
        assert result["category"] == "fcas"
        assert result["regions"] == ["NSW1", "QLD1", "VIC1", "SA1"]
        assert result["kind"] == "fcas"

    def test_islanding_system_token(self):
        result = parse_constraint_id("F_I+NIL_APD_TL_L60")
        assert result["category"] == "fcas"
        assert result["regions"] == []
        assert result["kind"] == "fcas"

    def test_tascap_prefix_matches_tas(self):
        result = parse_constraint_id("F_TASCAP_RREG_0220")
        assert result["category"] == "fcas"
        assert result["regions"] == ["TAS1"]
        assert result["kind"] == "fcas"


class TestNetworkThermalSeparators:
    def test_single_gt_thermal(self):
        result = parse_constraint_id("S>NIL_MHNW1_MHNW2")
        assert result["category"] == "network"
        assert result["regions"] == ["SA1"]
        assert result["kind"] == "thermal"

    def test_single_gt_thermal_numeric_suffix(self):
        for cid in ("N>NIL_94T", "N>NIL_901"):
            result = parse_constraint_id(cid)
            assert result["category"] == "network"
            assert result["regions"] == ["NSW1"]
            assert result["kind"] == "thermal"

    def test_gt_thermal_does_not_treat_nil_as_a_region(self):
        result = parse_constraint_id("Q>NIL_YLMR")
        assert result["regions"] == ["QLD1"]

    def test_double_gt_post_contingent(self):
        result = parse_constraint_id("N>>LDTM_964_81_OPEN")
        assert result["category"] == "network"
        assert result["regions"] == ["NSW1"]
        assert result["kind"] == "post-contingent"
        # "LDTM" contains non-region letters so it must not be treated as a region.
        assert "LDT" not in "".join(result["regions"])

    def test_double_gt_post_contingent_sa(self):
        result = parse_constraint_id("S>>NIL_RBTU_RBTU")
        assert result["regions"] == ["SA1"]
        assert result["kind"] == "post-contingent"

    def test_gt_thermal_wetx(self):
        result = parse_constraint_id("V>NIL_WETX_NIL")
        assert result["regions"] == ["VIC1"]
        assert result["kind"] == "thermal"


class TestNetworkVoltageAndTransient:
    def test_compound_svml_single_caret_voltage(self):
        result = parse_constraint_id("SVML^NIL_MH-CAP_ON")
        assert result["category"] == "network"
        assert result["regions"] == ["SA1", "VIC1"]
        assert result["kind"] == "voltage"

    def test_double_caret_voltage_with_right_side_region(self):
        result = parse_constraint_id("V^^N_NIL_1")
        assert result["category"] == "network"
        assert result["regions"] == ["VIC1", "NSW1"]
        assert result["kind"] == "voltage"

    def test_double_caret_same_region_both_sides_dedupes(self):
        result = parse_constraint_id("V^^V_NIL_KGTS")
        assert result["regions"] == ["VIC1"]
        assert result["kind"] == "voltage"

    def test_double_colon_transient(self):
        result = parse_constraint_id("T::T_NIL_1")
        assert result["category"] == "network"
        assert result["regions"] == ["TAS1"]
        assert result["kind"] == "transient"

    def test_double_colon_transient_two_regions(self):
        result = parse_constraint_id("V::N_NIL_V2")
        assert result["regions"] == ["VIC1", "NSW1"]
        assert result["kind"] == "transient"

    def test_double_colon_transient_n_utrv(self):
        result = parse_constraint_id("N::N_UTRV_2")
        assert result["regions"] == ["NSW1"]
        assert result["kind"] == "transient"


class TestNetworkNoSeparatorSingleRegion:
    def test_local_kind_when_no_keyword(self):
        result = parse_constraint_id("V_DUNDWF1_2_3_168")
        assert result["category"] == "network"
        assert result["regions"] == ["VIC1"]
        assert result["kind"] == "local"

    def test_system_strength_keyword(self):
        result = parse_constraint_id("Q_NIL_STRGTH_CKWF")
        assert result["category"] == "network"
        assert result["regions"] == ["QLD1"]
        assert result["kind"] == "system-strength"

    def test_rocof_keyword(self):
        result = parse_constraint_id("V_S_NIL_ROCOF")
        assert result["category"] == "network"
        assert result["regions"] == ["VIC1", "SA1"]
        assert result["kind"] == "rocof"

    def test_local_with_second_region_token(self):
        result = parse_constraint_id("V_S_HEYWOOD_UFLS")
        assert result["category"] == "network"
        assert result["regions"] == ["VIC1", "SA1"]
        assert result["kind"] == "local"

    def test_local_non_region_second_token(self):
        result = parse_constraint_id("N_MBTE1_B")
        assert result["category"] == "network"
        assert result["regions"] == ["NSW1"]
        assert result["kind"] == "local"


class TestNetworkCompoundNoSeparator:
    def test_qn_is_qld_nsw(self):
        result = parse_constraint_id("QN_590")
        assert result["category"] == "network"
        assert result["regions"] == ["QLD1", "NSW1"]
        assert result["kind"] == "local"

    def test_svml_is_sa_vic_murraylink(self):
        result = parse_constraint_id("SVML_ZERO")
        assert result["category"] == "network"
        assert result["regions"] == ["SA1", "VIC1"]
        assert result["kind"] == "local"

    def test_tvbl_is_tas_vic_basslink(self):
        result = parse_constraint_id("TVBL_NIL_CONT_456")
        assert result["category"] == "network"
        assert result["regions"] == ["TAS1", "VIC1"]
        assert result["kind"] == "local"

    def test_vs_is_vic_sa(self):
        result = parse_constraint_id("VS_600_TEST")
        assert result["category"] == "network"
        assert result["regions"] == ["VIC1", "SA1"]
        assert result["kind"] == "local"


class TestOtherCategory:
    def test_non_region_non_fcas_prefix_falls_to_other(self):
        # "I_" is not a network region letter and not the "F_" FCAS prefix.
        result = parse_constraint_id("I_6F_NS_150")
        assert result["category"] == "other"
        assert result["regions"] == []
        assert result["kind"] is None
        assert result["label"] == "I_6F_NS_150"


class TestAllFixturesClassify:
    """Every real-DB fixture ID must classify without error, into a known category."""

    FIXTURE_IDS = [
        "F_T+RREG_0050", "F_MAIN+NIL_MG_R1", "F_I+NIL_APD_TL_L60", "F_TASCAP_RREG_0220",
        "S>NIL_MHNW1_MHNW2", "N>NIL_94T", "N>NIL_901", "SVML^NIL_MH-CAP_ON",
        "V_DUNDWF1_2_3_168", "V^^N_NIL_1", "N>>LDTM_964_81_OPEN", "T::T_NIL_1",
        "V^^V_NIL_KGTS", "SVML_ZERO", "N_MBTE1_B", "I_6F_NS_150", "V_S_NIL_ROCOF",
        "TVBL_NIL_CONT_456", "V>NIL_WETX_NIL", "V::N_NIL_V2", "Q>NIL_YLMR", "QN_590",
        "S>>NIL_RBTU_RBTU", "Q_NIL_STRGTH_CKWF", "V_S_HEYWOOD_UFLS", "VS_600_TEST",
        "N::N_UTRV_2",
    ]

    def test_all_fixtures_classify_into_known_category(self):
        for cid in self.FIXTURE_IDS:
            result = parse_constraint_id(cid)
            assert result["category"] in {"fcas", "network", "other"}, cid
            assert isinstance(result["regions"], list)
            assert result["label"]

    def test_only_expected_fixture_falls_to_other(self):
        others = [cid for cid in self.FIXTURE_IDS if parse_constraint_id(cid)["category"] == "other"]
        assert others == ["I_6F_NS_150"]
