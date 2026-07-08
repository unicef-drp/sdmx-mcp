"""Tests for the compact-vs-verbose projection layer.

All HTTP calls are mocked so these run without network access.
"""
import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import server


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _run(coro):
    return asyncio.run(coro)


def _dataflow_payload() -> dict:
    return {
        "data": {
            "dataflows": [
                {"id": "NUTRITION", "agencyID": "UNICEF", "version": "1.0"},
                {"id": "EDUCATION", "agencyID": "UNICEF", "version": "1.0"},
            ]
        }
    }


def _nutrition_structure() -> dict:
    """Full NUTRITION-style DSD with several constant dimensions and coded attributes."""
    return {
        "structure": {
            "dataStructures": {
                "dataStructure": {
                    "TEST_DSD": {
                        "dataStructureComponents": {
                            "dimensionList": {
                                "dimensions": [
                                    {"id": "REF_AREA", "position": 1,
                                     "localRepresentation": {"enumeration": {"id": "CL_AREA"}}},
                                    {"id": "INDICATOR", "position": 2,
                                     "localRepresentation": {"enumeration": {"id": "CL_INDICATOR"}}},
                                    {"id": "SEX", "position": 3,
                                     "localRepresentation": {"enumeration": {"id": "CL_SEX"}}},
                                    {"id": "AGE", "position": 4,
                                     "localRepresentation": {"enumeration": {"id": "CL_AGE"}}},
                                    {"id": "WEALTH_QUINTILE", "position": 5,
                                     "localRepresentation": {"enumeration": {"id": "CL_WEALTH"}}},
                                    {"id": "RESIDENCE", "position": 6,
                                     "localRepresentation": {"enumeration": {"id": "CL_RESIDENCE"}}},
                                    {"id": "UNIT_MEASURE", "position": 7,
                                     "localRepresentation": {"enumeration": {"id": "CL_UNIT"}}},
                                    {"id": "TIME_PERIOD", "position": 8},
                                ]
                            },
                            "attributeList": {
                                "attributes": [
                                    # Coded attribute (enumerated).
                                    {"id": "OBS_CONF",
                                     "localRepresentation": {"enumeration": {"id": "CL_OBS_CONF"}}},
                                    # Free-text attributes (no codelist).
                                    {"id": "DATA_SOURCE"},
                                    {"id": "CUSTODIAN"},
                                    {"id": "SOURCE_LINK"},
                                ]
                            },
                        }
                    }
                }
            },
            "codelists": {
                "codelist": {
                    "CL_AREA": {"id": "CL_AREA", "codes": [
                        {"id": "NGA", "name": {"en": "Nigeria"}},
                        {"id": "NER", "name": {"en": "Niger"}},
                        {"id": "TCD", "name": {"en": "Chad"}},
                        {"id": "MLI", "name": {"en": "Mali"}},
                        {"id": "COD", "name": {"en": "DR Congo"}},
                        {"id": "ETH", "name": {"en": "Ethiopia"}},
                    ]},
                    "CL_INDICATOR": {"id": "CL_INDICATOR", "codes": [
                        {"id": "NT_ANT_HAZ_NE2_MOD", "name": {"en": "Moderate + severe stunting"}},
                    ]},
                    "CL_SEX": {"id": "CL_SEX", "codes": [
                        {"id": "_T", "name": {"en": "Total"}},
                        {"id": "F",  "name": {"en": "Female"}},
                        {"id": "M",  "name": {"en": "Male"}},
                    ]},
                    "CL_AGE": {"id": "CL_AGE", "codes": [
                        {"id": "_T", "name": {"en": "Total"}},
                    ]},
                    "CL_WEALTH": {"id": "CL_WEALTH", "codes": [
                        {"id": "_T", "name": {"en": "Total"}},
                    ]},
                    "CL_RESIDENCE": {"id": "CL_RESIDENCE", "codes": [
                        {"id": "_T", "name": {"en": "Total"}},
                    ]},
                    "CL_UNIT": {"id": "CL_UNIT", "codes": [
                        {"id": "PCNT", "name": {"en": "%"}},
                    ]},
                    "CL_OBS_CONF": {"id": "CL_OBS_CONF", "codes": [
                        {"id": "F", "name": {"en": "Free"}},
                    ]},
                }
            },
        }
    }


def _education_structure() -> dict:
    """EDUCATION-style DSD with EDUCATION_LEVEL dimension and varying SEX."""
    return {
        "structure": {
            "dataStructures": {
                "dataStructure": {
                    "TEST_DSD": {
                        "dataStructureComponents": {
                            "dimensionList": {
                                "dimensions": [
                                    {"id": "REF_AREA", "position": 1,
                                     "localRepresentation": {"enumeration": {"id": "CL_AREA"}}},
                                    {"id": "INDICATOR", "position": 2,
                                     "localRepresentation": {"enumeration": {"id": "CL_INDICATOR"}}},
                                    {"id": "SEX", "position": 3,
                                     "localRepresentation": {"enumeration": {"id": "CL_SEX"}}},
                                    {"id": "EDUCATION_LEVEL", "position": 4,
                                     "localRepresentation": {"enumeration": {"id": "CL_EDU"}}},
                                    {"id": "WEALTH_QUINTILE", "position": 5,
                                     "localRepresentation": {"enumeration": {"id": "CL_WEALTH"}}},
                                    {"id": "RESIDENCE", "position": 6,
                                     "localRepresentation": {"enumeration": {"id": "CL_RESIDENCE"}}},
                                    {"id": "UNIT_MEASURE", "position": 7,
                                     "localRepresentation": {"enumeration": {"id": "CL_UNIT"}}},
                                    {"id": "TIME_PERIOD", "position": 8},
                                ]
                            },
                            "attributeList": {
                                "attributes": [
                                    {"id": "DATA_SOURCE"},
                                    {"id": "OBS_CONF",
                                     "localRepresentation": {"enumeration": {"id": "CL_OBS_CONF"}}},
                                ]
                            },
                        }
                    }
                }
            },
            "codelists": {
                "codelist": {
                    "CL_AREA": {"id": "CL_AREA", "codes": [
                        {"id": "DZA", "name": {"en": "Algeria"}},
                    ]},
                    "CL_INDICATOR": {"id": "CL_INDICATOR", "codes": [
                        {"id": "ED_CR_L1", "name": {"en": "Primary completion rate"}},
                        {"id": "ED_CR_L2", "name": {"en": "Lower secondary completion rate"}},
                        {"id": "ED_CR_L3", "name": {"en": "Upper secondary completion rate"}},
                    ]},
                    "CL_SEX": {"id": "CL_SEX", "codes": [
                        {"id": "_T", "name": {"en": "Total"}},
                        {"id": "F",  "name": {"en": "Female"}},
                        {"id": "M",  "name": {"en": "Male"}},
                    ]},
                    "CL_EDU": {"id": "CL_EDU", "codes": [
                        {"id": "ISCED11_1", "name": {"en": "Primary education"}},
                        {"id": "ISCED11_2", "name": {"en": "Lower secondary"}},
                        {"id": "ISCED11_3", "name": {"en": "Upper secondary"}},
                    ]},
                    "CL_WEALTH": {"id": "CL_WEALTH", "codes": [
                        {"id": "_T", "name": {"en": "Total"}},
                    ]},
                    "CL_RESIDENCE": {"id": "CL_RESIDENCE", "codes": [
                        {"id": "_T", "name": {"en": "Total"}},
                    ]},
                    "CL_UNIT": {"id": "CL_UNIT", "codes": [
                        {"id": "PCNT", "name": {"en": "%"}},
                    ]},
                    "CL_OBS_CONF": {"id": "CL_OBS_CONF", "codes": [
                        {"id": "F", "name": {"en": "Free"}},
                    ]},
                }
            },
        }
    }


# ---------------------------------------------------------------------------
# Test 1 — compact default: constant dimensions dropped
# ---------------------------------------------------------------------------

_NUTRITION_MULTI_CSV = "\n".join([
    "REF_AREA,INDICATOR,SEX,AGE,WEALTH_QUINTILE,RESIDENCE,OBS_VALUE,TIME_PERIOD,UNIT_MEASURE,OBS_CONF,DATA_SOURCE,CUSTODIAN,SOURCE_LINK",
    "NGA,NT_ANT_HAZ_NE2_MOD,_T,_T,_T,_T,45.2,2022,PCNT,F,UNICEF,,",
    "NER,NT_ANT_HAZ_NE2_MOD,_T,_T,_T,_T,55.1,2022,PCNT,F,UNICEF,,",
    "TCD,NT_ANT_HAZ_NE2_MOD,_T,_T,_T,_T,39.8,2022,PCNT,F,UNICEF,,",
    "MLI,NT_ANT_HAZ_NE2_MOD,_T,_T,_T,_T,28.4,2022,PCNT,F,UNICEF,,",
    "COD,NT_ANT_HAZ_NE2_MOD,_T,_T,_T,_T,42.1,2022,PCNT,F,UNICEF,,",
    "ETH,NT_ANT_HAZ_NE2_MOD,_T,_T,_T,_T,37.5,2022,PCNT,F,UNICEF,,",
])


class TestCompactDefault(unittest.TestCase):
    """Compact mode (default): constant coded dimensions must be absent."""

    def _call(self, **extra):
        with patch.object(server, "_get_json", AsyncMock(return_value=_dataflow_payload())), \
             patch.object(server, "_get_text_with_status", AsyncMock(return_value=(200, _NUTRITION_MULTI_CSV))), \
             patch.object(server, "_get_flow_structure", AsyncMock(return_value=_nutrition_structure())):
            return _run(server.get_indicator_table(
                flowRef="UNICEF/NUTRITION/1.0",
                filters={"REF_AREA": "NGA+NER+TCD+MLI+COD+ETH",
                         "INDICATOR": "NT_ANT_HAZ_NE2_MOD",
                         "SEX": "_T", "AGE": "_T"},
                time="latest",
                **extra,
            ))

    def test_returns_six_rows(self):
        result = self._call()
        self.assertEqual(result["status"], "resolved")
        self.assertEqual(result["rowCount"], 6)

    def test_core_fields_present(self):
        result = self._call()
        for row in result["rows"]:
            for key in ("refArea", "refAreaName", "value", "period", "unit"):
                self.assertIn(key, row, f"{key!r} missing: {row}")

    def test_unit_resolved_to_percent(self):
        result = self._call()
        for row in result["rows"]:
            self.assertEqual(row.get("unit"), "%")

    def test_indicator_name_present(self):
        result = self._call()
        for row in result["rows"]:
            self.assertIn("INDICATOR", row)
            self.assertIn("indicatorName", row)

    def test_constant_sex_absent(self):
        result = self._call()
        for row in result["rows"]:
            self.assertNotIn("SEX", row, f"constant SEX should be dropped: {row}")
            self.assertNotIn("sexName", row)

    def test_constant_age_absent(self):
        result = self._call()
        for row in result["rows"]:
            self.assertNotIn("AGE", row, f"constant AGE should be dropped: {row}")

    def test_constant_wealth_quintile_absent(self):
        result = self._call()
        for row in result["rows"]:
            self.assertNotIn("WEALTH_QUINTILE", row)
            self.assertNotIn("wealthQuintileName", row)

    def test_constant_residence_absent(self):
        result = self._call()
        for row in result["rows"]:
            self.assertNotIn("RESIDENCE", row)

    def test_obs_conf_attribute_absent(self):
        result = self._call()
        for row in result["rows"]:
            self.assertNotIn("OBS_CONF", row,
                             f"coded attribute OBS_CONF should be dropped in compact: {row}")
            self.assertNotIn("obsConfName", row)

    def test_empty_attributes_absent(self):
        result = self._call()
        for row in result["rows"]:
            self.assertNotIn("CUSTODIAN", row)
            self.assertNotIn("SOURCE_LINK", row)


# ---------------------------------------------------------------------------
# Test 2 — varying dimensions retained in compact mode
# ---------------------------------------------------------------------------

# REF_AREA absent (single country DZA, injected from filter).
_EDUCATION_VARYING_CSV = "\n".join([
    "INDICATOR,SEX,EDUCATION_LEVEL,WEALTH_QUINTILE,RESIDENCE,OBS_VALUE,TIME_PERIOD,UNIT_MEASURE,OBS_CONF",
    "ED_CR_L1,F,ISCED11_1,_T,_T,80.5,2022,PCNT,F",
    "ED_CR_L1,M,ISCED11_1,_T,_T,82.3,2022,PCNT,F",
    "ED_CR_L2,F,ISCED11_2,_T,_T,70.1,2022,PCNT,F",
    "ED_CR_L2,M,ISCED11_2,_T,_T,73.2,2022,PCNT,F",
    "ED_CR_L3,F,ISCED11_3,_T,_T,60.5,2022,PCNT,F",
    "ED_CR_L3,M,ISCED11_3,_T,_T,63.4,2022,PCNT,F",
])


class TestVaryingDimensionRetention(unittest.TestCase):
    """Compact mode: SEX and EDUCATION_LEVEL vary → must be kept with labels."""

    def _call(self, **extra):
        with patch.object(server, "_get_json", AsyncMock(return_value=_dataflow_payload())), \
             patch.object(server, "_get_text_with_status",
                          AsyncMock(return_value=(200, _EDUCATION_VARYING_CSV))), \
             patch.object(server, "_get_flow_structure", AsyncMock(return_value=_education_structure())):
            return _run(server.get_indicator_table(
                flowRef="UNICEF/EDUCATION/1.0",
                filters={"REF_AREA": "DZA",
                         "INDICATOR": "ED_CR_L1+ED_CR_L2+ED_CR_L3",
                         "SEX": "F+M"},
                time="latest",
                **extra,
            ))

    def test_sex_present_because_it_varies(self):
        result = self._call()
        self.assertEqual(result["status"], "resolved")
        for row in result["rows"]:
            self.assertIn("SEX", row, f"varying SEX missing: {row}")
            self.assertIn("sexName", row, f"sexName missing: {row}")

    def test_education_level_present_because_it_varies(self):
        result = self._call()
        for row in result["rows"]:
            self.assertIn("EDUCATION_LEVEL", row, f"varying EDUCATION_LEVEL missing: {row}")
            self.assertIn("educationLevelName", row)

    def test_sex_name_not_raw_code(self):
        result = self._call()
        for row in result["rows"]:
            self.assertNotIn(row.get("sexName"), ("F", "M"),
                             f"sexName should be label, not code: {row}")

    def test_constant_wealth_quintile_absent(self):
        result = self._call()
        for row in result["rows"]:
            self.assertNotIn("WEALTH_QUINTILE", row)

    def test_constant_residence_absent(self):
        result = self._call()
        for row in result["rows"]:
            self.assertNotIn("RESIDENCE", row)

    def test_obs_conf_attribute_absent_in_compact(self):
        result = self._call()
        for row in result["rows"]:
            self.assertNotIn("OBS_CONF", row)


# ---------------------------------------------------------------------------
# Test 3 — verbose=True is a strict superset of compact keys
# ---------------------------------------------------------------------------

class TestVerboseSuperset(unittest.TestCase):
    """verbose=True must return a strict superset of the compact key set."""

    def _call(self, verbose):
        with patch.object(server, "_get_json", AsyncMock(return_value=_dataflow_payload())), \
             patch.object(server, "_get_text_with_status",
                          AsyncMock(return_value=(200, _EDUCATION_VARYING_CSV))), \
             patch.object(server, "_get_flow_structure", AsyncMock(return_value=_education_structure())):
            return _run(server.get_indicator_table(
                flowRef="UNICEF/EDUCATION/1.0",
                filters={"REF_AREA": "DZA",
                         "INDICATOR": "ED_CR_L1+ED_CR_L2+ED_CR_L3",
                         "SEX": "F+M"},
                time="latest",
                verbose=verbose,
            ))

    def test_verbose_is_superset_of_compact(self):
        compact = self._call(verbose=False)
        verbose = self._call(verbose=True)
        self.assertEqual(compact["status"], "resolved")
        self.assertEqual(verbose["status"], "resolved")

        compact_keys = set(compact["rows"][0].keys()) if compact["rows"] else set()
        verbose_keys = set(verbose["rows"][0].keys()) if verbose["rows"] else set()

        missing_from_verbose = compact_keys - verbose_keys
        self.assertFalse(
            missing_from_verbose,
            f"verbose missing keys that compact has: {missing_from_verbose}",
        )
        self.assertGreater(
            len(verbose_keys), len(compact_keys),
            "verbose must add at least one field beyond compact",
        )

    def test_verbose_includes_constant_dimensions(self):
        """Constant WEALTH_QUINTILE and RESIDENCE dropped in compact must appear in verbose."""
        verbose = self._call(verbose=True)
        for row in verbose["rows"]:
            # These are constant (_T) and dropped in compact but present in verbose.
            self.assertIn("WEALTH_QUINTILE", row,
                          f"verbose should keep WEALTH_QUINTILE: {row}")
            self.assertIn("RESIDENCE", row,
                          f"verbose should keep RESIDENCE: {row}")

    def test_verbose_includes_coded_attributes(self):
        """OBS_CONF is a coded attribute — dropped in compact, present in verbose."""
        verbose = self._call(verbose=True)
        for row in verbose["rows"]:
            self.assertIn("OBS_CONF", row, f"verbose should include OBS_CONF: {row}")


# ---------------------------------------------------------------------------
# Test 4 — no field is "" across all rows in either mode
# ---------------------------------------------------------------------------

class TestEmptyColumnDrop(unittest.TestCase):
    """No column may be the empty string across all rows in compact or verbose mode."""

    # CSV where CUSTODIAN and SOURCE_LINK are always empty.
    CSV = "\n".join([
        "REF_AREA,INDICATOR,SEX,AGE,WEALTH_QUINTILE,RESIDENCE,OBS_VALUE,TIME_PERIOD,UNIT_MEASURE,OBS_CONF,DATA_SOURCE,CUSTODIAN,SOURCE_LINK",
        "NGA,NT_ANT_HAZ_NE2_MOD,_T,_T,_T,_T,45.2,2022,PCNT,F,UNICEF,,",
        "NER,NT_ANT_HAZ_NE2_MOD,_T,_T,_T,_T,55.1,2022,PCNT,F,UNICEF,,",
    ])

    def _call(self, verbose):
        with patch.object(server, "_get_json", AsyncMock(return_value=_dataflow_payload())), \
             patch.object(server, "_get_text_with_status", AsyncMock(return_value=(200, self.CSV))), \
             patch.object(server, "_get_flow_structure", AsyncMock(return_value=_nutrition_structure())):
            return _run(server.get_indicator_table(
                flowRef="UNICEF/NUTRITION/1.0",
                filters={"REF_AREA": "NGA+NER", "INDICATOR": "NT_ANT_HAZ_NE2_MOD",
                         "SEX": "_T", "AGE": "_T"},
                time="latest",
                verbose=verbose,
            ))

    def _assert_no_all_empty_field(self, rows: list[dict]) -> None:
        if not rows:
            return
        all_keys: set[str] = set()
        for row in rows:
            all_keys.update(row.keys())
        for key in all_keys:
            all_empty = all(
                r.get(key) is None or str(r.get(key) or "").strip() == ""
                for r in rows
            )
            self.assertFalse(
                all_empty,
                f"Field {key!r} is empty/null across all rows — should have been dropped.",
            )

    def test_compact_no_all_empty_field(self):
        result = self._call(verbose=False)
        self._assert_no_all_empty_field(result["rows"])

    def test_verbose_no_all_empty_field(self):
        result = self._call(verbose=True)
        self._assert_no_all_empty_field(result["rows"])


# ---------------------------------------------------------------------------
# Test 5 — schema parity: single vs multi-location in compact mode
# ---------------------------------------------------------------------------

class TestSchemaParity(unittest.TestCase):
    """Single-location and multi-location compact output must have identical key sets."""

    # Single: REF_AREA absent (constant-column compaction).
    SINGLE_CSV = "\n".join([
        "INDICATOR,SEX,AGE,WEALTH_QUINTILE,RESIDENCE,OBS_VALUE,TIME_PERIOD,UNIT_MEASURE",
        "NT_ANT_HAZ_NE2_MOD,_T,_T,_T,_T,45.2,2022,PCNT",
    ])
    MULTI_CSV = "\n".join([
        "REF_AREA,INDICATOR,SEX,AGE,WEALTH_QUINTILE,RESIDENCE,OBS_VALUE,TIME_PERIOD,UNIT_MEASURE",
        "NGA,NT_ANT_HAZ_NE2_MOD,_T,_T,_T,_T,45.2,2022,PCNT",
        "NER,NT_ANT_HAZ_NE2_MOD,_T,_T,_T,_T,55.1,2022,PCNT",
    ])

    def _call(self, csv_text, ref_area_filter):
        with patch.object(server, "_get_json", AsyncMock(return_value=_dataflow_payload())), \
             patch.object(server, "_get_text_with_status", AsyncMock(return_value=(200, csv_text))), \
             patch.object(server, "_get_flow_structure", AsyncMock(return_value=_nutrition_structure())):
            return _run(server.get_indicator_table(
                flowRef="UNICEF/NUTRITION/1.0",
                filters={"REF_AREA": ref_area_filter,
                         "INDICATOR": "NT_ANT_HAZ_NE2_MOD",
                         "SEX": "_T", "AGE": "_T"},
                time="latest",
                verbose=False,
            ))

    def test_compact_schema_parity(self):
        single = self._call(self.SINGLE_CSV, "NGA")
        multi = self._call(self.MULTI_CSV, "NGA+NER")
        self.assertEqual(single["status"], "resolved", f"single failed: {single}")
        self.assertEqual(multi["status"], "resolved", f"multi failed: {multi}")

        single_keys = set(single["rows"][0].keys()) if single["rows"] else set()
        multi_keys = set(multi["rows"][0].keys()) if multi["rows"] else set()
        self.assertEqual(
            single_keys, multi_keys,
            f"schema mismatch — single-only: {single_keys - multi_keys}  "
            f"multi-only: {multi_keys - single_keys}",
        )

    def test_compact_single_has_indicator_name(self):
        single = self._call(self.SINGLE_CSV, "NGA")
        row = single["rows"][0]
        self.assertIn("indicatorName", row)
        self.assertEqual(row["indicatorName"], "Moderate + severe stunting")


if __name__ == "__main__":
    unittest.main()
