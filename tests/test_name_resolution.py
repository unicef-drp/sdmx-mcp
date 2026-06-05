"""Regression tests for name resolution across get_indicator_table,
get_time_series, and get_single_observation.

All HTTP calls are mocked so these run without network access.  The fixtures
are designed to pin the value→country pairing (not just non-null assertions)
to catch any positional or column-order bugs.
"""
import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import server


# ---------------------------------------------------------------------------
# Shared fixture builders
# ---------------------------------------------------------------------------

def _structure_payload(
    ref_area_codes: list[tuple[str, str]],
    indicator_codes: list[tuple[str, str]],
    unit_codes: list[tuple[str, str]] | None = None,
    extra_dims: list[tuple[str, str]] | None = None,
) -> dict:
    """Minimal SDMX structure payload with a DSD + codelists for key dimensions.

    extra_dims: list of (dimension_id, codelist_id) pairs to add beyond the standard set.
    """
    unit_codes = unit_codes or [("PCNT", "Percent")]
    extra_dims = extra_dims or []

    dimensions = [
        {"id": "REF_AREA", "position": 1,
         "localRepresentation": {"enumeration": {"id": "CL_AREA"}}},
        {"id": "INDICATOR", "position": 2,
         "localRepresentation": {"enumeration": {"id": "CL_INDICATOR"}}},
        {"id": "SEX", "position": 3,
         "localRepresentation": {"enumeration": {"id": "CL_SEX"}}},
        {"id": "AGE", "position": 4,
         "localRepresentation": {"enumeration": {"id": "CL_AGE"}}},
        {"id": "UNIT_MEASURE", "position": 5,
         "localRepresentation": {"enumeration": {"id": "CL_UNIT"}}},
        {"id": "TIME_PERIOD", "position": 6},
    ]
    for i, (dim_id, cl_id) in enumerate(extra_dims, start=7):
        dimensions.append({"id": dim_id, "position": i,
                            "localRepresentation": {"enumeration": {"id": cl_id}}})

    codelists = {
        "CL_AREA": {
            "id": "CL_AREA",
            "codes": [{"id": code, "name": {"en": name}} for code, name in ref_area_codes],
        },
        "CL_INDICATOR": {
            "id": "CL_INDICATOR",
            "codes": [{"id": code, "name": {"en": name}} for code, name in indicator_codes],
        },
        "CL_SEX": {
            "id": "CL_SEX",
            "codes": [
                {"id": "_T", "name": {"en": "Total"}},
                {"id": "F", "name": {"en": "Female"}},
                {"id": "M", "name": {"en": "Male"}},
            ],
        },
        "CL_AGE": {
            "id": "CL_AGE",
            "codes": [{"id": "_T", "name": {"en": "Total"}}],
        },
        "CL_UNIT": {
            "id": "CL_UNIT",
            "codes": [{"id": code, "name": {"en": name}} for code, name in unit_codes],
        },
    }

    return {
        "structure": {
            "dataStructures": {
                "dataStructure": {
                    "TEST_DSD": {
                        "dataStructureComponents": {
                            "dimensionList": {"dimensions": dimensions}
                        }
                    }
                }
            },
            "codelists": {"codelist": codelists},
        }
    }


def _dataflow_payload() -> dict:
    """Minimal dataflows payload so _data_path_for_query can resolve a version."""
    return {
        "data": {
            "dataflows": [
                {"id": "NUTRITION", "agencyID": "UNICEF", "version": "1.0"},
                {"id": "EDUCATION", "agencyID": "UNICEF", "version": "1.0"},
                {"id": "IMMUNISATION", "agencyID": "UNICEF", "version": "1.0"},
            ]
        }
    }


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# Test 1 — multi-country get_indicator_table (primary regression)
# ---------------------------------------------------------------------------

NUTRITION_STRUCTURE = _structure_payload(
    ref_area_codes=[("NGA", "Nigeria"), ("NER", "Niger"), ("TCD", "Chad")],
    indicator_codes=[("NT_ANT_HAZ_NE2_MOD", "Moderate + severe stunting")],
)

# Deliberately non-alphabetical value order to prove no positional shortcut.
NUTRITION_CSV = "\n".join([
    "REF_AREA,INDICATOR,OBS_VALUE,TIME_PERIOD,UNIT_MEASURE",
    "NER,NT_ANT_HAZ_NE2_MOD,55.1,2022,PCNT",
    "TCD,NT_ANT_HAZ_NE2_MOD,39.8,2022,PCNT",
    "NGA,NT_ANT_HAZ_NE2_MOD,45.2,2022,PCNT",
])


class TestMultiCountryIndicatorTable(unittest.TestCase):

    def _call(self):
        with patch.object(server, "_get_json", AsyncMock(return_value=_dataflow_payload())), \
             patch.object(server, "_get_text_with_status", AsyncMock(return_value=(200, NUTRITION_CSV))), \
             patch.object(server, "_get_flow_structure", AsyncMock(return_value=NUTRITION_STRUCTURE)):
            return _run(server.get_indicator_table(
                flowRef="UNICEF/NUTRITION/1.0",
                filters={"REF_AREA": "NGA+NER+TCD", "INDICATOR": "NT_ANT_HAZ_NE2_MOD",
                         "SEX": "_T", "AGE": "_T"},
                time="latest",
            ))

    def test_returns_three_rows(self):
        result = self._call()
        self.assertEqual(result["status"], "resolved")
        self.assertEqual(result["rowCount"], 3)

    def test_ref_area_non_null_and_distinct(self):
        result = self._call()
        ref_areas = [r["refArea"] for r in result["rows"]]
        self.assertEqual(len(set(ref_areas)), 3)
        for ra in ref_areas:
            self.assertIsNotNone(ra)

    def test_ref_area_name_populated(self):
        result = self._call()
        by_code = {r["refArea"]: r for r in result["rows"]}
        self.assertEqual(by_code["NGA"]["refAreaName"], "Nigeria")
        self.assertEqual(by_code["NER"]["refAreaName"], "Niger")
        self.assertEqual(by_code["TCD"]["refAreaName"], "Chad")

    def test_value_country_pairing_pinned(self):
        """Each country must carry its own value — not a neighbour's."""
        result = self._call()
        by_code = {r["refArea"]: r for r in result["rows"]}
        self.assertAlmostEqual(float(by_code["NGA"]["value"]), 45.2)
        self.assertAlmostEqual(float(by_code["NER"]["value"]), 55.1)
        self.assertAlmostEqual(float(by_code["TCD"]["value"]), 39.8)

    def test_unit_resolved_to_name(self):
        result = self._call()
        for row in result["rows"]:
            self.assertEqual(row.get("unit"), "Percent")


# ---------------------------------------------------------------------------
# Test 2 — single-country, multi-series (education regression)
# ---------------------------------------------------------------------------

EDUCATION_STRUCTURE = _structure_payload(
    ref_area_codes=[("DZA", "Algeria")],
    indicator_codes=[
        ("ED_CR_L1", "Primary completion rate"),
        ("ED_CR_L2", "Lower secondary completion rate"),
        ("ED_CR_L3", "Upper secondary completion rate"),
    ],
    unit_codes=[("PCNT", "Percent")],
)

# REF_AREA column intentionally absent — simulates SDMX constant-column compaction.
EDUCATION_CSV = "\n".join([
    "INDICATOR,SEX,OBS_VALUE,TIME_PERIOD,UNIT_MEASURE",
    "ED_CR_L1,F,80.5,2022,PCNT",
    "ED_CR_L1,M,82.3,2022,PCNT",
    "ED_CR_L2,F,70.1,2022,PCNT",
    "ED_CR_L2,M,73.2,2022,PCNT",
    "ED_CR_L3,F,60.5,2022,PCNT",
    "ED_CR_L3,M,63.4,2022,PCNT",
])


class TestSingleCountryMultiSeries(unittest.TestCase):

    def _call(self):
        with patch.object(server, "_get_json", AsyncMock(return_value=_dataflow_payload())), \
             patch.object(server, "_get_text_with_status", AsyncMock(return_value=(200, EDUCATION_CSV))), \
             patch.object(server, "_get_flow_structure", AsyncMock(return_value=EDUCATION_STRUCTURE)):
            return _run(server.get_indicator_table(
                flowRef="UNICEF/EDUCATION/1.0",
                filters={"REF_AREA": "DZA", "INDICATOR": "ED_CR_L1+ED_CR_L2+ED_CR_L3",
                         "SEX": "F+M"},
                time="latest",
            ))

    def test_returns_six_rows(self):
        result = self._call()
        self.assertEqual(result["status"], "resolved")
        self.assertEqual(result["rowCount"], 6)

    def test_no_null_values(self):
        result = self._call()
        for row in result["rows"]:
            self.assertIsNotNone(row["value"], f"value is null in row: {row}")

    def test_ref_area_backfilled_to_dza(self):
        result = self._call()
        for row in result["rows"]:
            self.assertEqual(row["refArea"], "DZA", f"refArea wrong in row: {row}")

    def test_ref_area_name_resolved(self):
        result = self._call()
        for row in result["rows"]:
            self.assertEqual(row.get("refAreaName"), "Algeria")

    def test_indicator_name_resolved(self):
        result = self._call()
        indicator_names = {r.get("indicatorName") for r in result["rows"]}
        self.assertIn("Primary completion rate", indicator_names)
        self.assertIn("Lower secondary completion rate", indicator_names)
        self.assertIn("Upper secondary completion rate", indicator_names)


# ---------------------------------------------------------------------------
# Test 3 — regional aggregate name resolution via flat codelist
# ---------------------------------------------------------------------------

IMMUNISATION_STRUCTURE = _structure_payload(
    ref_area_codes=[
        ("UNICEF_WCA", "West and Central Africa"),
        ("NGA", "Nigeria"),
    ],
    indicator_codes=[("IM_DTP3", "DTP3 immunisation coverage")],
    unit_codes=[("PCNT", "Percent")],
)

IMMUNISATION_CSV = "\n".join([
    "REF_AREA,INDICATOR,OBS_VALUE,TIME_PERIOD,UNIT_MEASURE",
    "UNICEF_WCA,IM_DTP3,72.4,2022,PCNT",
])


class TestRegionalNameResolution(unittest.TestCase):

    def _call(self):
        with patch.object(server, "_get_json", AsyncMock(return_value=_dataflow_payload())), \
             patch.object(server, "_get_text_with_status", AsyncMock(return_value=(200, IMMUNISATION_CSV))), \
             patch.object(server, "_get_flow_structure", AsyncMock(return_value=IMMUNISATION_STRUCTURE)):
            return _run(server.get_single_observation(
                flowRef="UNICEF/IMMUNISATION/1.0",
                filters={"REF_AREA": "UNICEF_WCA", "INDICATOR": "IM_DTP3"},
                time="latest",
            ))

    def test_ref_area_name_resolved_via_codelist(self):
        result = self._call()
        self.assertEqual(result.get("refAreaName"), "West and Central Africa",
                         f"unexpected result: {result}")

    def test_ref_area_code_present(self):
        result = self._call()
        self.assertEqual(result.get("refArea"), "UNICEF_WCA")

    def test_indicator_name_resolved(self):
        result = self._call()
        self.assertEqual(result.get("indicatorName"), "DTP3 immunisation coverage")


# ---------------------------------------------------------------------------
# Test 4 — _resolve_codes_from_payload unit tests
# ---------------------------------------------------------------------------

class TestResolveCodesFromPayload(unittest.TestCase):

    def _payload(self) -> dict:
        return _structure_payload(
            ref_area_codes=[("NGA", "Nigeria"), ("DZA", "Algeria")],
            indicator_codes=[("IM_DTP3", "DTP3 immunisation coverage")],
        )

    def test_known_codes_resolve_to_names(self):
        payload = self._payload()
        result = server._resolve_codes_from_payload(payload, "REF_AREA", {"NGA", "DZA"})
        self.assertEqual(result["NGA"], "Nigeria")
        self.assertEqual(result["DZA"], "Algeria")

    def test_unknown_code_falls_back_to_code(self):
        payload = self._payload()
        result = server._resolve_codes_from_payload(payload, "REF_AREA", {"XYZ"})
        self.assertEqual(result["XYZ"], "XYZ")

    def test_empty_code_ids_returns_empty(self):
        payload = self._payload()
        result = server._resolve_codes_from_payload(payload, "REF_AREA", set())
        self.assertEqual(result, {})

    def test_correct_dimension_used(self):
        payload = self._payload()
        result = server._resolve_codes_from_payload(payload, "INDICATOR", {"IM_DTP3"})
        self.assertEqual(result["IM_DTP3"], "DTP3 immunisation coverage")


# ---------------------------------------------------------------------------
# Test 5 — ping tool
# ---------------------------------------------------------------------------

class TestPingTool(unittest.TestCase):

    def test_ping_returns_ok_status(self):
        result = _run(server.ping())
        self.assertEqual(result["status"], "ok")

    def test_ping_includes_build(self):
        result = _run(server.ping())
        self.assertIn("build", result)
        self.assertIsNotNone(result["build"])

    def test_ping_includes_server_name(self):
        result = _run(server.ping())
        self.assertIn("server", result)


# ---------------------------------------------------------------------------
# Fixture helpers for new tests
# ---------------------------------------------------------------------------

def _structure_payload_with_unit_attr(
    ref_area_codes: list[tuple[str, str]],
    indicator_codes: list[tuple[str, str]],
) -> dict:
    """Structure where UNIT_MEASURE lives in attributeList, not dimensionList."""
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
                                    {"id": "TIME_PERIOD", "position": 3},
                                ]
                            },
                            "attributeList": {
                                "attributes": [
                                    {"id": "UNIT_MEASURE",
                                     "localRepresentation": {"enumeration": {"id": "CL_UNIT"}}},
                                ]
                            },
                        }
                    }
                }
            },
            "codelists": {
                "codelist": {
                    "CL_AREA": {
                        "id": "CL_AREA",
                        "codes": [{"id": c, "name": {"en": n}} for c, n in ref_area_codes],
                    },
                    "CL_INDICATOR": {
                        "id": "CL_INDICATOR",
                        "codes": [{"id": c, "name": {"en": n}} for c, n in indicator_codes],
                    },
                    "CL_UNIT": {
                        "id": "CL_UNIT",
                        "codes": [{"id": "PCNT", "name": {"en": "%"}}],
                    },
                }
            },
        }
    }


def _structure_payload_with_edu_level(
    ref_area_codes: list[tuple[str, str]],
    indicator_codes: list[tuple[str, str]],
) -> dict:
    """Structure with EDUCATION_LEVEL dimension and ISCED11_* codes."""
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
                                    {"id": "EDUCATION_LEVEL", "position": 3,
                                     "localRepresentation": {"enumeration": {"id": "CL_EDUCATION_LEVEL"}}},
                                    {"id": "UNIT_MEASURE", "position": 4,
                                     "localRepresentation": {"enumeration": {"id": "CL_UNIT"}}},
                                    {"id": "TIME_PERIOD", "position": 5},
                                ]
                            },
                        }
                    }
                }
            },
            "codelists": {
                "codelist": {
                    "CL_AREA": {
                        "id": "CL_AREA",
                        "codes": [{"id": c, "name": {"en": n}} for c, n in ref_area_codes],
                    },
                    "CL_INDICATOR": {
                        "id": "CL_INDICATOR",
                        "codes": [{"id": c, "name": {"en": n}} for c, n in indicator_codes],
                    },
                    "CL_EDUCATION_LEVEL": {
                        "id": "CL_EDUCATION_LEVEL",
                        "codes": [
                            {"id": "ISCED11_1", "name": {"en": "Primary education"}},
                            {"id": "ISCED11_2", "name": {"en": "Lower secondary education"}},
                            {"id": "ISCED11_3", "name": {"en": "Upper secondary education"}},
                        ],
                    },
                    "CL_UNIT": {
                        "id": "CL_UNIT",
                        "codes": [{"id": "PCNT", "name": {"en": "%"}}],
                    },
                }
            },
        }
    }


# ---------------------------------------------------------------------------
# Test 6 — UNIT_MEASURE in attributeList (multi-country NUTRITION)
# ---------------------------------------------------------------------------

class TestUnitMeasureAsAttribute(unittest.TestCase):
    """UNIT_MEASURE declared in attributeList must still resolve to its label."""

    STRUCTURE = _structure_payload_with_unit_attr(
        ref_area_codes=[("NGA", "Nigeria"), ("NER", "Niger"), ("TCD", "Chad")],
        indicator_codes=[("NT_ANT_HAZ_NE2_MOD", "Moderate + severe stunting")],
    )
    CSV = "\n".join([
        "REF_AREA,INDICATOR,OBS_VALUE,TIME_PERIOD,UNIT_MEASURE",
        "NER,NT_ANT_HAZ_NE2_MOD,55.1,2022,PCNT",
        "TCD,NT_ANT_HAZ_NE2_MOD,39.8,2022,PCNT",
        "NGA,NT_ANT_HAZ_NE2_MOD,45.2,2022,PCNT",
    ])

    def _call(self):
        with patch.object(server, "_get_json", AsyncMock(return_value=_dataflow_payload())), \
             patch.object(server, "_get_text_with_status", AsyncMock(return_value=(200, self.CSV))), \
             patch.object(server, "_get_flow_structure", AsyncMock(return_value=self.STRUCTURE)):
            return _run(server.get_indicator_table(
                flowRef="UNICEF/NUTRITION/1.0",
                filters={"REF_AREA": "NGA+NER+TCD", "INDICATOR": "NT_ANT_HAZ_NE2_MOD"},
                time="latest",
            ))

    def test_unit_not_raw_code(self):
        result = self._call()
        for row in result["rows"]:
            self.assertNotEqual(
                row.get("unit"), "PCNT",
                f"unit should be resolved label, got raw code in: {row}",
            )

    def test_unit_resolves_to_percent_symbol(self):
        result = self._call()
        for row in result["rows"]:
            self.assertEqual(row.get("unit"), "%",
                             f"expected unit='%', got {row.get('unit')!r}")


# ---------------------------------------------------------------------------
# Test 7 — EDUCATION_LEVEL dimension resolution
# ---------------------------------------------------------------------------

class TestEducationLevelResolution(unittest.TestCase):
    """EDUCATION_LEVEL dimension must carry educationLevelName, not the raw ISCED code."""

    STRUCTURE = _structure_payload_with_edu_level(
        ref_area_codes=[("DZA", "Algeria")],
        indicator_codes=[
            ("ED_CR_L1", "Primary completion rate"),
            ("ED_CR_L2", "Lower secondary completion rate"),
            ("ED_CR_L3", "Upper secondary completion rate"),
        ],
    )
    # REF_AREA absent — injected from filter (DZA).
    CSV = "\n".join([
        "INDICATOR,EDUCATION_LEVEL,OBS_VALUE,TIME_PERIOD,UNIT_MEASURE",
        "ED_CR_L1,ISCED11_1,80.5,2022,PCNT",
        "ED_CR_L2,ISCED11_2,70.1,2022,PCNT",
        "ED_CR_L3,ISCED11_3,60.5,2022,PCNT",
    ])

    def _call(self):
        with patch.object(server, "_get_json", AsyncMock(return_value=_dataflow_payload())), \
             patch.object(server, "_get_text_with_status", AsyncMock(return_value=(200, self.CSV))), \
             patch.object(server, "_get_flow_structure", AsyncMock(return_value=self.STRUCTURE)):
            return _run(server.get_indicator_table(
                flowRef="UNICEF/EDUCATION/1.0",
                filters={"REF_AREA": "DZA", "INDICATOR": "ED_CR_L1+ED_CR_L2+ED_CR_L3"},
                time="latest",
            ))

    def test_education_level_name_present(self):
        result = self._call()
        for row in result["rows"]:
            if row.get("EDUCATION_LEVEL"):
                self.assertIn("educationLevelName", row,
                              f"educationLevelName missing in: {row}")

    def test_isced11_1_resolves_to_primary_education(self):
        result = self._call()
        by_code = {r.get("EDUCATION_LEVEL"): r for r in result["rows"]}
        if "ISCED11_1" in by_code:
            self.assertEqual(by_code["ISCED11_1"].get("educationLevelName"), "Primary education")

    def test_education_level_name_not_raw_code(self):
        result = self._call()
        for row in result["rows"]:
            edu_code = row.get("EDUCATION_LEVEL")
            edu_name = row.get("educationLevelName")
            if edu_code and edu_name:
                self.assertNotEqual(edu_name, edu_code,
                                    f"educationLevelName equals raw code: {row}")


# ---------------------------------------------------------------------------
# Test 8 — Invariant: every coded component in output carries a non-empty label
# ---------------------------------------------------------------------------

class TestInvariantAllCodedComponentsHaveLabels(unittest.TestCase):
    """For each of three flows, every coded component present in output rows must have a label."""

    def _assert_coded_have_labels(self, rows: list[dict]) -> None:
        coded_keys = {"INDICATOR", "SEX", "AGE", "EDUCATION_LEVEL", "WEALTH_QUINTILE", "RESIDENCE"}
        for row in rows:
            for key in list(row.keys()):
                if key in coded_keys:
                    label_key = server._camel_name_key(key)
                    self.assertIn(label_key, row,
                                  f"{label_key} missing when {key}={row[key]!r}: {row}")
                    self.assertIsNotNone(row.get(label_key),
                                         f"{label_key} is None: {row}")
                    self.assertNotEqual(str(row.get(label_key, "")), "",
                                        f"{label_key} is empty: {row}")

    def test_nutrition_multi_country(self):
        structure = _structure_payload(
            ref_area_codes=[("NGA", "Nigeria"), ("NER", "Niger")],
            indicator_codes=[("NT_ANT_HAZ_NE2_MOD", "Moderate + severe stunting")],
            unit_codes=[("PCNT", "%")],
        )
        csv = "\n".join([
            "REF_AREA,INDICATOR,SEX,AGE,OBS_VALUE,TIME_PERIOD,UNIT_MEASURE",
            "NGA,NT_ANT_HAZ_NE2_MOD,_T,_T,45.2,2022,PCNT",
            "NER,NT_ANT_HAZ_NE2_MOD,_T,_T,55.1,2022,PCNT",
        ])
        with patch.object(server, "_get_json", AsyncMock(return_value=_dataflow_payload())), \
             patch.object(server, "_get_text_with_status", AsyncMock(return_value=(200, csv))), \
             patch.object(server, "_get_flow_structure", AsyncMock(return_value=structure)):
            result = _run(server.get_indicator_table(
                flowRef="UNICEF/NUTRITION/1.0",
                filters={"REF_AREA": "NGA+NER", "INDICATOR": "NT_ANT_HAZ_NE2_MOD",
                         "SEX": "_T", "AGE": "_T"},
                time="latest",
            ))
        self.assertEqual(result["status"], "resolved")
        self._assert_coded_have_labels(result["rows"])

    def test_education_multi_level(self):
        structure = _structure_payload_with_edu_level(
            ref_area_codes=[("DZA", "Algeria")],
            indicator_codes=[("ED_CR_L1", "Primary"), ("ED_CR_L2", "Secondary")],
        )
        csv = "\n".join([
            "INDICATOR,EDUCATION_LEVEL,OBS_VALUE,TIME_PERIOD,UNIT_MEASURE",
            "ED_CR_L1,ISCED11_1,80.5,2022,PCNT",
            "ED_CR_L2,ISCED11_2,70.1,2022,PCNT",
        ])
        with patch.object(server, "_get_json", AsyncMock(return_value=_dataflow_payload())), \
             patch.object(server, "_get_text_with_status", AsyncMock(return_value=(200, csv))), \
             patch.object(server, "_get_flow_structure", AsyncMock(return_value=structure)):
            result = _run(server.get_indicator_table(
                flowRef="UNICEF/EDUCATION/1.0",
                filters={"REF_AREA": "DZA", "INDICATOR": "ED_CR_L1+ED_CR_L2"},
                time="latest",
            ))
        self.assertEqual(result["status"], "resolved")
        self._assert_coded_have_labels(result["rows"])

    def test_cme_multi_country_with_sex(self):
        structure = _structure_payload(
            ref_area_codes=[("NGA", "Nigeria"), ("ETH", "Ethiopia")],
            indicator_codes=[("CME_MRY0T4", "Under-five mortality rate")],
            unit_codes=[("DEATHS_PER_1000", "Deaths per 1,000")],
        )
        csv = "\n".join([
            "REF_AREA,INDICATOR,SEX,OBS_VALUE,TIME_PERIOD,UNIT_MEASURE",
            "NGA,CME_MRY0T4,_T,110.3,2022,DEATHS_PER_1000",
            "ETH,CME_MRY0T4,_T,53.2,2022,DEATHS_PER_1000",
        ])
        with patch.object(server, "_get_json", AsyncMock(return_value=_dataflow_payload())), \
             patch.object(server, "_get_text_with_status", AsyncMock(return_value=(200, csv))), \
             patch.object(server, "_get_flow_structure", AsyncMock(return_value=structure)):
            result = _run(server.get_indicator_table(
                flowRef="UNICEF/CME/1.0",
                filters={"REF_AREA": "NGA+ETH", "INDICATOR": "CME_MRY0T4", "SEX": "_T"},
                time="latest",
            ))
        self.assertEqual(result["status"], "resolved")
        self._assert_coded_have_labels(result["rows"])


# ---------------------------------------------------------------------------
# Test 9 — Schema parity: single-location vs multi-location same key set
# ---------------------------------------------------------------------------

class TestSchemaParity(unittest.TestCase):
    """Single-location and multi-location get_indicator_table must return the same key set per row."""

    STRUCTURE = _structure_payload(
        ref_area_codes=[("NGA", "Nigeria"), ("NER", "Niger")],
        indicator_codes=[("NT_ANT_HAZ_NE2_MOD", "Moderate + severe stunting")],
        unit_codes=[("PCNT", "%")],
    )
    # Single-location: REF_AREA absent from CSV (constant-column compaction).
    SINGLE_CSV = "\n".join([
        "INDICATOR,SEX,AGE,OBS_VALUE,TIME_PERIOD,UNIT_MEASURE",
        "NT_ANT_HAZ_NE2_MOD,_T,_T,45.2,2022,PCNT",
    ])
    # Multi-location: REF_AREA present.
    MULTI_CSV = "\n".join([
        "REF_AREA,INDICATOR,SEX,AGE,OBS_VALUE,TIME_PERIOD,UNIT_MEASURE",
        "NGA,NT_ANT_HAZ_NE2_MOD,_T,_T,45.2,2022,PCNT",
        "NER,NT_ANT_HAZ_NE2_MOD,_T,_T,55.1,2022,PCNT",
    ])

    def _call_single(self):
        with patch.object(server, "_get_json", AsyncMock(return_value=_dataflow_payload())), \
             patch.object(server, "_get_text_with_status", AsyncMock(return_value=(200, self.SINGLE_CSV))), \
             patch.object(server, "_get_flow_structure", AsyncMock(return_value=self.STRUCTURE)):
            return _run(server.get_indicator_table(
                flowRef="UNICEF/NUTRITION/1.0",
                filters={"REF_AREA": "NGA", "INDICATOR": "NT_ANT_HAZ_NE2_MOD",
                         "SEX": "_T", "AGE": "_T"},
                time="latest",
            ))

    def _call_multi(self):
        with patch.object(server, "_get_json", AsyncMock(return_value=_dataflow_payload())), \
             patch.object(server, "_get_text_with_status", AsyncMock(return_value=(200, self.MULTI_CSV))), \
             patch.object(server, "_get_flow_structure", AsyncMock(return_value=self.STRUCTURE)):
            return _run(server.get_indicator_table(
                flowRef="UNICEF/NUTRITION/1.0",
                filters={"REF_AREA": "NGA+NER", "INDICATOR": "NT_ANT_HAZ_NE2_MOD",
                         "SEX": "_T", "AGE": "_T"},
                time="latest",
            ))

    def test_row_key_sets_match(self):
        single = self._call_single()
        multi = self._call_multi()
        self.assertEqual(single["status"], "resolved", f"single failed: {single}")
        self.assertEqual(multi["status"], "resolved", f"multi failed: {multi}")
        single_keys = set(single["rows"][0].keys()) if single["rows"] else set()
        multi_keys = set(multi["rows"][0].keys()) if multi["rows"] else set()
        self.assertEqual(
            single_keys, multi_keys,
            f"schema mismatch — single-only: {single_keys - multi_keys}  "
            f"multi-only: {multi_keys - single_keys}",
        )

    def test_empty_attribute_columns_dropped(self):
        """Attribute columns that are empty in every row must be stripped."""
        csv_with_attrs = "\n".join([
            "INDICATOR,SEX,AGE,OBS_VALUE,TIME_PERIOD,UNIT_MEASURE,UNIT_MULTIPLIER,SOURCE_LINK",
            "NT_ANT_HAZ_NE2_MOD,_T,_T,45.2,2022,PCNT,,",
        ])
        with patch.object(server, "_get_json", AsyncMock(return_value=_dataflow_payload())), \
             patch.object(server, "_get_text_with_status", AsyncMock(return_value=(200, csv_with_attrs))), \
             patch.object(server, "_get_flow_structure", AsyncMock(return_value=self.STRUCTURE)):
            result = _run(server.get_indicator_table(
                flowRef="UNICEF/NUTRITION/1.0",
                filters={"REF_AREA": "NGA", "INDICATOR": "NT_ANT_HAZ_NE2_MOD",
                         "SEX": "_T", "AGE": "_T"},
                time="latest",
            ))
        self.assertEqual(result["status"], "resolved")
        row = result["rows"][0]
        self.assertNotIn("UNIT_MULTIPLIER", row,
                         "empty UNIT_MULTIPLIER should have been dropped")
        self.assertNotIn("SOURCE_LINK", row,
                         "empty SOURCE_LINK should have been dropped")


if __name__ == "__main__":
    unittest.main()
