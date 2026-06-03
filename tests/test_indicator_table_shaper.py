"""Regression tests for get_indicator_table's response shaper.

Covers the constant-column compaction bug: when only one location is queried,
SDMX drops REF_AREA from the CSV response. The shaper must backfill it from
the request filters and return non-empty rows.
"""
import unittest

import server


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_row(**kwargs) -> dict:
    base = {
        "INDICATOR": "ED_CR_L1",
        "SEX": "T",
        "TIME_PERIOD": "2022",
        "OBS_VALUE": "82.5",
    }
    base.update(kwargs)
    return base


def _minimal_resolved(shaped: dict) -> dict:
    """Build a minimal _execute_query_data result with the given shaped payload."""
    return {
        "status": "resolved",
        "provenance": {
            "resolvedFlowRef": "UNICEF/GLOBAL_DATAFLOW/1.0",
            "requestedFlowRef": "GLOBAL_DATAFLOW",
            "key": "DZA.ED_CR_L1.T",
            "queryURL": "https://sdmx.example/data/...",
            "filters": {"REF_AREA": "DZA"},
            "appliedDefaults": {},
        },
        "shaped": shaped,
    }


# ---------------------------------------------------------------------------
# _inject_constant_dim_from_filters
# ---------------------------------------------------------------------------

class TestInjectConstantDimFromFilters(unittest.TestCase):

    def test_injects_missing_single_value(self):
        rows = [_make_row()]
        result = server._inject_constant_dim_from_filters(rows, "REF_AREA", {"REF_AREA": "DZA"})
        self.assertEqual(result[0]["REF_AREA"], "DZA")

    def test_noop_when_column_already_present(self):
        rows = [_make_row(REF_AREA="DZA")]
        result = server._inject_constant_dim_from_filters(rows, "REF_AREA", {"REF_AREA": "DZA"})
        self.assertEqual(len(result[0]), len(rows[0]))
        self.assertEqual(result[0]["REF_AREA"], "DZA")

    def test_noop_for_multi_value_filter(self):
        rows = [_make_row()]
        result = server._inject_constant_dim_from_filters(rows, "REF_AREA", {"REF_AREA": "DZA+EGY"})
        self.assertNotIn("REF_AREA", result[0])

    def test_noop_when_dim_absent_from_filters(self):
        rows = [_make_row()]
        result = server._inject_constant_dim_from_filters(rows, "REF_AREA", {"INDICATOR": "ED_CR_L1"})
        self.assertNotIn("REF_AREA", result[0])

    def test_noop_on_empty_rows(self):
        result = server._inject_constant_dim_from_filters([], "REF_AREA", {"REF_AREA": "DZA"})
        self.assertEqual(result, [])

    def test_noop_on_empty_filters(self):
        rows = [_make_row()]
        result = server._inject_constant_dim_from_filters(rows, "REF_AREA", {})
        self.assertNotIn("REF_AREA", result[0])

    def test_does_not_mutate_original_rows(self):
        original = _make_row()
        rows = [original]
        server._inject_constant_dim_from_filters(rows, "REF_AREA", {"REF_AREA": "DZA"})
        self.assertNotIn("REF_AREA", original)

    def test_plus_separated_single_value(self):
        # A filter value that looks like "DZA+" should still be treated as single.
        rows = [_make_row()]
        result = server._inject_constant_dim_from_filters(rows, "REF_AREA", {"REF_AREA": "DZA+"})
        self.assertEqual(result[0]["REF_AREA"], "DZA")

    def test_uppercase_dim_key_in_filters(self):
        rows = [_make_row()]
        result = server._inject_constant_dim_from_filters(rows, "ref_area", {"REF_AREA": "DZA"})
        self.assertEqual(result[0]["ref_area"], "DZA")


# ---------------------------------------------------------------------------
# _shape_latest_by_ref_area — REF_AREA present (normal path)
# ---------------------------------------------------------------------------

class TestShapeLatestByRefAreaNormal(unittest.TestCase):

    def test_returns_resolved_with_ref_area(self):
        rows = [_make_row(REF_AREA="DZA"), _make_row(REF_AREA="EGY", OBS_VALUE="90.1")]
        result = server._shape_latest_by_ref_area(rows)
        self.assertEqual(result["status"], "resolved")
        self.assertEqual(result["shape"], "latest_by_ref_area")
        ref_areas = [item["refArea"] for item in result["results"]]
        self.assertIn("DZA", ref_areas)
        self.assertIn("EGY", ref_areas)

    def test_single_location_after_injection_resolves(self):
        # Simulate: SDMX dropped REF_AREA, caller injected it before calling shaper.
        rows = [_make_row(REF_AREA="DZA")]
        result = server._shape_latest_by_ref_area(rows)
        self.assertEqual(result["status"], "resolved")
        self.assertEqual(len(result["results"]), 1)
        self.assertEqual(result["results"][0]["refArea"], "DZA")


# ---------------------------------------------------------------------------
# _shape_latest_by_ref_area — REF_AREA absent (degraded path)
# ---------------------------------------------------------------------------

class TestShapeLatestByRefAreaDegraded(unittest.TestCase):

    def test_shape_degraded_when_rows_present_but_no_ref_area(self):
        rows = [_make_row()]  # No REF_AREA column
        result = server._shape_latest_by_ref_area(rows)
        self.assertEqual(result["status"], "shape_degraded")
        self.assertIsNotNone(result.get("rows"))
        self.assertGreater(len(result["rows"]), 0)

    def test_no_observations_when_rows_empty_and_no_ref_area(self):
        result = server._shape_latest_by_ref_area([])
        self.assertEqual(result["status"], "no_observations")
        self.assertEqual(result.get("rows", []), [])


# ---------------------------------------------------------------------------
# _compact_indicator_table — the end-to-end regression fixture
# ---------------------------------------------------------------------------

class TestCompactIndicatorTable(unittest.TestCase):

    def _resolved_with_shape(self, shaped: dict) -> dict:
        return _minimal_resolved(shaped)

    # --- happy path ---

    def test_normal_multi_location_table(self):
        shaped = {
            "status": "resolved",
            "shape": "latest_by_ref_area",
            "refAreaColumn": "REF_AREA",
            "timeColumn": "TIME_PERIOD",
            "valueColumn": "OBS_VALUE",
            "results": [
                {"refArea": "DZA", "latestPeriod": "2022", "rowCountAtLatestPeriod": 1,
                 "value": "82.5", "rows": [_make_row(REF_AREA="DZA")]},
                {"refArea": "EGY", "latestPeriod": "2022", "rowCountAtLatestPeriod": 1,
                 "value": "90.1", "rows": [_make_row(REF_AREA="EGY", OBS_VALUE="90.1")]},
            ],
            "summary": {"rowCount": 2, "columns": [], "timeColumn": None, "valueColumn": None,
                        "latestPeriod": None, "latestRowCount": 0, "distinctCounts": {}, "preview": []},
        }
        result = server._compact_indicator_table(self._resolved_with_shape(shaped), max_rows=200)
        self.assertEqual(result["status"], "resolved")
        self.assertEqual(result["rowCount"], 2)
        ref_areas = [r["refArea"] for r in result["rows"]]
        self.assertIn("DZA", ref_areas)
        self.assertIn("EGY", ref_areas)

    # --- core regression: single location, REF_AREA dropped, injection succeeds ---

    def test_single_location_after_successful_injection(self):
        # _inject_constant_dim_from_filters put REF_AREA back → shaper resolves normally.
        shaped = {
            "status": "resolved",
            "shape": "latest_by_ref_area",
            "refAreaColumn": "REF_AREA",
            "timeColumn": "TIME_PERIOD",
            "valueColumn": "OBS_VALUE",
            "results": [
                {"refArea": "DZA", "latestPeriod": "2022", "rowCountAtLatestPeriod": 2,
                 "value": None,
                 "rows": [
                     _make_row(REF_AREA="DZA", SEX="F", OBS_VALUE="80.0"),
                     _make_row(REF_AREA="DZA", SEX="M", OBS_VALUE="85.0"),
                 ]},
            ],
            "summary": {"rowCount": 2, "columns": [], "timeColumn": None, "valueColumn": None,
                        "latestPeriod": "2022", "latestRowCount": 2, "distinctCounts": {}, "preview": []},
        }
        result = server._compact_indicator_table(self._resolved_with_shape(shaped), max_rows=200)
        self.assertEqual(result["status"], "resolved")
        self.assertGreater(len(result["rows"]), 0, "rows must be non-empty for single-location query")
        self.assertEqual(result["rows"][0]["refArea"], "DZA")

    # --- shape_degraded path: injection failed but observations still present ---

    def test_shape_degraded_returns_non_empty_rows(self):
        obs_rows = [_make_row(), _make_row(OBS_VALUE="77.3")]
        shaped = {
            "status": "shape_degraded",
            "shape": "latest_by_ref_area",
            "reason": "REF_AREA column was not present in the returned dataset; observations returned in long form.",
            "rows": obs_rows,
            "summary": {},
        }
        result = server._compact_indicator_table(self._resolved_with_shape(shaped), max_rows=200)
        self.assertEqual(result["status"], "resolved")
        self.assertGreater(len(result["rows"]), 0,
                           "rows must be non-empty when observations exist, even if REF_AREA is absent")

    def test_shape_degraded_includes_note(self):
        obs_rows = [_make_row()]
        shaped = {
            "status": "shape_degraded",
            "shape": "latest_by_ref_area",
            "reason": "REF_AREA column was not present.",
            "rows": obs_rows,
            "summary": {},
        }
        result = server._compact_indicator_table(self._resolved_with_shape(shaped), max_rows=200)
        self.assertIn("note", result)
        self.assertIsNotNone(result["note"])

    def test_shape_degraded_value_extracted(self):
        obs_rows = [_make_row(OBS_VALUE="55.5")]
        shaped = {
            "status": "shape_degraded",
            "shape": "latest_by_ref_area",
            "reason": "REF_AREA absent.",
            "rows": obs_rows,
            "summary": {},
        }
        result = server._compact_indicator_table(self._resolved_with_shape(shaped), max_rows=200)
        self.assertEqual(result["rows"][0]["value"], "55.5")
        self.assertEqual(result["rows"][0]["period"], "2022")

    # --- unresolved passthrough ---

    def test_unresolved_returns_empty_rows(self):
        result_payload = {
            "status": "unresolved_from_official_flows",
            "provenance": {},
            "error": {"message": "404 Not Found", "status": 404, "raw": ""},
            "shaped": {},
        }
        result = server._compact_indicator_table(result_payload, max_rows=200)
        self.assertNotEqual(result["status"], "resolved")
        self.assertIsNone(result.get("rows") or result.get("value"))

    # --- genuine no-data ---

    def test_no_observations_returns_non_resolved_status(self):
        shaped = {
            "status": "no_observations",
            "shape": "latest_by_ref_area",
            "reason": "The query returned no observations.",
            "summary": {},
        }
        result = server._compact_indicator_table(self._resolved_with_shape(shaped), max_rows=200)
        self.assertNotEqual(result["status"], "resolved")
        rows = result.get("rows") or []
        self.assertEqual(rows, [], "genuine no-data must yield empty rows")

    # --- max_rows respected ---

    def test_max_rows_respected(self):
        obs_rows = [_make_row(REF_AREA=c) for c in ["A", "B", "C", "D", "E"]]
        shaped = {
            "status": "resolved",
            "shape": "latest_by_ref_area",
            "refAreaColumn": "REF_AREA",
            "timeColumn": "TIME_PERIOD",
            "valueColumn": "OBS_VALUE",
            "results": [
                {"refArea": r["REF_AREA"], "latestPeriod": "2022",
                 "rowCountAtLatestPeriod": 1, "value": r["OBS_VALUE"], "rows": [r]}
                for r in obs_rows
            ],
            "summary": {"rowCount": 5, "columns": [], "timeColumn": None, "valueColumn": None,
                        "latestPeriod": None, "latestRowCount": 0, "distinctCounts": {}, "preview": []},
        }
        result = server._compact_indicator_table(self._resolved_with_shape(shaped), max_rows=3)
        self.assertEqual(result["rowCount"], 3)


if __name__ == "__main__":
    unittest.main()
