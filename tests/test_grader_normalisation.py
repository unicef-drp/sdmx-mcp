"""The grader must score retrieval, not reporting style -- and still catch errors.

Grading reported filters against SDMX codes failed 12 of 14 natural-language
cases whose values were all exactly right: the agent reported REF_AREA as
"San Marino" rather than "SMR", or omitted SEX because it never filtered on it.
That measured formatting. These tests pin the normalisation, and equally pin
that a genuinely wrong answer still fails.
"""

import importlib.util
import unittest
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "runner", Path(__file__).resolve().parents[1] / "scripts" / "sdmx_eval_runner.py"
)
runner = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(runner)

DIMS = {
    "REF_AREA": {"id": "SMR", "name": "San Marino"},
    "INDICATOR": {"id": "DM_DPR_OLD", "name": "Old-age dependency ratio"},
    "SEX": {"id": "_T", "name": "Total"},
}


class TestFilterClaimMatches(unittest.TestCase):
    def test_exact_code_matches(self) -> None:
        self.assertIs(runner._filter_claim_matches({"REF_AREA": "SMR"}, "REF_AREA", "SMR", DIMS), True)

    def test_label_matches_code(self) -> None:
        self.assertIs(runner._filter_claim_matches({"REF_AREA": "San Marino"}, "REF_AREA", "SMR", DIMS), True)

    def test_label_match_is_case_insensitive(self) -> None:
        self.assertIs(runner._filter_claim_matches({"SEX": "total"}, "SEX", "_T", DIMS), True)

    def test_omitted_dimension_is_unknown_not_wrong(self) -> None:
        self.assertIsNone(runner._filter_claim_matches({}, "SEX", "_T", DIMS))

    def test_empty_value_is_unknown_not_wrong(self) -> None:
        self.assertIsNone(runner._filter_claim_matches({"SEX": "  "}, "SEX", "_T", DIMS))

    def test_different_place_still_fails(self) -> None:
        self.assertIs(runner._filter_claim_matches({"REF_AREA": "ZZZ"}, "REF_AREA", "SMR", DIMS), False)

    def test_different_label_still_fails(self) -> None:
        self.assertIs(runner._filter_claim_matches({"REF_AREA": "Italy"}, "REF_AREA", "SMR", DIMS), False)


class TestRoundingNote(unittest.TestCase):
    def test_two_dp_rounding_is_noted(self) -> None:
        note = runner._rounding_note("96.20888714071654", "96.21")
        self.assertIsNotNone(note)
        self.assertIn("2 dp", note)

    def test_exact_value_has_no_note(self) -> None:
        self.assertIsNone(runner._rounding_note("96.21", "96.21"))

    def test_genuinely_different_number_is_not_rounding(self) -> None:
        self.assertIsNone(runner._rounding_note("96.20888714071654", "42.0"))

    def test_wrong_rounding_is_not_excused(self) -> None:
        # 96.2088... rounds to 96.21, not 96.22.
        self.assertIsNone(runner._rounding_note("96.20888714071654", "96.22"))

    def test_non_numeric_is_safe(self) -> None:
        self.assertIsNone(runner._rounding_note("abc", "96.21"))
        self.assertIsNone(runner._rounding_note("96.21", None))


class TestTraceHitsExpectedSeries(unittest.TestCase):
    def _response(self, payload: dict) -> dict:
        return {"provider_output": {"tool_trace": [{"type": "mcp_tool_use", "name": "t", "input": payload}]}}

    def _case(self) -> dict:
        return {"filters": {"REF_AREA": "SMR", "INDICATOR": "DM_DPR_OLD", "SEX": "_T"}, "dimensions": DIMS}

    def test_codes_in_trace_match(self) -> None:
        r = self._response({"filters": {"REF_AREA": "SMR", "INDICATOR": "DM_DPR_OLD"}})
        self.assertIs(runner._trace_hits_expected_series(r, self._case()), True)

    def test_labels_in_trace_match(self) -> None:
        """The compact tools take natural-language location/subject."""
        r = self._response({"location": "San Marino", "subject": "Old-age dependency ratio"})
        self.assertIs(runner._trace_hits_expected_series(r, self._case()), True)

    def test_wrong_place_does_not_match(self) -> None:
        r = self._response({"location": "Italy", "subject": "DM_DPR_OLD"})
        self.assertIs(runner._trace_hits_expected_series(r, self._case()), False)

    def test_missing_indicator_does_not_match(self) -> None:
        r = self._response({"location": "San Marino"})
        self.assertIs(runner._trace_hits_expected_series(r, self._case()), False)

    def test_no_trace_is_unknown_not_failure(self) -> None:
        self.assertIsNone(runner._trace_hits_expected_series({"provider_output": {"tool_trace": []}}, self._case()))


if __name__ == "__main__":
    unittest.main()
