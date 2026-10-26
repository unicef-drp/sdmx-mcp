"""Ground truth decays, and a stale value scores against the model.

Measured: a manifest built from a sweep captured on 5 October 2026 was graded
on the 20th. UNICEF had restated the PV_CHLD_* child-poverty family in that
window -- source "DHS VII" to "DHS 2018 (IPUMS)", MLI 2018 from
18.5535945892334 to 23.67173993 -- and removed four series outright. Models
were penalised twice: for reporting the current value, and for correctly
answering null where data no longer exists. Every model's prescriptive stratum
was capped at 94%.

Refreshing is free (registry reads, no model calls), so the rule is to refresh
immediately before grading. These tests pin the conservative behaviour: a value
is only rewritten when exactly one observation pins it.
"""

import asyncio
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "refresh_truth",
    Path(__file__).resolve().parents[1] / "scripts" / "sdmx_eval_refresh_ground_truth.py",
)
refresh = importlib.util.module_from_spec(_spec)
sys.modules["refresh_truth"] = refresh
_spec.loader.exec_module(refresh)


def _case(value, *, case_type="positive", period="2018", indicator="X"):
    return {
        "case_id": f'UNICEF/GLOBAL_DATAFLOW/1.0|prescriptive|{period}|{{"INDICATOR": "{indicator}"}}',
        "flowRef": "UNICEF/GLOBAL_DATAFLOW/1.0",
        "dimensionOrder": ["REF_AREA", "INDICATOR", "SEX"],
        "filters": {"REF_AREA": "MLI", "INDICATOR": indicator, "SEX": "_T"},
        "timePeriod": period,
        "caseType": case_type,
        "ground_truth": {"status": "resolved" if value is not None else "no_data",
                         "expected": {"status": "deterministic", "value": value}},
    }


class TestRestFlowRef(unittest.TestCase):
    def test_slashes_become_commas_for_the_rest_path(self) -> None:
        # The manifest carries the canonical form; the data path needs commas.
        self.assertEqual(refresh._rest_flow_ref("UNICEF/GLOBAL_DATAFLOW/1.0"),
                         "UNICEF,GLOBAL_DATAFLOW,1.0")

    def test_comma_form_is_left_alone(self) -> None:
        self.assertEqual(refresh._rest_flow_ref("UNICEF,GLOBAL_DATAFLOW,1.0"),
                         "UNICEF,GLOBAL_DATAFLOW,1.0")


class TestKeyFor(unittest.TestCase):
    def test_key_follows_the_declared_dimension_order(self) -> None:
        case = _case("1.0")
        self.assertEqual(refresh.key_for(case), "MLI.X._T")

    def test_a_missing_dimension_leaves_its_slot_wild(self) -> None:
        case = _case("1.0")
        case["filters"].pop("SEX")
        self.assertEqual(refresh.key_for(case), "MLI.X.")


class _Harness:
    """Runs the refresher with fetch stubbed to a scripted registry reply."""

    def __init__(self, replies):
        self.replies = replies

    def run(self, cases):
        async def fake_fetch(client, sem, case, base):
            return (case, *self.replies[case["filters"]["INDICATOR"]])

        original = refresh.fetch
        refresh.fetch = fake_fetch
        try:
            with tempfile.TemporaryDirectory() as tmp:
                src = Path(tmp) / "in.jsonl"
                dst = Path(tmp) / "out.jsonl"
                src.write_text("".join(json.dumps(c) + "\n" for c in cases))
                stats = asyncio.run(refresh.run(src, dst, "https://example.invalid", 2))
                rows = [json.loads(l) for l in dst.read_text().splitlines()]
                return stats, rows
        finally:
            refresh.fetch = original


class TestRefresh(unittest.TestCase):
    def test_a_restated_value_is_rewritten(self) -> None:
        stats, rows = _Harness({"X": (["23.67173993"], None)}).run(
            [_case("18.5535945892334")])
        self.assertEqual(stats["value_changed"], 1)
        self.assertEqual(rows[0]["ground_truth"]["expected"]["value"], "23.67173993")
        self.assertEqual(rows[0]["ground_truth"]["source"], "live_registry_refresh")

    def test_an_unchanged_value_is_not_touched(self) -> None:
        stats, rows = _Harness({"X": (["5.0"], None)}).run([_case("5.0")])
        self.assertEqual(stats["unchanged"], 1)
        self.assertEqual(stats["value_changed"], 0)

    def test_a_withdrawn_series_becomes_no_data(self) -> None:
        # Four series were removed outright; a model answering null there was
        # right and was being marked down.
        stats, rows = _Harness({"X": ([], None)}).run([_case("74.32402038574219")])
        self.assertEqual(stats["value_changed"], 1)
        self.assertIsNone(rows[0]["ground_truth"]["expected"]["value"])
        self.assertEqual(rows[0]["ground_truth"]["status"], "no_data")

    def test_a_negative_case_that_gained_data_is_counted(self) -> None:
        # Worth surfacing: a negative case with data is no longer a negative
        # case, and silently grading it as one would reward the wrong answer.
        stats, _ = _Harness({"X": (["7.0"], None)}).run(
            [_case(None, case_type="negative")])
        self.assertEqual(stats["negative_cases_that_now_have_data"], 1)

    def test_several_observations_leave_the_case_alone(self) -> None:
        # Ambiguous: nothing pins one value, so guessing would manufacture
        # ground truth.
        stats, rows = _Harness({"X": (["1.0", "2.0"], None)}).run([_case("1.0")])
        self.assertEqual(stats["left_alone"], 1)
        self.assertEqual(rows[0]["ground_truth"]["expected"]["value"], "1.0")

    def test_a_fetch_error_leaves_the_case_alone(self) -> None:
        # A transport failure must never be read as "the data is gone".
        stats, rows = _Harness({"X": (None, "HTTP 503")}).run([_case("9.9")])
        self.assertEqual(stats["left_alone"], 1)
        self.assertEqual(rows[0]["ground_truth"]["expected"]["value"], "9.9")

    def test_every_case_is_written_back(self) -> None:
        cases = [_case("1.0", indicator="A"), _case("2.0", indicator="B"),
                 _case(None, indicator="C", case_type="negative")]
        stats, rows = _Harness({"A": (["1.0"], None), "B": (["9.0"], None),
                                "C": ([], None)}).run(cases)
        self.assertEqual(len(rows), 3)
        self.assertEqual(stats["cases"], 3)


if __name__ == "__main__":
    unittest.main()


class TestExpectationFollowsGroundTruth(unittest.TestCase):
    """A withdrawn series must also change what the case expects.

    Rewriting ground_truth.expected.value to null while leaving the top-level
    caseType "positive" and expectedBehavior "return_value" left each case
    contradicting itself. The grader reads the top-level fields, so a model
    that correctly abstained scored manual_review rather than pass -- and
    manual_review is a verdict nobody ever went back to review. Six withdrawn
    PV_CHLD_* cases quietly cost all seven measured models a point each.
    """

    def test_a_withdrawn_series_flips_the_expectation(self) -> None:
        stats, rows = _Harness({"X": ([], None)}).run([_case("74.3")])
        self.assertEqual(rows[0]["caseType"], "negative")
        self.assertEqual(rows[0]["expectedBehavior"], "abstain_no_data")
        self.assertEqual(stats["reclassified_to_abstain"], 1)

    def test_a_half_refreshed_manifest_is_repaired(self) -> None:
        """The value was nulled by an earlier pass but the expectation was not.

        This is the state the real manifest was found in, so the repair must
        not depend on the value changing in this pass.
        """
        stale = _case(None)
        stale["caseType"] = "positive"
        stale["expectedBehavior"] = "return_value"
        stats, rows = _Harness({"X": ([], None)}).run([stale])
        self.assertEqual(stats["value_changed"], 0)
        self.assertEqual(stats["reclassified_to_abstain"], 1)
        self.assertEqual(rows[0]["caseType"], "negative")
        self.assertEqual(rows[0]["expectedBehavior"], "abstain_no_data")

    def test_a_case_with_data_keeps_its_expectation(self) -> None:
        live = _case("5.0")
        live["expectedBehavior"] = "return_value"
        stats, rows = _Harness({"X": (["5.0"], None)}).run([live])
        self.assertEqual(rows[0]["caseType"], "positive")
        self.assertEqual(rows[0]["expectedBehavior"], "return_value")
        self.assertEqual(stats["reclassified_to_abstain"], 0)

    def test_throttled_cases_are_reported_not_hidden(self) -> None:
        """494 of 500 cases came back 429 and the summary said value_changed: 0.

        A refresh that checked nothing must not read like a refresh that
        confirmed everything, so the reason is surfaced per case.
        """
        stats, _ = _Harness({"X": (None, "HTTP 429")}).run([_case("5.0")])
        self.assertEqual(stats["left_alone"], 1)
        self.assertEqual(stats["left_alone_reasons"], {"HTTP 429": 1})


class TestVerificationIsRecorded(unittest.TestCase):
    """Every answered case records that the registry confirmed it.

    The grader cannot otherwise distinguish "the model abstained and a value
    demonstrably exists" from "the model abstained and nobody checked". That
    gap put 55 cases into manual_review across seven models and cost two
    rounds of verifying them by hand.
    """

    def test_an_unchanged_case_is_still_marked_verified(self) -> None:
        _, rows = _Harness({"X": (["5.0"], None)}).run([_case("5.0")])
        self.assertIs(rows[0]["ground_truth"]["verified_against_registry"], True)

    def test_a_changed_case_is_marked_verified(self) -> None:
        _, rows = _Harness({"X": (["9.0"], None)}).run([_case("5.0")])
        self.assertIs(rows[0]["ground_truth"]["verified_against_registry"], True)

    def test_a_withdrawn_case_is_marked_verified(self) -> None:
        _, rows = _Harness({"X": ([], None)}).run([_case("5.0")])
        self.assertIs(rows[0]["ground_truth"]["verified_against_registry"], True)

    def test_an_unreachable_case_is_not_marked_verified(self) -> None:
        """A 429 must never look like a confirmation."""
        _, rows = _Harness({"X": (None, "HTTP 429")}).run([_case("5.0")])
        self.assertNotIn("verified_against_registry", rows[0]["ground_truth"])
