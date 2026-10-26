"""Abstaining where a value exists is a miss; abstaining on stale truth is not.

The grader could not tell those apart, so every such case became
manual_review -- a verdict nobody reviews. 55 of them accumulated across seven
models, and resolving them by hand against the registry found 54 genuine
misses and one withdrawn series. Meanwhile the opposite error, failing an
abstention against ground truth that had quietly gone stale, is what penalised
all seven models on six withdrawn PV_CHLD_* series.

So the rule is narrow: a confirmed abstention-miss is a fail only when the
refresher has just verified that ground truth against the live registry.
Unverified truth still yields manual_review, deliberately.
"""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "runner", Path(__file__).resolve().parents[1] / "scripts" / "sdmx_eval_runner.py"
)
runner = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(runner)

CASE_ID = 'UNICEF/GLOBAL_DATAFLOW/1.0|natural|2022|{"INDICATOR": "CME_MRY0"}'


def _case(*, verified, value="16.93", case_type="positive"):
    ground_truth = {
        "status": "resolved",
        "expected": {"status": "deterministic", "value": value},
    }
    if verified:
        ground_truth["verified_against_registry"] = True
    return {
        "case_id": CASE_ID,
        "flowRef": "UNICEF/GLOBAL_DATAFLOW/1.0",
        "dimensionOrder": ["REF_AREA", "INDICATOR", "SEX"],
        "filters": {"REF_AREA": "JAM", "INDICATOR": "CME_MRY0", "SEX": "_T"},
        "dimensions": {
            "REF_AREA": {"id": "JAM", "name": "Jamaica"},
            "INDICATOR": {"id": "CME_MRY0", "name": "Infant mortality rate"},
            "SEX": {"id": "_T", "name": "Total"},
        },
        "timePeriod": "2022",
        "caseType": case_type,
        "expectedBehavior": "return_value",
        "ground_truth": ground_truth,
    }


def _abstention():
    """A well-formed response that looked, then declined to answer."""
    return {
        "case_id": CASE_ID,
        "status": "ok",
        "provider_name": "gateway",
        "provider_output": {
            "status": "ok",
            "answer_text": "value: null",
            "claims": {
                "value": None,
                "time_period": None,
                "flowRef": "UNICEF/GLOBAL_DATAFLOW/1.0",
                "filters": {"REF_AREA": "JAM", "INDICATOR": "CME_MRY0", "SEX": "_T"},
            },
            "tool_trace": [
                {
                    "type": "mcp_tool_use",
                    "name": "get_single_observation",
                    "input": {
                        "flowRef": "UNICEF/GLOBAL_DATAFLOW/1.0",
                        "filters": {
                            "REF_AREA": "JAM",
                            "INDICATOR": "CME_MRY0",
                            "SEX": "_T",
                        },
                    },
                }
            ],
        },
    }


def _grade(case, response):
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        (d / "m.jsonl").write_text(json.dumps(case) + "\n")
        (d / "r.jsonl").write_text(json.dumps(response) + "\n")
        summary = runner.grade_results(d / "m.jsonl", d / "r.jsonl", d / "g.jsonl", 1e-6)
        rows = [json.loads(l) for l in (d / "g.jsonl").read_text().splitlines() if l.strip()]
        return summary, rows[0]


class TestAbstentionAgainstVerifiedTruth(unittest.TestCase):
    def test_verified_value_makes_an_abstention_a_fail(self) -> None:
        summary, row = _grade(_case(verified=True), _abstention())
        self.assertEqual(row["overall"], "fail")
        self.assertEqual(summary["failed"], 1)
        self.assertEqual(summary["manual_review"], 0)

    def test_unverified_value_still_defers(self) -> None:
        """Without confirmation the case stays open rather than scoring a miss."""
        summary, row = _grade(_case(verified=False), _abstention())
        self.assertEqual(row["overall"], "manual_review")
        self.assertEqual(summary["failed"], 0)

    def test_a_withdrawn_series_is_not_a_miss(self) -> None:
        """Refreshing nulls the value and flips the case; abstaining then passes."""
        case = _case(verified=True, value=None, case_type="negative")
        case["expectedBehavior"] = "abstain_no_data"
        case["ground_truth"]["status"] = "no_data"
        summary, row = _grade(case, _abstention())
        self.assertEqual(row["overall"], "pass")
        self.assertEqual(summary["passed"], 1)

    def test_a_correct_answer_still_passes(self) -> None:
        answered = _abstention()
        answered["provider_output"]["answer_text"] = "value: 16.93; period: 2022"
        answered["provider_output"]["claims"]["value"] = "16.93"
        answered["provider_output"]["claims"]["time_period"] = "2022"
        summary, row = _grade(_case(verified=True), answered)
        self.assertEqual(row["overall"], "pass")
        self.assertEqual(summary["passed"], 1)


if __name__ == "__main__":
    unittest.main()
