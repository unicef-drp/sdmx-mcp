"""The sweep's arithmetic, which is what the comparison claim rests on.

Written because the first version of the summariser counted verdicts named
"passed"/"failed" while the runner writes "pass"/"fail". Every model scored
0.0% and the table looked plausible -- a wrong number, not a crash. These tests
pin the vocabulary to the runner's and make an unknown verdict loud.
"""

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "model_sweep",
    Path(__file__).resolve().parents[1] / "scripts" / "sdmx_eval_model_sweep.py",
)
sweep = importlib.util.module_from_spec(_spec)
sys.modules["model_sweep"] = sweep
_spec.loader.exec_module(sweep)


def _write(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")


def _case(stratum: str, i: int) -> str:
    return f'UNICEF/GLOBAL_DATAFLOW/1.0|{stratum}|2020|{{"REF_AREA": "KEN{i}"}}'


class TestSummarise(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _summary(self, grades, responses):
        g, r = self.dir / "g.jsonl", self.dir / "r.jsonl"
        _write(g, grades)
        _write(r, responses)
        return sweep.summarise("m", r, g)

    def test_runner_verdict_vocabulary_is_counted(self) -> None:
        grades = [
            {"case_id": _case("natural", 0), "overall": "pass"},
            {"case_id": _case("natural", 1), "overall": "pass"},
            {"case_id": _case("natural", 2), "overall": "fail"},
            {"case_id": _case("natural", 3), "overall": "manual_review"},
        ]
        resp = [{"status": "ok", "provider_output": {"estimated_cost_usd": 0.01,
                                                     "tool_trace": []}} for _ in grades]
        s = self._summary(grades, resp)
        self.assertEqual((s["passed"], s["failed"], s["manual_review"]), (2, 1, 1))
        self.assertAlmostEqual(s["pass_rate_strict"], 50.0)
        # Manual review is neutral in the lenient bound, not a pass.
        self.assertAlmostEqual(s["pass_rate_lenient"], 75.0)

    def test_unknown_verdict_is_loud(self) -> None:
        grades = [{"case_id": _case("natural", 0), "overall": "passed"}]
        with self.assertRaises(SystemExit) as caught:
            self._summary(grades, [{"status": "ok", "provider_output": {}}])
        self.assertIn("passed", str(caught.exception))

    def test_per_stratum_rates_are_independent(self) -> None:
        grades = [
            {"case_id": _case("prescriptive", 0), "overall": "pass"},
            {"case_id": _case("prescriptive", 1), "overall": "pass"},
            {"case_id": _case("ambiguous", 0), "overall": "fail"},
            {"case_id": _case("ambiguous", 1), "overall": "pass"},
        ]
        s = self._summary(grades, [{"status": "ok", "provider_output": {}} for _ in grades])
        self.assertEqual(s["by_stratum"]["prescriptive"]["pass"], 2)
        self.assertEqual(s["by_stratum"]["ambiguous"]["fail"], 1)

    def test_cost_and_tool_calls_come_from_the_responses(self) -> None:
        resp = [
            {"status": "ok", "provider_output": {"estimated_cost_usd": 0.02, "tool_trace": [
                {"type": "mcp_tool_use"}, {"type": "mcp_tool_result"}, {"type": "mcp_tool_use"}]}},
            {"status": "ok", "provider_output": {"estimated_cost_usd": 0.04, "tool_trace": [
                {"type": "mcp_tool_use"}]}},
        ]
        s = self._summary([{"case_id": _case("natural", i), "overall": "pass"}
                           for i in range(2)], resp)
        self.assertAlmostEqual(s["cost_usd"], 0.06)
        self.assertAlmostEqual(s["cost_per_case"], 0.03)
        self.assertAlmostEqual(s["avg_tool_calls"], 1.5)  # tool_result is not a call

    def test_provider_errors_are_surfaced_not_folded_into_the_rate(self) -> None:
        # A crashed case must not quietly read as a correct abstention.
        resp = [{"status": "ok", "provider_output": {}},
                {"status": "provider_error", "provider_output": {}}]
        s = self._summary([{"case_id": _case("natural", 0), "overall": "pass"}], resp)
        self.assertEqual(s["provider_errors"], 1)


if __name__ == "__main__":
    unittest.main()


class TestTornFinalLine(unittest.TestCase):
    """A partial append must not kill a sweep; real corruption still must.

    The runner appends from several workers, so a read immediately after it
    exits can catch the last line mid-write. That raised a bare
    JSONDecodeError and ended a sweep after seven models were already
    measured -- and the file was valid again moments later.
    """

    def _write(self, text: str) -> Path:
        path = Path(self.tmp.name) / "responses.jsonl"
        path.write_text(text, encoding="utf-8")
        return path

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def test_complete_file_reads_fully(self) -> None:
        path = self._write('{"a": 1}\n{"a": 2}\n')
        self.assertEqual(len(sweep._read_jsonl(path)), 2)

    def test_torn_final_line_is_skipped(self) -> None:
        path = self._write('{"a": 1}\n{"a": 2}\n{"a": 3, "b": ')
        rows = sweep._read_jsonl(path)
        self.assertEqual([r["a"] for r in rows], [1, 2])

    def test_corruption_mid_file_still_raises(self) -> None:
        """Lines following a bad one mean it was fully written, so it is real."""
        path = self._write('{"a": 1}\n{"a": 2, "b": \n{"a": 3}\n')
        with self.assertRaises(SystemExit) as caught:
            sweep._read_jsonl(path)
        self.assertIn("line 2", str(caught.exception))

    def test_missing_file_is_empty(self) -> None:
        self.assertEqual(sweep._read_jsonl(Path(self.tmp.name) / "nope.jsonl"), [])
