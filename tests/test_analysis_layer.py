"""The analysis layer must refuse to mix runs, and must not hardcode a study.

These four scripts turn graded output into the numbers a report quotes, so
their failure mode is a published figure rather than a crash. Two properties
matter most.

Grades from one run beside responses from another silently reports accuracy
from one and cost from the other -- caught once with 500 graded cases against
100 re-run responses. Every one of them refuses that pairing rather than
averaging across it.

And nothing may be hardcoded to one study: an earlier version carried a list of
six model slugs, so every model added afterwards was silently omitted from the
results it was supposed to summarise.
"""

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = {
    "consolidate": ROOT / "scripts" / "sdmx_eval_consolidate.py",
    "stats": ROOT / "scripts" / "sdmx_eval_stats.py",
    "pooled": ROOT / "scripts" / "sdmx_eval_pooled.py",
    "variance": ROOT / "scripts" / "sdmx_eval_variance.py",
}


def _load(name):
    spec = importlib.util.spec_from_file_location(f"analysis_{name}", SCRIPTS[name])
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _case_id(stratum="natural", period="2020", indicator="CME_MRM0"):
    return f'UNICEF/GLOBAL_DATAFLOW/1.0|{stratum}|{period}|{{"INDICATOR": "{indicator}"}}'


def _response(case_id, *, model="vendor/model-a", cost=0.001, tools=3, status="ok"):
    return {
        "case_id": case_id,
        "status": status,
        "provider_output": {
            "status": status,
            "model": model,
            "estimated_cost_usd": cost,
            "usage": {"input_tokens": 1000, "output_tokens": 20},
            "tool_trace": [{"type": "mcp_tool_use", "name": "get_single_observation"}] * tools,
        },
    }


def _grade(case_id, overall="pass"):
    return {"case_id": case_id, "overall": overall, "checks": {}}


def _write(path, rows):
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")


class AnalysisTempDir(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)

    def _model(self, slug, n=4, *, graded=None, model="vendor/model-a", tools=3):
        ids = [_case_id(period=str(2000 + i)) for i in range(n)]
        _write(self.dir / f"responses.{slug}.jsonl",
               [_response(i, model=model, tools=tools) for i in ids])
        _write(self.dir / f"grades.{slug}.jsonl",
               [_grade(i) for i in ids[:graded if graded is not None else n]])
        return ids


class TestStats(AnalysisTempDir):
    def test_models_are_discovered_not_listed(self) -> None:
        self._model("vendor-a", model="vendor/a")
        self._model("vendor-b", model="vendor/b")
        stats = _load("stats")
        rows = [stats.summarise(s, self.dir) for s in ("vendor-a", "vendor-b")]
        self.assertEqual(sorted(r["model"] for r in rows), ["vendor/a", "vendor/b"])

    def test_mismatched_lengths_refuse(self) -> None:
        self._model("vendor-a", n=4, graded=2)
        stats = _load("stats")
        with self.assertRaises(SystemExit) as caught:
            stats.summarise("vendor-a", self.dir)
        self.assertIn("different runs", str(caught.exception))

    def test_wilson_interval_stays_inside_the_unit_range(self) -> None:
        stats = _load("stats")
        low, high = stats.wilson(1.0, 500)
        self.assertGreaterEqual(low, 0.0)
        self.assertLessEqual(high, 100.0)
        self.assertEqual(stats.wilson(0.5, 0), (0.0, 0.0))


class TestConsolidate(AnalysisTempDir):
    def _run(self, dest, *sources):
        return subprocess.run(
            [sys.executable, "-I", str(SCRIPTS["consolidate"]), str(dest), *map(str, sources)],
            capture_output=True, text=True)

    def test_later_source_supersedes_earlier(self) -> None:
        first, second, dest = self.dir / "a", self.dir / "b", self.dir / "out"
        for d, cost in ((first, 0.001), (second, 0.002)):
            d.mkdir()
            ids = [_case_id(period=str(2000 + i)) for i in range(3)]
            _write(d / "responses.m.jsonl", [_response(i, cost=cost) for i in ids])
            _write(d / "grades.m.jsonl", [_grade(i) for i in ids])
        result = self._run(dest, first, second)
        self.assertEqual(result.returncode, 0, result.stderr)
        rows = [json.loads(l) for l in (dest / "responses.m.jsonl").read_text().splitlines() if l.strip()]
        self.assertEqual(rows[0]["provider_output"]["estimated_cost_usd"], 0.002)

    def test_mismatched_pair_refuses(self) -> None:
        src, dest = self.dir / "a", self.dir / "out"
        src.mkdir()
        ids = [_case_id(period=str(2000 + i)) for i in range(3)]
        _write(src / "responses.m.jsonl", [_response(i) for i in ids])
        _write(src / "grades.m.jsonl", [_grade(i) for i in ids[:2]])
        result = self._run(dest, src)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("different runs", result.stdout + result.stderr)

    def test_duplicate_case_ids_refuse(self) -> None:
        src, dest = self.dir / "a", self.dir / "out"
        src.mkdir()
        dup = _case_id()
        _write(src / "responses.m.jsonl", [_response(dup), _response(dup)])
        _write(src / "grades.m.jsonl", [_grade(dup), _grade(dup)])
        result = self._run(dest, src)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("distinct case_ids", result.stdout + result.stderr)


class TestPooledExclusion(AnalysisTempDir):
    def test_a_model_that_never_calls_a_tool_is_excluded_from_spread(self) -> None:
        """Otherwise it scores zero everywhere and every range becomes 100 pts."""
        self._model("engaged", n=4, tools=3)
        self._model("silent", n=4, tools=0)
        out = self.dir / "pooled.json"
        result = subprocess.run(
            [sys.executable, "-I", str(SCRIPTS["pooled"]), str(self.dir)],
            capture_output=True, text=True,
            env={**__import__("os").environ, "SDMX_EVAL_OUT_POOLED": str(out)})
        self.assertEqual(result.returncode, 0, result.stderr)
        data = json.loads(out.read_text())
        self.assertEqual(data["models"], 2)
        self.assertEqual(data["spread_models"], 1)
        self.assertEqual(data["spread_excluded"], ["silent"])

    def test_outputs_land_in_the_run_directory(self) -> None:
        self._model("engaged", n=4, tools=3)
        result = subprocess.run(
            [sys.executable, "-I", str(SCRIPTS["pooled"]), str(self.dir)],
            capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue((self.dir / "pooled.json").exists())


if __name__ == "__main__":
    unittest.main()
