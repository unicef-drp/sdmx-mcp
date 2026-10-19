"""Provider runs fan out, and nothing about the results depends on that.

A 500-case manifest took ~90 minutes one case at a time, which priced
multi-model comparison out. Cases are independent, so they now run several at
once -- but only if concurrency cannot reorder, drop or duplicate a result, and
only if an interrupted run still keeps what it finished.
"""

import asyncio
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "eval_runner",
    Path(__file__).resolve().parents[1] / "scripts" / "sdmx_eval_runner.py",
)
runner = importlib.util.module_from_spec(_spec)
sys.modules["eval_runner"] = runner
_spec.loader.exec_module(runner)


def _config(concurrency=None):
    provider = {
        "type": "command",
        "name": "stub",
        "command": ["true"],
    }
    if concurrency is not None:
        provider["concurrency"] = concurrency
    return {"provider": provider, "registry": {}, "mcp": {"url": "https://example.invalid/mcp"}}


def _manifest(path: Path, n: int) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for i in range(n):
            handle.write(json.dumps({"case_id": f"case-{i:03d}", "prompt": f"q{i}"}) + "\n")


class _Tracker:
    """Stands in for the provider subprocess and records overlap."""

    def __init__(self, delay=0.02):
        self.delay = delay
        self.in_flight = 0
        self.peak = 0
        self.seen = []

    async def __call__(self, *, command, env, payload):
        self.in_flight += 1
        self.peak = max(self.peak, self.in_flight)
        case_id = payload["case"]["case_id"]
        self.seen.append(case_id)
        try:
            await asyncio.sleep(self.delay)
        finally:
            self.in_flight -= 1
        return {"status": "ok", "answer_text": case_id, "claims": {"value": case_id}}


class TestProviderConcurrency(unittest.TestCase):
    def setUp(self) -> None:
        self._real = runner._run_command_provider
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.manifest = self.dir / "manifest.jsonl"
        self.responses = self.dir / "responses.jsonl"

    def tearDown(self) -> None:
        runner._run_command_provider = self._real
        self._tmp.cleanup()

    def _run(self, *, cases, concurrency):
        _manifest(self.manifest, cases)
        tracker = _Tracker()
        runner._run_command_provider = tracker
        summary = asyncio.run(runner.run_provider(
            config=_config(concurrency),
            manifest_path=self.manifest,
            responses_path=self.responses,
        ))
        rows = [json.loads(line) for line in self.responses.read_text().splitlines()]
        return summary, rows, tracker

    def test_every_case_is_written_exactly_once(self) -> None:
        summary, rows, _ = self._run(cases=25, concurrency=8)
        ids = [row["case_id"] for row in rows]
        self.assertEqual(summary["cases_written"], 25)
        self.assertEqual(len(ids), 25)
        self.assertEqual(len(set(ids)), 25, "a case was written twice")
        self.assertEqual(set(ids), {f"case-{i:03d}" for i in range(25)})

    def test_results_stay_matched_to_their_own_case(self) -> None:
        # The real hazard of fanning out: case A's answer landing on case B.
        _, rows, _ = self._run(cases=25, concurrency=8)
        for row in rows:
            self.assertEqual(row["provider_output"]["claims"]["value"], row["case_id"])

    def test_cases_actually_overlap(self) -> None:
        _, _, tracker = self._run(cases=25, concurrency=8)
        self.assertGreater(tracker.peak, 1, "cases ran one at a time")
        self.assertLessEqual(tracker.peak, 8, "concurrency ceiling was exceeded")

    def test_default_is_sequential(self) -> None:
        # Configs that predate this option -- the Anthropic one -- must keep
        # behaving exactly as they did, so earlier results stay comparable.
        _, _, tracker = self._run(cases=6, concurrency=None)
        self.assertEqual(tracker.peak, 1)

    def test_completed_cases_survive_an_interrupted_run(self) -> None:
        _manifest(self.manifest, 10)

        failing = _Tracker(delay=0)
        calls = {"n": 0}

        async def flaky(*, command, env, payload):
            calls["n"] += 1
            if calls["n"] > 4:
                raise RuntimeError("provider died")
            return await failing(command=command, env=env, payload=payload)

        runner._run_command_provider = flaky
        with self.assertRaises(RuntimeError):
            asyncio.run(runner.run_provider(
                config=_config(1),
                manifest_path=self.manifest,
                responses_path=self.responses,
            ))
        kept = [json.loads(l)["case_id"] for l in self.responses.read_text().splitlines()]
        self.assertEqual(len(kept), 4, "finished cases were lost on crash")

        # Resuming must pick up only what is missing.
        runner._run_command_provider = _Tracker(delay=0)
        summary = asyncio.run(runner.run_provider(
            config=_config(4),
            manifest_path=self.manifest,
            responses_path=self.responses,
        ))
        self.assertEqual(summary["cases_written"], 6)
        allids = [json.loads(l)["case_id"] for l in self.responses.read_text().splitlines()]
        self.assertEqual(len(set(allids)), 10)


if __name__ == "__main__":
    unittest.main()
