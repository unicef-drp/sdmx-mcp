"""The same flow written two ways must grade as the same flow.

SDMX writes a flow reference as "UNICEF/GLOBAL_DATAFLOW/1.0" canonically and
"UNICEF,GLOBAL_DATAFLOW,1.0" in a REST path. The MCP returns query URLs
containing the comma form, so a model that reports what it saw gets marked as
a routing failure even with an exactly correct value -- observed once in a
500-case run, on an answer that was otherwise perfect.
"""

import importlib.util
import sys
import unittest
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "eval_runner_flow",
    Path(__file__).resolve().parents[1] / "scripts" / "sdmx_eval_runner.py",
)
runner = importlib.util.module_from_spec(_spec)
sys.modules["eval_runner_flow"] = runner
_spec.loader.exec_module(runner)

N = runner._normalize_flow_ref


class TestNormalizeFlowRef(unittest.TestCase):
    def test_comma_and_slash_forms_agree(self) -> None:
        self.assertEqual(N("UNICEF,GLOBAL_DATAFLOW,1.0"), N("UNICEF/GLOBAL_DATAFLOW/1.0"))

    def test_surrounding_whitespace_and_slashes_are_ignored(self) -> None:
        self.assertEqual(N("  UNICEF/GLOBAL_DATAFLOW/1.0/ "), N("UNICEF,GLOBAL_DATAFLOW,1.0"))

    def test_genuinely_different_flows_still_differ(self) -> None:
        # The fix must not make every flow compare equal.
        self.assertNotEqual(N("UNICEF,GLOBAL_DATAFLOW,1.0"), N("UNICEF,CME,1.0"))
        self.assertNotEqual(N("UNICEF,GLOBAL_DATAFLOW,1.0"), N("UNITED,GLOBAL_DATAFLOW,1.0"))
        self.assertNotEqual(N("UNICEF,GLOBAL_DATAFLOW,1.0"), N("UNICEF,GLOBAL_DATAFLOW,2.0"))

    def test_missing_values_normalise_to_empty_not_to_each_other(self) -> None:
        self.assertEqual(N(None), "")
        self.assertEqual(N(""), "")
        self.assertNotEqual(N(None), N("UNICEF/GLOBAL_DATAFLOW/1.0"))


if __name__ == "__main__":
    unittest.main()
