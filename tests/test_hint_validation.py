"""Ambiguous hints must distinguish their code from its siblings.

The first hint map said "newborn deaths" for CME_MRM0. That is the neonatal
mortality RATE; "newborn deaths" denotes a count, which is CME_TMM0. The agent
read the English correctly and was graded wrong 10 times out of 75. The map was
checked for one-to-one mapping, but never for whether the mapping was the
obvious reading -- so the check is now in code rather than in a comment.
"""

import importlib.util
import sys
import unittest
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "builder",
    Path(__file__).resolve().parents[1] / "scripts" / "sdmx_eval_build_cases_from_sweep.py",
)
builder = importlib.util.module_from_spec(_spec)
sys.modules["builder"] = builder
_spec.loader.exec_module(builder)

NAMES = {
    "CME_MRM0": "Neonatal mortality rate",
    "CME_TMM0": "Neonatal deaths",
    "CME_MRY0": "Infant mortality rate",
    "CME_TMY0": "Infant deaths",
    "NT_ANT_HAZ_NE2": "Height-for-age <-2 SD (stunting)",
    "NT_ANT_HAZ_NE2_MOD": "Height-for-age <-2 SD (stunting), Modeled Estimates",
    "DM_POP_TOT": "Total population",
}


class TestValidateHints(unittest.TestCase):
    def test_count_phrase_for_a_rate_code_is_caught(self) -> None:
        """The exact bug: 10 graded failures that were the agent reading correctly."""
        warnings = builder.validate_hints(
            {"CME_MRM0": "newborn deaths in the first month of life"}, NAMES
        )
        self.assertEqual(len(warnings), 1)
        self.assertIn("CME_TMM0", warnings[0])

    def test_explicit_rate_phrase_passes(self) -> None:
        self.assertEqual(
            builder.validate_hints({"CME_MRM0": "the neonatal mortality rate"}, NAMES), []
        )

    def test_superset_sibling_cannot_be_distinguished(self) -> None:
        """_MOD's name contains every word the base name has, so no phrase works."""
        warnings = builder.validate_hints({"NT_ANT_HAZ_NE2": "child stunting"}, NAMES)
        self.assertEqual(len(warnings), 1)
        self.assertIn("NT_ANT_HAZ_NE2_MOD", warnings[0])

    def test_code_without_siblings_passes(self) -> None:
        self.assertEqual(builder.validate_hints({"DM_POP_TOT": "the total population"}, NAMES), [])

    def test_shipped_hints_are_all_valid(self) -> None:
        """Guards the real map, not just synthetic inputs."""
        self.assertEqual(builder.validate_hints(builder.AMBIGUOUS_HINTS, NAMES), [])


if __name__ == "__main__":
    unittest.main()
