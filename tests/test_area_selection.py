"""Country-level sweeps must exclude aggregates on any registry, not just ISO3 ones.

The original filter hardcoded an ISO3 regex. That works for UNICEF, whose
CL_COUNTRY mixes 235 ISO3 countries with 224 differently-shaped aggregates, but
fails completely under M49: 004 is Afghanistan, 001 is World, 002 is Africa.
Both are three-digit numeric, so no pattern separates them and selection has to
be by membership.
"""

import importlib.util
import sys
import unittest
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "sweep", Path(__file__).resolve().parents[1] / "scripts" / "mcp_fidelity_sweep.py"
)
sweep = importlib.util.module_from_spec(_spec)
# dataclass(slots=True) resolves annotations via sys.modules, so register the
# module before executing it or the decorator raises on import.
sys.modules["sweep"] = sweep
_spec.loader.exec_module(sweep)

ISO3_KNOWN = {"KEN", "UGA", "FAO_GLOBAL", "UNDEV_002"}
M49_COUNTRIES = {"004", "008", "012"}
M49_ALL = M49_COUNTRIES | {"001", "002", "015"}


class TestIso3Default(unittest.TestCase):
    def setUp(self) -> None:
        self.scope = sweep.Scope()

    def test_country_kept(self) -> None:
        self.assertTrue(self.scope.area_ok("KEN", ISO3_KNOWN))

    def test_differently_shaped_aggregates_dropped(self) -> None:
        self.assertFalse(self.scope.area_ok("FAO_GLOBAL", ISO3_KNOWN))
        self.assertFalse(self.scope.area_ok("UNDEV_002", ISO3_KNOWN))

    def test_code_outside_the_codelist_dropped(self) -> None:
        self.assertFalse(self.scope.area_ok("ZZZ", ISO3_KNOWN))


class TestM49(unittest.TestCase):
    def test_pattern_alone_cannot_separate_m49(self) -> None:
        """Documents why membership is required, not a nicety."""
        scope = sweep.Scope(area_pattern=r"\d{3}")
        self.assertTrue(scope.area_ok("004", M49_ALL))   # Afghanistan
        self.assertTrue(scope.area_ok("001", M49_ALL))   # World -- leaks through
        self.assertTrue(scope.area_ok("002", M49_ALL))   # Africa -- leaks through

    def test_membership_separates_m49(self) -> None:
        scope = sweep.Scope(area_pattern="")
        self.assertTrue(scope.area_ok("004", M49_COUNTRIES))
        self.assertFalse(scope.area_ok("001", M49_COUNTRIES))
        self.assertFalse(scope.area_ok("002", M49_COUNTRIES))

    def test_empty_pattern_still_enforces_membership(self) -> None:
        scope = sweep.Scope(area_pattern="")
        self.assertFalse(scope.area_ok("999", M49_COUNTRIES))


class TestIncludeAggregates(unittest.TestCase):
    def test_everything_kept(self) -> None:
        scope = sweep.Scope(countries_only=False)
        for code in ("001", "FAO_GLOBAL", "KEN", "anything"):
            self.assertTrue(scope.area_ok(code, set()), code)


class TestLoadAreaList(unittest.TestCase):
    def test_reads_codes_ignoring_blanks_and_comments(self) -> None:
        import tempfile

        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as handle:
            handle.write("# M49 countries\n004\n\n008  # Albania\n012\n")
            path = handle.name
        self.assertEqual(sweep.load_area_list(path), {"004", "008", "012"})

    def test_empty_file_is_refused(self) -> None:
        import tempfile

        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as handle:
            handle.write("# nothing but comments\n")
            path = handle.name
        with self.assertRaises(SystemExit):
            sweep.load_area_list(path)


if __name__ == "__main__":
    unittest.main()
