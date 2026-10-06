"""Observation values must be written unambiguously, without changing them.

The registry serialises some values as ".2138" rather than "0.2138". Given
".213805064558983" for a sanitation indicator, the agent reported 21.38 --
multiplying by 100 because 0.21% looked implausible. It did the same on a
second case and not on a third, so the behaviour is inconsistent rather than
systematic, which is harder to defend against. Writing the leading zero is a
deterministic rule; asking a model not to rescale is a request it can ignore.
"""

import unittest

import server


class TestNormalizeObsValue(unittest.TestCase):
    def test_leading_dot_gains_a_zero(self) -> None:
        self.assertEqual(server._normalize_obs_value(".2138"), "0.2138")

    def test_the_value_that_was_rescaled(self) -> None:
        self.assertEqual(
            server._normalize_obs_value(".213805064558983"), "0.213805064558983"
        )

    def test_negative_leading_dot(self) -> None:
        self.assertEqual(server._normalize_obs_value("-.5"), "-0.5")

    def test_explicit_plus_is_kept(self) -> None:
        self.assertEqual(server._normalize_obs_value("+.25"), "+0.25")

    def test_surrounding_whitespace_is_handled(self) -> None:
        self.assertEqual(server._normalize_obs_value("  .25  "), "0.25")

    def test_precision_is_never_altered(self) -> None:
        """Only the representation changes -- no rounding, ever."""
        value = ".123456789012345678"
        self.assertEqual(server._normalize_obs_value(value), "0" + value)

    def test_already_normal_values_are_untouched(self) -> None:
        for value in ("0.2138", "21.38", "100", "0", "-3.5"):
            self.assertEqual(server._normalize_obs_value(value), value, value)

    def test_non_numeric_is_untouched(self) -> None:
        for value in ("", "_T", "abc", ".", "..3", "1e-5", ".5.5"):
            self.assertEqual(server._normalize_obs_value(value), value, repr(value))

    def test_non_strings_pass_through(self) -> None:
        for value in (None, 5, 0.25, True, [".2"], {"a": ".2"}):
            self.assertEqual(server._normalize_obs_value(value), value, repr(value))

    def test_numeric_equality_is_preserved(self) -> None:
        """The fidelity sweep compares numerically; normalisation must not move a value."""
        for value in (".2138", "-.5", ".000001", ".9999999999"):
            self.assertEqual(float(server._normalize_obs_value(value)), float(value), value)


if __name__ == "__main__":
    unittest.main()
