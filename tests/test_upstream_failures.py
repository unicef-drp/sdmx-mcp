"""Upstream transport failures must be distinguishable from absent data.

A throttled registry (429) and a flow with no matching observations are
different answers. Before this, both surfaced as ``unresolved_from_official_flows``
with no HTTP status, so a rate-limited caller could not tell "retry this" from
"there is no such data" -- and an agent reading the response would confidently
report the latter.
"""

import unittest

import server


class TestUpstreamFailureKind(unittest.TestCase):
    def test_429_is_rate_limited_and_retryable(self) -> None:
        status, retryable, message = server._upstream_failure_kind(429)
        self.assertEqual(status, "rate_limited")
        self.assertTrue(retryable)
        self.assertIn("429", message)

    def test_5xx_is_retryable(self) -> None:
        for code in (500, 502, 503, 504):
            status, retryable, _ = server._upstream_failure_kind(code)
            self.assertEqual(status, "upstream_unavailable", code)
            self.assertTrue(retryable, code)

    def test_timeout_codes_are_retryable(self) -> None:
        for code in (408, 425):
            status, retryable, _ = server._upstream_failure_kind(code)
            self.assertEqual(status, "upstream_unavailable", code)
            self.assertTrue(retryable, code)

    def test_404_is_not_retryable(self) -> None:
        """A genuine miss must not be dressed up as a transient failure."""
        status, retryable, _ = server._upstream_failure_kind(404)
        self.assertEqual(status, "unresolved_from_official_flows")
        self.assertFalse(retryable)

    def test_400_is_not_retryable(self) -> None:
        status, retryable, _ = server._upstream_failure_kind(400)
        self.assertEqual(status, "unresolved_from_official_flows")
        self.assertFalse(retryable)

    def test_none_status_is_not_retryable(self) -> None:
        status, retryable, _ = server._upstream_failure_kind(None)
        self.assertEqual(status, "unresolved_from_official_flows")
        self.assertFalse(retryable)

    def test_retryable_message_denies_absence(self) -> None:
        """The message is what an agent repeats to the user."""
        for code in (429, 503):
            _, _, message = server._upstream_failure_kind(code)
            self.assertIn("does not mean the data is absent", message)


class TestCompactUnresolvedCarriesUpstreamStatus(unittest.TestCase):
    """The compact projection is where the status used to be dropped."""

    def _compact(self, status_code, status_name="unresolved_from_official_flows"):
        return server._compact_unresolved(
            {
                "status": status_name,
                "error": {"status": status_code, "message": "", "raw": ""},
            },
            shape="time_series",
        )

    def test_429_surfaces_http_status_and_retryable(self) -> None:
        out = self._compact(429, "rate_limited")
        self.assertEqual(out["status"], "rate_limited")
        self.assertEqual(out["httpStatus"], 429)
        self.assertTrue(out["retryable"])

    def test_404_surfaces_status_but_not_retryable(self) -> None:
        out = self._compact(404)
        self.assertEqual(out["httpStatus"], 404)
        self.assertFalse(out["retryable"])

    def test_missing_error_block_does_not_crash(self) -> None:
        out = server._compact_unresolved({"status": "no_observations"}, shape="single_observation")
        self.assertIsNone(out["httpStatus"])
        self.assertFalse(out["retryable"])
        self.assertEqual(out["shape"], "single_observation")

    def test_explicit_retryable_on_result_wins(self) -> None:
        """_unresolved_response sets retryable; the projection must not re-derive it away."""
        out = server._compact_unresolved(
            {"status": "rate_limited", "retryable": True, "error": {"status": None}},
            shape="time_series",
        )
        self.assertTrue(out["retryable"])

    def test_shape_and_null_value_preserved(self) -> None:
        out = self._compact(429, "rate_limited")
        self.assertEqual(out["shape"], "time_series")
        self.assertIsNone(out["value"])
        self.assertIn("source", out)


if __name__ == "__main__":
    unittest.main()
