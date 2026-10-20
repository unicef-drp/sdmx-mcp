"""A transport failure must not reach the agent as an empty error message.

httpx raises ReadTimeout, ConnectError and friends with an EMPTY str(), and
the server had no handler for them, so one propagating produced a tool error
reading `Error calling tool 'get_single_observation': ` with nothing after the
colon. Neither the agent nor we could tell that from absent data.

Measured at 172 occurrences across 1,500 eval cases once the client issued 14
queries concurrently, and zero across 1,500 at six -- and the models measured
under the heavier load scored 22 points lower on the affected cases. A silent
infrastructure failure became an apparent model weakness.
"""

import unittest

import httpx

import server


class TestTransportFailureStatus(unittest.TestCase):
    def test_timeouts_map_to_gateway_timeout(self) -> None:
        for exc in (httpx.ConnectTimeout(""), httpx.ReadTimeout(""),
                    httpx.WriteTimeout(""), httpx.PoolTimeout("")):
            self.assertEqual(server._transport_failure_status(exc), 504, type(exc).__name__)

    def test_connect_failure_maps_to_unavailable(self) -> None:
        self.assertEqual(server._transport_failure_status(httpx.ConnectError("")), 503)

    def test_protocol_and_read_errors_map_to_bad_gateway(self) -> None:
        self.assertEqual(server._transport_failure_status(httpx.ReadError("")), 502)
        self.assertEqual(server._transport_failure_status(httpx.RemoteProtocolError("")), 502)

    def test_an_http_status_error_is_not_a_transport_failure(self) -> None:
        # That already carries a real status and is classified elsewhere;
        # swallowing it here would hide a 404.
        exc = httpx.HTTPStatusError("boom", request=None, response=None)
        self.assertIsNone(server._transport_failure_status(exc))

    def test_every_mapped_status_classifies_as_retryable(self) -> None:
        # The whole point is that the agent learns the query was not answered
        # rather than that the data is absent.
        for _, status in server._TRANSPORT_STATUS:
            name, retryable, message = server._upstream_failure_kind(status)
            self.assertTrue(retryable, status)
            self.assertEqual(name, "upstream_unavailable", status)
            self.assertTrue(message.strip())


class TestEmptyMessageNeverSurfaces(unittest.TestCase):
    def test_an_empty_exception_still_yields_a_message(self) -> None:
        # The root bug: str(ReadTimeout('')) is ''. The class name has to
        # stand in, or the agent is told nothing at all.
        self.assertEqual(str(httpx.ReadTimeout("")), "")
        for exc in (httpx.ReadTimeout(""), httpx.ConnectError("")):
            detail = str(exc).strip() or type(exc).__name__
            self.assertTrue(detail)
            self.assertIn(type(exc).__name__, detail)


if __name__ == "__main__":
    unittest.main()
