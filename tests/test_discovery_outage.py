"""A registry outage must not reach the model as "no such indicator".

find_indicator_candidates sweeps every scoped flow and skips the ones it cannot
read, which is right when one flow among many is broken. When the registry
itself was down, every fetch failed, every failure was skipped, and the tool
returned an empty list with no error. A model asked for "total population" was
told, in effect, that UNICEF holds no such indicator -- so it abstained, and an
eval run scored a string of honest nulls against a healthy ground truth.

Every other structure-reading tool surfaced the same 503 as an error. These
tests pin the three cases apart: nothing readable because of the transport
(raise), something readable (answer), and nothing readable for reasons that are
not the transport (empty, as before).
"""

import asyncio
import contextlib
import inspect
import unittest
from unittest.mock import patch

import httpx

import server

SCOPED_FLOWS = [
    {"id": "F1", "agencyID": "UNICEF", "version": "1.0", "name": "Flow one"},
    {"id": "F2", "agencyID": "UNICEF", "version": "1.0", "name": "Flow two"},
]


def _structure(code: str) -> dict:
    """A flow structure carrying one INDICATOR code."""
    return {
        "data": {
            "dataStructures": [
                {
                    "dataStructureComponents": {
                        "dimensionList": {
                            "dimensions": [
                                {
                                    "id": server.DIM_INDICATOR,
                                    "localRepresentation": {"enumeration": "CL_INDICATOR"},
                                }
                            ]
                        }
                    }
                }
            ],
            "codelists": [
                {
                    "id": "CL_INDICATOR",
                    "codes": [{"id": code, "name": "Neonatal mortality rate"}],
                }
            ],
        }
    }


def _503() -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://sdmx.example/dataflow")
    return httpx.HTTPStatusError(
        "Server error '503 Service Unavailable'",
        request=request,
        response=httpx.Response(503, request=request),
    )


class TestDiscoveryDuringOutage(unittest.TestCase):
    def _run(self, structures):
        """Call the real sweep with the dataflow list and per-flow results mocked."""

        async def fake_structure(flow_ref: str):
            result = structures[flow_ref]
            if isinstance(result, Exception):
                raise result
            return result

        async def go():
            # Unit annotation is a newer feature and is absent on branches that
            # predate it. This test is about outage behaviour, so it adapts
            # rather than requiring that feature to exist.
            patches = [
                patch.object(server, "_cached_dataflows", return_value={}),
                patch.object(server, "_extract_scoped_dataflows", return_value=SCOPED_FLOWS),
                patch.object(server, "_get_flow_structure", side_effect=fake_structure),
            ]
            if hasattr(server, "_annotate_units"):
                patches.append(patch.object(server, "_annotate_units", return_value=None))
            kwargs = {"query": "neonatal"}
            params = inspect.signature(server._find_indicator_candidates_impl).parameters
            if "includeUnits" in params:
                kwargs["includeUnits"] = False
            with contextlib.ExitStack() as stack:
                for p in patches:
                    stack.enter_context(p)
                return await server._find_indicator_candidates_impl(**kwargs)

        return asyncio.run(go())

    def test_total_transport_failure_raises(self) -> None:
        """No flow readable because of the transport: the caller must be told."""
        with self.assertRaises(httpx.HTTPStatusError):
            self._run({"UNICEF/F1/1.0": _503(), "UNICEF/F2/1.0": _503()})

    def test_one_readable_flow_still_answers(self) -> None:
        """The skip-one-bad-flow behaviour this guarded is preserved.

        Needs a working dimension policy, because scoring a candidate reads its
        stopword list. That policy is registry-specific and deliberately not
        shipped, so a checkout without one skips rather than fails -- the
        absence is a deployment decision, not a defect.
        """
        try:
            server._discovery_policy_config()
        except Exception as exc:  # noqa: BLE001 - any config failure is a skip
            self.skipTest(f"no discovery policy configured: {type(exc).__name__}")
        out = self._run(
            {"UNICEF/F1/1.0": _503(), "UNICEF/F2/1.0": _structure("CME_MRM0")}
        )
        self.assertEqual([item["id"] for item in out], ["CME_MRM0"])

    def test_non_transport_failures_still_return_empty(self) -> None:
        """A malformed structure is not an outage; an empty answer stays empty."""
        out = self._run(
            {"UNICEF/F1/1.0": ValueError("no codelist"), "UNICEF/F2/1.0": KeyError("codes")}
        )
        self.assertEqual(out, [])


if __name__ == "__main__":
    unittest.main()
