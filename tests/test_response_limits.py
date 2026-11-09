"""What a tool may send back: how much, and what it must say about itself.

A structure call on a large flow once returned 1.3 million characters. One
client refused it; our eval harness trimmed it and so hid it for 8,000 cases.
Nothing here may solve that by clipping quietly: a page says it is a page, and
"could not check" must never read as "no data".

All HTTP calls are mocked so these run without network access.
"""
import asyncio
import json
import os
import unittest
from unittest.mock import AsyncMock, patch

from fastmcp import Client
from fastmcp.exceptions import ToolError

import server


def _run(coro):
    return asyncio.run(coro)


def _structure(code_count: int = 3) -> dict:
    return {
        "meta": {"id": "x"},
        "data": {
            "dataStructures": [
                {
                    "dataStructureComponents": {
                        "dimensionList": {
                            "dimensions": [
                                {"id": "REF_AREA", "position": 1,
                                 "localRepresentation": {"enumeration": "CL_AREA"}},
                                {"id": "INDICATOR", "position": 2,
                                 "localRepresentation": {"enumeration": "CL_IND"}},
                            ]
                        }
                    }
                }
            ],
            "codelists": [
                {"id": "CL_AREA", "agencyID": "A", "version": "1.0", "name": "Areas",
                 "codes": [{"id": "EGY", "name": "Egypt"}, {"id": "FRA", "name": "France"}]},
                {"id": "CL_IND", "agencyID": "A", "version": "1.0", "name": "Indicators",
                 "codes": [{"id": f"I{n}", "name": "x" * 50} for n in range(code_count)]},
            ],
        },
    }


def _data_result(rows: int) -> dict:
    body = "".join(f"EGY,I{n},{n}\n" for n in range(rows))
    return {
        "status": "resolved",
        "provenance": {"key": "EGY"},
        "notes": {"maxObs": 100},
        "shaped": {"status": "resolved_single_value", "value": "1"},
        "raw_csv": "REF_AREA,INDICATOR,OBS_VALUE\n" + body,
    }


class TestFlowStructureCap(unittest.TestCase):
    def test_small_structure_is_untouched(self) -> None:
        payload = _structure()
        self.assertIs(server._cap_flow_structure(payload), payload)

    def test_large_structure_summarizes_codelists_without_touching_the_cache(self) -> None:
        payload = _structure(code_count=2000)
        with patch.object(server, "RESPONSE_CHAR_CAP", 5_000):
            capped = server._cap_flow_structure(payload)
        self.assertLessEqual(server._serialized_chars(capped), 5_000)
        self.assertTrue(capped["truncation"]["truncated"])
        self.assertEqual(capped["data"]["codelists"][1]["memberCount"], 2000)
        self.assertNotIn("codes", capped["data"]["codelists"][1])
        # Discovery reads this same cached object and needs the codes.
        self.assertEqual(len(payload["data"]["codelists"][1]["codes"]), 2000)
        self.assertNotIn("truncation", payload)


class TestRawCsvPaging(unittest.TestCase):
    def test_small_result_is_untouched(self) -> None:
        result = _data_result(3)
        self.assertIs(server._page_raw_csv(result), result)

    def test_a_page_says_it_is_a_page_and_stays_under_the_cap(self) -> None:
        with patch.object(server, "RESPONSE_CHAR_CAP", 3_000):
            page = server._page_raw_csv(_data_result(1_000))
        self.assertLessEqual(server._serialized_chars(page), 3_000)
        note = page["notes"]["responsePage"]
        self.assertEqual(note["totalRows"], 1_000)
        self.assertEqual(note["nextOffset"], note["returnedRows"])
        self.assertIn("Do not treat it as the whole result", page["notes"]["responsePageGuidance"])

    def test_paging_recovers_every_row_once(self) -> None:
        result = _data_result(1_000)
        seen: list[str] = []
        offset = 0
        with patch.object(server, "RESPONSE_CHAR_CAP", 3_000):
            while offset is not None:
                page = server._page_raw_csv(result, offset=offset)
                lines = page["raw_csv"].splitlines()
                self.assertEqual(lines[0], "REF_AREA,INDICATOR,OBS_VALUE")
                seen += lines[1:]
                offset = page["notes"]["responsePage"]["nextOffset"]
        self.assertEqual(seen, result["raw_csv"].splitlines()[1:])

    def test_a_wrapped_result_leaves_room_for_its_wrapper(self) -> None:
        """resolve_and_query_data once went 760 characters over and was refused."""
        with patch.object(server, "RESPONSE_CHAR_CAP", 3_000):
            page = server._page_raw_csv(_data_result(1_000), reserve=1_000)
        self.assertLessEqual(server._serialized_chars(page) + 1_000, 3_000)

    def test_shaped_answer_survives_paging(self) -> None:
        with patch.object(server, "RESPONSE_CHAR_CAP", 3_000):
            page = server._page_raw_csv(_data_result(1_000))
        self.assertEqual(page["shaped"]["value"], "1")

    def test_query_data_pages_at_the_tool_not_inside_execution(self) -> None:
        """Shaping reads raw_csv inside execution, so it must see every row."""
        with patch.object(server, "_execute_query_data", AsyncMock(return_value=_data_result(1_000))), \
                patch.object(server, "RESPONSE_CHAR_CAP", 3_000):
            page = _run(server.query_data("A/F/1.0", offset=10))
        self.assertEqual(page["notes"]["responsePage"]["offset"], 10)
        self.assertTrue(page["raw_csv"].splitlines()[1].startswith("EGY,I10,"))


class TestListCodesOffset(unittest.TestCase):
    def test_offset_pages_through_a_codelist(self) -> None:
        with patch.object(server, "_get_flow_structure", AsyncMock(return_value=_structure(code_count=5))):
            page = _run(server.list_codes("A/F/1.0", "INDICATOR", limit=2, offset=2, includeHierarchyHints=False))
        self.assertEqual([item["id"] for item in page], ["I2", "I3"])


class TestProvenanceStamp(unittest.TestCase):
    def _context(self) -> dict:
        return server._query_context(
            flow_details={"agencyID": "A", "flowID": "F", "version": "1.0",
                          "requestedFlowRef": "A/F/1.0", "resolvedFlowRef": "A/F/1.0"},
            key="EGY", query_url="https://x", dimension_order=[], format="csv", labels="id",
            startPeriod=None, endPeriod=None, lastNObservations=None, filters={},
        )

    def test_data_responses_say_when_and_by_what(self) -> None:
        provenance = self._context()["provenance"]
        self.assertRegex(provenance["retrievedAt"], r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$")
        self.assertEqual(provenance["serverVersion"], server.SERVER_VERSION)

    def test_no_licence_is_invented(self) -> None:
        with patch.dict(os.environ, {"SDMX_DATA_LICENCE": ""}):
            self.assertNotIn("licence", self._context()["provenance"])

    def test_configured_licence_reaches_the_compact_source(self) -> None:
        with patch.dict(os.environ, {"SDMX_DATA_LICENCE": "CC BY 4.0"}):
            source = server._compact_source(self._context())
        self.assertEqual(source["licence"], "CC BY 4.0")
        self.assertIn("retrievedAt", source)
        self.assertEqual(source["flowRef"], "A/F/1.0")


def _candidate(code: str, flow: str = "A/F/1.0") -> dict:
    return {
        "id": code, "name": f"Name {code}", "description": "",
        "recommendedFlowRef": flow,
        "recommendedFlow": {"flowRef": flow, "summary": "s" * 200},
        "dataflows": [{"flowRef": flow, "agencyID": "A", "flowID": "F", "flowName": "Flow",
                       "flowDescription": "d" * 200, "isCrossSectional": False,
                       "flowSummary": {"s": "s" * 200}}],
        "dataflowsNote": "n" * 100,
    }


class TestCompactSearch(unittest.TestCase):
    def test_candidate_keeps_what_the_next_call_needs(self) -> None:
        compact = server._compact_candidate({**_candidate("I1"), "unit": "%"})
        self.assertEqual(compact["recommendedFlowRef"], "A/F/1.0")
        self.assertEqual(compact["unit"], "%")
        self.assertEqual(compact["dataflows"], [{"flowRef": "A/F/1.0", "flowName": "Flow"}])
        self.assertNotIn("recommendedFlow", compact)

    def test_compaction_does_not_reach_the_candidate_it_was_given(self) -> None:
        """guided_discover picks flows from these fields inside the same request."""
        candidate = _candidate("I1")
        server._compact_candidate(candidate)
        self.assertIn("flowSummary", candidate["dataflows"][0])

    def test_guided_answer_drops_the_plan_it_already_acted_on(self) -> None:
        compact = server._compact_guided_discover(
            {"status": "resolved", "plan": {"indicatorCandidates": []}, "resolution": {"x": 1},
             "attemptHistory": [{"flowRef": "A/F/1.0", "flowSummary": {"s": 1}, "status": "resolved"}],
             "result": _data_result(2)})
        self.assertNotIn("plan", compact)
        self.assertNotIn("resolution", compact)
        self.assertEqual(compact["attemptHistory"], [{"flowRef": "A/F/1.0", "status": "resolved"}])

    def test_unresolved_guided_answer_keeps_the_plan(self) -> None:
        compact = server._compact_guided_discover(
            {"status": "unresolved", "plan": {"indicatorCandidates": [_candidate("I1")],
                                              "prioritizedDimensions": [1]}})
        self.assertEqual(compact["plan"]["indicatorCandidates"][0]["dataflows"][0]["flowName"], "Flow")
        self.assertNotIn("prioritizedDimensions", compact["plan"])


class TestAreaAvailability(unittest.TestCase):
    def setUp(self) -> None:
        server._area_indicator_cache.clear()
        self.addCleanup(server._area_indicator_cache.clear)
        base = patch.object(server, "BASE", "https://sdmx.example/rest")
        base.start()
        self.addCleanup(base.stop)

    def _held(self, status: int, text: str, area: str = "EGY"):
        with patch.object(server, "_get_flow_structure", AsyncMock(return_value=_structure())), \
                patch.object(server, "_dimension_order_for_flow", AsyncMock(return_value=["REF_AREA", "INDICATOR"])), \
                patch.object(server, "_policy_for_role", return_value=None), \
                patch.object(server, "_get_text_with_status", AsyncMock(return_value=(status, text))) as fetch:
            held = _run(server._indicators_held_for_area("A/F/1.0", area))
        return held, fetch

    def test_reads_series_keys_for_the_area(self) -> None:
        held, fetch = self._held(200, "REF_AREA,INDICATOR\nEGY,I1\nEGY,I2\n")
        self.assertEqual(held, {"I1", "I2"})
        self.assertIn("/data/A,F,1.0/EGY.?detail=serieskeysonly", fetch.call_args.args[0])

    def test_area_name_resolves_to_its_code(self) -> None:
        _, fetch = self._held(200, "REF_AREA,INDICATOR\nEGY,I1\n", area="egypt")
        self.assertIn("/EGY.?", fetch.call_args.args[0])

    def test_registry_saying_nothing_held_is_an_empty_set(self) -> None:
        body = "<com:Text>No data for data query against the dataflow: urn:x</com:Text>"
        self.assertEqual(self._held(404, body)[0], set())

    def test_a_404_for_any_other_reason_is_not_no_data(self) -> None:
        """Both of these arrive as 404 with SDMX error code 100."""
        body = '<message:ErrorMessage code="100"><com:Text>Data provider does not exist.</com:Text>'
        self.assertIsNone(self._held(404, body)[0])

    def test_a_throttle_is_not_no_data(self) -> None:
        self.assertIsNone(self._held(429, "Too many")[0])

    def test_an_outage_is_not_no_data(self) -> None:
        self.assertIsNone(self._held(503, "no healthy upstream")[0])

    def test_an_unrecognised_area_is_not_no_data(self) -> None:
        held, fetch = self._held(200, "", area="Atlantis")
        self.assertIsNone(held)
        fetch.assert_not_called()

    def _annotate(self, candidates, held):
        with patch.object(server, "_indicators_held_for_area", AsyncMock(return_value=held)):
            return _run(server._annotate_area_availability(candidates, "EGY"))

    def test_holders_come_first(self) -> None:
        candidates = [_candidate("I1"), _candidate("I2")]
        outcome, _ = self._annotate(candidates, {"I2"})
        self.assertEqual(outcome, "checked")
        self.assertEqual([c["id"] for c in candidates], ["I2", "I1"])
        self.assertIs(candidates[1]["hasDataForArea"], False)

    def test_failed_check_marks_nothing(self) -> None:
        candidates = [_candidate("I1")]
        outcome, _ = self._annotate(candidates, None)
        self.assertEqual(outcome, "unavailable")
        self.assertNotIn("hasDataForArea", candidates[0])

    def test_unread_flow_is_not_counted_as_absence(self) -> None:
        candidates = [_candidate("I1", flow=f"A/F{n}/1.0") for n in range(5)]
        with patch.object(server, "AREA_PROBE_FLOW_LIMIT", 2):
            outcome, checked = self._annotate(candidates, set())
        self.assertEqual(len(checked), 2)
        self.assertEqual(outcome, "checked")
        self.assertEqual(sum("hasDataForArea" not in c for c in candidates), 3)

    def test_find_candidates_is_never_empty_when_nothing_is_held(self) -> None:
        """An empty list reads as 'no such indicator', which is a different claim."""
        with patch.object(server, "_find_indicator_candidates_impl", AsyncMock(return_value=[_candidate("I1")])), \
                patch.object(server, "_indicators_held_for_area", AsyncMock(return_value=set())):
            out = _run(server.find_indicator_candidates("x", refArea="EGY"))
        self.assertEqual(out[0]["hasDataForArea"], False)
        self.assertIn("flows checked", out[0]["assistant_guidance"])

    def _plan(self, held):
        plan = {"question": "x", "indicatorCandidates": [_candidate("I1")], "prioritizedDimensions": [],
                "recommendedSequence": ["s"]}
        with patch.object(server, "_plan_topic_query_impl", AsyncMock(return_value=plan)), \
                patch.object(server, "_indicators_held_for_area", AsyncMock(return_value=held)):
            return _run(server.plan_topic_query("x", refArea="EGY"))

    def test_plan_stops_early_when_nothing_is_held(self) -> None:
        out = self._plan(set())
        self.assertEqual(out["status"], "no_data_in_checked_flows")
        self.assertNotIn("recommendedSequence", out)
        self.assertLess(server._serialized_chars(out), 1_000)

    def test_plan_carries_on_when_the_check_fails(self) -> None:
        out = self._plan(None)
        self.assertNotIn("status", out)
        self.assertEqual(out["areaCheck"]["outcome"], "unavailable")
        self.assertEqual(out["indicatorCandidates"][0]["id"], "I1")


class TestResponseSizeGuard(unittest.TestCase):
    def _call(self):
        async def go():
            async with Client(server.mcp) as client:
                return await client.call_tool("ping", {})
        return _run(go())

    def test_result_within_the_cap_passes(self) -> None:
        result = self._call()
        self.assertEqual(json.loads(result.content[0].text)["status"], "ok")

    def test_result_over_the_cap_is_an_error_not_a_clip(self) -> None:
        with patch.object(server, "RESPONSE_CHAR_CAP", 10):
            with self.assertRaises(ToolError) as caught:
                self._call()
        self.assertIn("response limit", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
