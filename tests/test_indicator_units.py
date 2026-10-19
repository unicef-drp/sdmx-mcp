"""Units on discovery results, which is what separates sibling indicators.

Discovery used to return id, name and description only. For a pair like
CME_MRM0 "Neonatal mortality rate" and CME_TMM0 "Neonatal deaths" the agent
had to pick one, query it, and only then learn from the observation that it had
chosen a count. UNIT_MEASURE is an observation-level attribute, so it has to be
probed from data and cached rather than read from structure.
"""

import asyncio
import csv
import io
import unittest
from unittest import mock

import server


HEADER = (
    "REF_AREA,Geographic area,INDICATOR,Indicator,SEX,Sex,TIME_PERIOD,OBS_VALUE,"
    "UNIT_MULTIPLIER,Unit multiplier,UNIT_MEASURE,Unit of measure\n"
)


def _row(indicator, unit_code, unit_label, mult_code="", mult_label="", area="ETH"):
    # Written through the csv module: real unit labels contain commas
    # ("Deaths per 1,000 live births"), which the registry quotes.
    buffer = io.StringIO()
    csv.writer(buffer, lineterminator="\n").writerow(
        [area, "Ethiopia", indicator, "Some name", "_T", "Total", "2020", "1.0",
         mult_code, mult_label, unit_code, unit_label]
    )
    return buffer.getvalue()


class TestLabelColumnPairing(unittest.TestCase):
    """labels=both emits the code column followed by its human label."""

    def test_label_column_follows_its_code_column(self) -> None:
        fields = ["INDICATOR", "Indicator", "UNIT_MEASURE", "Unit of measure"]
        self.assertEqual(server._label_column_for(fields, "UNIT_MEASURE"), "Unit of measure")

    def test_another_code_column_is_not_mistaken_for_a_label(self) -> None:
        # Without labels=both there is no label column; returning the next code
        # column would put "OBS_STATUS" in the unit field.
        fields = ["INDICATOR", "UNIT_MEASURE", "OBS_STATUS"]
        self.assertIsNone(server._label_column_for(fields, "UNIT_MEASURE"))

    def test_missing_or_trailing_code_column(self) -> None:
        self.assertIsNone(server._label_column_for(["A", "B"], "UNIT_MEASURE"))
        self.assertIsNone(server._label_column_for(["INDICATOR", "UNIT_MEASURE"], "UNIT_MEASURE"))


class TestParseUnitRows(unittest.TestCase):
    def test_label_is_preferred_over_the_code(self) -> None:
        # "D_PER_1000_B" tells an agent far less than the label does.
        out = server._parse_unit_rows(HEADER + _row("CME_MRM0", "D_PER_1000_B", "Deaths per 1,000 live births"))
        self.assertEqual(out["CME_MRM0"]["unit"], "Deaths per 1,000 live births")
        self.assertEqual(out["CME_MRM0"]["unitCode"], "D_PER_1000_B")

    def test_the_rate_and_count_siblings_come_back_distinct(self) -> None:
        text = HEADER + _row("CME_MRM0", "D_PER_1000_B", "Deaths per 1,000 live births") \
                      + _row("CME_TMM0", "D", "Number of deaths")
        out = server._parse_unit_rows(text)
        self.assertEqual(out["CME_MRM0"]["unit"], "Deaths per 1,000 live births")
        self.assertEqual(out["CME_TMM0"]["unit"], "Number of deaths")

    def test_first_row_per_indicator_wins(self) -> None:
        text = HEADER + _row("X", "PCNT", "%") + _row("X", "D", "Number of deaths", area="IND")
        self.assertEqual(server._parse_unit_rows(text)["X"]["unit"], "%")

    def test_zero_multiplier_is_omitted(self) -> None:
        # 10^0 is no scaling; reporting it would invite an agent to rescale.
        out = server._parse_unit_rows(HEADER + _row("X", "PCNT", "%", "0", ""))
        self.assertNotIn("unitMultiplier", out["X"])

    def test_real_multiplier_is_carried(self) -> None:
        # A population written 56.084 with multiplier Thousands is exactly the
        # case an agent gets wrong.
        out = server._parse_unit_rows(HEADER + _row("DM_POP_TOT", "PS", "Persons", "3", "Thousands"))
        self.assertEqual(out["DM_POP_TOT"]["unitMultiplier"], "Thousands")

    def test_rows_without_a_unit_are_skipped_not_blanked(self) -> None:
        out = server._parse_unit_rows(HEADER + _row("X", "", ""))
        self.assertEqual(out, {})

    def test_empty_input(self) -> None:
        self.assertEqual(server._parse_unit_rows(""), {})


class TestIndicatorUnitsCaching(unittest.TestCase):
    def setUp(self) -> None:
        server._indicator_unit_cache.clear()

    def tearDown(self) -> None:
        server._indicator_unit_cache.clear()

    def test_a_miss_is_cached_so_it_is_not_reprobed(self) -> None:
        # An indicator with no observations anywhere would otherwise be probed
        # again on every discovery call.
        calls = []

        async def fake_probe(flow_ref, indicators):
            calls.append(list(indicators))
            return {}

        with mock.patch.object(server, "_probe_indicator_units", fake_probe):
            asyncio.run(server._indicator_units("F", ["NO_DATA"]))
            asyncio.run(server._indicator_units("F", ["NO_DATA"]))
        self.assertEqual(len(calls), 1, "a cached miss was probed twice")

    def test_only_uncached_indicators_are_probed(self) -> None:
        calls = []

        async def fake_probe(flow_ref, indicators):
            calls.append(list(indicators))
            return {code: {"unit": "%"} for code in indicators}

        with mock.patch.object(server, "_probe_indicator_units", fake_probe):
            asyncio.run(server._indicator_units("F", ["A", "B"]))
            asyncio.run(server._indicator_units("F", ["B", "C"]))
        self.assertEqual(calls, [["A", "B"], ["C"]])

    def test_the_same_code_in_two_flows_is_cached_separately(self) -> None:
        async def fake_probe(flow_ref, indicators):
            return {code: {"unit": flow_ref} for code in indicators}

        with mock.patch.object(server, "_probe_indicator_units", fake_probe):
            a = asyncio.run(server._indicator_units("FLOW_A", ["X"]))
            b = asyncio.run(server._indicator_units("FLOW_B", ["X"]))
        self.assertEqual(a["X"]["unit"], "FLOW_A")
        self.assertEqual(b["X"]["unit"], "FLOW_B")

    def test_duplicates_are_requested_once(self) -> None:
        calls = []

        async def fake_probe(flow_ref, indicators):
            calls.append(list(indicators))
            return {}

        with mock.patch.object(server, "_probe_indicator_units", fake_probe):
            asyncio.run(server._indicator_units("F", ["A", "A", ""]))
        self.assertEqual(calls, [["A"]])


class TestAnnotateUnitsIsBestEffort(unittest.TestCase):
    def setUp(self) -> None:
        server._indicator_unit_cache.clear()

    def test_a_probe_failure_leaves_the_candidates_usable(self) -> None:
        # Candidates are still worth returning without a unit, so discovery
        # must not fail because an annotation did.
        items = [{"id": "CME_MRM0", "name": "Neonatal mortality rate"}]

        async def boom(flow_ref, indicators):
            raise RuntimeError("registry down")

        with mock.patch.object(server, "_indicator_units", boom):
            asyncio.run(server._annotate_units("F", items, include_units=True))
        self.assertEqual(items[0]["name"], "Neonatal mortality rate")
        self.assertNotIn("unit", items[0])

    def test_opting_out_does_no_lookup(self) -> None:
        called = False

        async def fake(flow_ref, indicators):
            nonlocal called
            called = True
            return {}

        with mock.patch.object(server, "_indicator_units", fake):
            asyncio.run(server._annotate_units("F", [{"id": "X"}], include_units=False))
        self.assertFalse(called)

    def test_annotation_is_capped(self) -> None:
        items = [{"id": f"I{n}"} for n in range(server.UNIT_ANNOTATE_LIMIT + 5)]
        seen: list[str] = []

        async def fake(flow_ref, indicators):
            seen.extend(indicators)
            return {}

        with mock.patch.object(server, "_indicator_units", fake):
            asyncio.run(server._annotate_units("F", items, include_units=True))
        self.assertEqual(len(seen), server.UNIT_ANNOTATE_LIMIT)


if __name__ == "__main__":
    unittest.main()
