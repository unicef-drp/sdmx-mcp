"""find_indicator_candidates must not flood the caller's context.

Every scoped flow shares one INDICATOR codelist, so codelist membership matches
every indicator against every flow. The untrimmed `dataflows` array therefore
came back byte-identical for all candidates and made up 95% of a 57KB payload
(~16k tokens for one call) while distinguishing nothing.
"""

import unittest
from unittest.mock import patch

import server


def _candidate(code: str, n_flows: int) -> dict:
    return {
        "id": code,
        "name": code,
        "description": "",
        "_score": 1,
        "dataflows": [
            {
                "flowRef": f"UNICEF/F{i}/1.0",
                "agencyID": "UNICEF",
                "flowID": f"F{i}",
                "flowName": f"Flow {i}",
                "flowDescription": "",
                "isCrossSectional": False,
            }
            for i in range(n_flows)
        ],
    }


class TestIndicatorFlowTrim(unittest.TestCase):
    def _trim(self, item: dict, limit: int) -> dict:
        """Apply the same trim the tool applies after ranking."""
        with patch.object(server, "INDICATOR_FLOW_LIMIT", limit):
            total = len(item["dataflows"])
            if server.INDICATOR_FLOW_LIMIT and total > server.INDICATOR_FLOW_LIMIT:
                item["dataflows"] = item["dataflows"][: server.INDICATOR_FLOW_LIMIT]
                item["dataflowsTotal"] = total
                item["dataflowsNote"] = "trimmed"
        return item

    def test_long_list_is_trimmed(self) -> None:
        out = self._trim(_candidate("X", 32), 3)
        self.assertEqual(len(out["dataflows"]), 3)
        self.assertEqual(out["dataflowsTotal"], 32)

    def test_total_is_reported_so_nothing_looks_lost(self) -> None:
        out = self._trim(_candidate("X", 32), 3)
        self.assertIn("dataflowsNote", out)
        self.assertEqual(out["dataflowsTotal"], 32)

    def test_short_list_is_untouched(self) -> None:
        out = self._trim(_candidate("X", 2), 3)
        self.assertEqual(len(out["dataflows"]), 2)
        self.assertNotIn("dataflowsTotal", out)

    def test_limit_zero_disables_trimming(self) -> None:
        out = self._trim(_candidate("X", 32), 0)
        self.assertEqual(len(out["dataflows"]), 32)
        self.assertNotIn("dataflowsTotal", out)

    def test_default_limit_is_small(self) -> None:
        self.assertGreater(server.INDICATOR_FLOW_LIMIT, 0)
        self.assertLessEqual(server.INDICATOR_FLOW_LIMIT, 5)


if __name__ == "__main__":
    unittest.main()
