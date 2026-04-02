import unittest
import os
from unittest.mock import patch

import server


def _payload_with_subject_geo_and_time() -> dict:
    return {
        "structure": {
            "dataStructures": {
                "dataStructure": {
                    "TEST_DSD": {
                        "dataStructureComponents": {
                            "dimensionList": {
                                "dimensions": [
                                    {
                                        "id": "REF_AREA",
                                        "position": 1,
                                        "name": {"en": "Reference Area"},
                                        "localRepresentation": {"enumeration": {"id": "CL_GEO"}},
                                    },
                                    {
                                        "id": "SUBJECT",
                                        "position": 2,
                                        "name": {"en": "Subject"},
                                        "localRepresentation": {"enumeration": {"id": "CL_SUBJECT"}},
                                    },
                                    {
                                        "id": "TIME_PERIOD",
                                        "position": 3,
                                        "name": {"en": "Time Period"},
                                    },
                                ]
                            }
                        }
                    }
                }
            },
            "codelists": {
                "codelist": {
                    "CL_GEO": {
                        "id": "CL_GEO",
                        "name": {"en": "Geographic Areas"},
                        "codes": [
                            {"id": "AFG", "name": {"en": "Afghanistan"}},
                            {"id": "IND", "name": {"en": "India"}},
                            {"id": "PAK", "name": {"en": "Pakistan"}},
                        ],
                    },
                    "CL_SUBJECT": {
                        "id": "CL_SUBJECT",
                        "name": {"en": "Subjects"},
                        "codes": [
                            {"id": "U5MR", "name": {"en": "Under-five mortality rate"}},
                            {"id": "NMR", "name": {"en": "Neonatal mortality rate"}},
                        ],
                    },
                }
            },
        }
    }


class QueryDimensionPolicyTests(unittest.IsolatedAsyncioTestCase):
    def test_policy_file_is_auto_discovered_when_env_not_set(self) -> None:
        server._query_dimension_policy_config.cache_clear()
        with patch.dict(
            os.environ,
            {
                "SDMX_QUERY_DIMENSION_POLICY_FILE": "",
                "SDMX_QUERY_DIMENSION_POLICY_JSON": "",
            },
            clear=False,
        ):
            policy = server._query_dimension_policy_config()
        server._query_dimension_policy_config.cache_clear()
        self.assertTrue(any(entry.name == "location" for entry in policy.default_query_dimensions))

    async def test_query_plan_defaults_to_csv_name_labels_and_latest_observation(self) -> None:
        with patch.dict(os.environ, {"SDMX_DEFAULT_LAST_N_OBSERVATIONS": "TRUE"}, clear=False), patch(
            "server._resolved_flow_details", return_value={"resolvedFlowRef": "UNICEF/TEST_FLOW/1.0"}
        ), patch(
            "server._data_path_for", return_value="UNICEF,TEST_FLOW,1.0"
        ), patch("server._dimension_order_for_flow", return_value=["REF_AREA", "SUBJECT"]), patch(
            "server._normalize_filters_to_code_ids", return_value={"REF_AREA": "IND"}
        ):
            plan = await server._query_plan(
                flowRef="UNICEF,TEST_FLOW,1.0",
                filters={"REF_AREA": "IND"},
                key=None,
                startPeriod=None,
                endPeriod=None,
                lastNObservations=None,
                format="csv",
                labels=None,
                resultShape=None,
            )

        self.assertEqual(plan["format"], "csv")
        self.assertEqual(plan["labels"], "name")
        self.assertEqual(plan["lastNObservations"], 1)
        self.assertIn("lastNObservations=1", plan["queryURL"])
        self.assertIn("format=csv", plan["queryURL"])
        self.assertIn("labels=name", plan["queryURL"])

    async def test_query_plan_skips_default_last_n_when_env_disabled(self) -> None:
        with patch.dict(os.environ, {"SDMX_DEFAULT_LAST_N_OBSERVATIONS": "FALSE"}, clear=False), patch(
            "server._resolved_flow_details", return_value={"resolvedFlowRef": "UNICEF/TEST_FLOW/1.0"}
        ), patch("server._data_path_for", return_value="UNICEF,TEST_FLOW,1.0"), patch(
            "server._dimension_order_for_flow", return_value=["REF_AREA", "SUBJECT"]
        ), patch("server._normalize_filters_to_code_ids", return_value={"REF_AREA": "IND"}):
            plan = await server._query_plan(
                flowRef="UNICEF,TEST_FLOW,1.0",
                filters={"REF_AREA": "IND"},
                key=None,
                startPeriod=None,
                endPeriod=None,
                lastNObservations=None,
                format="csv",
                labels=None,
                resultShape=None,
            )

        self.assertEqual(plan["lastNObservations"], None)
        self.assertNotIn("lastNObservations=", plan["queryURL"])

    async def test_query_plan_allows_unbounded_time_for_compact_series(self) -> None:
        with patch("server._resolved_flow_details", return_value={"resolvedFlowRef": "UNICEF/TEST_FLOW/1.0"}), patch(
            "server._data_path_for", return_value="UNICEF,TEST_FLOW,1.0"
        ), patch("server._dimension_order_for_flow", return_value=["REF_AREA", "SUBJECT"]), patch(
            "server._normalize_filters_to_code_ids", return_value={"REF_AREA": "IND"}
        ):
            plan = await server._query_plan(
                flowRef="UNICEF,TEST_FLOW,1.0",
                filters={"REF_AREA": "IND"},
                key=None,
                startPeriod=None,
                endPeriod=None,
                lastNObservations=None,
                format="csv",
                labels=None,
                resultShape="compact_series",
            )

        self.assertEqual(plan["lastNObservations"], None)
        self.assertNotIn("lastNObservations=", plan["queryURL"])

    async def test_coded_dimension_resolution_uses_policy_codelist_source(self) -> None:
        payload = _payload_with_subject_geo_and_time()
        policy = server.QueryDimensionPolicyEntry(
            name="subject",
            role="subject",
            required_for_retrieval=True,
            priority=2,
            preferred_sources=[server.QueryDimensionSource(type="codelist", id="UNICEF/CL_SUBJECT/1.0")],
        )

        resolved = await server._resolve_coded_dimension_value("UNICEF,TEST_FLOW,1.0", payload, "Under-five mortality rate", policy)

        self.assertEqual(resolved["dimension_id"], "SUBJECT")
        self.assertEqual(resolved["values"], ["U5MR"])
        self.assertEqual(resolved["matches"][0]["source"], {"type": "codelist", "id": "UNICEF/CL_SUBJECT/1.0"})

    async def test_location_resolution_expands_hierarchy_members_when_enabled(self) -> None:
        payload = _payload_with_subject_geo_and_time()
        policy = server.QueryDimensionPolicyEntry(
            name="location",
            role="geography",
            required_for_retrieval=True,
            priority=1,
            preferred_sources=[
                server.QueryDimensionSource(type="codelist", id="UNICEF/CL_GEO/1.0"),
                server.QueryDimensionSource(type="hierarchical_codelist", id="UNICEF/UNICEF_REPORTING_REGIONS"),
            ],
            allow_hierarchy_resolution=True,
            allow_member_expansion=True,
        )
        hierarchy = {
            "id": "UNICEF_REPORTING_REGIONS",
            "nodes": [
                {"id": "SA", "name": "South Asia", "parent_id": None, "children": ["AFG", "IND", "PAK"]},
                {"id": "AFG", "name": "Afghanistan", "parent_id": "SA", "children": []},
                {"id": "IND", "name": "India", "parent_id": "SA", "children": []},
                {"id": "PAK", "name": "Pakistan", "parent_id": "SA", "children": []},
            ],
        }

        with patch("server._get_hierarchical_codelist_detail", return_value=hierarchy):
            resolved = await server._resolve_coded_dimension_value("UNICEF,TEST_FLOW,1.0", payload, "South Asia", policy)

        self.assertEqual(resolved["dimension_id"], "REF_AREA")
        self.assertEqual(resolved["values"], ["AFG", "IND", "PAK"])
        self.assertEqual(resolved["matches"][0]["match_type"], "hierarchy_node")

    async def test_location_resolution_can_keep_hierarchy_node_without_member_expansion(self) -> None:
        payload = _payload_with_subject_geo_and_time()
        policy = server.QueryDimensionPolicyEntry(
            name="location",
            role="geography",
            required_for_retrieval=True,
            priority=1,
            preferred_sources=[
                server.QueryDimensionSource(type="codelist", id="UNICEF/CL_GEO/1.0"),
                server.QueryDimensionSource(type="hierarchical_codelist", id="UNICEF/UNICEF_REPORTING_REGIONS"),
            ],
            allow_hierarchy_resolution=True,
            allow_member_expansion=False,
        )
        hierarchy = {
            "id": "UNICEF_REPORTING_REGIONS",
            "nodes": [
                {"id": "SA", "name": "South Asia", "parent_id": None, "children": ["AFG", "IND"]},
                {"id": "AFG", "name": "Afghanistan", "parent_id": "SA", "children": []},
                {"id": "IND", "name": "India", "parent_id": "SA", "children": []},
            ],
        }

        with patch("server._get_hierarchical_codelist_detail", return_value=hierarchy):
            resolved = await server._resolve_coded_dimension_value("UNICEF,TEST_FLOW,1.0", payload, "SA", policy)

        self.assertEqual(resolved["values"], ["SA"])
        self.assertEqual(resolved["matches"][0]["id"], "SA")

    async def test_resolution_order_follows_policy_priority_not_hardcoded_names(self) -> None:
        payload = _payload_with_subject_geo_and_time()
        policy = server.QueryDimensionPolicyConfig(
            default_query_dimensions=[
                server.QueryDimensionPolicyEntry(
                    name="location",
                    role="geography",
                    required_for_retrieval=True,
                    priority=1,
                    preferred_sources=[server.QueryDimensionSource(type="codelist", id="UNICEF/CL_GEO/1.0")],
                ),
                server.QueryDimensionPolicyEntry(
                    name="subject",
                    role="subject",
                    required_for_retrieval=True,
                    priority=2,
                    preferred_sources=[server.QueryDimensionSource(type="codelist", id="UNICEF/CL_SUBJECT/1.0")],
                ),
                server.QueryDimensionPolicyEntry(
                    name="time",
                    role="time",
                    required_for_retrieval=True,
                    priority=3,
                ),
            ]
        )

        with patch("server._get_flow_structure", return_value=payload), patch(
            "server._query_dimension_policy_config", return_value=policy
        ):
            resolved = await server._resolve_query_dimension_inputs(
                "UNICEF,TEST_FLOW,1.0",
                {
                    "geography": "India",
                    "subject": "U5MR",
                    "time_range": "2020",
                },
            )

        self.assertEqual(resolved["_resolution_order"], ["location", "subject", "time"])
        self.assertEqual(resolved["location"]["dimension_id"], "REF_AREA")
        self.assertEqual(resolved["subject"]["dimension_id"], "SUBJECT")
        self.assertEqual(resolved["time"]["dimension_id"], "TIME_PERIOD")

    async def test_latest_time_range_maps_to_latest_observation_mode(self) -> None:
        payload = _payload_with_subject_geo_and_time()
        policy = server.QueryDimensionPolicyEntry(
            name="time",
            role="time",
            required_for_retrieval=True,
            priority=3,
        )

        resolved = await server._resolve_time_value("UNICEF,TEST_FLOW,1.0", payload, "latest", policy)

        self.assertIsNone(resolved["startPeriod"])
        self.assertIsNone(resolved["endPeriod"])
        self.assertTrue(resolved["useLatestObservation"])

    async def test_chart_time_range_maps_to_all_observations_mode(self) -> None:
        payload = _payload_with_subject_geo_and_time()
        policy = server.QueryDimensionPolicyEntry(
            name="time",
            role="time",
            required_for_retrieval=True,
            priority=3,
        )

        resolved = await server._resolve_time_value("UNICEF,TEST_FLOW,1.0", payload, "chart", policy)

        self.assertIsNone(resolved["startPeriod"])
        self.assertIsNone(resolved["endPeriod"])
        self.assertTrue(resolved["useAllObservations"])
        self.assertFalse(resolved["useLatestObservation"])

    def test_codelist_key_supports_agency_id_version_format(self) -> None:
        self.assertEqual(server._codelist_key("UNICEF/CL_GEO/1.0"), "CL_GEO")

    def test_legacy_default_last_n_env_name_is_supported(self) -> None:
        with patch.dict(
            os.environ,
            {
                "SDMX_DEFAULT_LAST_N_OBSERVATIONS": "",
                "defaultLastNobservations": "FALSE",
            },
            clear=False,
        ):
            self.assertFalse(server._default_last_n_observations_enabled())


if __name__ == "__main__":
    unittest.main()
