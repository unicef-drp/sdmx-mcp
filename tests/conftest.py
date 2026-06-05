"""pytest configuration: clear registry-specific env vars so tests run against defaults.

server.py calls load_dotenv() at import time, which re-populates env vars from .env.
We import server first, then remove the registry-specific policy-file vars and clear the
lru_cache on the policy loader so tests fall back to the built-in default policy instead
of failing with FileNotFoundError when the registry-specific policy files are absent.
"""
import os
import server  # noqa: F401  — triggers load_dotenv before we remove the vars

os.environ.pop("SDMX_QUERY_DIMENSION_POLICY_FILE", None)
os.environ.pop("SDMX_DISCOVERY_POLICY_FILE", None)
server._query_dimension_policy_config.cache_clear()
