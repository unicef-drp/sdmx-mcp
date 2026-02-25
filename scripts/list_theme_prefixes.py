#!/usr/bin/env python3
import argparse
import asyncio
import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import server


def _to_csv(rows: list[dict]) -> None:
    writer = csv.writer(sys.stdout)
    writer.writerow(["prefix", "count", "example_id", "example_name"])
    for row in rows:
        examples = row.get("examples") or []
        prefix = row.get("prefix", "")
        count = row.get("count", 0)
        if not examples:
            writer.writerow([prefix, count, "", ""])
            continue
        for item in examples:
            if not isinstance(item, dict):
                continue
            writer.writerow([prefix, count, item.get("id", ""), item.get("name", "")])


def _to_theme_map_template_csv(rows: list[dict]) -> None:
    writer = csv.writer(sys.stdout)
    writer.writerow(["prefix", "count", "example_id", "example_name", "domain"])
    for row in rows:
        examples = row.get("examples") or []
        prefix = row.get("prefix", "")
        count = row.get("count", 0)
        if not examples:
            writer.writerow([prefix, count, "", "", ""])
            continue
        for item in examples:
            if not isinstance(item, dict):
                continue
            writer.writerow([prefix, count, item.get("id", ""), item.get("name", ""), ""])


async def _run(limit: int, output_format: str) -> None:
    results = await server.list_theme_prefixes(limit=limit)
    if output_format == "csv":
        _to_csv(results)
    elif output_format == "theme-map-template":
        _to_theme_map_template_csv(results)
    else:
        print(json.dumps(results, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="List common SDMX dataflow id prefixes.")
    parser.add_argument("--limit", type=int, default=50, help="Max prefixes to return.")
    parser.add_argument(
        "--format",
        choices=["json", "csv", "theme-map-template"],
        default="json",
        help="Output format.",
    )
    args = parser.parse_args()
    asyncio.run(_run(args.limit, args.format))


if __name__ == "__main__":
    main()
