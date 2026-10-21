#!/usr/bin/env python3
"""Back-fill claims from the terse answer line in existing response files.

The gateway adapter recovers a value stated only in the terse
"value: <v>; period: <p>" line, because the grader keys off the claims object
and some models fill one but not the other. Response files collected before
that existed -- or during the change -- would otherwise be scored on a
different rule than later ones, which would make a model comparison measure
the harness rather than the models.

This applies the adapter's own recovery function to files already on disk, so
every run is scored by one rule whenever it was collected. It is idempotent:
rows the adapter already recovered are unchanged, and a value the model itself
supplied is never touched.

    python3 scripts/sdmx_eval_normalise_claims.py tmp/sweep/responses.*.jsonl

Re-grade afterwards; the grades are what change.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location(
    "_gateway_provider", _HERE / "sdmx_eval_provider_gateway.py"
)
_gateway = importlib.util.module_from_spec(_spec)
sys.modules["_gateway_provider"] = _gateway
_spec.loader.exec_module(_gateway)


def normalise_file(path: Path, *, dry_run: bool = False) -> dict[str, int]:
    stats = {"rows": 0, "already": 0, "recovered": 0, "unchanged": 0}
    out: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        stats["rows"] += 1
        output = row.get("provider_output") or {}
        claims = output.get("claims")
        if not isinstance(claims, dict):
            stats["unchanged"] += 1
            out.append(json.dumps(row, ensure_ascii=True))
            continue
        if claims.get("recovered_from_answer_text"):
            stats["already"] += 1
            out.append(json.dumps(row, ensure_ascii=True))
            continue
        before = claims.get("value")
        merged = _gateway._recover_terse_claims(output.get("answer_text") or "", claims)
        if merged.get("value") != before:
            stats["recovered"] += 1
            # Keep the key present and false elsewhere so a grade can always be
            # traced to whether the value came from claims or the answer line.
            merged.setdefault("recovered_from_answer_text", True)
            output["claims"] = merged
            row["provider_output"] = output
        else:
            stats["unchanged"] += 1
        out.append(json.dumps(row, ensure_ascii=True))

    if not dry_run and stats["recovered"]:
        path.write_text("\n".join(out) + "\n", encoding="utf-8")
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("paths", nargs="+", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    for path in args.paths:
        if not path.exists():
            print(f"  {path.name}: missing, skipped")
            continue
        s = normalise_file(path, dry_run=args.dry_run)
        print(f"  {path.name}: {s['rows']} rows | recovered {s['recovered']} | "
              f"already {s['already']} | unchanged {s['unchanged']}")
    if args.dry_run:
        print("(dry run, nothing written)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
