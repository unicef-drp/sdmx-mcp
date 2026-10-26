#!/usr/bin/env python3
"""Gather every final model run into one directory for stats and pooling.

The runs accumulated across several directories as the study grew, and the
analysis scripts should not have to know that history. This copies the final
responses and grades for each model into one place, preferring a retried run
over the original where both exist, and refuses to copy a pair whose lengths
disagree -- grades from one run beside responses from another silently reports
accuracy from one and cost from the other, which happened once with 500 graded
cases against 100 re-run responses.

Usage: python3 sdmx_eval_consolidate.py <dest-dir> <source-dir> [<source-dir> ...]

Later sources win, so list a retried run after the original it supersedes.
"""
import json
import shutil
import sys
from pathlib import Path




def _count(path: Path) -> int:
    with path.open() as fh:
        return sum(1 for line in fh if line.strip())


def main() -> int:
    if len(sys.argv) < 3:
        raise SystemExit(__doc__)
    dest = Path(sys.argv[1])
    sources = [Path(a) for a in sys.argv[2:]]
    dest.mkdir(parents=True, exist_ok=True)

    chosen: dict[str, tuple[Path, Path]] = {}
    for source in sources:
        if not source.is_dir():
            print(f"  (skipping absent {source})")
            continue
        for resp in sorted(source.glob("responses.*.jsonl")):
            if resp.name.endswith(".bak") or ".rep" in resp.name:
                continue
            slug = resp.name[len("responses."):-len(".jsonl")]
            grades = source / f"grades.{slug}.jsonl"
            if not grades.exists():
                print(f"  (skipping {slug} in {source.name}: no grades)")
                continue
            chosen[slug] = (resp, grades)

    if not chosen:
        raise SystemExit("no paired responses/grades found in any source")

    copied = 0
    for slug, (resp, grades) in sorted(chosen.items()):
        n_resp, n_grades = _count(resp), _count(grades)
        if n_resp != n_grades:
            raise SystemExit(
                f"{slug}: {n_grades} graded cases against {n_resp} responses in "
                f"{resp.parent.name}. These are different runs; re-grade before "
                "consolidating."
            )
        ids = set()
        with resp.open() as fh:
            for line in fh:
                if line.strip():
                    ids.add(json.loads(line)["case_id"])
        if len(ids) != n_resp:
            raise SystemExit(
                f"{slug}: {n_resp} rows but {len(ids)} distinct case_ids -- "
                "duplicate records would be graded twice."
            )
        shutil.copy(resp, dest / f"responses.{slug}.jsonl")
        shutil.copy(grades, dest / f"grades.{slug}.jsonl")
        print(f"  {slug:36s} {n_resp:4d} cases  <- {resp.parent.name}")
        copied += 1

    print(f"\n{copied} models consolidated into {dest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
