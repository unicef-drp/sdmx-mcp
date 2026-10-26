#!/usr/bin/env python3
"""How far does a published figure move when you re-run it unchanged?

Three identical runs per model over one case set, at the settings the headline
table uses. Reports four things:

  spread        -- max minus min accuracy across the repeats. This is the
                   honest error bar on any single published number.
  flipped       -- share of individual cases that did not return the same
                   verdict in all three runs. A model can post a stable
                   accuracy while churning underneath, and an office
                   re-running a specific query cares about the churn.
  error spread  -- provider-error rate across repeats, which is where a bad
                   serving route shows up first.
  routes        -- which inference hosts answered. For an open-weight model a
                   gateway picks a host per request, and precision, sampling
                   defaults and the tool-call parser all belong to the host.

The commercial model is the control: first-party, one route, named version. If
the open-weight models move markedly more than it does, the serving layer is
implicated rather than ordinary sampling noise.

Usage: python3 variance.py <run-dir>
"""
import collections
import glob
import json
import os
import re
import sys


def load(path):
    with open(path) as fh:
        return [json.loads(line) for line in fh if line.strip()]


def main() -> int:
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    run_dir = sys.argv[1]

    reps = collections.defaultdict(dict)       # slug -> rep -> {case: verdict}
    errs = collections.defaultdict(dict)       # slug -> rep -> error count
    routes = collections.defaultdict(collections.Counter)
    route_fail = collections.defaultdict(lambda: collections.defaultdict(lambda: [0, 0]))

    for grades_path in sorted(glob.glob(os.path.join(run_dir, "grades.*.rep*.jsonl"))):
        name = os.path.basename(grades_path)
        match = re.match(r"grades\.(.+)\.rep(\d+)\.jsonl$", name)
        if not match:
            continue
        slug, rep = match.group(1), int(match.group(2))
        resp_path = os.path.join(run_dir, f"responses.{slug}.rep{rep}.jsonl")
        if not os.path.exists(resp_path):
            continue
        grades = load(grades_path)
        responses = {r.get("case_id"): r for r in load(resp_path)}
        if len(grades) != len(responses):
            print(f"  (skipping {slug} rep{rep}: {len(grades)} grades vs "
                  f"{len(responses)} responses -- different runs)")
            continue
        reps[slug][rep] = {g["case_id"]: g["overall"] for g in grades}
        errs[slug][rep] = sum(
            1 for r in responses.values()
            if (r.get("provider_output") or {}).get("status") == "provider_error")
        for g in grades:
            served = ((responses.get(g["case_id"]) or {}).get("provider_output") or {}).get("served_by") or []
            for host in served:
                routes[slug][host] += 1
                slot = route_fail[slug][host]
                slot[1] += 1
                if g["overall"] == "pass":
                    slot[0] += 1

    if not reps:
        raise SystemExit(f"No paired repeat output in {run_dir}")

    print(f"{'model':30s} {'acc per repeat':>26s} {'spread':>7s} {'flipped':>8s} {'err/repeat':>14s}")
    print("-" * 92)
    report = {}
    for slug in sorted(reps, key=lambda s: -len(reps[s])):
        by_rep = reps[slug]
        if len(by_rep) < 2:
            print(f"{slug:30s}  only {len(by_rep)} repeat(s) -- skipped")
            continue
        accs = []
        for rep in sorted(by_rep):
            verdicts = by_rep[rep]
            accs.append(100 * sum(1 for v in verdicts.values() if v == "pass") / len(verdicts))
        shared = set.intersection(*(set(v) for v in by_rep.values()))
        flipped = sum(
            1 for case in shared
            if len({by_rep[rep][case] for rep in by_rep}) > 1)
        spread = max(accs) - min(accs)
        err_list = [errs[slug][rep] for rep in sorted(by_rep)]
        report[slug] = {
            "accuracies": [round(a, 1) for a in accs],
            "spread": round(spread, 1),
            "flipped": flipped,
            "cases": len(shared),
            "flipped_pct": round(100 * flipped / max(1, len(shared)), 1),
            "errors_per_repeat": err_list,
            "routes": dict(routes[slug]),
        }
        acc_str = " ".join(f"{a:5.1f}" for a in accs)
        print(f"{slug:30s} {acc_str:>26s} {spread:6.1f} "
              f"{flipped:4d}/{len(shared):3d} {str(err_list):>14s}")

    print()
    print("routes observed (requests served per host):")
    for slug in sorted(routes):
        hosts = ", ".join(f"{h}={n}" for h, n in routes[slug].most_common())
        print(f"  {slug:30s} {hosts or '(none captured)'}")

    interesting = {s: d for s, d in route_fail.items() if len(d) > 1}
    if interesting:
        print()
        print("pass rate by serving host (only models served by more than one):")
        for slug, hosts in sorted(interesting.items()):
            print(f"  {slug}")
            for host, (ok, n) in sorted(hosts.items(), key=lambda kv: -kv[1][1]):
                print(f"     {host:24s} {ok:4d}/{n:4d} = {100*ok/n:5.1f}%")

    dest = os.environ.get("SDMX_EVAL_OUT_VARIANCE") or os.path.join(run_dir, "variance.json")
    with open(dest, "w") as fh:
        json.dump(report, fh, indent=2)
    print()
    print(f"written to {dest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
