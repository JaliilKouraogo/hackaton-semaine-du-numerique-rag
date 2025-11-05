#!/usr/bin/env python3
"""Generate a robots report JSONL for a list of seeds.

Reads a hard-coded seed list (same as run_crawl.ps1) and calls
scripts/check_robots.py for each seed, writing one JSON object per line
into data/robots_report.jsonl.

Usage: python scripts/generate_robots_report.py --out data/robots_report.jsonl
"""
from __future__ import annotations
import argparse
from pathlib import Path
import importlib.util
import sys

SEEDS = [
    "https://www.sig.gov.bf/",
    "https://www.presidencedufaso.bf/",
    "https://www.finances.gov.bf/",
    "https://dgi.bf/",
    "https://www.jobf.gov.bf/",
    "https://servicepublic.gov.bf/",
    "https://www.commerce.gov.bf/",
    "https://www.investburkina.com/",
    "https://www.insd.bf/",
    "https://douanes.bf/",
    "https://www.brvm.org/",
    "https://lefaso.net/",
    "https://burkina24.com/",
    "https://www.sidwaya.info/",
    "https://www.aib.media/",
    "https://esintax.bf/",
]


def load_check_module():
    # load scripts/check_robots.py as a module without executing main()
    path = Path(__file__).parent / "check_robots.py"
    spec = importlib.util.spec_from_file_location("check_robots", str(path))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def run_check(mod, url: str) -> str:
    try:
        base = mod.canonical_base(url)
    except Exception as e:
        return '{"base_url": "%s", "error": "invalid url: %s"}' % (url, str(e))

    res = mod.analyze(base, "/", "DataCollectorBot/1.0")
    import json
    return json.dumps(res, ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="data/robots_report.jsonl")
    args = parser.parse_args()
    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    mod = load_check_module()
    with outp.open("w", encoding="utf-8") as f:
        for s in SEEDS:
            print(f"Checking {s}")
            txt = run_check(mod, s)
            if not txt:
                f.write('{"base_url": "%s", "error": "no output"}\n' % s)
            else:
                single = " ".join(l.strip() for l in txt.splitlines())
                f.write(single + "\n")

    print("Wrote", outp)


if __name__ == "__main__":
    main()
