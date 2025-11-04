#!/usr/bin/env python3
import argparse, yaml
from ohaasa.collect import run_collect

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--limit_per_kw", type=int, default=None)  # 디버깅용
    args = ap.parse_args()
    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    run_collect(cfg, limit_per_kw=args.limit_per_kw)

if __name__ == "__main__":
    main()
