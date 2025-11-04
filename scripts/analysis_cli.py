#!/usr/bin/env python3
import argparse, yaml
from ohaasa.analysis import run_analysis

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    run_analysis(cfg)

if __name__ == "__main__":
    main()
