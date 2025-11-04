#!/usr/bin/env python3
import argparse, yaml
from ohaasa.collect import run_collect

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    run_collect(cfg)

if __name__ == "__main__":
    main()
