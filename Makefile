.PHONY: venv install collect analyze

venv:
\tpython -m venv .venv && . .venv/bin/activate && pip install -U pip

install:
\t. .venv/bin/activate && pip install -r requirements.txt

collect:
\t. .venv/bin/activate && python scripts/collect_cli.py --config configs/project.yaml

analyze:
\t. .venv/bin/activate && python scripts/analysis_cli.py --config configs/project.yaml
