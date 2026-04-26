.PHONY: venv install run run-2024 run-web clean

PYTHON := .venv/bin/python
PIP := PIP_DISABLE_PIP_VERSION_CHECK=1 $(PYTHON) -m pip

venv:
	python -m venv .venv

install: venv
	$(PIP) install -r requirements.txt

run: venv
	$(PYTHON) frost_depth.py

run-2024: venv
	$(PYTHON) frost_depth.py --start 2024-07-01 --end 2025-06-30 --winter 2024-2025

run-web: venv
	$(PYTHON) webapp.py

clean:
	rm -rf output
