.PHONY: venv install clean

PY := .venv/bin/python

venv:
	python3 -m venv .venv

install: venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install py_clob_client
	.venv/bin/pip install requests
	.venv/bin/pip install websockets
	.venv/bin/pip install polymarket-apis
	.venv/bin/pip install python-dotenv
	.venv/bin/pip install scipy
	.venv/bin/pip install numpy
	.venv/bin/pip install pandas

flow:
	$(PY) flow.py

flow-resume:
	$(PY) flow.py --resume

flow-dry:
	$(PY) flow.py --dry-run

# ── sniper ──────────────────────────────────────────────────────────
sniper:
	$(PY) sniper.py

# ── claimer ─────────────────────────────────────────────────────────
claim:
	$(PY) claimer.py

claim-loop:
	$(PY) claimer.py --loop 30

claim-batch:
	$(PY) claimer.py --batch 10

clean:
	rm -rf .venv
