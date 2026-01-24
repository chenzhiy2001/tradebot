.PHONY: venv install clean run

venv:
	python3 -m venv .venv

install: venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install py_clob_client

run:
	.venv/bin/python main.py

clean:
	rm -rf .venv
