.PHONY: venv install clean run

venv:
	python3 -m venv .venv

install: venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install py_clob_client
	.venv/bin/pip install requests
run:
	.venv/bin/python utility.py
	.venv/bin/python strategy.py

update-wallets:
	.venv/bin/python utility.py

clean:
	rm -rf .venv
