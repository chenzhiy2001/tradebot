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

just-run:
	.venv/bin/python strategy.py --resume

highprob:
	.venv/bin/python highprob.py

flow:
	.venv/bin/python flow.py

flow-resume:
	.venv/bin/python flow.py --resume

flow-dry:
	.venv/bin/python flow.py --dry-run

update-wallets:
	.venv/bin/python utility.py

# give full paths of strategy_log.txt, strategy_trades.json and data.json
strategy_logs_paths:
	@echo "Full paths:"
	@echo $(CURDIR)/strategy_log.txt
	@echo $(CURDIR)/strategy_trades.json
	@echo $(CURDIR)/data.json
	@echo $(CURDIR)/perf.json
	
flow_logs_paths:
	@echo "Full paths:"
	@echo $(CURDIR)/flow_log.txt
	@echo $(CURDIR)/flow_trades.json

clean:
	rm -rf .venv
