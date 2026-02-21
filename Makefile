.PHONY: venv install clean run

venv:
	python3 -m venv .venv

install: venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install py_clob_client
	.venv/bin/pip install requests
	.venv/bin/pip install websockets

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
	@echo $(CURDIR)/flow_positions.json

burst_logs_paths:
	@echo "Full paths:"
	@echo $(CURDIR)/burst_log.txt
	@echo $(CURDIR)/burst_trades.json

# zip logs into archives/ folder with zip path/filename -9 filename2 filename1 ...
# and delete original files
flow_logs_backup:
	mkdir -p archives
	zip -j -9 archives/flow_logs_$$(date +%Y%m%d_%H%M%S).zip flow_log.txt flow_trades.json flow_positions.json
	rm -f flow_log.txt flow_trades.json flow_positions.json

burst_logs_backup:
	mkdir -p archives
	zip -j -9 archives/burst_logs_$$(date +%Y%m%d_%H%M%S).zip burst_log.txt burst_trades.json
	rm -f burst_log.txt burst_trades.json

sniper_logs_backup:
	mkdir -p archives
	zip -j -9 archives/sniper_logs_$$(date +%Y%m%d_%H%M%S).zip sniper_data.jsonl sniper_trades.json sniper_log.txt
	rm -f sniper_data.jsonl sniper_trades.json sniper_log.txt

# git add . and git commit -m "backup logs" and git push"
git_logs_backup:
	git add .
	git commit -m "backup logs"
	git push

clean:
	rm -rf .venv
