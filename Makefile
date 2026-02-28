.PHONY: venv install clean run

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

run:
	$(PY) utility.py
	$(PY) strategy.py

just-run:
	$(PY) strategy.py --resume

highprob:
	$(PY) highprob.py

flow:
	$(PY) flow.py

flow-resume:
	$(PY) flow.py --resume

flow-dry:
	$(PY) flow.py --dry-run

update-wallets:
	$(PY) utility.py

# ── sniper ──────────────────────────────────────────────────────────
sniper:
	$(PY) sniper.py

analyze:
	$(PY) analyze_trades.py

# ── claimer ─────────────────────────────────────────────────────────
claim:
	$(PY) claimer.py

claim-loop:
	$(PY) claimer.py --loop 30

claim-batch:
	$(PY) claimer.py --batch 10

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

sniper_logs_paths:
	@echo "Full paths:"
	@echo $(CURDIR)/sniper_log.txt
	@echo $(CURDIR)/sniper_trades.json
	@echo $(CURDIR)/sniper_data.jsonl

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

follower_logs_backup:
	mkdir -p archives
	zip -j -9 archives/follower_logs_$$(date +%Y%m%d_%H%M%S).zip follower_log.txt follower_trades.json
	rm -f follower_log.txt follower_trades.json
# git add . and git commit -m "backup logs" and git push"

gapbot_logs_backup:
	mkdir -p archives
	zip -j -9 archives/gapbot_logs_$$(date +%Y%m%d_%H%M%S).zip gapbot_log.txt gapbot_trades.json
	rm -f gapbot_log.txt gapbot_trades.json

btc80_logs_backup:
	mkdir -p archives
	zip -j -9 archives/btc80_logs_$$(date +%Y%m%d_%H%M%S).zip btc80_log.txt btc80_trades.json
	rm -f btc80_log.txt btc80_trades.json

theta_logs_backup:
	mkdir -p archives
	zip -j -9 archives/theta_logs_$$(date +%Y%m%d_%H%M%S).zip theta_log.txt theta_trades.json theta_ticks.csv
	rm -f theta_log.txt theta_trades.json theta_ticks.csv

git_logs_backup:
	git add .
	git commit -m "backup logs"
	git push

clean:
	rm -rf .venv
