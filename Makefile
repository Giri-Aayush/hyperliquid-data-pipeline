.PHONY: install test demo orderbook indicators lint

install:
	python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

test:
	.venv/bin/python -m pytest tests/ -v

demo:
	.venv/bin/python scripts/run_pipeline.py test-realtime --symbols BTC --duration 30

orderbook:
	.venv/bin/python examples/orderbook_metrics.py --symbol BTC --duration 30

indicators:
	.venv/bin/python examples/ohlcv_indicators.py

lint:
	.venv/bin/black --check src tests examples
	.venv/bin/isort --check-only src tests examples
