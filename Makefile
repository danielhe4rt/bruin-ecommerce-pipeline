.DEFAULT_GOAL := help

TABLE ?= mart.sales_daily

.PHONY: help generate-data run-pipeline connections-list connections-test-duckdb connections-test-postgres query mart-list

help: ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-24s\033[0m %s\n", $$1, $$2}'


generate-data: ## Generate demo data into PostgreSQL with uv (requires DSN and START)
	@test -n "$(DSN)" || (echo "[ERR] DSN is required, e.g. postgresql://user:pass@localhost:5432/ecommerce" && exit 1)
	@test -n "$(START)" || (echo "[ERR] START is required, e.g. 2024-01-01" && exit 1)
	uv run generate-data -- --dsn $(DSN) --start-date $(START) $(EXTRA)

# Bruin pipeline helpers
run-pipeline: ## Run the Bruin pipeline defined at ecommerce/pipeline.yml
	bruin run ecommerce/pipeline.yml

connections-list: ## List available Bruin connections
	bruin connections list

connections-test-duckdb: ## Test the duckdb-default connection
	bruin connections test --name duckdb-default

connections-test-postgres: ## Test the pg-default connection
	bruin connections test --name pg-default

# Query helpers for DuckDB via Bruin CLI
# You can either set SQL explicitly or provide a TABLE name (default: $(TABLE))
# Examples:
#   make query TABLE=mart.sales_daily
#   make query SQL="select count(*) from mart.product_performance"
query: ## Run a SQL query against DuckDB via Bruin (TABLE=mart.sales_daily or set SQL="...")
	@if [ -z "$(SQL)" ]; then SQL="select * from $(TABLE) limit 10"; else SQL="$(SQL)"; fi; \
	  bruin query --c duckdb-default --q "$$SQL"

mart-list: ## Preview the first 10 rows from common mart tables in DuckDB
	@for t in mart.sales_daily mart.product_performance mart.variant_profitability; do \
	  echo "==> $$t"; \
	  bruin query --c duckdb-default --q "select * from $$t limit 10"; \
	  echo; \
	done