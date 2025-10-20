# ===================================================================
# 🧰 Bruin E-commerce Demo Project Makefile
# ===================================================================
.DEFAULT_GOAL := help

# -------------------------------------------------------------------
# 🔧 Environment Configuration
# -------------------------------------------------------------------
DSN ?= postgresql://postgres:postgres@localhost:5432/postgres
TABLE ?= mart.sales_daily
STARTING_AT ?= $(shell date -u +"%Y-%m-%dT00:00:00")
ENDING_AT   ?= $(shell date -u +"%Y-%m-%dT23:59:59")
BASE_CUSTOMERS = 100
BASE_PRODUCTS  = 50
BASE_ORDERS    = 500

# -------------------------------------------------------------------
# 🎛️ General Commands
# -------------------------------------------------------------------
.PHONY: help docker-up docker-down

help: ## Show available commands
	@echo ""
	@echo "🧭 Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
	@echo ""

docker-up: ## Start Docker containers in detached mode
	@echo "🐳 Starting Docker services..."
	@docker compose up -d
	@echo "✅ Docker containers are now running."

docker-down: ## Stop all running Docker containers
	@echo "🛑 Stopping Docker services..."
	@docker compose down
	@echo "✅ Containers stopped."

# -------------------------------------------------------------------
# 🧪 Data Generation Commands
# -------------------------------------------------------------------
.PHONY: generate-data generate-data-scale generate-data-chaos

# 🧪 Default dataset
generate-data: ## Generate default dataset (base scale, no chaos)
	@echo ""
	@echo "🧪 Generating default dataset..."
	@echo "→ DSN:      $(DSN)"
	@echo "→ Window:   $(STARTING_AT) → $(ENDING_AT)"
	@echo ""
	@uv run generate_data.py \
		--dsn $(DSN) \
		--starting-at $(STARTING_AT) \
		--ending-at $(ENDING_AT)
	@echo ""
	@echo "✅ Default data generation complete!"

# 📈 Scaled dataset (e.g. make generate-data-scale SCALE=3)
generate-data-scale: ## Generate dataset scaled by N× (e.g. SCALE=3)
	@if [ -z "$(SCALE)" ]; then \
		echo "\033[31m[ERR]\033[0m SCALE is required, e.g. make generate-data-scale SCALE=3"; exit 1; \
	fi
	@echo ""
	@echo "📈 Generating dataset scaled by $(SCALE)×..."
	@echo "→ DSN:      $(DSN)"
	@echo "→ Window:   $(STARTING_AT) → $(ENDING_AT)"
	@uv run generate_data.py \
		--dsn $(DSN) \
		--customers $$(( $(BASE_CUSTOMERS) * $(SCALE) )) \
		--products  $$(( $(BASE_PRODUCTS)  * $(SCALE) )) \
		--orders    $$(( $(BASE_ORDERS)    * $(SCALE) )) \
		--scale $(SCALE) \
		--starting-at $(STARTING_AT) \
		--ending-at $(ENDING_AT)
	@echo ""
	@echo "✅ Scaled dataset generation complete!"

# 🔥 Scaled + chaotic dataset (e.g. make generate-data-chaos SCALE=3 CHAOS=10)
generate-data-chaos: ## Generate dataset with scale and chaos (e.g. SCALE=3 CHAOS=10)
	@if [ -z "$(SCALE)" ]; then echo "\033[31m[ERR]\033[0m SCALE is required, e.g. make generate-data-chaos SCALE=3 CHAOS=10"; exit 1; fi
	@if [ -z "$(CHAOS)" ]; then echo "\033[31m[ERR]\033[0m CHAOS is required, e.g. make generate-data-chaos SCALE=3 CHAOS=10"; exit 1; fi
	@echo ""
	@echo "🔥 Generating dataset scaled by $(SCALE)× with $(CHAOS)% chaos..."
	@echo "→ DSN:      $(DSN)"
	@echo "→ Window:   $(STARTING_AT) → $(ENDING_AT)"
	@uv run generate_data.py \
		--dsn $(DSN) \
		--customers $$(( $(BASE_CUSTOMERS) * $(SCALE) )) \
		--products  $$(( $(BASE_PRODUCTS)  * $(SCALE) )) \
		--orders    $$(( $(BASE_ORDERS)    * $(SCALE) )) \
		--scale $(SCALE) \
		--chaos-percent $(CHAOS) \
		--starting-at $(STARTING_AT) \
		--ending-at $(ENDING_AT)
	@echo ""
	@echo "✅ Chaotic dataset generation complete!"

# -------------------------------------------------------------------
# 🧱 Database Management
# -------------------------------------------------------------------

# Apply DDL migrations inside the bruin-ecommerce container
.PHONY: db-migrate
db-migrate: ## Apply DDL migrations inside the bruin-ecommerce container
	@echo ""
	@echo "📦 Applying schema to Postgres..."
	@echo ""
	@cat sql/ddl.sql | docker exec -i bruin-ecommerce psql -U postgres -d postgres -q > /dev/null
	@echo "✅ Migration complete. Listing tables in 'postgres' database:"
	@docker exec -i bruin-ecommerce psql -U postgres -d postgres -q -c "\dt" | grep -E '^[[:space:]]*[a-z]' || true
	@echo ""
	@echo "💡 You can now generate data using: make generate-data"

.PHONY: db-reset
db-reset: ## Drop and recreate the 'public' schema, then re-run migrations
	@echo ""
	@echo "📦 Resetting schema in database 'postgres'..."
	@echo ""
	@docker exec bruin-ecommerce psql -U postgres -d postgres -q -c "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;" > /dev/null
	@echo ""
	@echo "✅ Schema reset completed. Applying migrations..."
	@$(MAKE) -s db-migrate


# -------------------------------------------------------------------
# 🚀 Bruin Pipeline Helpers
# -------------------------------------------------------------------
.PHONY: run-pipeline connections-list connections-test-duckdb connections-test-postgres query mart-list

run-pipeline: ## Run the Bruin pipeline defined at ecommerce/pipeline.yml
	@echo "🚀 Running Bruin pipeline..."
	bruin run ecommerce/pipeline.yml
	@echo "✅ Pipeline execution complete!"

connections-list: ## List available Bruin connections
	@echo "🔗 Listing Bruin connections..."
	bruin connections list

connections-test-duckdb: ## Test the duckdb-default connection
	@echo "🧪 Testing DuckDB connection..."
	bruin connections test --name duckdb-default

connections-test-postgres: ## Test the pg-default connection
	@echo "🧪 Testing Postgres connection..."
	bruin connections test --name pg-default

# -------------------------------------------------------------------
# 🧮 Query Utilities
# -------------------------------------------------------------------
.PHONY: query mart-list

query: ## Run a SQL query against DuckDB via Bruin (use TABLE= or SQL="...")
	@if [ -z "$(SQL)" ]; then SQL="select * from $(TABLE) limit 10"; fi; \
	echo "💬 Running query:"; echo "→ $$SQL"; echo ""; \
	bruin query --c duckdb-default --q "$$SQL"

mart-list: ## Preview first 10 rows from core mart tables in DuckDB
	@echo "📊 Previewing common mart tables..."
	@for t in mart.sales_daily mart.product_performance mart.variant_profitability; do \
	  echo "────────────────────────────────────────────────────────────"; \
	  echo "🔍 $$t"; \
	  bruin query --c duckdb-default --q "select * from $$t limit 10"; \
	  echo ""; \
	done
	@echo "✅ Preview complete."
