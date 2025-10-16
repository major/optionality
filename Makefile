.PHONY: test lint typecheck complexity deadcode all update verify clean help

# 🧪 Development targets
test:
	uv run pytest

lint:
	uv run ruff check --fix

typecheck:
	uv run pyright src/*

complexity:
	uv run radon cc src/ -s -a

deadcode:
	uv run vulture src/ --min-confidence 80

all: lint test typecheck complexity deadcode

# 🚀 Data management targets
update:
	@echo "🌟 Running incremental update..."
	uv run optionality update

verify:
	@echo "🔍 Running stock price spot checks..."
	uv run optionality verify

check-files:
	@echo "🔍 Checking if new flatfiles are available..."
	uv run optionality check-files

clean:
	@echo "🗑️  Cleaning and re-initializing Delta Lake tables..."
	uv run optionality clean

# 📖 Help
help:
	@echo "Available targets:"
	@echo "  make update        - Run incremental load (splits + stocks + options + validation)"
	@echo "  make verify        - Run spot checks on stock prices (validate splits)"
	@echo "  make check-files   - Check if new flatfiles are available from Polygon"
	@echo "  make clean         - Delete all Delta Lake tables and re-initialize (DANGEROUS)"
	@echo "  make test          - Run pytest tests"
	@echo "  make lint          - Run ruff linter"
	@echo "  make typecheck     - Run pyright type checker"
	@echo "  make complexity    - Calculate code complexity with radon"
	@echo "  make deadcode      - Find dead code with vulture"
	@echo "  make all           - Run lint, test, typecheck, complexity, and deadcode"
