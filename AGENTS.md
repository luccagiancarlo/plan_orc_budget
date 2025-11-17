# Repository Guidelines

## Project Structure & Module Organization
Core ETL scripts live at the repository root (`imp_*.py`, `conferencia.py`, `inserir_lancamentos.py`) and exchange data across the spreadsheets stored alongside them. Text extracts used during reconciliation reside under `abas_cols/`, and workbook assets such as `BASE 2026 DRE.xlsx` and `orc_2026.xlsx` are expected to stay in the root so the loaders can resolve relative paths. When modelling new flows, keep reader helpers near their corresponding import script and document any expected source files in a module-level docstring.

## Build, Test, and Development Commands
Create the recommended Conda environment before running anything: `conda env create -f env-xlsx-tools.yml && conda activate xlsx-tools`. During development, execute scripts directly, e.g. `python imp_orc_financeiro.py` to ingest forecast data or `python conferencia.py --help` to review available arguments. Use `python -m pip install --upgrade pandas openpyxl pymysql` inside the environment if you need a quick refresh without recreating it.

## Coding Style & Naming Conventions
Follow PEP 8 with four-space indentation, snake_case for functions and variables, and ALL_CAPS for constants such as workbook sheet names. Each script should expose a `main()` entry point and guard execution with `if __name__ == "__main__":` so modules remain importable. Group spreadsheet-specific helpers by prefix (`imp_`, `xlsx_`, `conferencia`) to make intent obvious, and prefer descriptive worksheet labels like `sheet_plano_metas` over generic `sheet1`.

## Testing Guidelines
No automated suite ships with the project yet; when adding one, place tests in a new `tests/` directory, mirror the module names (`test_imp_orc_financeiro.py`), and exercise both happy paths and data-validation failures using trimmed sample workbooks. Run prospective tests with `pytest` (`python -m pytest -q`) and include fixture spreadsheets under `tests/fixtures/` to avoid polluting production data.

## Commit & Pull Request Guidelines
Existing history favours short, lower-case summaries (`ok`, `inicio`). Please expand on that convention with concise, imperative verbs (Portuguese or English) under 50 characters, e.g. `git commit -m "Ajusta rateio financeiro"`. Pull requests should explain the affected data sources, list manual checks (scripts run, spreadsheets validated), and link to any external ticket. Attach sanitized screenshots or diff excerpts when changes alter workbook layouts.

## Data & Configuration Tips
Treat the `.xlsx` workbooks as single sources of truth; never strip formatting or formulas unless a script actually requires it, and call out any manual preprocessing in the README. Keep sensitive credentials out of the repo—use environment variables (`PLAN_ORC_DB_URL`) for database targets instead of hardcoding connection strings inside the loaders.
