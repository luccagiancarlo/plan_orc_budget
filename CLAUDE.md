# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python utility for extracting and documenting Excel spreadsheet structure from budget planning workbooks. The primary script (`xlsx_listar_abas_colunas.py`) reads `.xlsx` files and generates documentation about their sheets and columns.

The project was created to analyze budget files (e.g., "1.0 2026 Orçamento DESCARBOX.xlsx"), which contain complex multi-sheet structures with financial data including DRE (Demonstração do Resultado do Exercício), cost projections, revenue forecasts, and organizational accounting structures.

## Environment Setup

The project uses conda for dependency management:

```bash
# Create and activate the environment
conda env create -f env-xlsx-tools.yml
conda activate xlsx-tools
```

Dependencies:
- Python 3.11
- pandas
- openpyxl
- pymysql

## Running the Tool

Basic usage:
```bash
python xlsx_listar_abas_colunas.py "1.0 2026 Orçamento DESCARBOX.xlsx"
```

With custom output directory and ZIP compression:
```bash
python xlsx_listar_abas_colunas.py "arquivo.xlsx" --outdir ./resultado --zip
```

The script outputs:
1. `plans.txt` - List of all sheet names in the workbook
2. `abas_cols/` directory - Individual `.txt` files for each sheet containing column names
3. Optional ZIP file with all column documentation files

## Code Architecture

**Single-file utility structure:**

The script follows a functional design with clear separation:

1. **Filename sanitization** (`sanitize_filename`, `unique_name`): Converts Excel sheet names to safe filesystem names, handling special characters, duplicate names, and length limits

2. **Main extraction logic** (`listar_abas_e_colunas`):
   - Opens Excel file using pandas
   - Extracts sheet names → writes to `plans.txt`
   - For each sheet: reads header row only (nrows=0) to get column names efficiently
   - Writes individual column documentation files
   - Optional ZIP creation

3. **CLI interface** (`parse_args`, `main`): Standard argparse-based command-line interface

**Key implementation details:**
- Uses `pd.read_excel(..., nrows=0)` to read only headers, avoiding memory issues with large datasets
- Handles malformed sheets gracefully with try/except blocks
- Ensures unique output filenames even when sheet names clash (case-insensitive)
- All text files use UTF-8 encoding by default (configurable via `--encoding`)

## Working with Excel Files

The Excel files analyzed typically contain Brazilian financial budget structures with sheets like:
- DRE sheets (profit/loss statements) for different business units
- Fixed cost bases ("Base fixo...")
- Revenue projections ("PROJEÇÃO...", "Receita...")
- Financial planning ("BASE DO ORÇAMENTO FINANCEIRO")
- Account structures ("plano de contas")

Many sheets have multi-level headers or merged cells, which may result in columns appearing as "Unnamed: N" in the output.

## Database Import Scripts

The project includes several scripts to import Excel data into MySQL:

### 1. imp_plano_contas.py
Imports the "plano de contas" (chart of accounts) sheet to MySQL.

```bash
python imp_plano_contas.py
python imp_plano_contas.py --excel orc_2026.xlsx --sheet "plano de contas"
```

Features:
- Converts FlagNivel (15-digit format) to nu_conta (dotted notation: 001.001.001)
- Synchronizes with val_orc_plancontas (UPDATE existing + INSERT new records)
- Always drops and recreates imp_plan_contas table

### 2. imp_base_fixo.py
Imports all "Base fixo" sheets (one per company) with monthly budget values.

```bash
python imp_base_fixo.py
python imp_base_fixo.py --abas "Base fixo Distribuidora,Base fixo Unibox"
```

Features:
- Processes 5 company sheets: Distribuidora (1), Unibox (2), industria Máscara (3), Uniplast (4), UNIPACK (7)
- Extracts cod_interno, cod_unidade, monthly values (janeiro-dezembro)
- Detects cod_unidade in two patterns (handles different Excel layouts)
- Creates imp_base_fixo table with cd_empresa field

### 3. imp_orc_financeiro.py
Imports "BASE DO ORÇAMENTO FINANCEIRO" sheet.

```bash
python imp_orc_financeiro.py
python imp_orc_financeiro.py --excel orc_2026.xlsx
```

Features:
- Extracts cod_conta and monthly PREVISTO values
- Creates imp_orc_financeiro table
- Simpler structure than base fixo (no cod_unidade)

### 4. inserir_lancamentos.py
Transforms imp_base_fixo (columnar format) to val_orc_lancamentos (row format).

```bash
python inserir_lancamentos.py --ano 2026
python inserir_lancamentos.py --ano 2026 --limpar  # Removes existing year first
```

Features:
- Converts 1 row × 12 columns → 12 rows (one per month)
- Maps: cod_interno→cd_conta, cod_unidade→cd_unidade, cd_empresa→cd_empresa
- tp_lancamento = "Saldo Inicial"
- Auto-increments cd_lancamento from max existing ID
- Batch insertion (5000 records at a time)

### 5. conferencia.py
Conference/verification tool for account balances by unit.

```bash
python conferencia.py --conta 110
python conferencia.py --conta 153 --ano 2026 --tipo "Saldo Inicial"
python conferencia.py --conta 110 --ano 2026 --empresa 1
python conferencia.py --conta 153 --ano 2026 --unidade 40
python conferencia.py --conta 153 --ano 2026 --mes 6
python conferencia.py --conta 153 --ano 2026 --empresa 4 --unidade 40 --mes 6
```

Features:
- Shows month-by-month values for a specific account (cd_conta)
- Groups by unit (cd_unidade) with descriptions from val_orc_unidade
- Displays totals per month and per unit
- Optional filters (if not specified, shows ALL):
  - --ano: Filter by year (if not specified, shows all years combined)
  - --mes: Filter by specific month (1-12)
  - --unidade: Filter by specific unit (cd_unidade)
  - --tipo: Filter by tp_lancamento (e.g., "Saldo Inicial")
  - --empresa: Filter by cd_empresa
- Formatted table output with company/unit grouping
- Shows applied filters in header

## Database Schema

Key tables:
- **imp_plan_contas**: Imported chart of accounts (temporary staging)
- **val_orc_plancontas**: Production chart of accounts (cd_conta, nu_conta, de_conta)
- **imp_base_fixo**: Imported budget base (cod_interno, cod_unidade, monthly values)
- **val_orc_lancamentos**: Production budget entries (cd_lancamento, cd_conta, cd_unidade, nu_mes, vl_lancamento)
- **val_orc_unidade**: Organizational units (cd_unidade, de_unidade)

Database connection (all scripts):
- Host: 127.0.0.1
- Database: budget
- User/password: configured in DB_CONFIG dict in each script
