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
