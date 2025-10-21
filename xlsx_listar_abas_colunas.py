
#!/usr/bin/env python3
"""
xlsx_listar_abas_colunas.py
---------------------------------
Lê um arquivo .xlsx, lista as abas em plans.txt e,
para cada aba, gera um arquivo .txt com as colunas detectadas.

Uso:
  python xlsx_listar_abas_colunas.py /caminho/para/arquivo.xlsx --outdir ./saida --zip

Requisitos:
  - pandas
  - openpyxl  (para arquivos .xlsx)

Exemplos:
  python xlsx_listar_abas_colunas.py "1.0 2026 Orçamento DESCARBOX.xlsx"
  python xlsx_listar_abas_colunas.py "planilha.xlsx" --outdir ./resultado --zip
"""
import argparse
import re
import sys
import zipfile
from pathlib import Path
from typing import List, Dict

import pandas as pd

def sanitize_filename(name: str) -> str:
    """
    Transforma o nome da aba em um nome de arquivo seguro:
    - substitui caracteres inválidos
    - remove espaços duplicados
    - limita o tamanho total
    """
    # Substitui barras e caracteres problemáticos por underscore
    name = re.sub(r'[\\/:*?"<>|]', "_", name)
    # Troca quebras de linha e tabs por espaço
    name = re.sub(r'[\r\n\t]+', " ", name)
    # Remove espaços duplicados
    name = re.sub(r'\s+', " ", name).strip()
    if not name:
        name = "aba_sem_nome"
    return name[:120]

def unique_name(base_name: str, used: Dict[str, bool]) -> str:
    """Garante unicidade do nome final, adicionando sufixos _2, _3, ... quando necessário."""
    final = base_name
    n = 2
    while final.lower() in used:
        final = f"{base_name}_{n}"
        n += 1
    used[final.lower()] = True
    return final

def listar_abas_e_colunas(excel_path: Path, outdir: Path, do_zip: bool = False, encoding: str = "utf-8") -> None:
    if not excel_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {excel_path}")
    outdir.mkdir(parents=True, exist_ok=True)
    abas_dir = outdir / "abas_cols"
    abas_dir.mkdir(parents=True, exist_ok=True)

    print(f"Lendo arquivo: {excel_path}")
    xls = pd.ExcelFile(excel_path)
    abas = xls.sheet_names

    # 1) Escreve plans.txt
    plans_txt = outdir / "plans.txt"
    with plans_txt.open("w", encoding=encoding) as f:
        for aba in abas:
            f.write(f"{aba}\n")
    print(f"> Abas salvas em: {plans_txt}")

    # 2) Para cada aba, salva as colunas em um arquivo .txt
    usados = {}
    for aba in abas:
        try:
            # Lê apenas o cabeçalho (sem carregar dados) para capturar as colunas
            df = pd.read_excel(excel_path, sheet_name=aba, nrows=0, engine=None)
            colunas = [str(c) for c in df.columns]
        except Exception as e:
            colunas = []
            print(f"AVISO: Não foi possível ler a aba '{aba}': {e}", file=sys.stderr)

        base_name = sanitize_filename(aba)
        final_name = unique_name(base_name, usados)
        out_file = abas_dir / f"{final_name}.txt"

        with out_file.open("w", encoding=encoding) as f:
            f.write(f"Colunas da aba: {aba}\n")
            f.write("=" * (13 + len(aba)) + "\n")
            if colunas:
                for c in colunas:
                    f.write(f"{c}\n")
            else:
                f.write("(Nenhuma coluna detectada ou aba vazia)\n")

    print(f"> Arquivos de colunas salvos em: {abas_dir}")

    # (Opcional) ZIP com todos os arquivos .txt das colunas
    if do_zip:
        zip_path = outdir / "abas_colunas.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for p in abas_dir.glob("*.txt"):
                zf.write(p, arcname=p.name)
        print(f"> ZIP criado em: {zip_path}")

def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Lista abas e colunas de um arquivo .xlsx")
    p.add_argument("excel", type=Path, help="Caminho para o arquivo .xlsx")
    p.add_argument("--outdir", type=Path, default=Path("."), help="Diretório de saída (default: diretório atual)")
    p.add_argument("--zip", action="store_true", help="Se informado, cria um ZIP com os arquivos de colunas")
    p.add_argument("--encoding", default="utf-8", help="Encoding dos arquivos de texto (default: utf-8)")
    return p.parse_args(argv)

def main(argv: List[str]) -> int:
    args = parse_args(argv)
    try:
        listar_abas_e_colunas(args.excel, args.outdir, do_zip=args.zip, encoding=args.encoding)
        return 0
    except Exception as e:
        print(f"ERRO: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
