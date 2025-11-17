#!/usr/bin/env python3
"""
imp_cad_receita.py
------------------
Importa dados da aba "CADASTRO RECEITA" do arquivo orc_2026.xlsx
para a tabela val_orc_cad_rec_temp no banco de dados MySQL.

Uso:
  python imp_cad_receita.py
  python imp_cad_receita.py --excel orc_2026.xlsx
  python imp_cad_receita.py --excel orc_2026.xlsx --sheet "CADASTRO RECEITA"

Requisitos:
  - pandas
  - openpyxl
  - pymysql
"""
import argparse
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
import pymysql
from pymysql import Error as MySQLError


# Configuração do banco de dados
DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'glucca',
    'password': 'Gi3510prbi!',
    'database': 'budget',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}


def criar_tabela(connection) -> None:
    """
    Cria a tabela val_orc_cad_rec_temp (sempre recria).
    """
    cursor = connection.cursor()

    try:
        # Remove a tabela se existir
        cursor.execute("DROP TABLE IF EXISTS val_orc_cad_rec_temp")
        print("Tabela val_orc_cad_rec_temp removida (se existia)")

        # Cria a tabela
        create_sql = """
            CREATE TABLE val_orc_cad_rec_temp (
                id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID auto-incrementado',
                nro_conta INT NULL COMMENT 'Número da conta (NRO CONTA)',
                conta_contabil VARCHAR(20) NULL COMMENT 'Código da conta contábil (CONTA CONTÁBIL)',
                cd_empresa INT NULL COMMENT 'Código da empresa (EMPRESA)',
                nome_empresa VARCHAR(100) NULL COMMENT 'Nome da empresa (NOME DA EMPRESA)',
                cod_produto INT NULL COMMENT 'Código do produto (COD PRODUTO)',
                nome_produto VARCHAR(200) NULL COMMENT 'Nome do produto (NOME DO PRODUTO)',
                cod_venda VARCHAR(100) NULL COMMENT 'Códigos de venda (COD DE VENDA)',
                data_importacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Data/hora da importação',
                INDEX idx_nro_conta (nro_conta),
                INDEX idx_empresa (cd_empresa),
                INDEX idx_produto (cod_produto)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            COMMENT='Tabela temporária para importação do cadastro de receita'
        """

        cursor.execute(create_sql)
        connection.commit()
        print("Tabela val_orc_cad_rec_temp criada com sucesso!")

    except MySQLError as e:
        print(f"Erro ao criar tabela: {e}", file=sys.stderr)
        raise
    finally:
        cursor.close()


def ler_planilha_cadastro_receita(excel_path: Path, sheet_name: str) -> pd.DataFrame:
    """
    Lê a planilha CADASTRO RECEITA com tratamento especial para o layout.

    A planilha tem cabeçalhos na linha 4 (índice 4) e os dados começam na linha 5.
    Algumas linhas podem ter apenas COD PRODUTO e NOME DO PRODUTO preenchidos,
    representando produtos adicionais do mesmo registro anterior.
    """
    print(f"Lendo arquivo: {excel_path}")
    print(f"Aba: {sheet_name}")

    # Lê a planilha com header=None para processar manualmente
    df_raw = pd.read_excel(excel_path, sheet_name=sheet_name, header=None)

    # A linha 4 (índice 4) contém os cabeçalhos
    # Pega os valores da linha 4 começando da coluna 1 (índice 1)
    headers_row = df_raw.iloc[4, 1:8].tolist()  # Colunas B até H (índices 1-7)

    print(f"\nCabeçalhos encontrados: {headers_row}")

    # Define nomes padronizados das colunas
    column_names = [
        'NRO CONTA',
        'CONTA CONTÁBIL',
        'EMPRESA',
        'NOME DA EMPRESA',
        'COD PRODUTO',
        'NOME DO PRODUTO',
        'COD DE VENDA'
    ]

    # Lê os dados a partir da linha 5 (índice 5), usando apenas as colunas B-H (1-7)
    df_data = df_raw.iloc[5:, 1:8].copy()
    df_data.columns = column_names

    # Remove linhas completamente vazias
    df_data = df_data.dropna(how='all')

    # Para linhas que têm apenas COD PRODUTO e NOME DO PRODUTO,
    # preenche os campos anteriores com os valores do registro anterior
    # (forward fill para as colunas que representam o "registro principal")
    fill_columns = ['NRO CONTA', 'CONTA CONTÁBIL', 'EMPRESA', 'NOME DA EMPRESA', 'COD DE VENDA']
    for col in fill_columns:
        df_data[col] = df_data[col].ffill()

    # Remove linhas onde COD PRODUTO está vazio (não são registros válidos)
    df_data = df_data.dropna(subset=['COD PRODUTO'])

    # Remove linhas que são cabeçalhos repetidos ou títulos de seção
    # Detecta linhas onde 'NRO CONTA' contém texto ao invés de número
    def is_valid_row(row):
        """Verifica se a linha contém dados válidos (não é cabeçalho ou título)."""
        nro_conta = row['NRO CONTA']
        cod_produto = row['COD PRODUTO']

        # Se NRO CONTA for string e contiver texto de cabeçalho, não é válido
        if isinstance(nro_conta, str):
            if 'NRO' in nro_conta.upper() or 'CONTA' in nro_conta.upper():
                return False

        # Se COD PRODUTO for string e contiver texto de cabeçalho, não é válido
        if isinstance(cod_produto, str):
            if 'COD' in cod_produto.upper() or 'PRODUTO' in cod_produto.upper():
                return False

        # Tenta converter para verificar se são números válidos
        try:
            if pd.notna(nro_conta):
                float(nro_conta)  # Aceita tanto int quanto float
            if pd.notna(cod_produto):
                float(cod_produto)
            return True
        except (ValueError, TypeError):
            return False

    # Aplica o filtro
    linhas_antes = len(df_data)
    df_data = df_data[df_data.apply(is_valid_row, axis=1)]
    linhas_removidas = linhas_antes - len(df_data)

    if linhas_removidas > 0:
        print(f"\nRemovidas {linhas_removidas} linhas de cabeçalhos/títulos repetidos")

    print(f"\nDados lidos: {len(df_data)} registros válidos")
    print(f"Colunas: {', '.join(df_data.columns)}")

    # Mostra algumas linhas de exemplo
    print("\nPrimeiras linhas:")
    print(df_data.head(10).to_string())

    return df_data


def inserir_dados(connection, df: pd.DataFrame) -> int:
    """
    Insere dados do DataFrame na tabela val_orc_cad_rec_temp.
    Retorna o número de linhas inseridas.
    """
    if df.empty:
        print("DataFrame vazio. Nenhum dado para inserir.")
        return 0

    cursor = connection.cursor()

    try:
        insert_sql = """
            INSERT INTO val_orc_cad_rec_temp
            (nro_conta, conta_contabil, cd_empresa, nome_empresa,
             cod_produto, nome_produto, cod_venda)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        # Prepara dados para inserção
        rows_to_insert = []
        linhas_ignoradas = 0

        for idx, row in df.iterrows():
            try:
                # Converte valores com tratamento de erro
                nro_conta = None
                if pd.notna(row['NRO CONTA']):
                    try:
                        nro_conta = int(float(row['NRO CONTA']))
                    except (ValueError, TypeError):
                        pass

                conta_contabil = str(row['CONTA CONTÁBIL']) if pd.notna(row['CONTA CONTÁBIL']) else None

                cd_empresa = None
                if pd.notna(row['EMPRESA']):
                    try:
                        cd_empresa = int(float(row['EMPRESA']))
                    except (ValueError, TypeError):
                        pass

                nome_empresa = str(row['NOME DA EMPRESA']) if pd.notna(row['NOME DA EMPRESA']) else None

                cod_produto = None
                if pd.notna(row['COD PRODUTO']):
                    try:
                        cod_produto = int(float(row['COD PRODUTO']))
                    except (ValueError, TypeError):
                        # COD PRODUTO é obrigatório, pula esta linha
                        linhas_ignoradas += 1
                        continue

                nome_produto = str(row['NOME DO PRODUTO']) if pd.notna(row['NOME DO PRODUTO']) else None
                cod_venda = str(row['COD DE VENDA']) if pd.notna(row['COD DE VENDA']) else None

                rows_to_insert.append((
                    nro_conta, conta_contabil, cd_empresa, nome_empresa,
                    cod_produto, nome_produto, cod_venda
                ))

            except Exception as e:
                print(f"\nAviso: Linha {idx} ignorada devido a erro: {e}")
                linhas_ignoradas += 1
                continue

        if linhas_ignoradas > 0:
            print(f"\n{linhas_ignoradas} linhas foram ignoradas devido a erros de conversão")

        # Insere em lotes para melhor performance
        batch_size = 1000
        total_inserted = 0

        for i in range(0, len(rows_to_insert), batch_size):
            batch = rows_to_insert[i:i + batch_size]
            cursor.executemany(insert_sql, batch)
            connection.commit()
            total_inserted += len(batch)
            print(f"Inseridas {total_inserted}/{len(rows_to_insert)} linhas...", end='\r')

        print(f"\nTotal de {total_inserted} linhas inseridas com sucesso!")
        return total_inserted

    except MySQLError as e:
        connection.rollback()
        print(f"\nErro ao inserir dados: {e}", file=sys.stderr)
        raise
    finally:
        cursor.close()


def importar_cadastro_receita(excel_path: Path, sheet_name: str) -> None:
    """
    Função principal que importa a aba CADASTRO RECEITA para o MySQL.
    """
    # Validação do arquivo
    if not excel_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {excel_path}")

    # Lê a planilha
    try:
        df = ler_planilha_cadastro_receita(excel_path, sheet_name)
    except ValueError as e:
        # Tenta encontrar nome similar
        xls = pd.ExcelFile(excel_path)
        matching = [s for s in xls.sheet_names if sheet_name.lower() in s.lower()]
        if matching:
            print(f"Aba '{sheet_name}' não encontrada. Usando '{matching[0]}' em vez disso.")
            df = ler_planilha_cadastro_receita(excel_path, matching[0])
        else:
            raise ValueError(f"Aba '{sheet_name}' não encontrada. Abas disponíveis: {xls.sheet_names}")

    registros_lidos = len(df)

    # Conecta ao MySQL
    print(f"\nConectando ao MySQL ({DB_CONFIG['host']}:{DB_CONFIG['database']})...")
    try:
        connection = pymysql.connect(**DB_CONFIG)
        print("Conexão estabelecida!")

        # Cria a tabela
        print("\nCriando tabela val_orc_cad_rec_temp...")
        criar_tabela(connection)

        # Insere os dados
        print("\nInserindo dados na tabela val_orc_cad_rec_temp...")
        registros_inseridos = inserir_dados(connection, df)

        # Relatório final
        print("\n" + "=" * 60)
        print("RESUMO DA IMPORTAÇÃO")
        print("=" * 60)
        print(f"Registros lidos da planilha: {registros_lidos}")
        print(f"Registros inseridos em val_orc_cad_rec_temp: {registros_inseridos}")

        if registros_lidos != registros_inseridos:
            print("\n⚠️  ALERTA: A quantidade de registros lidos é diferente da quantidade inserida!")
            print(f"   Diferença: {abs(registros_lidos - registros_inseridos)} registros")
        else:
            print("\n✓ Todos os registros foram importados com sucesso!")
        print("=" * 60)

        print("\nImportação concluída!")

    except MySQLError as e:
        print(f"\nErro de MySQL: {e}", file=sys.stderr)
        raise
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()
            print("Conexão fechada.")


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Processa argumentos da linha de comando."""
    parser = argparse.ArgumentParser(
        description='Importa cadastro de receita do Excel para val_orc_cad_rec_temp no MySQL'
    )
    parser.add_argument(
        '--excel',
        type=Path,
        default=Path('orc_2026.xlsx'),
        help='Caminho para o arquivo Excel (padrão: orc_2026.xlsx)'
    )
    parser.add_argument(
        '--sheet',
        type=str,
        default='CADASTRO RECEITA',
        help='Nome da aba a importar (padrão: "CADASTRO RECEITA")'
    )

    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    """Função principal."""
    args = parse_args(argv)

    try:
        importar_cadastro_receita(
            excel_path=args.excel,
            sheet_name=args.sheet
        )
        return 0
    except Exception as e:
        print(f"\nERRO: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
