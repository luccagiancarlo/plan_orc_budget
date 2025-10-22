#!/usr/bin/env python3
"""
imp_orc_financeiro.py
---------------------
Importa dados da aba "BASE DO ORÇAMENTO FINANCEIRO"
para a tabela imp_orc_financeiro no MySQL.

Extrai:
- Código da conta (código interno)
- Descrição da conta
- Valores previstos mensais (janeiro a dezembro)

Uso:
  python imp_orc_financeiro.py

Requisitos:
  - pandas
  - openpyxl
  - pymysql
"""
import argparse
import sys
from pathlib import Path
from typing import List

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

# Mapeamento das colunas de valores previstos
# Colunas 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24 = Jan a Dez (PREVISTO)
MESES_COLUNAS = {
    'janeiro': 2,
    'fevereiro': 4,
    'marco': 6,
    'abril': 8,
    'maio': 10,
    'junho': 12,
    'julho': 14,
    'agosto': 16,
    'setembro': 18,
    'outubro': 20,
    'novembro': 22,
    'dezembro': 24
}


def extrair_dados_orcamento_financeiro(excel_path: Path, sheet_name: str) -> pd.DataFrame:
    """
    Extrai dados da aba de orçamento financeiro.

    Retorna DataFrame com colunas:
    - cod_conta: código da conta
    - descricao: descrição da conta
    - janeiro, fevereiro, ..., dezembro: valores previstos
    - fonte: nome da aba
    """
    # Lê a aba sem processar cabeçalho
    df_raw = pd.read_excel(excel_path, sheet_name=sheet_name, header=None, engine='openpyxl')

    registros = []

    for idx in range(len(df_raw)):
        row = df_raw.iloc[idx]

        # Verifica se tem código numérico na coluna 0
        if pd.isna(row[0]):
            continue

        codigo_str = str(row[0]).strip()
        if not codigo_str.isdigit():
            continue

        cod_conta = int(codigo_str)

        # Descrição na coluna 1
        descricao = str(row[1]).strip() if pd.notna(row[1]) else ''
        if not descricao:
            continue

        # Extrai valores mensais (colunas PREVISTO)
        valores = {}
        for mes, col_idx in MESES_COLUNAS.items():
            valor = row[col_idx]
            valores[mes] = float(valor) if pd.notna(valor) else 0.0

        # Monta registro
        registro = {
            'cod_conta': cod_conta,
            'descricao': descricao,
            'fonte': sheet_name,
            **valores
        }

        registros.append(registro)

    df_result = pd.DataFrame(registros)
    return df_result


def criar_tabela_imp_orc_financeiro(connection) -> None:
    """
    Cria a tabela imp_orc_financeiro (sempre recria).
    """
    cursor = connection.cursor()

    try:
        # Remove a tabela se existir
        cursor.execute("DROP TABLE IF EXISTS `imp_orc_financeiro`")
        print("Tabela `imp_orc_financeiro` removida (se existia)")

        # Cria a tabela
        create_sql = """
            CREATE TABLE `imp_orc_financeiro` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `cod_conta` INT NOT NULL,
                `descricao` TEXT,
                `fonte` VARCHAR(255),
                `janeiro` DECIMAL(15,2) DEFAULT 0,
                `fevereiro` DECIMAL(15,2) DEFAULT 0,
                `marco` DECIMAL(15,2) DEFAULT 0,
                `abril` DECIMAL(15,2) DEFAULT 0,
                `maio` DECIMAL(15,2) DEFAULT 0,
                `junho` DECIMAL(15,2) DEFAULT 0,
                `julho` DECIMAL(15,2) DEFAULT 0,
                `agosto` DECIMAL(15,2) DEFAULT 0,
                `setembro` DECIMAL(15,2) DEFAULT 0,
                `outubro` DECIMAL(15,2) DEFAULT 0,
                `novembro` DECIMAL(15,2) DEFAULT 0,
                `dezembro` DECIMAL(15,2) DEFAULT 0,
                `data_importacao` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_cod_conta (`cod_conta`),
                INDEX idx_fonte (`fonte`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

        cursor.execute(create_sql)
        connection.commit()
        print("Tabela `imp_orc_financeiro` criada com sucesso!")

    except MySQLError as e:
        print(f"Erro ao criar tabela: {e}", file=sys.stderr)
        raise
    finally:
        cursor.close()


def inserir_dados(connection, df: pd.DataFrame) -> int:
    """
    Insere dados do DataFrame na tabela imp_orc_financeiro.
    """
    if df.empty:
        print("DataFrame vazio. Nenhum dado para inserir.")
        return 0

    cursor = connection.cursor()

    try:
        insert_sql = """
            INSERT INTO `imp_orc_financeiro`
                (cod_conta, descricao, fonte,
                 janeiro, fevereiro, marco, abril, maio, junho,
                 julho, agosto, setembro, outubro, novembro, dezembro)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Prepara dados para inserção
        rows_to_insert = []
        for _, row in df.iterrows():
            row_data = (
                row['cod_conta'],
                row['descricao'],
                row['fonte'],
                row['janeiro'],
                row['fevereiro'],
                row['marco'],
                row['abril'],
                row['maio'],
                row['junho'],
                row['julho'],
                row['agosto'],
                row['setembro'],
                row['outubro'],
                row['novembro'],
                row['dezembro']
            )
            rows_to_insert.append(row_data)

        # Insere em lotes
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


def importar_orcamento_financeiro(excel_path: Path, sheet_name: str) -> None:
    """
    Função principal que importa orçamento financeiro para o MySQL.
    """
    # Validação do arquivo
    if not excel_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {excel_path}")

    print(f"Lendo arquivo: {excel_path}")
    print(f"Aba: {sheet_name}")

    # Extrai dados
    print("\nExtraindo dados da planilha...")
    df = extrair_dados_orcamento_financeiro(excel_path, sheet_name)

    registros_lidos = len(df)
    print(f"Registros extraídos: {registros_lidos}")

    if registros_lidos > 0:
        # Mostra amostra
        print("\nAmostra dos dados extraídos:")
        print(df[['cod_conta', 'descricao', 'janeiro', 'dezembro']].head(15).to_string())

        # Estatísticas
        total_janeiro = df['janeiro'].sum()
        total_ano = df[list(MESES_COLUNAS.keys())].sum().sum()
        print(f"\nTotal Janeiro: R$ {total_janeiro:,.2f}")
        print(f"Total Ano: R$ {total_ano:,.2f}")

    # Conecta ao MySQL
    print(f"\nConectando ao MySQL ({DB_CONFIG['host']}:{DB_CONFIG['database']})...")
    registros_inseridos = 0

    try:
        connection = pymysql.connect(**DB_CONFIG)
        print("Conexão estabelecida!")

        # Cria a tabela
        print("\nRecriando tabela `imp_orc_financeiro`...")
        criar_tabela_imp_orc_financeiro(connection)

        # Insere os dados
        if registros_lidos > 0:
            print("\nInserindo dados na tabela `imp_orc_financeiro`...")
            registros_inseridos = inserir_dados(connection, df)

        # Relatório final
        print("\n" + "=" * 60)
        print("RESUMO DA IMPORTAÇÃO")
        print("=" * 60)
        print(f"Aba processada: {sheet_name}")
        print(f"Registros extraídos: {registros_lidos}")
        print(f"Registros inseridos: {registros_inseridos}")

        if registros_lidos != registros_inseridos:
            print("\n⚠️  ALERTA: A quantidade de registros extraídos é diferente da quantidade inserida!")
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


def parse_args(argv: List[str]) -> argparse.Namespace:
    """Processa argumentos da linha de comando."""
    parser = argparse.ArgumentParser(
        description='Importa orçamento financeiro do Excel para a tabela imp_orc_financeiro no MySQL'
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
        default='BASE DO ORÇAMENTO FINANCEIRO',
        help='Nome da aba a importar (padrão: "BASE DO ORÇAMENTO FINANCEIRO")'
    )

    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    """Função principal."""
    args = parse_args(argv)

    try:
        importar_orcamento_financeiro(
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
