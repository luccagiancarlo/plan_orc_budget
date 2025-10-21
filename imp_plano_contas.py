#!/usr/bin/env python3
"""
imp_plano_contas.py
-------------------
Importa dados da aba "plano de contas" do arquivo orc_2026.xlsx
para a tabela imp_plan_contas no banco de dados MySQL.

Uso:
  python imp_plano_contas.py
  python imp_plano_contas.py --excel orc_2026.xlsx

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


def sanitize_column_name(col_name: str) -> str:
    """
    Converte nome de coluna do Excel para nome válido de coluna MySQL.
    Remove espaços, caracteres especiais e garante que não seja palavra reservada.
    """
    # Remove espaços e substitui por underscore
    name = str(col_name).strip().replace(' ', '_')
    # Remove caracteres especiais
    name = ''.join(c if c.isalnum() or c == '_' else '_' for c in name)
    # Remove underscores duplicados
    while '__' in name:
        name = name.replace('__', '_')
    # Remove underscores do início e fim
    name = name.strip('_')
    # Se começar com número, adiciona prefixo
    if name and name[0].isdigit():
        name = f'col_{name}'
    # Se vazio, usa nome genérico
    if not name:
        name = 'coluna'
    return name.lower()


def get_mysql_type(dtype, col_name: str, sample_data) -> str:
    """
    Determina o tipo MySQL apropriado baseado no tipo pandas e nos dados.
    """
    dtype_str = str(dtype)

    # Analisa valores não-nulos para strings
    if dtype_str == 'object':
        max_len = 0
        if sample_data is not None and len(sample_data) > 0:
            non_null = [str(x) for x in sample_data if pd.notna(x)]
            if non_null:
                max_len = max(len(x) for x in non_null)

        # Decide entre VARCHAR e TEXT baseado no tamanho
        if max_len == 0:
            return 'VARCHAR(255)'
        elif max_len <= 255:
            return f'VARCHAR({min(max_len + 50, 255)})'
        elif max_len <= 1000:
            return 'VARCHAR(1000)'
        else:
            return 'TEXT'

    elif 'int' in dtype_str:
        return 'BIGINT'

    elif 'float' in dtype_str:
        return 'DOUBLE'

    elif 'datetime' in dtype_str or 'date' in dtype_str:
        return 'DATETIME'

    elif 'bool' in dtype_str:
        return 'BOOLEAN'

    else:
        return 'TEXT'


def create_table_from_dataframe(connection, table_name: str, df: pd.DataFrame, drop_if_exists: bool = False) -> None:
    """
    Cria uma tabela MySQL baseada na estrutura do DataFrame.
    """
    cursor = connection.cursor()

    try:
        # Remove a tabela se solicitado
        if drop_if_exists:
            cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
            print(f"Tabela `{table_name}` removida (se existia)")

        # Verifica se a tabela já existe
        cursor.execute(f"""
            SELECT COUNT(*) as count
            FROM information_schema.tables
            WHERE table_schema = '{DB_CONFIG['database']}'
            AND table_name = '{table_name}'
        """)
        result = cursor.fetchone()

        if result['count'] > 0:
            print(f"Tabela `{table_name}` já existe. Usando estrutura existente.")
            return

        # Mapeia colunas do DataFrame para colunas MySQL
        columns_def = []
        for col in df.columns:
            col_name = sanitize_column_name(col)
            col_type = get_mysql_type(df[col].dtype, col, df[col])
            columns_def.append(f"`{col_name}` {col_type}")

        # Adiciona chave primária auto-incremento
        create_sql = f"""
            CREATE TABLE `{table_name}` (
                `pk_id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                {', '.join(columns_def)},
                `data_importacao` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

        cursor.execute(create_sql)
        connection.commit()
        print(f"Tabela `{table_name}` criada com sucesso!")
        print(f"Colunas: {', '.join([sanitize_column_name(c) for c in df.columns])}")

    except MySQLError as e:
        print(f"Erro ao criar tabela: {e}", file=sys.stderr)
        raise
    finally:
        cursor.close()


def flag_nivel_to_nu_conta(flag_nivel) -> Optional[str]:
    """
    Converte FlagNivel para nu_conta.
    FlagNivel tem 15 dígitos divididos em grupos de 3.
    Exemplo: 001001001000000 -> 001.001.001

    Args:
        flag_nivel: Valor numérico do FlagNivel

    Returns:
        String formatada como nu_conta (ex: 001.001.001) ou None se inválido
    """
    if pd.isna(flag_nivel):
        return None

    # Converte para string, garantindo que seja inteiro primeiro
    flag_int = int(flag_nivel)
    flag_str = str(flag_int)

    # Adiciona zeros à esquerda até ter 15 dígitos
    flag_str = flag_str.zfill(15)

    # Divide em grupos de 3 dígitos
    grupos = []
    for i in range(0, 15, 3):
        grupos.append(flag_str[i:i+3])

    # Remove grupos '000' do final
    while grupos and grupos[-1] == '000':
        grupos.pop()

    return '.'.join(grupos) if grupos else None


def insert_dataframe_to_mysql(connection, table_name: str, df: pd.DataFrame) -> int:
    """
    Insere dados do DataFrame na tabela MySQL.
    Retorna o número de linhas inseridas.
    """
    if df.empty:
        print("DataFrame vazio. Nenhum dado para inserir.")
        return 0

    cursor = connection.cursor()

    try:
        # Mapeia nomes de colunas
        columns = [sanitize_column_name(col) for col in df.columns]
        columns_str = ', '.join([f'`{col}`' for col in columns])
        placeholders = ', '.join(['%s'] * len(columns))

        insert_sql = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"

        # Prepara dados para inserção
        rows_to_insert = []
        for _, row in df.iterrows():
            # Converte NaN para None (NULL no MySQL)
            row_data = []
            for val in row:
                if pd.isna(val):
                    row_data.append(None)
                elif isinstance(val, (pd.Timestamp, pd.DatetimeTZDtype)):
                    row_data.append(val.to_pydatetime())
                else:
                    row_data.append(val)
            rows_to_insert.append(tuple(row_data))

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


def sincronizar_val_orc_plancontas(connection) -> tuple[int, int]:
    """
    Sincroniza dados de imp_plan_contas para val_orc_plancontas.

    - Insere registros novos (que não existem em val_orc_plancontas)
    - Atualiza registros existentes com os dados mais recentes

    Mapeamento:
    - val_orc_plancontas.cd_conta = imp_plan_contas.id
    - val_orc_plancontas.cd_conta_pai = imp_plan_contas.idparent
    - val_orc_plancontas.nu_conta = imp_plan_contas.nu_conta
    - val_orc_plancontas.de_conta = imp_plan_contas.descricao
    - val_orc_plancontas.fl_ativo = 'S'
    - val_orc_plancontas.cd_empresa = 0
    - val_orc_plancontas.nu_conta_legado = imp_plan_contas.flagnivel

    Returns:
        tuple: (registros_inseridos, registros_atualizados)
    """
    cursor = connection.cursor()

    try:
        # 1. Atualiza registros existentes
        update_sql = """
            UPDATE val_orc_plancontas t1
            INNER JOIN imp_plan_contas t2 ON t1.cd_conta = t2.id
            SET
                t1.cd_conta_pai = t2.idparent,
                t1.nu_conta = t2.nu_conta,
                t1.de_conta = t2.descricao,
                t1.fl_ativo = 'S',
                t1.cd_empresa = 0,
                t1.nu_conta_legado = t2.flagnivel
        """

        cursor.execute(update_sql)
        registros_atualizados = cursor.rowcount
        connection.commit()
        print(f"Registros atualizados em val_orc_plancontas: {registros_atualizados}")

        # 2. Insere registros novos (que não existem em val_orc_plancontas)
        insert_sql = """
            INSERT INTO val_orc_plancontas
                (cd_conta, cd_conta_pai, nu_conta, de_conta, fl_ativo, cd_empresa, nu_conta_legado)
            SELECT
                t2.id,
                t2.idparent,
                t2.nu_conta,
                t2.descricao,
                'S',
                0,
                t2.flagnivel
            FROM imp_plan_contas t2
            LEFT JOIN val_orc_plancontas t1 ON t1.cd_conta = t2.id
            WHERE t1.cd_conta IS NULL
        """

        cursor.execute(insert_sql)
        registros_inseridos = cursor.rowcount
        connection.commit()
        print(f"Registros inseridos em val_orc_plancontas: {registros_inseridos}")

        return (registros_inseridos, registros_atualizados)

    except MySQLError as e:
        connection.rollback()
        print(f"\nErro ao sincronizar val_orc_plancontas: {e}", file=sys.stderr)
        raise
    finally:
        cursor.close()


def importar_planilha(excel_path: Path, sheet_name: str, table_name: Optional[str] = None) -> None:
    """
    Função principal que importa uma aba da planilha para o MySQL.
    A tabela será sempre recriada (DROP + CREATE) para garantir que a estrutura esteja atualizada.
    """
    # Validação do arquivo
    if not excel_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {excel_path}")

    print(f"Lendo arquivo: {excel_path}")
    print(f"Aba: {sheet_name}")

    # Lê a planilha
    try:
        df = pd.read_excel(excel_path, sheet_name=sheet_name, engine='openpyxl')
    except ValueError as e:
        # Tenta encontrar nome similar
        xls = pd.ExcelFile(excel_path)
        matching = [s for s in xls.sheet_names if sheet_name.lower() in s.lower()]
        if matching:
            print(f"Aba '{sheet_name}' não encontrada. Usando '{matching[0]}' em vez disso.")
            df = pd.read_excel(excel_path, sheet_name=matching[0], engine='openpyxl')
            sheet_name = matching[0]
        else:
            raise ValueError(f"Aba '{sheet_name}' não encontrada. Abas disponíveis: {xls.sheet_names}")

    registros_lidos = len(df)
    print(f"Dados lidos: {registros_lidos} linhas, {len(df.columns)} colunas")

    # Adiciona o campo nu_conta baseado no FlagNivel
    if 'FlagNivel' in df.columns:
        print("\nGerando campo nu_conta a partir de FlagNivel...")
        df['nu_conta'] = df['FlagNivel'].apply(flag_nivel_to_nu_conta)
        # Mostra alguns exemplos
        exemplos = df[['FlagNivel', 'nu_conta']].head(10)
        print("Exemplos de conversão FlagNivel -> nu_conta:")
        for idx, row in exemplos.iterrows():
            print(f"  {int(row['FlagNivel']) if pd.notna(row['FlagNivel']) else 'N/A':15d} -> {row['nu_conta']}")
    else:
        print("\nAVISO: Coluna 'FlagNivel' não encontrada. Campo nu_conta não será criado.")

    # Define o nome da tabela
    if table_name is None:
        table_name = sanitize_column_name(sheet_name)

    # Conecta ao MySQL
    print(f"\nConectando ao MySQL ({DB_CONFIG['host']}:{DB_CONFIG['database']})...")
    registros_inseridos = 0
    try:
        connection = pymysql.connect(**DB_CONFIG)
        print("Conexão estabelecida!")

        # Cria a tabela (sempre recria por padrão)
        print(f"\nRecriando tabela `{table_name}`...")
        create_table_from_dataframe(connection, table_name, df, drop_if_exists=True)

        # Insere os dados
        print(f"\nInserindo dados na tabela `{table_name}`...")
        registros_inseridos = insert_dataframe_to_mysql(connection, table_name, df)

        # Sincroniza com val_orc_plancontas
        print("\n" + "=" * 60)
        print("SINCRONIZANDO COM val_orc_plancontas")
        print("=" * 60)
        inseridos_val, atualizados_val = sincronizar_val_orc_plancontas(connection)

        # Relatório final
        print("\n" + "=" * 60)
        print("RESUMO DA IMPORTAÇÃO")
        print("=" * 60)
        print(f"Registros lidos da planilha: {registros_lidos}")
        print(f"Registros inseridos em imp_plan_contas: {registros_inseridos}")
        print(f"Registros inseridos em val_orc_plancontas: {inseridos_val}")
        print(f"Registros atualizados em val_orc_plancontas: {atualizados_val}")

        if registros_lidos != registros_inseridos:
            print("\n⚠️  ALERTA: A quantidade de registros lidos é diferente da quantidade inserida!")
            print(f"   Diferença: {abs(registros_lidos - registros_inseridos)} registros")
        else:
            print("\n✓ Todos os registros foram importados com sucesso!")
        print("=" * 60)

        print("\nImportação e sincronização concluídas!")

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
        description='Importa plano de contas do Excel para a tabela imp_plan_contas no MySQL'
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
        default='plano de contas',
        help='Nome da aba a importar (padrão: "plano de contas")'
    )
    parser.add_argument(
        '--table',
        type=str,
        default='imp_plan_contas',
        help='Nome da tabela MySQL (padrão: imp_plan_contas)'
    )

    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    """Função principal."""
    args = parse_args(argv)

    try:
        importar_planilha(
            excel_path=args.excel,
            sheet_name=args.sheet,
            table_name=args.table
        )
        return 0
    except Exception as e:
        print(f"\nERRO: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
