#!/usr/bin/env python3
"""
inserir_lancamentos.py
----------------------
Insere lançamentos de orçamento na tabela val_orc_lancamentos
a partir dos dados da tabela imp_base_fixo.

Converte formato colunar (janeiro, fevereiro, ..., dezembro)
para formato de linhas (nu_mes = 1 a 12).

IMPORTANTE: Execute primeiro um SELECT na tabela val_orc_lancamentos
para ver o padrão dos campos e ajustar o script se necessário:

  SELECT * FROM val_orc_lancamentos
  WHERE tp_lancamento='Saldo Inicial' AND cd_conta=110 LIMIT 1;

Uso:
  python inserir_lancamentos.py --ano 2026
  python inserir_lancamentos.py --ano 2026 --limpar  # Remove ano antes

Requisitos:
  - pymysql
"""
import argparse
import sys
from typing import List, Dict, Any
from datetime import datetime

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

# Mapeamento de meses para números
MESES_NOMES = {
    1: 'janeiro',
    2: 'fevereiro',
    3: 'marco',
    4: 'abril',
    5: 'maio',
    6: 'junho',
    7: 'julho',
    8: 'agosto',
    9: 'setembro',
    10: 'outubro',
    11: 'novembro',
    12: 'dezembro'
}


def analisar_estrutura_tabela(connection) -> Dict[str, Any]:
    """
    Analisa a estrutura da tabela val_orc_lancamentos e um registro de exemplo.
    """
    cursor = connection.cursor()
    try:
        # Obtém estrutura
        cursor.execute("DESCRIBE val_orc_lancamentos")
        colunas_estrutura = {row['Field']: row for row in cursor.fetchall()}

        # Obtém um registro de exemplo
        cursor.execute("""
            SELECT * FROM val_orc_lancamentos
            WHERE tp_lancamento='Saldo Inicial'
            LIMIT 1
        """)
        exemplo = cursor.fetchone()

        # Identifica chave primária
        pk_column = None
        for col_name, col_info in colunas_estrutura.items():
            if col_info['Key'] == 'PRI':
                pk_column = col_name
                break

        return {
            'estrutura': colunas_estrutura,
            'exemplo': exemplo,
            'pk_column': pk_column or 'id',
            'colunas_disponiveis': list(colunas_estrutura.keys())
        }

    finally:
        cursor.close()


def obter_proximo_id(connection, pk_column: str) -> int:
    """
    Obtém o próximo ID disponível para inserção.
    """
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT COALESCE(MAX(`{pk_column}`), 0) as max_id FROM val_orc_lancamentos")
        result = cursor.fetchone()
        return result['max_id'] + 1
    finally:
        cursor.close()


def ler_dados_imp_base_fixo(connection) -> List[Dict]:
    """
    Lê todos os dados da tabela imp_base_fixo.
    """
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT
                cod_interno,
                cod_unidade,
                cd_empresa,
                fonte,
                janeiro, fevereiro, marco, abril, maio, junho,
                julho, agosto, setembro, outubro, novembro, dezembro
            FROM imp_base_fixo
            ORDER BY cd_empresa, cod_unidade, cod_interno
        """)

        registros = cursor.fetchall()
        return registros

    finally:
        cursor.close()


def criar_lancamento(registro: Dict, nu_mes: int, nome_mes: str, nu_ano: int,
                     current_id: int) -> Dict:
    """
    Cria um dicionário de lançamento baseado no registro de origem.

    Estrutura baseada na tabela val_orc_lancamentos:
    cd_lancamento, tp_lancamento, cd_conta, cd_unidade, nu_mes, nu_ano,
    de_historico, vl_lancamento, fl_ativo, cd_empresa, dt_registro,
    dt_alteracao, cd_usuario
    """
    vl_lancamento = float(registro[nome_mes]) if registro[nome_mes] is not None else 0.0

    now = datetime.now()

    # Monta o lançamento seguindo a estrutura exata da tabela
    lancamento = {
        'cd_lancamento': current_id,
        'tp_lancamento': 'Saldo Inicial',
        'cd_conta': registro['cod_interno'],
        'cd_unidade': registro['cod_unidade'] if registro['cod_unidade'] is not None else 0,
        'nu_mes': nu_mes,
        'nu_ano': nu_ano,
        'de_historico': 'Saldo inicial previsto',
        'vl_lancamento': vl_lancamento,
        'fl_ativo': 'S',
        'cd_empresa': registro['cd_empresa'],
        'dt_registro': now,
        'dt_alteracao': now,
        'cd_usuario': 1
    }

    return lancamento


def transformar_para_lancamentos(registros: List[Dict], nu_ano: int, proximo_id: int) -> List[Dict]:
    """
    Transforma registros de formato colunar para formato de linhas.
    """
    lancamentos = []
    current_id = proximo_id

    for registro in registros:
        # Para cada mês (1 a 12)
        for nu_mes, nome_mes in MESES_NOMES.items():
            lancamento = criar_lancamento(
                registro, nu_mes, nome_mes, nu_ano, current_id
            )

            lancamentos.append(lancamento)
            current_id += 1

    return lancamentos


def inserir_lancamentos(connection, lancamentos: List[Dict]) -> int:
    """
    Insere lançamentos na tabela val_orc_lancamentos.
    """
    if not lancamentos:
        print("Nenhum lançamento para inserir.")
        return 0

    cursor = connection.cursor()

    try:
        # Pega as colunas do primeiro lançamento
        colunas = list(lancamentos[0].keys())
        colunas_str = ', '.join([f"`{col}`" for col in colunas])
        placeholders = ', '.join(['%s'] * len(colunas))

        insert_sql = f"""
            INSERT INTO val_orc_lancamentos
                ({colunas_str})
            VALUES
                ({placeholders})
        """

        print(f"\nSQL de inserção preparado com {len(colunas)} colunas:")
        print(f"Colunas: {', '.join(colunas[:10])}{'...' if len(colunas) > 10 else ''}")

        # Prepara dados para inserção (tuplas na ordem das colunas)
        rows_to_insert = []
        for lanc in lancamentos:
            row_data = tuple(lanc[col] for col in colunas)
            rows_to_insert.append(row_data)

        # Insere em lotes
        batch_size = 5000
        total_inserted = 0

        for i in range(0, len(rows_to_insert), batch_size):
            batch = rows_to_insert[i:i + batch_size]
            cursor.executemany(insert_sql, batch)
            connection.commit()
            total_inserted += len(batch)
            print(f"Inseridos {total_inserted}/{len(rows_to_insert)} lançamentos...", end='\r')

        print(f"\nTotal de {total_inserted} lançamentos inseridos com sucesso!")
        return total_inserted

    except MySQLError as e:
        connection.rollback()
        print(f"\nErro ao inserir lançamentos: {e}", file=sys.stderr)
        print(f"Exemplo de dados que causou erro: {lancamentos[0] if lancamentos else 'N/A'}")
        raise
    finally:
        cursor.close()


def processar_insercao(nu_ano: int, limpar_ano: bool = False) -> None:
    """
    Função principal que processa a inserção de lançamentos.
    """
    print(f"Processando inserção de lançamentos para o ano {nu_ano}")
    print("=" * 70)

    try:
        connection = pymysql.connect(**DB_CONFIG)
        print(f"✓ Conexão estabelecida com {DB_CONFIG['database']}")

        # Analisa estrutura da tabela
        print("\n1. Analisando estrutura da tabela val_orc_lancamentos...")
        info_tabela = analisar_estrutura_tabela(connection)

        print(f"   Colunas na tabela: {len(info_tabela['colunas_disponiveis'])}")
        print(f"   Chave primária: {info_tabela['pk_column']}")
        print(f"   Colunas: {', '.join(info_tabela['colunas_disponiveis'])}")

        if info_tabela['exemplo']:
            print(f"\n   Exemplo de registro encontrado:")
            for key, value in list(info_tabela['exemplo'].items())[:10]:
                print(f"     {key}: {value}")
            if len(info_tabela['exemplo']) > 10:
                print(f"     ... e mais {len(info_tabela['exemplo']) - 10} campos")

        # Limpa lançamentos do ano se solicitado
        if limpar_ano:
            print(f"\n2. Limpando lançamentos existentes para o ano {nu_ano}...")
            cursor = connection.cursor()
            cursor.execute("""
                DELETE FROM val_orc_lancamentos
                WHERE nu_ano = %s AND tp_lancamento = 'Saldo Inicial'
            """, (nu_ano,))
            linhas_deletadas = cursor.rowcount
            connection.commit()
            cursor.close()
            print(f"   ✓ Deletados {linhas_deletadas} lançamentos existentes")
        else:
            print(f"\n2. Pulando limpeza de lançamentos existentes")

        # Obtém próximo ID
        print(f"\n3. Obtendo próximo ID disponível...")
        proximo_id = obter_proximo_id(connection, info_tabela['pk_column'])
        print(f"   Próximo ID: {proximo_id}")

        # Lê dados de imp_base_fixo
        print("\n4. Lendo dados de imp_base_fixo...")
        registros = ler_dados_imp_base_fixo(connection)
        print(f"   ✓ Lidos {len(registros)} registros")

        if not registros:
            print("\n   ⚠️  AVISO: Nenhum registro encontrado em imp_base_fixo!")
            print("   Execute primeiro: python imp_base_fixo.py")
            return

        # Transforma para formato de lançamentos
        print(f"\n5. Transformando registros em lançamentos mensais...")
        lancamentos = transformar_para_lancamentos(
            registros, nu_ano, proximo_id
        )
        print(f"   ✓ Gerados {len(lancamentos)} lançamentos")
        print(f"     ({len(registros)} registros × 12 meses)")

        # Mostra exemplo do primeiro lançamento
        if lancamentos:
            print(f"\n   Exemplo do primeiro lançamento:")
            primeiro = lancamentos[0]
            for key, value in list(primeiro.items())[:8]:
                print(f"     {key}: {value}")

        # Insere lançamentos
        print("\n6. Inserindo lançamentos em val_orc_lancamentos...")
        total_inserido = inserir_lancamentos(connection, lancamentos)

        # Relatório final
        print("\n" + "=" * 70)
        print("RESUMO DA INSERÇÃO")
        print("=" * 70)
        print(f"Ano: {nu_ano}")
        print(f"Tipo de lançamento: Saldo Inicial")
        print(f"Registros origem (imp_base_fixo): {len(registros)}")
        print(f"Lançamentos inseridos: {total_inserido}")
        print(f"Meses por registro: 12")
        print(f"ID inicial: {proximo_id}")
        print(f"ID final: {proximo_id + total_inserido - 1}")
        print("=" * 70)

        print("\n✓ Inserção concluída com sucesso!")

    except MySQLError as e:
        print(f"\n✗ Erro de MySQL: {e}", file=sys.stderr)
        raise
    except Exception as e:
        print(f"\n✗ Erro: {e}", file=sys.stderr)
        raise
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()
            print("\nConexão fechada.")


def parse_args(argv: List[str]) -> argparse.Namespace:
    """Processa argumentos da linha de comando."""
    parser = argparse.ArgumentParser(
        description='Insere lançamentos de orçamento em val_orc_lancamentos a partir de imp_base_fixo',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python inserir_lancamentos.py --ano 2026
  python inserir_lancamentos.py --ano 2026 --limpar  # Remove lançamentos existentes do ano primeiro

Notas:
  - Cada registro de imp_base_fixo gera 12 lançamentos (um por mês)
  - cd_conta = cod_interno da imp_base_fixo
  - cd_unidade = cod_unidade da imp_base_fixo
  - cd_empresa = cd_empresa da imp_base_fixo
  - tp_lancamento sempre = 'Saldo Inicial'
  - nu_mes = 1 a 12
  - vl_lancamento = valor do mês correspondente
  - O ID é incrementado automaticamente a partir do último registro + 1
  - Outros campos seguem o padrão encontrado nos registros existentes

Pré-requisitos:
  - Tabela imp_base_fixo deve estar populada
  - Execute: python imp_base_fixo.py
        """
    )
    parser.add_argument(
        '--ano',
        type=int,
        default=2026,
        help='Ano dos lançamentos (padrão: 2026)'
    )
    parser.add_argument(
        '--limpar',
        action='store_true',
        help='Remove lançamentos existentes do ano antes de inserir'
    )

    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    """Função principal."""
    args = parse_args(argv)

    try:
        processar_insercao(
            nu_ano=args.ano,
            limpar_ano=args.limpar
        )
        return 0
    except Exception as e:
        print(f"\nERRO: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
