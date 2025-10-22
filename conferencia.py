#!/usr/bin/env python3
"""
conferencia.py
--------------
Programa para conferir valores de lançamentos por conta, agrupados por unidade,
mostrando valores mês a mês.

Uso:
  python conferencia.py --conta 110
  python conferencia.py --conta 110 --ano 2026
  python conferencia.py --conta 110 --ano 2026 --empresa 1
  python conferencia.py --conta 110 --ano 2026 --tipo "Saldo Inicial"

Requisitos:
  - pymysql
"""
import argparse
import sys
from typing import List, Dict, Any, Optional
from decimal import Decimal

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

MESES_NOMES = [
    'Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun',
    'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'
]


def obter_info_conta(connection, cd_conta: int) -> Optional[Dict]:
    """
    Obtém informações da conta na tabela val_orc_plancontas.
    """
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT cd_conta, nu_conta, de_conta, cd_empresa
            FROM val_orc_plancontas
            WHERE cd_conta = %s
        """, (cd_conta,))

        return cursor.fetchone()
    finally:
        cursor.close()


def obter_lancamentos_por_unidade(connection, cd_conta: int, nu_ano: Optional[int] = None,
                                   tp_lancamento: Optional[str] = None,
                                   cd_empresa: Optional[int] = None,
                                   cd_unidade: Optional[int] = None,
                                   nu_mes: Optional[int] = None) -> List[Dict]:
    """
    Obtém lançamentos agrupados por unidade, com valores mensais.

    Retorna lista de dicionários:
    [
        {
            'cd_unidade': 5,
            'de_unidade': 'GESTAO DA PRESIDENCIA',
            'cd_empresa': 1,
            'valores_mensais': {1: 100.0, 2: 200.0, ...}
        },
        ...
    ]
    """
    cursor = connection.cursor()

    try:
        # Monta a query base
        where_clauses = ["l.cd_conta = %s"]
        params = [cd_conta]

        if nu_ano is not None:
            where_clauses.append("l.nu_ano = %s")
            params.append(nu_ano)

        if tp_lancamento:
            where_clauses.append("l.tp_lancamento = %s")
            params.append(tp_lancamento)

        if cd_empresa is not None:
            where_clauses.append("l.cd_empresa = %s")
            params.append(cd_empresa)

        if cd_unidade is not None:
            where_clauses.append("l.cd_unidade = %s")
            params.append(cd_unidade)

        if nu_mes is not None:
            where_clauses.append("l.nu_mes = %s")
            params.append(nu_mes)

        where_sql = " AND ".join(where_clauses)

        # Query para buscar lançamentos com informações de unidade
        query = f"""
            SELECT
                l.cd_unidade,
                u.de_unidade,
                l.cd_empresa,
                l.nu_mes,
                SUM(l.vl_lancamento) as vl_total
            FROM val_orc_lancamentos l
            LEFT JOIN val_orc_unidade u ON l.cd_unidade = u.cd_unidade
            WHERE {where_sql}
            GROUP BY l.cd_unidade, u.de_unidade, l.cd_empresa, l.nu_mes
            ORDER BY l.cd_empresa, l.cd_unidade, l.nu_mes
        """

        cursor.execute(query, params)
        resultados = cursor.fetchall()

        # Organiza por unidade
        unidades_dict = {}

        for row in resultados:
            cd_unidade = row['cd_unidade']

            if cd_unidade not in unidades_dict:
                unidades_dict[cd_unidade] = {
                    'cd_unidade': cd_unidade,
                    'de_unidade': row['de_unidade'] or f'Unidade {cd_unidade}',
                    'cd_empresa': row['cd_empresa'],
                    'valores_mensais': {}
                }

            nu_mes = row['nu_mes']
            vl_total = float(row['vl_total']) if row['vl_total'] is not None else 0.0
            unidades_dict[cd_unidade]['valores_mensais'][nu_mes] = vl_total

        # Converte para lista ordenada
        unidades_list = sorted(unidades_dict.values(),
                              key=lambda x: (x['cd_empresa'], x['cd_unidade']))

        return unidades_list

    finally:
        cursor.close()


def formatar_valor(valor: float) -> str:
    """
    Formata valor monetário.
    """
    return f"{valor:>12,.2f}"


def exibir_relatorio(cd_conta: int, nu_ano: Optional[int], info_conta: Dict,
                     unidades: List[Dict], tp_lancamento: Optional[str] = None,
                     cd_empresa: Optional[int] = None, cd_unidade: Optional[int] = None,
                     nu_mes: Optional[int] = None) -> None:
    """
    Exibe relatório formatado dos lançamentos.
    """
    print("=" * 180)
    print(f"CONFERÊNCIA DE LANÇAMENTOS - CONTA {cd_conta}")
    print("=" * 180)
    print(f"Conta: {info_conta['nu_conta']} - {info_conta['de_conta']}")

    # Mostra filtros aplicados
    filtros = []
    if nu_ano is not None:
        filtros.append(f"Ano: {nu_ano}")
    else:
        filtros.append("Ano: TODOS")

    if tp_lancamento:
        filtros.append(f"Tipo: {tp_lancamento}")

    if cd_empresa is not None:
        filtros.append(f"Empresa: {cd_empresa}")

    if cd_unidade is not None:
        filtros.append(f"Unidade: {cd_unidade}")

    if nu_mes is not None:
        filtros.append(f"Mês: {nu_mes} ({MESES_NOMES[nu_mes-1]})")

    print(" | ".join(filtros))
    print("=" * 180)

    if not unidades:
        print("\n⚠️  Nenhum lançamento encontrado para os filtros especificados.")
        print("=" * 180)
        return

    # Cabeçalho da tabela
    print(f"\n{'Empresa':>7s} {'Unidade':>7s} {'Descrição da Unidade':40s}", end='')
    for mes_nome in MESES_NOMES:
        print(f" {mes_nome:>12s}", end='')
    print(f" {'TOTAL ANO':>12s}")
    print("-" * 180)

    # Totalizadores
    totais_mensais = {mes: 0.0 for mes in range(1, 13)}
    total_geral = 0.0

    empresa_atual = None

    for unidade in unidades:
        cd_empresa = unidade['cd_empresa']
        cd_unidade = unidade['cd_unidade']
        de_unidade = unidade['de_unidade']
        valores = unidade['valores_mensais']

        # Quebra por empresa
        if empresa_atual != cd_empresa:
            if empresa_atual is not None:
                print("-" * 180)
            empresa_atual = cd_empresa

        # Linha da unidade
        print(f"{cd_empresa:>7d} {cd_unidade:>7d} {de_unidade[:40]:40s}", end='')

        total_unidade = 0.0
        for mes in range(1, 13):
            valor = valores.get(mes, 0.0)
            print(f" {formatar_valor(valor)}", end='')
            totais_mensais[mes] += valor
            total_unidade += valor

        print(f" {formatar_valor(total_unidade)}")
        total_geral += total_unidade

    # Linha de totais
    print("=" * 180)
    print(f"{'':>7s} {'':>7s} {'TOTAL GERAL':40s}", end='')
    for mes in range(1, 13):
        print(f" {formatar_valor(totais_mensais[mes])}", end='')
    print(f" {formatar_valor(total_geral)}")
    print("=" * 180)

    # Resumo
    print(f"\nResumo:")
    print(f"  Total de unidades: {len(unidades)}")
    print(f"  Total de empresas: {len(set(u['cd_empresa'] for u in unidades))}")
    print(f"  Valor total do ano: R$ {total_geral:,.2f}")
    print("=" * 180)


def conferir_conta(cd_conta: int, nu_ano: Optional[int] = None,
                   tp_lancamento: Optional[str] = None,
                   cd_empresa: Optional[int] = None,
                   cd_unidade: Optional[int] = None,
                   nu_mes: Optional[int] = None) -> None:
    """
    Função principal que executa a conferência de uma conta.
    """
    try:
        connection = pymysql.connect(**DB_CONFIG)

        # Obtém informações da conta
        info_conta = obter_info_conta(connection, cd_conta)

        if not info_conta:
            print(f"⚠️  Conta {cd_conta} não encontrada na tabela val_orc_plancontas!")
            return

        # Obtém lançamentos por unidade
        unidades = obter_lancamentos_por_unidade(
            connection, cd_conta, nu_ano, tp_lancamento, cd_empresa, cd_unidade, nu_mes
        )

        # Exibe relatório
        exibir_relatorio(cd_conta, nu_ano, info_conta, unidades, tp_lancamento,
                        cd_empresa, cd_unidade, nu_mes)

    except MySQLError as e:
        print(f"✗ Erro de MySQL: {e}", file=sys.stderr)
        raise
    except Exception as e:
        print(f"✗ Erro: {e}", file=sys.stderr)
        raise
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()


def parse_args(argv: List[str]) -> argparse.Namespace:
    """Processa argumentos da linha de comando."""
    parser = argparse.ArgumentParser(
        description='Conferência de lançamentos por conta, agrupados por unidade',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python conferencia.py --conta 110
  python conferencia.py --conta 110 --ano 2026
  python conferencia.py --conta 110 --ano 2026 --empresa 1
  python conferencia.py --conta 110 --ano 2026 --tipo "Saldo Inicial"
  python conferencia.py --conta 153 --ano 2026 --empresa 4
  python conferencia.py --conta 153 --ano 2026 --unidade 5
  python conferencia.py --conta 153 --ano 2026 --mes 1
  python conferencia.py --conta 153 --ano 2026 --empresa 4 --unidade 40 --mes 6

Dicas:
  - Se não informar --ano, mostra TODOS os anos
  - Se não informar --mes, mostra TODOS os meses
  - Se não informar --unidade, mostra TODAS as unidades
  - Use --tipo para filtrar por tipo de lançamento (ex: "Saldo Inicial")
  - Use --empresa para filtrar por empresa específica
  - O relatório mostra valores agrupados por unidade com totais mensais
        """
    )
    parser.add_argument(
        '--conta',
        type=int,
        required=True,
        help='Código da conta (cd_conta) para conferir'
    )
    parser.add_argument(
        '--ano',
        type=int,
        default=None,
        help='Ano dos lançamentos (opcional, se não informado mostra todos)'
    )
    parser.add_argument(
        '--mes',
        type=int,
        default=None,
        choices=range(1, 13),
        help='Mês específico (1-12, opcional, se não informado mostra todos)'
    )
    parser.add_argument(
        '--unidade',
        type=int,
        default=None,
        help='Código da unidade para filtrar (opcional, se não informado mostra todas)'
    )
    parser.add_argument(
        '--tipo',
        type=str,
        default=None,
        help='Tipo de lançamento (ex: "Saldo Inicial")'
    )
    parser.add_argument(
        '--empresa',
        type=int,
        default=None,
        help='Código da empresa para filtrar (opcional)'
    )

    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    """Função principal."""
    args = parse_args(argv)

    try:
        conferir_conta(
            cd_conta=args.conta,
            nu_ano=args.ano,
            tp_lancamento=args.tipo,
            cd_empresa=args.empresa,
            cd_unidade=args.unidade,
            nu_mes=args.mes
        )
        return 0
    except Exception as e:
        print(f"\nERRO: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
