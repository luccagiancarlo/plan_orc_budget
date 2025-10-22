#!/usr/bin/env python3
"""
imp_base_fixo.py
----------------
Importa dados das abas de base fixa para a tabela imp_base_fixo no MySQL.

Por padrão, processa TODAS as abas de base fixa automaticamente:
- Base fixo Distribuidora (cd_empresa = 1)
- Base fixo Unibox (cd_empresa = 2)
- Base fixo industria Máscara (cd_empresa = 3)
- Base fixo Uniplast (cd_empresa = 4)
- Base fixo UNIPACK (cd_empresa = 7)

Extrai:
- Código da conta (nu_conta) do plano de contas
- Valores orçados mensais (janeiro a dezembro)
- Código da empresa (cd_empresa)
- Nome da aba/fonte de dados

Uso:
  python imp_base_fixo.py                    # Processa todas as abas
  python imp_base_fixo.py --sheet "Base fixo Distribuidora"  # Apenas uma aba

Requisitos:
  - pandas
  - openpyxl
  - pymysql
"""
import argparse
import re
import sys
from pathlib import Path
from typing import Optional, List, Dict

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

# Mapeamento das abas para códigos de empresa
ABAS_EMPRESAS = {
    'Base fixo Distribuidora': 1,
    'Base fixo Unibox': 2,
    'Base fixo industria Máscara': 3,
    'Base fixo Uniplast': 4,
    'Base fixo UNIPACK': 7,
}

# Mapeamento das colunas de valores orçados
# Colunas 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27 = Jan a Dez (ORÇADO)
MESES_COLUNAS = {
    'janeiro': 5,
    'fevereiro': 7,
    'marco': 9,
    'abril': 11,
    'maio': 13,
    'junho': 15,
    'julho': 17,
    'agosto': 19,
    'setembro': 21,
    'outubro': 23,
    'novembro': 25,
    'dezembro': 27
}


def extrair_codigo_conta(texto: str) -> Optional[str]:
    """
    Extrai o código da conta de uma string como:
    '004.001.001.001.003 - PRÊMIOS DE PRODUÇÃO'
    '    04.01.01.01.04 - GRATIFICAÇÕES'

    Retorna o código normalizado (sem zeros à esquerda nos grupos).
    """
    if pd.isna(texto):
        return None

    texto = str(texto).strip()

    # Procura padrão: números.números - descrição
    match = re.search(r'([\d.]+)\s*-', texto)
    if not match:
        return None

    codigo = match.group(1).strip()

    # Remove espaços e zeros à esquerda de cada grupo
    # Ex: "004.001.001" -> "4.1.1" ou mantém "004.001.001"
    # Vamos normalizar removendo zeros à esquerda de cada grupo
    grupos = codigo.split('.')
    grupos_normalizados = []
    for grupo in grupos:
        # Remove zeros à esquerda, mas mantém pelo menos um dígito
        grupo_limpo = grupo.lstrip('0') or '0'
        grupos_normalizados.append(grupo_limpo)

    return '.'.join(grupos_normalizados)


def extrair_dados_aba(excel_path: Path, sheet_name: str, cd_empresa: int) -> pd.DataFrame:
    """
    Extrai dados de uma aba de base fixa.

    Args:
        excel_path: Caminho do arquivo Excel
        sheet_name: Nome da aba
        cd_empresa: Código da empresa

    Retorna DataFrame com colunas:
    - cod_interno: código interno da linha (coluna 1)
    - cod_unidade: código da unidade orçamentária (coluna 0)
    - nu_conta: código da conta do plano de contas
    - descricao: descrição da conta
    - cd_empresa: código da empresa
    - janeiro, fevereiro, ..., dezembro: valores orçados
    - fonte: nome da aba
    """
    # Lê a aba sem processar cabeçalho
    df_raw = pd.read_excel(excel_path, sheet_name=sheet_name, header=None, engine='openpyxl')

    registros = []
    conta_pattern = re.compile(r'\d{1,3}\.\d{1,3}')  # Padrão mínimo de código de conta

    # Variável para rastrear o código de unidade atual
    cod_unidade_atual = None

    for idx in range(len(df_raw)):
        row = df_raw.iloc[idx]

        # Detecta código de unidade:
        # - Padrão 1: coluna 0 tem número e coluna 1 está vazia
        # - Padrão 2: coluna 0 está vazia, coluna 1 tem número e coluna 2 não parece código de conta
        #   (ex: Base fixo Uniplast onde col1=5, col2="Gestão da Presidência")
        if pd.notna(row[0]) and pd.isna(row[1]):
            try:
                cod_unidade_atual = int(row[0])
                continue
            except (ValueError, TypeError):
                pass

        if pd.isna(row[0]) and pd.notna(row[1]):
            try:
                cod_unidade_candidato = int(row[1])
                # Verifica se col2 não parece ser um código de conta (não tem padrão de números com pontos)
                col2_str = str(row[2]) if pd.notna(row[2]) else ''
                # Se col2 não tem padrão de conta (ex: "04.01.01.01"), então col1 é cod_unidade
                if not conta_pattern.search(col2_str):
                    cod_unidade_atual = cod_unidade_candidato
                    continue
            except (ValueError, TypeError):
                pass

        # Verifica se tem dados nas colunas 1 (COD) e 2 (CONTA)
        if pd.isna(row[1]) or pd.isna(row[2]):
            continue

        texto_conta = str(row[2])

        # Verifica se parece com uma linha de conta
        if not conta_pattern.search(texto_conta):
            continue

        # Extrai código da conta
        nu_conta = extrair_codigo_conta(texto_conta)
        if not nu_conta:
            continue

        # Código interno (coluna 1)
        cod_interno = row[1]

        # Extrai descrição (após o hífen)
        match_desc = re.search(r'-\s*(.+)$', texto_conta)
        descricao = match_desc.group(1).strip() if match_desc else texto_conta

        # Extrai valores mensais (colunas ORÇADO)
        valores = {}
        for mes, col_idx in MESES_COLUNAS.items():
            valor = row[col_idx]
            valores[mes] = float(valor) if pd.notna(valor) else 0.0

        # Monta registro
        registro = {
            'cod_interno': cod_interno,
            'cod_unidade': cod_unidade_atual,
            'nu_conta': nu_conta,
            'descricao': descricao,
            'cd_empresa': cd_empresa,
            'fonte': sheet_name,
            **valores
        }

        registros.append(registro)

    df_result = pd.DataFrame(registros)
    return df_result


def criar_tabela_imp_base_fixo(connection) -> None:
    """
    Cria a tabela imp_base_fixo (sempre recria).
    """
    cursor = connection.cursor()

    try:
        # Remove a tabela se existir
        cursor.execute("DROP TABLE IF EXISTS `imp_base_fixo`")
        print("Tabela `imp_base_fixo` removida (se existia)")

        # Cria a tabela
        create_sql = """
            CREATE TABLE `imp_base_fixo` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `cod_interno` VARCHAR(50),
                `cod_unidade` INT,
                `nu_conta` VARCHAR(50) NOT NULL,
                `descricao` TEXT,
                `cd_empresa` INT NOT NULL,
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
                INDEX idx_nu_conta (`nu_conta`),
                INDEX idx_cod_unidade (`cod_unidade`),
                INDEX idx_cd_empresa (`cd_empresa`),
                INDEX idx_fonte (`fonte`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

        cursor.execute(create_sql)
        connection.commit()
        print("Tabela `imp_base_fixo` criada com sucesso!")

    except MySQLError as e:
        print(f"Erro ao criar tabela: {e}", file=sys.stderr)
        raise
    finally:
        cursor.close()


def inserir_dados(connection, df: pd.DataFrame) -> int:
    """
    Insere dados do DataFrame na tabela imp_base_fixo.
    """
    if df.empty:
        print("DataFrame vazio. Nenhum dado para inserir.")
        return 0

    cursor = connection.cursor()

    try:
        insert_sql = """
            INSERT INTO `imp_base_fixo`
                (cod_interno, cod_unidade, nu_conta, descricao, cd_empresa, fonte,
                 janeiro, fevereiro, marco, abril, maio, junho,
                 julho, agosto, setembro, outubro, novembro, dezembro)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Prepara dados para inserção
        rows_to_insert = []
        for _, row in df.iterrows():
            row_data = (
                row['cod_interno'],
                row['cod_unidade'] if pd.notna(row['cod_unidade']) else None,
                row['nu_conta'],
                row['descricao'],
                row['cd_empresa'],
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


def importar_base_fixo(excel_path: Path, sheet_names: Optional[List[str]] = None) -> None:
    """
    Função principal que importa base fixa para o MySQL.

    Args:
        excel_path: Caminho do arquivo Excel
        sheet_names: Lista de abas para processar. Se None, processa todas as abas configuradas.
    """
    # Validação do arquivo
    if not excel_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {excel_path}")

    print(f"Lendo arquivo: {excel_path}")

    # Define quais abas processar
    if sheet_names is None:
        # Processa todas as abas configuradas
        abas_processar = list(ABAS_EMPRESAS.keys())
        print(f"\nProcessando TODAS as abas de base fixa ({len(abas_processar)} abas)")
    else:
        # Processa apenas as abas especificadas
        abas_processar = sheet_names
        print(f"\nProcessando {len(abas_processar)} aba(s)")

    # Conecta ao MySQL
    print(f"\nConectando ao MySQL ({DB_CONFIG['host']}:{DB_CONFIG['database']})...")

    try:
        connection = pymysql.connect(**DB_CONFIG)
        print("Conexão estabelecida!")

        # Cria a tabela (uma única vez)
        print("\nRecriando tabela `imp_base_fixo`...")
        criar_tabela_imp_base_fixo(connection)

        # Processa cada aba
        total_registros_extraidos = 0
        total_registros_inseridos = 0
        resumo_por_aba = []

        for sheet_name in abas_processar:
            # Verifica se a aba está no mapeamento
            if sheet_name not in ABAS_EMPRESAS:
                print(f"\n⚠️  AVISO: Aba '{sheet_name}' não está no mapeamento de empresas. Pulando...")
                continue

            cd_empresa = ABAS_EMPRESAS[sheet_name]

            print(f"\n{'='*60}")
            print(f"Processando: {sheet_name} (cd_empresa={cd_empresa})")
            print(f"{'='*60}")

            # Extrai dados
            try:
                df = extrair_dados_aba(excel_path, sheet_name, cd_empresa)
                registros_lidos = len(df)
                print(f"Registros extraídos: {registros_lidos}")

                if registros_lidos > 0:
                    # Estatísticas
                    total_janeiro = df['janeiro'].sum()
                    total_ano = df[list(MESES_COLUNAS.keys())].sum().sum()
                    print(f"Total Janeiro: R$ {total_janeiro:,.2f}")
                    print(f"Total Ano: R$ {total_ano:,.2f}")

                    # Insere os dados
                    print(f"Inserindo dados...")
                    registros_inseridos = inserir_dados(connection, df)

                    total_registros_extraidos += registros_lidos
                    total_registros_inseridos += registros_inseridos

                    resumo_por_aba.append({
                        'aba': sheet_name,
                        'cd_empresa': cd_empresa,
                        'extraidos': registros_lidos,
                        'inseridos': registros_inseridos
                    })
                else:
                    print("Nenhum registro encontrado nesta aba.")

            except Exception as e:
                print(f"⚠️  Erro ao processar aba '{sheet_name}': {e}")
                continue

        # Relatório final
        print("\n" + "=" * 70)
        print("RESUMO GERAL DA IMPORTAÇÃO")
        print("=" * 70)
        print(f"{'Aba':<35} {'Empresa':<8} {'Extraídos':<12} {'Inseridos':<12}")
        print("-" * 70)
        for item in resumo_por_aba:
            print(f"{item['aba']:<35} {item['cd_empresa']:<8} {item['extraidos']:<12} {item['inseridos']:<12}")
        print("-" * 70)
        print(f"{'TOTAL':<35} {'':<8} {total_registros_extraidos:<12} {total_registros_inseridos:<12}")
        print("=" * 70)

        if total_registros_extraidos != total_registros_inseridos:
            print("\n⚠️  ALERTA: A quantidade de registros extraídos é diferente da quantidade inserida!")
            print(f"   Diferença: {abs(total_registros_extraidos - total_registros_inseridos)} registros")
        else:
            print("\n✓ Todos os registros foram importados com sucesso!")

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
        description='Importa base fixa do Excel para a tabela imp_base_fixo no MySQL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Abas disponíveis e seus códigos de empresa:
  - Base fixo Distribuidora    (cd_empresa = 1)
  - Base fixo Unibox           (cd_empresa = 2)
  - Base fixo industria Máscara (cd_empresa = 3)
  - Base fixo Uniplast         (cd_empresa = 4)
  - Base fixo UNIPACK          (cd_empresa = 7)

Exemplos:
  python imp_base_fixo.py                                    # Processa todas as abas
  python imp_base_fixo.py --sheet "Base fixo Distribuidora"  # Apenas uma aba específica
        """
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
        default=None,
        help='Nome de UMA aba específica para importar. Se não informado, processa TODAS as abas configuradas.'
    )

    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    """Função principal."""
    args = parse_args(argv)

    try:
        # Se --sheet foi especificado, processa apenas essa aba
        # Caso contrário, processa todas
        sheet_names = [args.sheet] if args.sheet else None

        importar_base_fixo(
            excel_path=args.excel,
            sheet_names=sheet_names
        )
        return 0
    except Exception as e:
        print(f"\nERRO: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
