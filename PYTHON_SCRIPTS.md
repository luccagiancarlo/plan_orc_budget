# Guia dos Scripts Python

## `conferencia.py`
- **Objetivo:** Gera um relatório mensal dos lançamentos de uma conta, agrupados por unidade e empresa, a partir das tabelas `val_orc_plancontas`, `val_orc_lancamentos` e `val_orc_unidade`.
- **Entrada principal:** argumentos CLI como `--conta` (obrigatório), `--ano`, `--mes`, `--empresa`, `--unidade` e `--tipo`.
- **Saída:** tabela formatada no terminal com totais por unidade, totais mensais e resumo geral.
- **Fluxo:** conecta ao MySQL via `pymysql`, consulta os lançamentos com filtros opcionais, reorganiza os valores por mês e imprime o relatório. Exibe avisos amigáveis caso nada seja encontrado.
- **Uso típico:** `python conferencia.py --conta 110 --ano 2026 --empresa 1`

## `imp_base_fixo.py`
- **Objetivo:** Lê abas específicas de `orc_2026.xlsx` contendo custos fixos (Distribuidora, Unibox, Máscara, Uniplast, UNIPACK) e popula a tabela `imp_base_fixo`.
- **Entrada principal:** argumentos CLI `--excel` (arquivo fonte) e `--sheet` (aba opcional); por padrão processa todas as abas mapeadas.
- **Saída:** recria a tabela `imp_base_fixo`, insere os registros e imprime um resumo com totais por aba.
- **Fluxo:** normaliza códigos de conta, identifica unidades orçamentárias, extrai valores de janeiro a dezembro e insere em lotes de 1000 linhas. Cada execução dropa a tabela antes de recriá-la.
- **Uso típico:** `python imp_base_fixo.py` (todas as abas) ou `python imp_base_fixo.py --sheet "Base fixo Unibox"`

## `imp_cad_receita.py`
- **Objetivo:** Importa a aba `CADASTRO RECEITA` de `orc_2026.xlsx` e preenche a tabela `val_orc_cad_rec_temp`.
- **Entrada principal:** argumentos CLI `--excel` (arquivo fonte) e `--sheet` (aba "CADASTRO RECEITA" por padrão).
- **Saída:** tabela `val_orc_cad_rec_temp` recriada e preenchida com 222 registros de produtos de receita distribuídos entre 5 empresas.
- **Fluxo:** lê a planilha (cabeçalhos na linha 4), aplica forward fill para dados de empresa em linhas de produtos adicionais, filtra cabeçalhos repetidos e linhas inválidas, converte valores defensivamente com tratamento de erro, e insere em lotes de 1000.
- **Uso típico:** `python imp_cad_receita.py --excel orc_2026.xlsx`

## `imp_orc_financeiro.py`
- **Objetivo:** Importa a aba `BASE DO ORÇAMENTO FINANCEIRO` de `orc_2026.xlsx` e preenche a tabela `imp_orc_financeiro`.
- **Entrada principal:** argumentos CLI `--excel` e `--sheet` (padrão já aponta para a aba correta).
- **Saída:** tabela `imp_orc_financeiro` recriada e preenchida com os valores previstos mensais; relatório com totais e contagem de linhas processadas.
- **Fluxo:** filtra linhas cujo código da conta está na primeira coluna, captura descrição e os valores previstos (colunas pares) de janeiro a dezembro, e realiza inserts em lotes de 1000.
- **Uso típico:** `python imp_orc_financeiro.py --excel orc_2026.xlsx`

## `imp_plano_contas.py`
- **Objetivo:** Importa a aba `plano de contas`, gera a tabela `imp_plan_contas`, e sincroniza `val_orc_plancontas`.
- **Entrada principal:** argumentos `--excel`, `--sheet` e `--table` (nome da tabela destino).
- **Saída:** tabela criada dinamicamente conforme as colunas da planilha, dados inseridos, e sincronização (insert/update) na tabela oficial `val_orc_plancontas`.
- **Fluxo:** converte `FlagNivel` em `nu_conta`, higieniza nomes de colunas para o MySQL, recria a tabela destino, insere em lotes e, por fim, executa SQLs de atualização/inserção na tabela de valores.
- **Uso típico:** `python imp_plano_contas.py --excel orc_2026.xlsx --sheet "plano de contas"`

## `inserir_lancamentos.py`
- **Objetivo:** Converte os dados importados em `imp_base_fixo` para lançamentos mensais na tabela `val_orc_lancamentos`.
- **Entrada principal:** argumentos `--ano` (obrigatório) e `--limpar` (remove lançamentos do ano antes de inserir).
- **Saída:** registros inseridos na tabela de lançamentos (`Saldo Inicial`) e relatório com quantidade de linhas, faixa de IDs e estatísticas.
- **Fluxo:** lê toda `imp_base_fixo`, gera 12 lançamentos por registro (um por mês), calcula o próximo ID disponível, opcionalmente limpa o ano alvo e insere em lotes de 5000 registros usando a estrutura detectada da tabela destino.
- **Uso típico:** `python inserir_lancamentos.py --ano 2026 --limpar`

## `xlsx_listar_abas_colunas.py`
- **Objetivo:** Faz inventário das abas de um arquivo `.xlsx`, salva a lista em `plans.txt` e gera arquivos de texto com as colunas de cada aba dentro de `abas_cols/`.
- **Entrada principal:** caminho para o Excel, além de `--outdir`, `--zip` e `--encoding`.
- **Saída:** `plans.txt` com os nomes das abas, múltiplos `.txt` com as colunas detectadas e, opcionalmente, um `abas_colunas.zip`.
- **Fluxo:** abre a planilha com `pandas`, coleta os nomes das abas, cria nomes de arquivo seguros (deduplicados), escreve a estrutura detectada e pode compactar os arquivos resultantes.
- **Uso típico:** `python xlsx_listar_abas_colunas.py "orc_2026.xlsx" --outdir ./relatorios --zip`
