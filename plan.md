# Phase1
Crie um novo programa em python importar_planilhas.py que inicialmente abra o arquivo orc_2026.xls e leia o conteudo da aba plano de contas e
com base nas colunas existentes, crie uma tabela (se não existir) no mysql (mysql -u glucca -h 127.0.0.1 -pGi3510prbi! budget) e insira todos os dados lá

agora atualize a tabela val_orc_plancontas t1 com os registros existentes na tabela imp_plan_contas t2 que não existam em t1 onde o t1.cd_conta = t2.id ,
para inserir os registros em t1 as correspondencias serão t1.cd_conta = t2.id, t1.cd_conta_pai=t2.idparent, t1.nu_conta = t2.nu_conta, t1.de_conta = t2.descricao, t1.fl_ativo= 'S', t1.cd_empresa=0, t1.nu_conta_legado = t2.flagnivel, caso haja correspondencia dos dados atualize os registros em t1 com os dados de t2


Agora observe a estrutura da minha tabela val_orc_lancamentos, observe os lancamentos de tp_lancamento = 'Saldo Inicial' , vc pode fazer isso executando um select *
  from val_orc_lancamentos a where a.tp_lancamento='Saldo Inicial' and cd_conta=110; . Observe que aqui os valores de janeiro a dezembro estao dispostos em linhas e nao
  em colunas, ou seja o nu_mes vai variar de 1 a 12. Observe que o campo chave deve ser incrementado com o valor do ultimo registro + 1 e os demais campos tp_lancamento
  sempre será Saldo Inicial quando estou lançando os valores iniciais do orçamento do ano, como exemplo o nu_ano=2025. Pois bem, eu preciso de um programa em python inserir_lancamentos.py agora  para inserir os valores da
  tabela imp_base_fixo que vc me ajudou a criar para a tabela val_orc_lancamentos, veja que o campo cd_conta corresponde ao campo cod_interno da tabela imp_base_fixo, o cd_unidade ao cod_unidade, os
  valores de janeiro a dezembro deverã ser registros em linha correspondenao ao campo nu_mes de 1 a 12, o nu_ano será 2026 . Os demais campos seguem o mesmo padrão que vc observou no select. Pode me ajudar com isso?