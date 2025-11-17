-- Script de criação da tabela val_orc_cad_rec_temp
-- Tabela temporária para cadastro de receita
-- Origem: Planilha "CADASTRO RECEITA" do arquivo orc_2026.xlsx

DROP TABLE IF EXISTS val_orc_cad_rec_temp;

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
COMMENT='Tabela temporária para importação do cadastro de receita';
