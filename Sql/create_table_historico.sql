-- -----------------------------------------------------------------------------
-- PROJETO: Automated Exchange Pipeline (USD/EUR)
-- SCRIPT:  create_table_historico.sql
-- OBJETO:  Tabela [dbo].[HISTORICO_COTACAO]
-- FUNÇÃO:  Armazenar as cotações históricas e diárias processadas via Python.
-- DESTAQUES: 
--   - PK Composta (data_consulta, moeda_origem) para integridade.
--   - Uso de 'smallmoney' para otimização de armazenamento financeiro.
--   - 'datetime2(0)' para precisão de timestamp da API sem milissegundos.
-- AUTOR: Gabriel Braga (Analista de TI)
-- DATA: 29/04/2026
-- -----------------------------------------------------------------------------

USE [PROJETO_1]
GO
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[HISTORICO_COTACAO](
	[moeda_origem] [char](3) NOT NULL,
	[moeda_destino] [char](3) NOT NULL,
	[valor_compra] [smallmoney] NOT NULL,
	[data_consulta] [date] NOT NULL,
	[data_atualizacao_api] [datetime2](0) NULL,
 CONSTRAINT [PK_Historico] PRIMARY KEY CLUSTERED 
(
	[data_consulta] ASC,
	[moeda_origem] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO



