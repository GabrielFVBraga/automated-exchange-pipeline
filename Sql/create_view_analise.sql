-- -----------------------------------------------------------------------------
-- PROJETO: Automated Exchange Pipeline
-- OBJETO:  [dbo].[VW_ANALISE_MOEDAS]
-- FUNÇÃO:  Simplificar o consumo de dados pelo Power BI e tratar nomes de colunas.
-- -----------------------------------------------------------------------------

CREATE VIEW [dbo].[VW_ANALISE_MOEDAS] AS
SELECT 
    moeda_origem AS MOEDA,
    valor_compra AS PRECO,
    data_consulta AS DATA_REFERENCIA,
    data_atualizacao_api AS DH_ATUALIZACAO_API
FROM [dbo].[HISTORICO_COTACAO];
GO
