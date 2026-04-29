Automated Exchange Pipeline (USD/EUR) 🚀
Este repositório contém uma solução completa de Engenharia de Dados e Business Intelligence voltada para o monitoramento automatizado de câmbio. O projeto integra extração de dados via API, persistência em banco de dados relacional e visualização analítica.

🛠️ Stack Tecnológica
Linguagem: Python 3.11+ (Pandas, SQLAlchemy, Requests)

Banco de Dados: Microsoft SQL Server

Visualização: Power BI (DAX)

Distribuição: PyInstaller (Compilação para executável .exe)

📂 Estrutura do Repositório
A organização segue as melhores práticas de governança de TI:

/src: Scripts Python para carga histórica (360 dias) e carga diária incremental.

/sql: DDL da tabela HISTORICO_COTACAO e DML da VW_ANALISE_MOEDAS para consumo do BI.

/dashboard: Arquivo .pbix e documentação visual do relatório.

requirements.txt: Lista de dependências para replicação do ambiente.

⚙️ Arquitetura e Fluxo de Dados
Extração: Consumo da AwesomeAPI para obtenção de cotações em tempo real e históricas.

Transformação: Tratamento de tipos, normalização de datas e limpeza de dados utilizando a biblioteca Pandas.

Carga (ETL/Upsert): Implementação de lógica de Merge no SQL Server para garantir a integridade, evitando duplicidade de registros por data.

Consumo: Uma View SQL atua como camada de abstração para o Power BI, otimizando a performance das consultas.

📊 O Dashboard
O relatório final fornece KPIs estratégicos para tomada de decisão:

Análise de máximas e mínimas históricas independentes de filtros.

Tendência mensal de fechamento (Dólar e Euro).

Timestamp de auditoria da última atualização bem-sucedida do pipeline.

💡 Diferenciais do Projeto
Resiliência: Sistema de logs automáticos em .txt para monitoramento de falhas de rede ou infraestrutura.

Portabilidade: O script de carga diária foi convertido em executável para facilitar o agendamento via Windows Task Scheduler.

Escalabilidade: Estrutura de banco de dados otimizada com chaves primárias compostas e tipos de dados de alta precisão financeira (smallmoney).

Desenvolvido por: Gabriel Braga – Analista de TI & Graduado em ADS.
