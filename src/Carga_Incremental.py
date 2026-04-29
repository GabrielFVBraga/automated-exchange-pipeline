# ---------------------------------------------------------
# Projeto: Automated Exchange Pipeline
# Script: carga_incremental.py
# Função: Carga incremental e monitoramento diário (Upsert)
# Destaque: Implementação de logs e lógica de integridade SQL
# AUTOR: Gabriel Braga (Analista de TI / Graduado em ADS)
# DATA: 29/04/2026
# ---------------------------------------------------------
import requests # EXTRAI
import pandas as pd # TRANSFORMA 
from sqlalchemy import create_engine, text, types # CARREGA
import logging # MONITORA
import os # GESTÃO DE DIRETÓRIOS
from datetime import datetime # TRATAMENTO DE TEMPO

# --- CONFIGURAÇÃO DE LOGS DINÂMICA ---
PASTA_LOGS = "logs"

# Cria a pasta automaticamente se não existir
os.makedirs(PASTA_LOGS, exist_ok=True)

# Define o nome do arquivo apenas com a data abreviada (DD-MM-AA)
# Nota: Usamos hífens pois o Windows não permite barras em nomes de arquivos
nome_log = datetime.now().strftime("%d-%m-%y.txt")
caminho_log = os.path.join(PASTA_LOGS, nome_log)

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.FileHandler(caminho_log, encoding='utf-8'), # Grava no TXT
        logging.StreamHandler() # Exibe no console
    ]
)

# --- PARÂMETROS DE CONEXÃO ---
SERVER = r'GABRIEL\SQLEXPRESS'
DATABASE = 'PROJETO_1'
DRIVER = 'ODBC Driver 17 for SQL Server'
TABLE_NAME = 'HISTORICO_COTACAO'
CONN_STR = f"mssql+pyodbc://@{SERVER}/{DATABASE}?driver={DRIVER}&trusted_connection=yes"

engine = create_engine(CONN_STR, fast_executemany=True)

def extrair_e_transformar():
    """Busca dados atuais e realiza o saneamento técnico."""
    url = "https://economia.awesomeapi.com.br/last/USD-BRL,EUR-BRL"
    logging.info("OPERACAO: EXTRAÇÃO | Iniciando requisição à API.")
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        dados = response.json()
        
        df = pd.DataFrame(dados.values())
        df = df[['code', 'codein', 'bid', 'create_date']]
        df.columns = ['moeda_origem', 'moeda_destino', 'valor_compra', 'data_atualizacao_api']
        
        df['valor_compra'] = pd.to_numeric(df['valor_compra'])
        df['data_consulta'] = pd.to_datetime(df['data_atualizacao_api']).dt.date
        df['data_atualizacao_api'] = pd.to_datetime(df['data_atualizacao_api'])
        
        logging.info(f"OPERACAO: TRANSFORMAÇÃO | STATUS: {len(df)} moedas processadas.")
        return df
    except Exception as e:
        logging.error(f"OPERACAO: EXTRAÇÃO/TRANSFORMAÇÃO | ERRO: {str(e)}")
        return None

def upsert_incremental(df_novo):
    """Realiza a sincronização incremental (MERGE) no SQL Server."""
    dtype_map = {
        'moeda_origem': types.VARCHAR(3),
        'moeda_destino': types.VARCHAR(3),
        'valor_compra': types.Float(),
        'data_consulta': types.Date(),
        'data_atualizacao_api': types.DateTime()
    }
    
    try:
        logging.info(f"OPERACAO: CARGA INCREMENTAL | TABELA: {TABLE_NAME}.")
        with engine.begin() as conn:
            df_novo.to_sql("#temp_snapshot", conn, if_exists='replace', index=False, dtype=dtype_map)
            
            sql = f"""
            MERGE INTO {TABLE_NAME} AS T
            USING #temp_snapshot AS S
            ON (T.data_consulta = S.data_consulta AND T.moeda_origem = S.moeda_origem)
            
            WHEN MATCHED THEN
                UPDATE SET 
                    T.valor_compra = S.valor_compra,
                    T.data_atualizacao_api = S.data_atualizacao_api
            
            WHEN NOT MATCHED THEN
                INSERT (moeda_origem, moeda_destino, valor_compra, data_consulta, data_atualizacao_api)
                VALUES (S.moeda_origem, S.moeda_destino, S.valor_compra, S.data_consulta, S.data_atualizacao_api)
            
            OUTPUT $action, inserted.moeda_origem, inserted.data_consulta;
            """
            result = conn.execute(text(sql))
            for action, moeda, data in result:
                logging.info(f"STATUS BANCO: {action} | ATIVO: {moeda} | REF: {data}")
            
        logging.info("OPERACAO: CARGA INCREMENTAL | STATUS: Finalizado com sucesso.")
    except Exception as e:
        logging.error(f"OPERACAO: CARGA INCREMENTAL | ERRO: {str(e)}")

if __name__ == "__main__":
    logging.info("--- INICIANDO WORKFLOW DIARIO (INCREMENTAL) ---")
    df_diario = extrair_e_transformar()
    if df_diario is not None and not df_diario.empty:
        upsert_incremental(df_diario)
    else:
        logging.warning("OPERACAO: WORKFLOW | STATUS: Abortado (Sem dados válidos).")
    logging.info("--- WORKFLOW FINALIZADO ---")
    
