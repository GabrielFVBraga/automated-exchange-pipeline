import requests # EXTRAI
import pandas as pd # TRANSFORMA 
from sqlalchemy import create_engine, text # CARREGA
import logging # MONITORA
import sys # INTERRUPÇÃO DO SCRIPT

# CONFIGURAÇÃO DOS LOGS
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# VARIAVEIS DE CONEXÃO 
SERVER = r'GABRIEL\SQLEXPRESS'
DATABASE = 'PROJETO_1'
DRIVER = 'ODBC Driver 17 for SQL Server'
TABLE_NAME = 'HISTORICO_COTACAO'
CONEXAO = f"mssql+pyodbc://@{SERVER}/{DATABASE}?driver={DRIVER}&trusted_connection=yes"

# ENGINE DE DADOS / OTIMIZAÇÃO DE CARGA
engine = create_engine(CONEXAO, fast_executemany=True)

def extrair_historico_completo(moeda, dias=360):
    """Busca dados retroativos e realiza o saneamento para carga inicial."""
    logging.info(f"OPERACAO: EXTRAÇÃO | MOEDA: {moeda} | PERIODO: {dias} dias.")
    url = f"https://economia.awesomeapi.com.br/json/daily/{moeda}-BRL/{dias}"
    
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        dados = response.json()
        
        if not dados:
            logging.warning(f"OPERACAO: EXTRAÇÃO | MOEDA: {moeda} | STATUS: Nenhum dado retornado.")
            return None
         
        # ROTULAGEM DOS DADOS 
        df = pd.DataFrame(dados)
        df['moeda_origem'] = moeda
        df['moeda_destino'] = 'BRL'
        
        # SELEÇAO DE DADOS E RENOMEAÇÃO DAS COLUNAS
        df = df[['moeda_origem', 'moeda_destino', 'bid', 'timestamp']].copy()
        df.columns = ['moeda_origem', 'moeda_destino', 'valor_compra', 'data_atualizacao_api']
        
        # TRATAMENTO DE DATA E MOEDA
        df['dt_temp'] = pd.to_datetime(pd.to_numeric(df['data_atualizacao_api'], errors='coerce'), unit='s')
        df['data_consulta'] = df['dt_temp'].dt.date
        df['data_atualizacao_api'] = df['dt_temp']
        df['valor_compra'] = pd.to_numeric(df['valor_compra'])
        
        # LIMPEZA DE DADOS E REMOÇÃO DE NULOS E DUPLICATAS (Essencial para PK)
        df = df.drop(columns=['dt_temp']).dropna(subset=['data_consulta'])
        df = df.drop_duplicates(subset=['moeda_origem', 'data_consulta'])
        
        logging.info(f"OPERACAO: EXTRAÇÃO | MOEDA: {moeda} | STATUS: {len(df)} registros processados.")
        return df

    except Exception as e:
        logging.error(f"OPERACAO: EXTRAÇÃO | MOEDA: {moeda} | ERRO: {str(e)}")
        return None

def carregar_historico_massivo(df_total):
    """Realiza a limpeza da tabela (TRUNCATE) e a inserção em lote no SQL Server."""
    if df_total is None or df_total.empty:
        logging.warning("OPERACAO: CARGA | STATUS: Cancelada (DataFrame vazio).")
        return

    try:
        # EXECUÇÃO DA LIMPEZA DA TABELA
        with engine.begin() as connection:
            logging.info(f"OPERACAO: LIMPEZA | Executando TRUNCATE na tabela {TABLE_NAME}...")
            connection.execute(text(f"TRUNCATE TABLE {TABLE_NAME}"))
            logging.info("OPERACAO: LIMPEZA | STATUS: Tabela limpa para reinscrição.")

        # CARGA DOS DADOS
        logging.info(f"OPERACAO: CARGA | REGISTROS: {len(df_total)} | TABELA: {TABLE_NAME}.")
        df_total.to_sql(TABLE_NAME, engine, if_exists='append', index=False)
        logging.info("OPERACAO: CARGA | STATUS: Sucesso. Carga histórica finalizada.")
        
    except Exception as e:
        logging.error(f"OPERACAO: CARGA | ERRO: {str(e)}")

if __name__ == "__main__":
    logging.info("--- INICIANDO PROCESSO DE CARGA HISTÓRICA ---")

    # AVISO E CONFIRMAÇÃO DO USUÁRIO
    print("\n" + "!"*60)
    print(" ALERTA DE SEGURANÇA: REINSCRIÇÃO DE DADOS HISTÓRICOS")
    print(f" A tabela '{TABLE_NAME}' será totalmente Reinscrita.")
    print("!"*60 + "\n")
    
    confirmacao = input("Deseja prosseguir com a limpeza e carga? (S/N): ").strip().upper()

    if confirmacao == 'S':
        moedas = ['USD', 'EUR']
        lista_dfs = []
        
        for m in moedas:
            df_moeda = extrair_historico_completo(m, dias=360)
            if df_moeda is not None:
                lista_dfs.append(df_moeda)
                
        if lista_dfs:
            df_final = pd.concat(lista_dfs)
            carregar_historico_massivo(df_final)
        else:
            logging.error("OPERACAO: WORKFLOW | STATUS: Falha geral. Nenhum dado extraído.")
    else:
        logging.warning("OPERACAO: WORKFLOW | STATUS: Abortado pelo usuário.")
        print("\nOperação cancelada com segurança. Nenhum dado foi alterado.")
        
    logging.info("--- WORKFLOW HISTÓRICO FINALIZADO ---")
