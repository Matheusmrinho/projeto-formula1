# main.py
import os
import requests
import pandas as pd
from dotenv import load_dotenv
import psycopg2 # Ou outro conector apropriado
from psycopg2 import sql
from sqlalchemy import create_engine 
import sqlalchemy
import hashlib # Para o diferencial de MD5 

# Carregar variáveis de ambiente do arquivo .env 
load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# SQLAlchemy engine (opcional, mas útil com Pandas)
# A string de conexão pode variar dependendo do SGBD
# Exemplo para PostgreSQL:
db_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
db_engine = create_engine(db_string)


# URLs dos arquivos CSV 
CSV_URLS = {
    "constructors": "https://github.com/CaioSobreira/dti_arquivos/raw/main/constructors.csv",
    "drivers": "https://github.com/CaioSobreira/dti_arquivos/raw/main/drivers.csv",
    "races": "https://github.com/CaioSobreira/dti_arquivos/raw/main/races.csv",
    "results": "https://github.com/CaioSobreira/dti_arquivos/raw/main/results.csv"
}
# URL do arquivo de hash MD5 (para o diferencial) 
MD5_HASH_URL = "https://github.com/CaioSobreira/dti_arquivos/raw/main/arquivos_hash_md5sum.csv"


EXTRACAO_FOLDER = "extracao" # 

def calculate_md5(file_path):
    """Calcula o hash MD5 de um arquivo."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def download_csv_files():
    """
    Baixa os arquivos CSV.
    Cria a pasta 'extracao' se não existir. 
    Verifica MD5 para evitar redownload (diferencial). 
    Trata erros de download. 
    """
    os.makedirs(EXTRACAO_FOLDER, exist_ok=True) # 
    print(f"Pasta '{EXTRACAO_FOLDER}' criada/verificada.")

    # Diferencial: Baixar e carregar hashes MD5 esperados 
    expected_hashes = {}
    try:
        print(f"Baixando arquivo de hash MD5 de: {MD5_HASH_URL}")
        response = requests.get(MD5_HASH_URL)
        response.raise_for_status()
        # O arquivo de hash é um CSV com nome_arquivo,hash_md5
        # Ex: constructors.csv,d41d8cd98f00b204e9800998ecf8427e
        lines = response.text.strip().split('\n')
        for line in lines[1:]: # Ignora cabeçalho
            if ',' in line:
                filename, md5_hash = line.split(',')
                expected_hashes[filename.strip()] = md5_hash.strip()
        print("Hashes MD5 esperados carregados.")
    except requests.exceptions.RequestException as e:
        print(f"Erro ao baixar o arquivo de hash MD5: {e}. Downloads prosseguirão sem verificação de hash.")
        # Continuar sem verificação de hash se o arquivo MD5 não puder ser baixado.

    for name, url in CSV_URLS.items():
        file_name = f"{name}.csv"
        file_path = os.path.join(EXTRACAO_FOLDER, file_name)

        # Diferencial: Verificação de MD5 
        if os.path.exists(file_path) and file_name in expected_hashes:
            local_md5 = calculate_md5(file_path)
            if local_md5 == expected_hashes[file_name]:
                print(f"Arquivo '{file_path}' já existe e MD5 confere. Pulando download.")
                continue # Pula para o próximo arquivo
            else:
                print(f"Arquivo '{file_path}' existe, mas MD5 não confere ({local_md5} vs {expected_hashes[file_name]}). Baixando novamente.")
        elif os.path.exists(file_path):
             print(f"Arquivo '{file_path}' já existe, mas não foi possível verificar o hash. Baixando novamente para garantir.")


        try:
            print(f"Baixando '{file_name}' de '{url}'...")
            response = requests.get(url, timeout=30)
            response.raise_for_status()  # Verifica se houve erro no HTTP 
            with open(file_path, 'wb') as f:
                f.write(response.content)
            print(f"'{file_name}' baixado com sucesso em '{file_path}'.")
        except requests.exceptions.Timeout:
            print(f"Erro de Timeout ao tentar baixar '{file_name}' de '{url}'.") # 
        except requests.exceptions.HTTPError as http_err:
            print(f"Erro HTTP ao tentar baixar '{file_name}': {http_err}") # 
        except requests.exceptions.RequestException as e:
            print(f"Erro ao baixar '{file_name}': {e}") # 
        except Exception as e_gen:
            print(f"Ocorreu um erro inesperado durante o download de '{file_name}': {e_gen}")


def transform_and_load_data():
    """
    Lê os CSVs da pasta 'extracao', transforma os dados
    e carrega no banco de dados. 
    Garante que não haja duplicidade de dados. 
    """
    conn = None
    try:
        # Conexão direta com psycopg2 para TRUNCATE
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        cur = conn.cursor()

        # Ordem de TRUNCATE para respeitar chaves estrangeiras
        # (results depende das outras)
        tables_to_truncate = ["results", "races", "drivers", "constructors"]
        for table in tables_to_truncate:
            print(f"Limpando dados da tabela '{table}' (TRUNCATE)...")
            cur.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(sql.Identifier(table))) #  (para evitar duplicidade)
        conn.commit()
        print("Tabelas limpas com sucesso.")

        # 2.1 - Arquivo CONSTRUCTORS 
        df_constructors = pd.read_csv(os.path.join(EXTRACAO_FOLDER, "constructors.csv"),encoding='latin1')
        constructors_transformed = df_constructors[['constructorId', 'name']].copy() # 
        # Tipos de dados: constructorId INT, name TEXT 
        # Pandas geralmente infere INT corretamente. TEXT é padrão para strings.
        constructors_transformed['constructorId'] = constructors_transformed['constructorId'].astype(int)
        constructors_transformed.to_sql('constructors', db_engine, if_exists='append', index=False)
        print(f"Dados de 'constructors' carregados. Linhas: {len(constructors_transformed)}")

        # 2.2 - Arquivo DRIVERS 
        df_drivers = pd.read_csv(os.path.join(EXTRACAO_FOLDER, "drivers.csv"),encoding='latin1')
        # Juntar colunas forename e surname para fullname 
        df_drivers['fullname'] = df_drivers['forename'] + ' ' + df_drivers['surname']
        drivers_transformed = df_drivers[['driverId', 'fullname']].copy() # 
        # Tipos: driverId INT, fullname TEXT 
        drivers_transformed['driverId'] = drivers_transformed['driverId'].astype(int)
        drivers_transformed.to_sql('drivers', db_engine, if_exists='append', index=False)
        print(f"Dados de 'drivers' carregados. Linhas: {len(drivers_transformed)}")

        # 2.3 - Arquivo RACES 
        df_races = pd.read_csv(os.path.join(EXTRACAO_FOLDER, "races.csv"),encoding='latin1')
        races_transformed = df_races[['raceId', 'year', 'name', 'date']].copy() # 
        # Tipos: raceId INT, year INT, name TEXT, date DATE 
        races_transformed['raceId'] = races_transformed['raceId'].astype(int)
        races_transformed['year'] = races_transformed['year'].astype(int)
        races_transformed['date'] = pd.to_datetime(races_transformed['date'], errors='coerce') # Converte para datetime, 'coerce' para NaN em erros
        races_transformed.to_sql('races', db_engine, if_exists='append', index=False)
        print(f"Dados de 'races' carregados. Linhas: {len(races_transformed)}")

        # 2.4 - Arquivo RESULTS 
        df_results = pd.read_csv(os.path.join(EXTRACAO_FOLDER, "results.csv"),encoding='latin1')
        # No documento está "raceld", o correto é raceId. Ajustar se o CSV realmente tiver "raceld"
        # Supondo que o CSV tenha 'raceId' ou que precise ser renomeado se for 'raceld'
        # df_results.rename(columns={'raceld': 'raceId'}, inplace=True) # Descomente se necessário
        
        results_transformed = df_results[['resultId', 'raceId', 'driverId', 'constructorId', 'positionOrder', 'points', 'fastestLapTime']].copy() # 
        # Tipos: resultId INT, raceId INT, driverId INT, constructorId INT, positionOrder INT, points INT, fastestLapTime TIME 
        results_transformed['resultId'] = results_transformed['resultId'].astype(int)
        results_transformed['raceId'] = results_transformed['raceId'].astype(int)
        results_transformed['driverId'] = results_transformed['driverId'].astype(int)
        results_transformed['constructorId'] = results_transformed['constructorId'].astype(int)
        results_transformed['positionOrder'] = results_transformed['positionOrder'].astype(int)
        results_transformed['points'] = results_transformed['points'].astype(float).astype(int) # Pontos podem ser float no CSV, mas são INT no BD

        # Tratamento para fastestLapTime: deve ser TIME. Pandas pode ler como string.
        # Se o formato for HH:MM:SS.mmm, pode precisar de ajuste ou o SGBD pode aceitar.
        # Se houver '\N' para nulos, substituir por None para o SGBD entender.
        results_transformed['fastestLapTime'] = results_transformed['fastestLapTime'].replace({'\\N': None})
        # A conversão para pd.to_datetime e depois .dt.time pode ser uma opção,
        # mas 'to_sql' pode lidar com strings no formato correto para TIME.
        # Exemplo: results_transformed['fastestLapTime'] = pd.to_datetime(results_transformed['fastestLapTime'], format='%M:%S.%f', errors='coerce').dt.time

        results_transformed.to_sql('results', db_engine, if_exists='append', index=False, dtype={'fastestLapTime': sqlalchemy.types.Time()})
        print(f"Dados de 'results' carregados. Linhas: {len(results_transformed)}")

        print("Processo de Transformação e Carga concluído.")

    except FileNotFoundError as e:
        print(f"Erro: Arquivo CSV não encontrado. Execute a etapa de extração primeiro. Detalhes: {e}")
    except pd.errors.EmptyDataError as e:
        print(f"Erro: Arquivo CSV está vazio. Detalhes: {e}")
    except KeyError as e:
        print(f"Erro: Coluna não encontrada no CSV. Verifique os nomes das colunas. Detalhes: {e}")
    except psycopg2.Error as e: # Captura erros do psycopg2
        print(f"Erro de banco de dados (psycopg2): {e}")
        if conn:
            conn.rollback() # Importante reverter em caso de erro
    except Exception as e: # Captura outros erros
        print(f"Ocorreu um erro inesperado na transformação e carga: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Conexão com o banco de dados (psycopg2) fechada.")


def execute_sql_scripts(folder_path):
    """
    Executa scripts SQL de uma pasta.
    Útil para criar tabelas e views se não forem criadas automaticamente pelo Docker.
    """
    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        cur = conn.cursor()
        
        for filename in sorted(os.listdir(folder_path)):
            if filename.endswith(".sql"):
                file_path = os.path.join(folder_path, filename)
                with open(file_path, 'r') as f:
                    sql_script = f.read()
                    # Remover comentários simples e blocos de comentário para evitar problemas com alguns SGBDs
                    # (Este é um parser simples, pode não cobrir todos os casos)
                    # sql_script_no_comments = "\n".join(line for line in sql_script.splitlines() if not line.strip().startswith('--'))
                    # sql_script_no_comments = re.sub(r'/\*.*?\*/', '', sql_script_no_comments, flags=re.DOTALL)
                    
                    # Dividir em statements individuais, se necessário (psycopg2 geralmente lida com múltiplos statements)
                    print(f"Executando script: {filename}...")
                    cur.execute(sql_script)
                    conn.commit()
                    print(f"Script '{filename}' executado com sucesso.")
        
        print("Todos os scripts SQL da pasta foram executados.")

    except FileNotFoundError:
        print(f"Erro: Pasta de scripts SQL '{folder_path}' não encontrada.")
    except psycopg2.Error as e:
        print(f"Erro de banco de dados ao executar scripts SQL: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"Ocorreu um erro inesperado ao executar scripts SQL: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    print("Iniciando processo ETL de Fórmula 1...")

    # ETAPA 1: EXTRAÇÃO 
    print("\n--- ETAPA 1: EXTRAÇÃO ---")
    download_csv_files()

    # ANTES DA CARGA: Garantir que tabelas e views existam
    # O documento menciona que a criação de tabelas pode ser manual 
    # mas os scripts devem ser salvos.
    # Se não estiver usando Docker para criar automaticamente, pode-se executar os scripts aqui.
    print("\n--- CONFIGURAÇÃO DO BANCO DE DADOS (Tabelas e Views) ---")
    # Esta função é opcional se você criar as tabelas/views manualmente ou via Docker.
    # execute_sql_scripts("sql_scripts") # Descomente se quiser executar via script Python

    # ETAPA 2: TRANSFORMAÇÃO E CARGA 
    print("\n--- ETAPA 2: TRANSFORMAÇÃO E CARGA ---")
    transform_and_load_data()
    
    # ETAPA 3: BANCO DE DADOS (Criação de Views) já está coberta pelos scripts SQL
    # Se execute_sql_scripts("sql_scripts") foi chamado, as views já foram criadas.
    # Se não, devem ser criadas manualmente ou via Docker, conforme os scripts em sql_scripts/create_views.sql. 

    print("\nProcesso ETL concluído.")
    print(f"Python Version Used (from .env): {os.getenv('PYTHON_VERSION')}") # 
    print(f"SGBD Used (from .env): {os.getenv('SGBD_NAME')}") # 
    print(f"SGBD Version (from .env): {os.getenv('SGBD_VERSION')}") #