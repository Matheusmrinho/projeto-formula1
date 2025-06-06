# config/settings.py
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Configurações do Banco de Dados
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'formula1_db')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_TYPE = os.getenv('DB_TYPE', 'postgresql') # Adicione o tipo de DB (postgresql, mysql, sqlite)

# String de conexão SQLAlchemy (ajuste conforme o DB_TYPE)
if DB_TYPE == 'postgresql':
    DB_STRING = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
elif DB_TYPE == 'mysql':
    DB_STRING = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
elif DB_TYPE == 'sqlite':
    # Para SQLite, DB_NAME pode ser o caminho do arquivo, ex: 'formula1.db'
    # Se DB_NAME for um caminho absoluto, use-o diretamente.
    # Se for relativo, construa o caminho a partir da raiz do projeto.
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_file_path = os.path.join(project_root, DB_NAME)
    DB_STRING = f"sqlite:///{db_file_path}"
else:
    DB_STRING = None # Ou levante um erro

# Pastas
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # Raiz do projeto
EXTRACAO_FOLDER_NAME = "extracao"
EXTRACAO_PATH = os.path.join(BASE_DIR, EXTRACAO_FOLDER_NAME)
SQL_SCRIPTS_PATH = os.path.join(BASE_DIR, "sql_scripts")

# URLs dos arquivos CSV
CSV_URLS = {
    "constructors": "https://github.com/CaioSobreira/dti_arquivos/raw/main/constructors.csv",
    "drivers": "https://github.com/CaioSobreira/dti_arquivos/raw/main/drivers.csv",
    "races": "https://github.com/CaioSobreira/dti_arquivos/raw/main/races.csv",
    "results": "https://github.com/CaioSobreira/dti_arquivos/raw/main/results.csv"
}
MD5_HASH_URL = "https://github.com/CaioSobreira/dti_arquivos/raw/main/arquivos_hash_md5sum.csv"

# Informações do Ambiente (para o README)
PYTHON_VERSION_USED = os.getenv('PYTHON_VERSION', 'Não especificado')
SGBD_NAME_USED = os.getenv('SGBD_NAME', 'Não especificado')
SGBD_VERSION_USED = os.getenv('SGBD_VERSION', 'Não especificado')

# Nomes das tabelas (para consistência)
TABLE_NAMES = {
    "constructors": "constructors",
    "drivers": "drivers",
    "races": "races",
    "results": "results"
}