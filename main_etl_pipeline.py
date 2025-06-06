# main_etl_pipeline.py (Corrigido)
from config import settings
from data_extraction import extractor
from data_transformation import transformer
# ---- LINHA CORRIGIDA ----
from data_loading.loader import get_db_engine, truncate_tables, load_dataframe_to_db, results_dtype_mapping
# -------------------------
import pandas as pd
import os # Para checagem de arquivos

def run_etl_pipeline():
    """Executa o pipeline completo de ETL."""
    print("Iniciando pipeline ETL de Fórmula 1...")

    # ETAPA 1: EXTRAÇÃO
    print("\n--- ETAPA 1: EXTRAÇÃO ---")
    downloaded_file_paths = extractor.download_csv_files()

    # Verificar se todos os arquivos foram baixados
    for name, path in downloaded_file_paths.items():
        if not path or not os.path.exists(path):
            print(f"Erro crítico: Arquivo {name}.csv não foi baixado ou não encontrado em {path}. Abortando pipeline.")
            return

    # ETAPA 2: TRANSFORMAÇÃO
    print("\n--- ETAPA 2: TRANSFORMAÇÃO ---")
    dataframes_raw = {}
    for name, file_path in downloaded_file_paths.items():
        try:
            dataframes_raw[name] = pd.read_csv(file_path)
            print(f"Arquivo {name}.csv carregado para DataFrame.")
        except Exception as e:
            print(f"Erro ao ler {name}.csv: {e}")
            dataframes_raw[name] = None


    # Aplicar transformações
    df_constructors_tf = transformer.transform_constructors_df(dataframes_raw.get("constructors"))
    df_drivers_tf = transformer.transform_drivers_df(dataframes_raw.get("drivers"))
    df_races_tf = transformer.transform_races_df(dataframes_raw.get("races"))
    df_results_tf = transformer.transform_results_df(dataframes_raw.get("results"))

    # ETAPA 3: CARGA
    print("\n--- ETAPA 3: CARGA NO BANCO DE DADOS ---")
    db_engine = get_db_engine()
    if not db_engine:
        print("Pipeline abortado: Não foi possível obter a engine do banco de dados.")
        return

    # Limpar tabelas antes de carregar (para idempotência)
    truncate_tables(db_engine)

    # Carregar DataFrames transformados
    # Note que agora chamamos as funções diretamente (ex: load_dataframe_to_db em vez de loader.load_dataframe_to_db)
    load_dataframe_to_db(df_constructors_tf, settings.TABLE_NAMES["constructors"], db_engine)
    load_dataframe_to_db(df_drivers_tf, settings.TABLE_NAMES["drivers"], db_engine)
    load_dataframe_to_db(df_races_tf, settings.TABLE_NAMES["races"], db_engine)
    load_dataframe_to_db(df_results_tf, settings.TABLE_NAMES["results"], db_engine, dtype_mapping=results_dtype_mapping)

    print("\nPipeline ETL concluído com sucesso!")
    print(f"Informações do Ambiente (conforme .env):")
    print(f"  Python Version: {settings.PYTHON_VERSION_USED}")
    print(f"  SGBD: {settings.SGBD_NAME_USED}")
    print(f"  SGBD Version: {settings.SGBD_VERSION_USED}")

if __name__ == "__main__":
    run_etl_pipeline()