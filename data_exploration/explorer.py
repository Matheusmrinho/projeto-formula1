# data_exploration/explorer.py
import pandas as pd
import os
from config import settings
import numpy as np

def load_csv_to_df(file_key_name, base_path=None):
    """Carrega um arquivo CSV específico em um DataFrame."""
    if base_path is None:
        base_path = settings.EXTRACAO_PATH
        
    file_name = f"{file_key_name}.csv"
    file_path = os.path.join(base_path, file_name)

    if not os.path.exists(file_path):
        print(f"Arquivo '{file_path}' não encontrado.")
        return None
    try:
        df = pd.read_csv(file_path)
        print(f"Arquivo '{file_path}' carregado com sucesso.")
        return df
    except Exception as e:
        print(f"Erro ao carregar o arquivo '{file_path}': {e}")
        return None

def display_df_info(df, df_name="DataFrame"):
    """Mostra informações básicas sobre o DataFrame."""
    if df is None:
        print(f"{df_name} está vazio ou não pôde ser carregado.")
        return
    
    print(f"\n--- Informações sobre: {df_name} ---")
    print(f"\nForma (linhas, colunas): {df.shape}")

    print("\nPrimeiras 5 linhas (.head()):")
    print(df.head())

    print("\nInformações gerais (.info()):")
    df.info()

    # Verificar se há colunas numéricas antes de chamar .describe()
    numeric_cols = df.select_dtypes(include=np.number).columns
    if not numeric_cols.empty:
        print("\nEstatísticas descritivas para colunas numéricas (.describe()):")
        print(df.describe())
    else:
        print("\nEstatísticas descritivas (.describe() para todas as colunas):")
        print(df.describe(include='all'))


    print("\nContagem de valores nulos por coluna:")
    print(df.isnull().sum())
    
    print(f"--- Fim das informações sobre: {df_name} ---\n")

if __name__ == '__main__':
    # Teste rápido do módulo de exploração
    import numpy as np # Adicionado para np.number
    print("Testando o módulo de exploração de dados...")
    
    # Tenta carregar e exibir informações do 'constructors.csv'
    # Certifique-se que o arquivo foi baixado pela extração primeiro.
    # Você pode executar extractor.py ou o pipeline principal antes.
    
    # Para testar independentemente, crie um CSV dummy ou execute o extractor antes.
    # Supondo que extractor.py foi executado e baixou os arquivos:
    
    df_constructors_raw = load_csv_to_df("constructors")
    if df_constructors_raw is not None:
        display_df_info(df_constructors_raw, "Constructors (Raw CSV)")

    # Exemplo de como você poderia usar isso com dados transformados (simulado)
    # Suponha que df_constructors_transformed veio do transformer.py
    # df_constructors_transformed = ... (resultado de transformer.transform_constructors_df(...))
    # if df_constructors_transformed is not None:
    # display_df_info(df_constructors_transformed, "Constructors (Transformado)")