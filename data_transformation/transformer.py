# data_transformation/transformer.py
import pandas as pd

def transform_constructors_df(df_constructors):
    """Transforma o DataFrame de constructors."""
    if df_constructors is None: return None
    transformed_df = df_constructors[['constructorId', 'name']].copy()
    transformed_df['constructorId'] = transformed_df['constructorId'].astype(int)
    print("DataFrame 'constructors' transformado.")
    return transformed_df

def transform_drivers_df(df_drivers):
    """Transforma o DataFrame de drivers."""
    if df_drivers is None: return None
    df_drivers['fullname'] = df_drivers['forename'] + ' ' + df_drivers['surname']
    transformed_df = df_drivers[['driverId', 'fullname']].copy()
    transformed_df['driverId'] = transformed_df['driverId'].astype(int)
    print("DataFrame 'drivers' transformado.")
    return transformed_df

def transform_races_df(df_races):
    """Transforma o DataFrame de races."""
    if df_races is None: return None
    transformed_df = df_races[['raceId', 'year', 'name', 'date']].copy()
    transformed_df['raceId'] = transformed_df['raceId'].astype(int)
    transformed_df['year'] = transformed_df['year'].astype(int)
    transformed_df['date'] = pd.to_datetime(transformed_df['date'], errors='coerce')
    print("DataFrame 'races' transformado.")
    return transformed_df

def transform_results_df(df_results):
    """Transforma o DataFrame de results."""
    if df_results is None: return None
    # Renomear 'raceld' para 'raceId' se necessário (verificar nome da coluna no CSV)
    if 'raceld' in df_results.columns and 'raceId' not in df_results.columns:
        df_results.rename(columns={'raceld': 'raceId'}, inplace=True)
        
    required_cols = ['resultId', 'raceId', 'driverId', 'constructorId', 'positionOrder', 'points', 'fastestLapTime']
    # Checar se todas as colunas existem
    if not all(col in df_results.columns for col in required_cols):
        print(f"Erro: Colunas esperadas não encontradas no DataFrame 'results'. Esperadas: {required_cols}, Encontradas: {df_results.columns.tolist()}")
        return None

    transformed_df = df_results[required_cols].copy()
    
    transformed_df['resultId'] = transformed_df['resultId'].astype(int)
    transformed_df['raceId'] = transformed_df['raceId'].astype(int)
    transformed_df['driverId'] = transformed_df['driverId'].astype(int)
    transformed_df['constructorId'] = transformed_df['constructorId'].astype(int)
    transformed_df['positionOrder'] = transformed_df['positionOrder'].astype(int)
    
    # Tratar '\N' como NaN antes de converter para float e depois int
    transformed_df['points'] = pd.to_numeric(transformed_df['points'].replace({'\\N': pd.NA}), errors='coerce').fillna(0).astype(int)
    
    transformed_df['fastestLapTime'] = transformed_df['fastestLapTime'].replace({'\\N': None})
    # A conversão para o tipo TIME do SQL será feita pelo SQLAlchemy ou pelo SGBD
    print("DataFrame 'results' transformado.")
    return transformed_df

if __name__ == '__main__':
    # Teste rápido (requer arquivos CSV na pasta de extração)
    from config import settings
    import os
    
    print("Testando o módulo de transformação...")
    # Exemplo para constructors (simular DataFrame)
    constructor_file = os.path.join(settings.EXTRACAO_PATH, "constructors.csv")
    if os.path.exists(constructor_file):
        df_c = pd.read_csv(constructor_file)
        df_c_transformed = transform_constructors_df(df_c.copy()) # Passar cópia
        if df_c_transformed is not None:
            print("\nConstructors Transformado Head:")
            print(df_c_transformed.head())
            print(df_c_transformed.info())
    else:
        print(f"Arquivo {constructor_file} não encontrado para teste.")

    # Adicionar testes para outros DataFrames similarmente...