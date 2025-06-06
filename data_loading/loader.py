# data_loading/loader.py
from sqlalchemy import create_engine, text, types as sql_types
from config import settings
import pandas as pd

def get_db_engine():
    """Retorna uma engine SQLAlchemy para a conexão com o banco."""
    if not settings.DB_STRING:
        raise ValueError("String de conexão com o banco (DB_STRING) não configurada.")
    
    return create_engine(settings.DB_STRING)

def truncate_tables(engine, table_order=None):
    """Trunca as tabelas especificadas para evitar duplicidade."""
    if table_order is None:
        table_order = [
            settings.TABLE_NAMES["results"],
            settings.TABLE_NAMES["races"],
            settings.TABLE_NAMES["drivers"],
            settings.TABLE_NAMES["constructors"]
        ]
        
    with engine.connect() as connection:
        for table_name in table_order:
            try:
                # RESTART IDENTITY é específico do PostgreSQL. Outros SGBDs podem usar sintaxe diferente ou não suportar.
                # Para SQLite, TRUNCATE não é diretamente suportado, DELETE FROM é usado.
                # CASCADE pode ser necessário se houver FKs.
                if engine.name == 'postgresql':
                    connection.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;"))
                elif engine.name == 'mysql':
                    connection.execute(text(f"SET FOREIGN_KEY_CHECKS = 0;")) # Desabilitar checagem de FK
                    connection.execute(text(f"TRUNCATE TABLE {table_name};"))
                    connection.execute(text(f"SET FOREIGN_KEY_CHECKS = 1;")) # Habilitar checagem de FK
                elif engine.name == 'sqlite':
                    connection.execute(text(f"DELETE FROM {table_name};"))
                    # Para resetar autoincrement em SQLite (se a tabela usa INTEGER PRIMARY KEY)
                    # connection.execute(text(f"DELETE FROM sqlite_sequence WHERE name='{table_name}';")) # Opcional
                else: # Fallback genérico (pode falhar com FKs)
                    connection.execute(text(f"DELETE FROM {table_name};"))

                print(f"Tabela '{table_name}' truncada/limpa.")
            except Exception as e:
                print(f"Erro ao truncar/limpar tabela '{table_name}': {e}")
                # Considerar rollback ou tratamento mais robusto aqui
        connection.commit()
    print("Todas as tabelas especificadas foram limpas.")


def load_dataframe_to_db(df, table_name, engine, dtype_mapping=None):
    """Carrega um DataFrame para uma tabela no banco de dados."""
    if df is None:
        print(f"DataFrame para a tabela '{table_name}' está vazio. Nada a carregar.")
        return
    if df.empty:
        print(f"DataFrame para a tabela '{table_name}' está vazio após transformações. Nada a carregar.")
        return

    try:
        df.to_sql(table_name, engine, if_exists='append', index=False, dtype=dtype_mapping)
        print(f"Dados carregados na tabela '{table_name}'. Linhas: {len(df)}")
    except Exception as e:
        print(f"Erro ao carregar dados na tabela '{table_name}': {e}")

# Mapeamento de tipo específico para a coluna 'fastestLapTime'
results_dtype_mapping = {
    'fastestLapTime': sql_types.Time()
}

if __name__ == '__main__':
    print("Testando o módulo de carregamento...")
    engine = get_db_engine()
    if engine:
        print(f"Engine do banco de dados '{engine.url.drivername}' criada com sucesso.")
        # Testar truncate (CUIDADO: isso apagará dados se as tabelas existirem e tiverem dados)
        # print("\nTestando truncate de tabelas (simulação)...")
        # truncate_tables(engine) # Comente para não executar em testes simples

        # Exemplo de carregar um DataFrame dummy (simular um df transformado)
        # Supondo que a tabela 'constructors' exista
        dummy_constructors_data = {'constructorId': [998, 999], 'name': ['Test Constructor A', 'Test Constructor B']}
        dummy_df = pd.DataFrame(dummy_constructors_data)
        
        print("\nTestando carregamento de DataFrame dummy para 'constructors'...")
        # Antes de carregar, você pode querer truncar APENAS a tabela de teste ou usar if_exists='replace'
        # para este teste específico, mas 'append' é o default do pipeline.
        # Por segurança, este teste não truncará automaticamente.
        try:
             # Para teste, é mais seguro usar replace, mas o pipeline usa append.
            dummy_df.to_sql(settings.TABLE_NAMES["constructors"], engine, if_exists='replace', index=False)
            print("DataFrame dummy carregado (com replace) para 'constructors' para fins de teste.")
        except Exception as e:
            print(f"Erro no teste de carregamento do DataFrame dummy: {e}")
            print("Certifique-se que a tabela 'constructors' existe ou crie-a antes de testar.")
    else:
        print("Não foi possível criar a engine do banco.")