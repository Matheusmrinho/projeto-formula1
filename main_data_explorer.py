# main_data_explorer.py
from data_exploration import explorer
from data_extraction import extractor # Para garantir que os dados sejam baixados
from data_transformation import transformer # Para explorar dados transformados também
import pandas as pd
import os
from config import settings

def explore_data_interactive():
    """Permite ao usuário escolher qual conjunto de dados explorar."""
    print("--- Ferramenta de Exploração de Dados de Fórmula 1 ---")

    # Passo 0: Garantir que os CSVs estão baixados
    print("\nVerificando/Baixando arquivos CSV necessários...")
    downloaded_files = extractor.download_csv_files()
    
    # Checar se downloads foram bem sucedidos
    if not all(downloaded_files.values()):
        print("Alguns arquivos não puderam ser baixados. A exploração pode ser limitada.")
        # Continuar mesmo assim para o que foi baixado

    while True:
        print("\nEscolha uma opção para explorar:")
        print("Dados Brutos (CSVs originais):")
        idx = 1
        raw_options = {}
        for key in settings.CSV_URLS.keys():
            print(f"  {idx}. {key.capitalize()} (raw)")
            raw_options[str(idx)] = key
            idx += 1
        
        print("\nDados Transformados (após processamento em memória):")
        transformed_options = {}
        for key in settings.CSV_URLS.keys():
            print(f"  {idx}. {key.capitalize()} (transformed)")
            transformed_options[str(idx)] = key
            idx += 1

        print(f"\n  {idx}. Sair")
        exit_option = str(idx)

        choice = input("Digite o número da sua escolha: ").strip()

        if choice == exit_option:
            print("Saindo do explorador de dados.")
            break
        
        selected_key = None
        data_type = None

        if choice in raw_options:
            selected_key = raw_options[choice]
            data_type = "raw"
            print(f"\nExplorando dados brutos para: {selected_key.capitalize()}")
            df = explorer.load_csv_to_df(selected_key)
            if df is not None:
                explorer.display_df_info(df, f"{selected_key.capitalize()} (Raw CSV)")
            else:
                print(f"Não foi possível carregar os dados brutos de {selected_key}.")

        elif choice in transformed_options:
            selected_key = transformed_options[choice]
            data_type = "transformed"
            print(f"\nExplorando dados transformados para: {selected_key.capitalize()}")
            
            # Carregar o CSV bruto primeiro
            raw_df_path = os.path.join(settings.EXTRACAO_PATH, f"{selected_key}.csv")
            if not os.path.exists(raw_df_path):
                print(f"Arquivo CSV bruto {raw_df_path} não encontrado. Não é possível transformar.")
                continue
            try:
                raw_df = pd.read_csv(raw_df_path)
            except Exception as e:
                print(f"Erro ao ler o CSV bruto {raw_df_path}: {e}")
                continue

            # Aplicar a transformação correspondente
            transformed_df = None
            if selected_key == "constructors":
                transformed_df = transformer.transform_constructors_df(raw_df.copy())
            elif selected_key == "drivers":
                transformed_df = transformer.transform_drivers_df(raw_df.copy())
            elif selected_key == "races":
                transformed_df = transformer.transform_races_df(raw_df.copy())
            elif selected_key == "results":
                transformed_df = transformer.transform_results_df(raw_df.copy())
            
            if transformed_df is not None:
                explorer.display_df_info(transformed_df, f"{selected_key.capitalize()} (Transformado)")
            else:
                print(f"Não foi possível transformar os dados de {selected_key}.")
        else:
            print("Opção inválida. Tente novamente.")

if __name__ == "__main__":
    explore_data_interactive()