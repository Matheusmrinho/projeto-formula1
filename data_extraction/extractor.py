# data_extraction/extractor.py
import os
import requests
import hashlib
from config import settings # Importa as configurações

def calculate_md5(file_path):
    """Calcula o hash MD5 de um arquivo."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def download_csv_files(force_download=False):
    """
    Baixa os arquivos CSV.
    Cria a pasta 'extracao' se não existir.
    Verifica MD5 para evitar redownload (diferencial), a menos que force_download seja True.
    Trata erros de download.
    Retorna um dicionário com os caminhos dos arquivos baixados.
    """
    os.makedirs(settings.EXTRACAO_PATH, exist_ok=True)
    print(f"Pasta de extração: '{settings.EXTRACAO_PATH}'")

    downloaded_files = {}
    expected_hashes = {}

    # Baixar e carregar hashes MD5 esperados
    if not force_download:
        try:
            print(f"Baixando arquivo de hash MD5 de: {settings.MD5_HASH_URL}")
            response = requests.get(settings.MD5_HASH_URL, timeout=10)
            response.raise_for_status()
            lines = response.text.strip().split('\n')
            for line in lines[1:]: # Ignora cabeçalho
                if ',' in line:
                    filename_csv, md5_hash = line.split(',')
                    expected_hashes[filename_csv.strip()] = md5_hash.strip()
            print("Hashes MD5 esperados carregados.")
        except requests.exceptions.RequestException as e:
            print(f"Erro ao baixar o arquivo de hash MD5: {e}. Downloads prosseguirão sem verificação de hash ou forçando se especificado.")
            if not force_download: # Se não for forçado, e o hash falhou, é melhor baixar
                 force_download = True # Considera forçar se o arquivo de hash falhou

    for name, url in settings.CSV_URLS.items():
        file_name = f"{name}.csv"
        file_path = os.path.join(settings.EXTRACAO_PATH, file_name)
        downloaded_files[name] = file_path

        if not force_download and os.path.exists(file_path) and file_name in expected_hashes:
            local_md5 = calculate_md5(file_path)
            if local_md5 == expected_hashes[file_name]:
                print(f"Arquivo '{file_path}' já existe e MD5 confere. Pulando download.")
                continue
            else:
                print(f"Arquivo '{file_path}' existe, mas MD5 não confere. Baixando novamente.")
        elif not force_download and os.path.exists(file_path):
             print(f"Arquivo '{file_path}' já existe, mas hash não foi verificado. Baixando novamente para garantir.")

        try:
            print(f"Baixando '{file_name}' de '{url}'...")
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            with open(file_path, 'wb') as f:
                f.write(response.content)
            print(f"'{file_name}' baixado com sucesso em '{file_path}'.")
        except requests.exceptions.Timeout:
            print(f"Erro de Timeout ao tentar baixar '{file_name}'.")
        except requests.exceptions.HTTPError as http_err:
            print(f"Erro HTTP ao tentar baixar '{file_name}': {http_err}")
        except requests.exceptions.RequestException as e:
            print(f"Erro ao baixar '{file_name}': {e}")
        except Exception as e_gen:
            print(f"Ocorreu um erro inesperado durante o download de '{file_name}': {e_gen}")
            downloaded_files[name] = None # Indica falha no download
            
    return downloaded_files

if __name__ == '__main__':
    # Teste rápido do módulo de extração
    print("Testando o módulo de extração...")
    paths = download_csv_files()
    if paths:
        print("\nCaminhos dos arquivos baixados:")
        for name, path in paths.items():
            print(f"- {name}: {path} (Existe: {os.path.exists(path) if path else False})")