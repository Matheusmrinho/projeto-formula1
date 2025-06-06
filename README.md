# Projeto ETL de Dados da Fórmula 1

![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14-336791?style=for-the-badge&logo=postgresql)
![Pandas](https://img.shields.io/badge/Pandas-1.5.3-150458?style=for-the-badge&logo=pandas)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0-d71f00?style=for-the-badge&logo=sqlalchemy)

## 📜 Descrição

Este projeto consiste em um pipeline de ETL (Extração, Transformação e Carga) automatizado, desenvolvido em Python. [cite_start]Ele foi projetado para extrair dados de corridas de Fórmula 1 de um conjunto de arquivos CSV, processá-los e carregá-los em um banco de dados relacional PostgreSQL. [cite_start]O objetivo final é disponibilizar esses dados de forma estruturada através de views no banco, prontas para serem consumidas por ferramentas de relatórios e análise.

[cite_start]Os dados são uma amostra do conjunto de dados público **Ergast**, disponibilizados em um repositório específico no GitHub para este desafio.

## ✨ Funcionalidades

-   **Extração:** Download automático dos arquivos CSV de URLs especificadas. [cite_start]O script cria a pasta de destino e verifica hashes MD5 para evitar downloads repetidos e desnecessários.
-   **Transformação:** Processamento dos dados brutos com a biblioteca Pandas, incluindo:
    -   Seleção de colunas relevantes.
    -   Criação de novas colunas (ex: `fullname` a partir de `forename` e `surname`).
    -   Ajuste e garantia da consistência dos tipos de dados.
-   **Carga:** Carregamento dos dados transformados em um banco de dados PostgreSQL. O script garante a idempotência, limpando as tabelas antes de cada carga para evitar duplicidade.
-   **Banco de Dados:** Criação de tabelas e views SQL para responder a perguntas de negócio específicas, como:
    -   O resultado de cada corredor por ano (vitórias e pontos).
    -   O piloto com a volta mais rápida para cada Grande Prêmio.
-   **Explorador de Dados:** Um script interativo (`main_data_explorer.py`) para inspecionar e verificar os dados brutos e transformados diretamente no terminal.

## 🛠️ Tecnologias Utilizadas

-   **Linguagem:** Python 3.12
-   **Banco de Dados:** PostgreSQL 14
-   **Principais Bibliotecas Python:**
    -   `pandas`: Para manipulação e transformação de dados.
    -   `requests`: Para o download dos arquivos.
    -   `SQLAlchemy` e `psycopg2`: Para a interação com o banco de dados PostgreSQL.
    -   `python-dotenv`: Para gerenciamento de variáveis de ambiente e credenciais.

## 🗂️ Estrutura do Projeto

```
formula1_etl/
├── config/
│   └── settings.py             # Configurações centrais
├── data_extraction/
│   └── extractor.py            # Módulo de extração de dados
├── data_exploration/
│   └── explorer.py             # Módulo de exploração de dados
├── data_transformation/
│   └── transformer.py          # Módulo de transformação de dados
├── data_loading/
│   └── loader.py               # Módulo de carga de dados
├── sql_scripts/
│   ├── create_tables.sql       # Script de criação das tabelas
│   └── create_views.sql        # Script de criação das views
├── .env.example                # Arquivo de exemplo para variáveis de ambiente
├── .gitignore                  # Arquivos a serem ignorados pelo Git
├── main_data_explorer.py       # Ponto de entrada para explorar dados
├── main_etl_pipeline.py        # Ponto de entrada para executar o ETL
└── README.md                   # Este arquivo
```

## ⚙️ Pré-requisitos

Antes de começar, garanta que você tenha os seguintes softwares instalados em sua máquina:
-   [Python](https://www.python.org/downloads/) (versão 3.9 ou superior)
-   [Git](https://git-scm.com/downloads/)
-   [PostgreSQL](https://www.postgresql.org/download/) (versão 12 ou superior)

## 🚀 Instalação e Configuração

Siga estes passos para configurar o ambiente e executar o projeto.

### 1. Clonar o Repositório
```bash
git clone <URL_DO_SEU_REPOSITORIO_AQUI>
cd formula1_etl
```

### 2. Configurar o Banco de Dados PostgreSQL

Esta é a etapa mais crítica. O script precisa se conectar a um banco de dados que já exista.

1.  **Instale o PostgreSQL** caso ainda não o tenha. Durante a instalação, defina uma senha para o superusuário `postgres`.
2.  **Abra o pgAdmin** (ou outro cliente SQL) e crie um novo banco de dados.
    -   **Nome do Banco:** `formula1_db`
    -   **Codificação (Encoding):** **`UTF8`** (extremamente importante para evitar erros).
3.  **Crie as Tabelas e Views:**
    -   Ainda no pgAdmin, conectado ao banco `formula1_db`, abra a ferramenta de consulta (Query Tool).
    -   Copie o conteúdo do arquivo `sql_scripts/create_tables.sql` e execute-o.
    -   Em seguida, copie o conteúdo de `sql_scripts/create_views.sql` e execute-o.

### 3. Configurar Variáveis de Ambiente

1.  Crie uma cópia do arquivo `.env.example` e renomeie-a para `.env`.
    ```bash
    # No Windows (usando PowerShell ou cmd)
    copy .env.example .env
    ```
2.  Abra o arquivo `.env` e preencha com as suas credenciais do PostgreSQL.
    ```ini
    # .env
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=formula1_db
    DB_USER=postgres
    DB_PASSWORD=sua_senha_aqui 
    DB_TYPE=postgresql
    # ... outras variáveis
    ```

### 4. Configurar o Ambiente Python

É uma boa prática usar um ambiente virtual (`venv`).

1.  **Crie o ambiente virtual:**
    ```bash
    python -m venv .venv
    ```
2.  **Ative o ambiente:**
    ```bash
    # No Windows (PowerShell)
    .\.venv\Scripts\Activate.ps1
    # No Windows (cmd)
    .\.venv\Scripts\activate.bat
    ```
3.  **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```

## ▶️ Executando o Projeto

Com tudo configurado, você pode executar os scripts principais.

### Executar o Pipeline ETL Completo
Para extrair, transformar e carregar todos os dados no banco de dados:
```bash
python main_etl_pipeline.py
```

### Executar o Explorador de Dados
Para analisar os dados brutos ou transformados de forma interativa pelo terminal:
```bash
python main_data_explorer.py
```

## ✅ Verificação

Após a execução bem-sucedida do pipeline, você pode verificar o resultado final diretamente no banco de dados. Conecte-se ao `formula1_db` com o pgAdmin e execute as seguintes consultas:

```sql
-- Consultar os resultados da View 1 (pilotos por ano)
SELECT * FROM view_driver_yearly_results LIMIT 20;

-- Consultar os resultados da View 2 (voltas mais rápidas)
SELECT * FROM view_grand_prix_fastest_laps LIMIT 20;
```
Os resultados devem corresponder às saídas de exemplo fornecidas na especificação do projeto.

---
## ✍️ Autor

**Matheus Romero Rodrigues Marinho**

-   [LinkedIn](Linkedin.com/in/matheusmrinho)
-   [GitHub](github.com/matheusmrinho)
