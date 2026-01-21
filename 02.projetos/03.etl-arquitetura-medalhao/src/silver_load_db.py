from db import DB
import pandas as pd
import os

SILVER_DIR = "../data/silver-validated"

# Cria a conexão com o banco de dados PostgreSQL
db = DB(
    host="localhost", 
    port=5432, 
    database="etl-arquitetura-medalhao", 
    user="postgres", 
    password="postgres"
)

def run_load():
    print("🟦 Iniciando armazenamento no banco...")
    
    for file in os.listdir(SILVER_DIR):
        # Ignora arquivos que não sejam parquet
        if not file.endswith(".parquet"):
            continue
        
        # Lê o arquivo parquet e carrega os dados em um DataFrame
        df = pd.read_parquet(f"{SILVER_DIR}/{file}")
        
        # Usa o nome do arquivo (sem extensão) como nome da tabela
        table_name = (
            file.replace(".parquet", "")
            .replace("-", "_")
            .lower()
        )
        
        # Cria a tabela no banco caso ela ainda não exista.
        db.create_table(table_name, df)
        
        # Insere os dados do DataFrame na tabela correspondente
        db.insert_data(table_name, df)

    # Fecha a conexão com o banco após processar todos os arquivos
    db.close()
    
    print("🟦 Armazenamento finalizado")