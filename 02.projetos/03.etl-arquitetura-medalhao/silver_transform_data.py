import pandas as pd
import os

BRONZE_DIR = "01.bronze-raw"
SILVER_DIR = "02.silver-validated"
SUPPORTED_EXT = {".csv", ".json", ".parquet"}
os.makedirs(SILVER_DIR, exist_ok=True)

# ==============================
# PIPELINE SILVER – TRANSFORMAÇÃO
# ==============================

# FUNÇÃO DE LEITURA GENÉRICA
def read_file(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()

    if ext == ".csv":
        return pd.read_csv(path)
    elif ext == ".json":
        return pd.read_json(path)
    elif ext == ".parquet":
        return pd.read_parquet(path)
    else:
        raise ValueError(f"Formato não suportado: {ext}")

# TRANSFORMAÇÃO
def silver_validate(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    # Normaliza nomes de colunas
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    
    # Converte listas e dicionários em string
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: str(x) if isinstance(x, (list, dict)) else x
        )

    # Limpeza básica
    df = df.dropna(how="all")   # Remove linhas completamente vazias
    df = df.replace({"": None}) # Substitui strings vazias por None
    df = df.drop_duplicates()   # Remove duplicados completos

    return df

# EXECUÇÃO
def run_silver():
    print("🥈 Iniciando Silver...")

    for file in os.listdir(BRONZE_DIR):
        bronze_path = os.path.join(BRONZE_DIR, file)

        # Ignora diretórios e arquivos não suportados
        if not os.path.isfile(bronze_path):
            continue

        ext = os.path.splitext(file)[1].lower()
        if ext not in SUPPORTED_EXT:
            continue

        print(f"🔄 Processando: {file}")
        
        try:
            # Leitura e validação universal
            df = read_file(bronze_path)
            
            # Ignora DataFrame vazio
            if df.empty:
                print(f"⚠️ Arquivo vazio ignorado: {file}")
                continue
            
            df_valid = silver_validate(df)

            # Salva Silver com mesmo nome, extensão parquet
            silver_path = os.path.join(
                SILVER_DIR, 
                os.path.splitext(file)[0].replace("_raw", "") + ".parquet"
            )
            
            df_valid.to_parquet(silver_path, index=False)
            
            print(f"✅ Silver gerada: {silver_path} ({len(df_valid)} registros)")
            
        except Exception as e:
            print(f"❌ Erro ao processar {file}: {e}")

    print("🥈 Pipeline finalizado")