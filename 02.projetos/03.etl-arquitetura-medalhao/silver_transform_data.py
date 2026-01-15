import pandas as pd
import os

BRONZE_DIR = "01.bronze-raw"
SILVER_DIR = "02.silver-validated"
os.makedirs(SILVER_DIR, exist_ok=True)

# ------------------------------
# Função de leitura genérica
# ------------------------------
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

# ------------------------------
# Validações Silver
# ------------------------------
def silver_validate(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    # Normaliza colunas (nomes)
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    
    # Converte colunas não escalares (listas, dicts) em string
    for col in df.columns:
        df[col] = df[col].apply(lambda x: str(x) if isinstance(x, (list, dict)) else x)

    # Remove linhas completamente vazias
    df = df.dropna(how="all")

    # Substitui strings vazias por None
    df = df.replace({"": None})

    # Remove duplicados completos
    df = df.drop_duplicates()

    return df

# ------------------------------
# Execução Silver
# ------------------------------
for file in os.listdir(BRONZE_DIR):
    bronze_path = os.path.join(BRONZE_DIR, file)

    # Ignora diretórios e arquivos não suportados
    if not os.path.isfile(bronze_path):
        continue

    ext = os.path.splitext(file)[1].lower()
    if ext not in {".csv", ".json", ".parquet"}:
        print(f"⚠️ Ignorado (formato não suportado): {file}")
        continue

    print(f"🔄 Processando: {file}")
    
    try:
        # Leitura e validação universal
        df = read_file(bronze_path)
        df_valid = silver_validate(df)

        # Salva Silver com mesmo nome, extensão parquet
        silver_path = os.path.join(SILVER_DIR, os.path.splitext(file)[0] + ".parquet")
        df_valid.to_parquet(silver_path, index=False)

        print(f"✅ Silver gerada: {silver_path} ({len(df_valid)} registros)")
    except Exception as e:
        print(f"❌ Erro ao processar {file}: {e}")

print("Pipeline Silver finalizado")