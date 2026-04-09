import pandas as pd
import os

BRONZE_DIR = "../data/bronze-raw"
SILVER_DIR = "../data/silver-validated"
SUPPORTED_EXT = {".csv", ".json", ".parquet"}

os.makedirs(SILVER_DIR, exist_ok=True)

# ==============================
# LEITURA GENÉRICA
# ==============================
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

# ==============================
# VALIDAÇÃO GENÉRICA (TODOS OS DF)
# ==============================
def silver_validate(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    # Normaliza nomes de colunas
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    
    # Converte tipos
    if "preco" in df.columns:
        df["preco"] = pd.to_numeric(df["preco"], errors="coerce")

    if "quantidade" in df.columns:
        df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce").astype("Int64")
    
    if "data_nascimento" in df.columns:
        df["data_nascimento"] = pd.to_datetime(
            df["data_nascimento"], errors="coerce"
        )

    # Limpeza básica
    df = df.dropna(how="all")   # Remove linhas completamente vazias
    df = df.replace({"": None}) # Substitui strings vazias por None
    # df = df.drop_duplicates()   # Remove duplicados completos

    return df

# ==============================
# NORMALIZAÇÃO ESPECÍFICA – PRODUCTS
# ==============================
def normalize_products(df: pd.DataFrame):
    """
    Se existir coluna tags (lista), cria tabela product_tags normalizada
    """
    if "tags" not in df.columns:
        return df, None

    df = df.copy()

    # Garante que tags é lista
    df["tags"] = df["tags"].apply(
        lambda x: x if isinstance(x, list) else []
    )

    # Cria tabela normalizada
    product_tags = (
        df[["id", "tags"]]
        .explode("tags")
        .rename(columns={"id": "product_id", "tags": "tag"})
        .dropna()
        .drop_duplicates()
    )

    # Remove tags da tabela principal
    df = df.drop(columns=["tags"])

    return df, product_tags

# ==============================
# EXECUÇÃO DO SILVER
# ==============================
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
            
            # Validação genérica
            df_valid = silver_validate(df)

            base_name = os.path.splitext(file)[0].replace("_raw", "")
            
            # Normalização específica (products)
            if base_name == "products":
                products_df, product_tags_df = normalize_products(df_valid)

                products_df.to_parquet(
                    os.path.join(SILVER_DIR, "products.parquet"),
                    index=False
                )

                product_tags_df.to_parquet(
                    os.path.join(SILVER_DIR, "product_tags.parquet"),
                    index=False
                )
            else:
                silver_path = os.path.join(SILVER_DIR, base_name + ".parquet")
                df_valid.to_parquet(silver_path, index=False)

        except Exception as e:
            print(f"❌ Erro ao processar {file}: {e}")

    print("🥈 Pipeline finalizado")