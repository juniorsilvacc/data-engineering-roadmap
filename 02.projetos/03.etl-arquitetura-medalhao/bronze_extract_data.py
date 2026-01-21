import requests
import pandas as pd
import time
import os

BRONZE_DIR = "01.bronze-raw"
os.makedirs(BRONZE_DIR, exist_ok=True)

# ==============================
# PIPELINE BRONZE
# ==============================

# NORMALIZAÇÃO DO CEP
def normalize_cep(cep):
    if pd.isna(cep):
        return None

    cep = str(cep)

    # Remove tudo que não for número
    cep = "".join(filter(str.isdigit, cep))

    if len(cep) != 8:
        return None

    return cep

# EXTRAÇÃO DE DADOS
def get_cep_data(cep, timeout=5):
    """
    Consulta dados de CEP na API ViaCEP.
    """
    url = f"https://viacep.com.br/ws/{cep}/json/"

    try:
        response = requests.get(url, timeout=timeout)
        
        # Lança exceção para erros HTTP (4xx / 5xx)
        response.raise_for_status()
        
        data = response.json()

        # ViaCEP retorna {"erro": true} para CEP inexistente
        if data.get("erro"):
            print(f"⚠️ CEP inexistente ignorado: {cep}")
            return None

        return data

    except requests.exceptions.RequestException as error:
        # Erros de rede, timeout, DNS, conexão resetada, etc
        print(f"❌ Erro CEP {cep}: {error}")
        return None

# EXECUÇÃO
def run_bronze():
    print("🥉 Iniciando Bronze...")
    
    # Leitura dos dados brutos de usuários
    users_df = pd.read_csv(f"{BRONZE_DIR}/users.csv")

    # Remove CEPs duplicados (menos chamadas à API)
    cep_list = (
        users_df["cep"]
        .apply(normalize_cep)
        .dropna()
        .unique()
    )

    cep_results = []

    # Consulta cada CEP
    for cep in cep_list:
        cep_info = get_cep_data(cep)
        if cep_info:  # só adiciona se não for None
            cep_results.append(cep_info)
        time.sleep(0.2)

    # Converte para DataFrame
    bronze_cep_df = pd.DataFrame(cep_results)
    
    if bronze_cep_df.empty:
        print("⚠️ Nenhum CEP válido encontrado. Bronze CEP não gerado.")
        return

    # Salva dados crus (sem transformação semântica)
    bronze_cep_df.to_csv(
        f"{BRONZE_DIR}/cep_info.csv",
        index=False
    )

    print("🥉 Bronze finalizado")
