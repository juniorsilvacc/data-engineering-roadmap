import requests
import pandas as pd
import time
import os

# ==============================
# PIPELINE BRONZE – EXTRAÇÃO
# ==============================
def get_cep_data(cep, timeout=5):
    
    if len(cep) != 8 or not cep.isdigit():
       return None

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
        print(f"❌ Erro ao consultar o CEP {cep}: {error}")
        return None

# Garante que o diretório Bronze existe
os.makedirs("01.bronze-raw", exist_ok=True)

# Leitura dos dados brutos de usuários
users_df = pd.read_csv("01.bronze-raw/users.csv")

# Lista de CEPs
cep_list = (users_df["cep"].to_list())

cep_results = []

# Consulta cada CEP
for cep in cep_list:
    cep_info = get_cep_data(cep)
    if cep_info:  # só adiciona se não for None
        cep_results.append(cep_info)
    else:
        print(f"⚠️ CEP ignorado: {cep}")
        
    time.sleep(0.2)

# Converte para DataFrame
bronze_cep_df = pd.DataFrame(cep_results)

# Salva dados crus (sem transformação semântica)
bronze_cep_df.to_csv(
    "01.bronze-raw/cep_info_raw.csv",
    index=False
)

print("🥉 Bronze gerado com sucesso")
print(f"📊 Registros válidos: {len(bronze_cep_df)}")