import requests
from tinydb import TinyDB

def extrair():
    url = "https://api.coinbase.com/v2/prices/spot"
    response = requests.get(url)
    return response.json()

def transformar(dados_json):
    if 'data' in dados_json:
        data = dados_json.get('data', {}) # 'data' não existir, data vira um dicionário vazio {}
        valor = data.get('amount')
        criptomoeda = data.get('base')
        moeda = data.get('currency')
        
        dados_tratados = {
            "valor": valor,
            "criptomoeda": criptomoeda,
            "moeda": moeda
        }
        
        return dados_tratados
    else: 
        return {"erro": "Dados inválidos ou incompletos"}

def load(dados_tratados):
    db = TinyDB('db.json')
    db.insert(dados_tratados)
    print("Dados salvos com sucesso!")
    
# Esse trecho roda somente se o arquivo for executado diretamente (e não importado
# Chama a função extrair() para obter os dados da API.
# Depois, chama transformar() para tratar esses dados.
# Por fim, imprime os dados formatados.
if __name__ == "__main__":
    dados_json = extrair()
    dados_tratados = transformar(dados_json)
    load(dados_tratados)