from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
import requests
from datetime import datetime
from dotenv import load_dotenv
from time import sleep
import os

# Carrega variáveis do .env
load_dotenv()

# Pega os valores do .env
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
dbname = os.getenv("DB_NAME")

# Monta a URL
DATABASE_URL = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

# Para API
URL_API = os.getenv("URL_API")

# Criação do engine e sessão
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# Definição do modelo de dados
class BitcoinDados(Base):
    __tablename__ = "bitcoin_dados"
    
    id = Column(Integer, primary_key=True)
    valor = Column(Float)
    criptomoeda = Column(String(10))
    moeda = Column(String(10))
    timestamp = Column(DateTime)

# Cria a tabela (se não existir)
Base.metadata.create_all(engine)

def extrair_dados_bitcoin():
    """Extrai o JSON completo da API da Coinbase."""
    url = URL_API
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    else: 
        print(f"Erro ao acessar a API: {response.status_code}")
        return None

def transformar_dados_bitcoin(dados_json):
    """Transforma os dados brutos da API e adiciona timestamp."""
    if 'data' in dados_json:
        data = dados_json.get('data', {})
        valor = data.get('amount')
        criptomoeda = data.get('base')
        moeda = data.get('currency')
        
        dados_tratados = BitcoinDados (
            valor = valor,
            criptomoeda = criptomoeda,
            moeda = moeda,
            timestamp = datetime.now()
        )
        
        return dados_tratados
    else: 
        return {"erro": "Dados inválidos ou incompletos"}

def salvar_dados_sqlalchemy(dados):
    """Salva os dados no PostgreSQL usando SQLAlchemy."""
    with Session() as session:
        session.add(dados)
        session.commit()
        print("Dados salvos no PostgreSQL!")
    
if __name__ == "__main__":
    while True: # Enquanto isso for VERDADEIRO, execute a cada 15 segundos
        
        # Extração e tratamento dos dados
        dados_json = extrair_dados_bitcoin()
        dados_tratados = transformar_dados_bitcoin(dados_json)
        
        print("Dados Tratados") # Mostrar dados tratados
        salvar_dados_sqlalchemy(dados_tratados) # Salvar dados no PostgreSQL
        
        # Pausar por 15 segundos
        print("Aguardando 15 segundos...")
        sleep(15)