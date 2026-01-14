import streamlit as st
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

# Lê a variável DATABASE_URL
DATABASE_URL = os.getenv("DATABASE_KEY")

def ler_dados_postgres():
    """Lê os dados do banco PostgreSQL e retorna como DataFrame."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        query = "SELECT * FROM bitcoin_dados ORDER BY timestamp DESC"
        df = pd.read_sql(query, conn) # Executa a consulta e carrega os dados em um DataFrame do pandas
        conn.close()
        return df
    except Exception as e:
        st.error(f"Erro ao conectar no PostgreSQL: {e}")
        return pd.DataFrame() # Retorna DataFrame vazio para evitar travar

# Função principal que monta o dashboard
def main():
    st.set_page_config(page_title="Dashboard de Preços do Bitcoin", layout="wide")
    st.title("📊 Dashboard de Preços do Bitcoin")
    st.write("Este dashboard exibe os dados do preço do Bitcoin coletados periodicamente em um banco PostgreSQL.")

    # Chama a função para carregar os dados do banco
    df = ler_dados_postgres() 

    # Se existirem dados, exibe os elementos abaixo
    if not df.empty:
        st.subheader("📋 Dados Recentes")
        st.dataframe(df) # Exibição da tabela com os dados

        # Converte coluna 'timestamp' para o tipo datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Ordena o DataFrame por data (ascendente)
        df = df.sort_values(by='timestamp')
        
        # Subtítulo e exibição do gráfico de linha com a evolução dos preços
        st.subheader("📈 Evolução do Preço do Bitcoin")
        st.line_chart(data=df, x='timestamp', y='valor', use_container_width=True)

        st.subheader("🔢 Estatísticas Gerais")
        
        # Divide o layout em 3 colunas para mostrar os indicadores
        col1, col2, col3 = st.columns(3)
        col1.metric("Preço Atual", f"${df['valor'].iloc[-1]:,.2f}") # Mostra o último valor registrado
        col2.metric("Preço Máximo", f"${df['valor'].max():,.2f}") # Mostra o valor máximo encontrado
        col3.metric("Preço Mínimo", f"${df['valor'].min():,.2f}") # Mostra o valor mínimo encontrado
    else:
        st.warning("Nenhum dado encontrado no banco de dados PostgreSQL.")

# Executa a função principal se o arquivo for executado diretamente
if __name__ == "__main__":
    main()