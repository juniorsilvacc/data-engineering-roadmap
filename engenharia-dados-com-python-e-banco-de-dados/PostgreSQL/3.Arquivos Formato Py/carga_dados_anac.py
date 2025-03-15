# %%
import pandas as pd
import psycopg2
import chardet

caminho_arquivo = "/home/juniorsilvadev/engenharia-dados/engenharia-dados-com-python-e-banco-de-dados/PostgreSQL/Arquivos JSON/V_OCORRENCIA_AMPLA.json"

with open(caminho_arquivo, "rb") as f:
    raw_data = f.read(10000)
    result = chardet.detect(raw_data)
    encoding_detectado = result['encoding']

df = pd.read_json(caminho_arquivo, encoding=encoding_detectado)

# %%
colunas = ['Numero_da_Ocorrencia', 'Classificacao_da_Ocorrência', 'Data_da_Ocorrencia', 'Municipio', 'UF', 'Regiao', 'Nome_do_Fabricante']
df = df[colunas]
df.rename(columns={'Classificacao_da_Ocorrência': 'Classificacao_da_Ocorrencia'}, inplace=True)
df.columns = df.columns.str.strip()

# %%
dbname   = 'anac_db'
user     = 'postgres'
password = 'root'
host     = 'localhost'
port     = '5432'

try:
    conexao = psycopg2.connect(dbname=dbname,
                            user=user,
                            password=password,
                            host=host,
                            port=port)

    cursor = conexao.cursor()
    conexao.commit()
except Exception as e:
    print('Error ao conectar com banco de dados:', e)

# %%
create_table_query = """
    CREATE TABLE IF NOT EXISTS anac (
        Numero_da_Ocorrencia int,
        Classificacao_da_Ocorrencia VARCHAR(50),
        Data_da_Ocorrencia DATE,
        Municipio VARCHAR(50),
        UF VARCHAR(30),
        Regiao VARCHAR(50),
        Nome_do_Fabricante VARCHAR(50)
    );
"""

try:
    cursor.execute(create_table_query)
    conexao.commit()
except Exception as e:
    print('Error ao criar a tabela', e)
    conexao.rollback()

# %%
cursor.execute("TRUNCATE TABLE anac")

# %%
insert_query = """
    INSERT INTO anac (
        Numero_da_Ocorrencia, Classificacao_da_Ocorrencia, Data_da_Ocorrencia, Municipio, UF, Regiao, Nome_do_Fabricante
    ) VALUES (%s, %s, %s, %s, %s, %s, %s);
"""

try:
    for index, row in df.iterrows():
        cursor.execute(insert_query, (
            row['Numero_da_Ocorrencia'],
            row['Classificacao_da_Ocorrencia'],
            row['Data_da_Ocorrencia'],
            row['Municipio'],
            row['UF'],
            row['Regiao'],
            row['Nome_do_Fabricante']
        ))
    
    conexao.commit()
    print('Carga de dados inserida com sucesso')
except Exception as e:
    print('Error ao inserir dados:', e)
    conexao.rollback()
finally:
    cursor.close()
    conexao.close()


