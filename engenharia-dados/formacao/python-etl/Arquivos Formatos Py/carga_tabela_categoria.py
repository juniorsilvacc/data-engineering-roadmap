# %%
import pandas as pd
import psycopg2

# %%
server = 'localhost'
database = 'python_etl'
user = 'postgres'
password = 'root'

connection_string = f"dbname={database} user={user} password={password} host={server}"

try:
    conn = psycopg2.connect(connection_string)
    print("Conexão bem-sucedida ao PostgreSQL!")
    
    cursor = conn.cursor()
except Exception as e:
    print("Erro ao conectar ao PostgreSQL:", e)

# %%
dados = pd.read_excel("/home/juniorsilvadev/engenharia-dados/python-etl/Origem/Origem/arquivos_excel/Categoria.xlsx")

# %%
str(dados.columns).replace("'", "")

# %%
create_table_query = """
CREATE TABLE IF NOT EXISTS categorias (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);
"""

try:
    cursor.execute(create_table_query)
    conn.commit()
    print("Tabela 'categorias' criada com sucesso!")
except Exception as e:
    print("Erro ao criar a tabela 'categorias':", e)
    conn.rollback()

# %%
cursor.execute("TRUNCATE TABLE categorias")

# %%
insert_query = """
INSERT INTO categorias (id, name) 
VALUES (%s, %s)
"""

try:
    for index, row in dados.iterrows():
        cursor.execute(insert_query, (row['id'], row['name']))
    
    conn.commit()
    print("Dados inseridos com sucesso!")
except Exception as e:
    print("Erro ao inserir dados:", e)
    conn.rollback()
finally:
    cursor.close()
    conn.close()


