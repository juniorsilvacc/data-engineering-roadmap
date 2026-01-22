import psycopg2
import pandas as pd
from psycopg2.extras import execute_batch

def map_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "NUMERIC"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "TEXT"

class DB:
    def __init__(self, host, port, database, user, password):
        """Inicializa a conexão com o banco de dados"""
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        
    def create_table(self, table_name, df: pd.DataFrame):
        cursor = self.conn.cursor()

        columns = []
        
        for col, dtype in df.dtypes.items():
            sql_type = map_dtype(dtype)
            columns.append(f"{col} {sql_type}")

        columns_sql = ", ".join(columns)

        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql})"
        )

        self.conn.commit()
        cursor.close()

    def insert_data(self, table_name, df: pd.DataFrame):
        cursor = self.conn.cursor()
        
        # Converte numpy → Python e NaN → None
        df = df.astype(object).where(pd.notnull(df), None)

        # Cria os placeholders (%s) de acordo com a quantidade de colunas
        placeholders = ", ".join(["%s"] * len(df.columns))
        
        sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

        # Converte o DataFrame em uma lista de tuplas
        data = list(df.itertuples(index=False))
        
        # Insere os dados em lote (mais rápido e seguro)
        execute_batch(cursor, sql, data)

        self.conn.commit()
        cursor.close()
    
    def execute_query(self, query):
        cursor = self.conn.cursor()
        
        cursor.execute(query)
        
        # Recupera os dados retornados pela query
        result = cursor.fetchall()
        
        cursor.close()
        return result
    
    def select_all_data_from_table(self, table_name, limit=10):
        return self.execute_query(
            f"SELECT * FROM {table_name} LIMIT {limit}"
        )
    
    def close(self):
        self.conn.close()