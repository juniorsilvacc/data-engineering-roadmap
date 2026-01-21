from bronze_extract_data import run_bronze
from silver_transform_data import run_silver
from silver_load_db import run_load

def main():
    print("🚀 Pipeline ETL iniciado")

    run_bronze()
    run_silver()
    run_load()

    print("✅ Pipeline ETL finalizado com sucesso")

if __name__ == "__main__":  
    main()
