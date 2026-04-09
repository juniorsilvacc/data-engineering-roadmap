import pandas as pd
import os
import glob

# Caminho para ler os arquivos
folder_path = 'src/data/raw'

# Lista todos os arquivos de excel
excel_files = glob.glob(os.path.join(folder_path , '*.xlsx'))

if not excel_files:
    print("Nenhum arquivo compatível encontrado")
else: 
    # Dataframe  = tabela na memória para guardar os conteúdos dos arquivos
    dfs = []
    
    for excel_file in excel_files:
        try:
            # Ler o arquivo de excel
            df = pd.read_excel(excel_file)
            
            # Pega o nome do arquivo
            file_name = os.path.basename(excel_file)
            
            df['filename'] = file_name
            
            # Cria uma nova coluna chamada location
            if 'brasil' in file_name.lower():
                df['location'] = 'br'
            elif 'france' in file_name.lower():
                df['location'] = 'fr'
            elif 'italian' in file_name.lower():
                df['location'] = 'it'
            
            # Cria uma nova coluna chamada campaign
            df['campaign'] = df['utm_link'].str.extract(r'utm_campaign=(.*)')

            # Guarda dados tratados dentro de uma dataframe
            dfs.append(df)
            
        except Exception as e:
            print(f"Erro ao ler o arquivo {excel_files}: {e}")
    
if dfs:
    # Concatena todas as tabelas salvas no dfs em uma única tabela
    result = pd.concat(dfs, ignore_index=True)
    
    # Caminho de saída (Onde o arquivo será salvo)
    output_file = os.path.join('src', 'data', 'ready', 'clean.xlsx')
    
    # Configuração do motor de escrita (xlsxwriter)
    writer = pd.ExcelWriter(output_file, engine='xlsxwriter')
    
    # Escreve o DataFrame no Excel
    result.to_excel(writer, index=False)
    
    # Salva o arquivo
    writer._save()
else:
    print("Nenhum dado para ser salvo")