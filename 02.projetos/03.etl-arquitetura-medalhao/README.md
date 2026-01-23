# 🏗️ Projeto ETL com Arquitetura Medalhão (Bronze • Silver • Gold)

Este projeto implementa um **pipeline ETL completo** seguindo o padrão de **Arquitetura Medalhão**, muito utilizado em ambientes modernos de Engenharia de Dados e Analytics.

O objetivo é demonstrar, de forma prática, como extrair dados brutos, tratá-los, normalizá-los, armazená-los de forma estruturada e, por fim, gerar **análises e indicadores de negócio**.

## 🎯 Objetivo do Projeto

- Construir um **ETL robusto e reutilizável**
- Aplicar boas práticas de Engenharia de Dados
- Simular um ambiente real de ingestão, transformação e análise
- Gerar **indicadores analíticos** a partir dos dados processados
- Servir como **projeto de portfólio** para Data Engineer / Data Analyst

## 🏛️ Arquitetura Utilizada

O projeto segue o padrão **Medalhão**:


### 🥉 Bronze — Raw Data
Camada responsável pela **extração dos dados**, sem transformação semântica.

**Fontes:**
- Arquivos CSV e JSON
- API externa (ViaCEP)

**Principais características:**
- Dados armazenados no formato bruto
- Tratamento de erros de API (timeout, CEP inválido)
- Redução de chamadas duplicadas à API
- Nenhuma regra de negócio aplicada

📌 Objetivo: **preservar os dados originais**

---

### 🥈 Silver — Validated & Normalized
Camada responsável pela **limpeza, padronização e normalização** dos dados.

**Transformações aplicadas:**
- Padronização de nomes de colunas
- Conversão de tipos (int, float, datetime)
- Tratamento de valores nulos
- Remoção de registros inválidos
- Normalização de relacionamentos (1:N)

**Aplicado:**
- Coluna `tags` de produtos foi normalizada em uma tabela separada (`product_tags`)

**Formato de armazenamento:**
- Parquet (otimizado para analytics)

📌 Objetivo: **dados confiáveis, consistentes e prontos para análise**

---

### 🟦 Gold — Analytics & Insights
Camada focada em **análise de dados e geração de indicadores de negócio**.

Nesta etapa, os dados da Silver são utilizados para gerar **métricas e visualizações**, apoiando a tomada de decisão.

**Análises realizadas:**
- Agregação
- Indicadores
- Métricas de negócio
- Visualizações com Matplotlib

📌 Objetivo: **transformar dados em informação acionável**

## 🛠️ Tecnologias Utilizadas
- Python
- Pandas
- Matplotlib
- PostgreSQL
- Parquet
- Requests (API)
- Docker
- Arquitetura Medalhão

## 📊 Resultados Obtidos
- Pipeline ETL completo e funcional
- Dados normalizados e organizados
- Separação clara entre dados operacionais e analíticos
- Geração de indicadores de negócio
- Base sólida para dashboards e BI

### 👤 Autor
`Projeto desenvolvido como estudo prático de Engenharia de Dados e Análise de Dados, com foco em boas práticas, arquitetura e visão de negócio.`
