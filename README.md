# 🛠️ Data Engineering Roadmap

Este repositório é um compilado de **projetos práticos**, **anotações de estudo** e **experimentos** relacionados à minha jornada em **Engenharia de Dados**. A proposta é centralizar tudo o que venho aprendendo de forma aplicada, com foco em ferramentas, boas práticas e pipelines.

## 🚀 Objetivo

> Aprender, praticar e compartilhar conhecimentos por meio de projetos, utilizando tecnologias modernas e estratégias de engenharia de dados.

### Áreas abordadas:

- 📥 **Coleta de dados** — APIs públicas, arquivos CSV/JSON, Web Scraping
- 🗃️ **Armazenamento** — Bancos relacionais (PostgreSQL), NoSQL (MongoDB), Cloud (BigQuery, Redshift, S3)
- 🛠️ **Transformação** — Pandas, PySpark, DuckDB, dbt-core
- 🧠 **Modelagem de dados** — Data Vault, Dimensional, Star Schema
- ⏱️ **Orquestração e automação** — Airflow, cron, scripts com agendamento
- 🚀 **Performance & otimização** — Parquet, particionamento
- 🔁 **Versionamento e modularização** — Git, ambientes isolados, scripts reaproveitáveis

## 📁 Organização do repositório

```
├── data-engineering-roadmap/
│ ├── 01-etl-1bilhao-linhas/
│ ├── ...
│ └── ...
├── resumos/ 
│ ├── arquitetura-dados.md
│ ├── pipelines.md
│ └── conceitos-snowflake.md
├── datasets/
└── README.md
```

## 📚 Projetos

| Projeto                       | Descrição                                                                |
|-------------------------------|--------------------------------------------------------------------------|
| `01-etl-1bilhao-linhas`       | Pipeline ETL simulado para processamento de 1 bilhão de linahs           |

## 🧰 Tecnologias e Ferramentas

- **Linguagens:** Python, SQL
- **Processamento:** Pandas, PySpark, DuckDB
- **Transformação:** dbt-core
- **Bancos de dados:** PostgreSQL, MongoDB, BigQuery, Redshift
- **Armazenamento:** AWS S3, Google Drive, CSV
- **Orquestração:** Apache Airflow
- **Containerização:** Docker
- **Versionamento:** Git + GitHub

## 📦 Como executar os projetos

Cada projeto possui seu próprio `README.md` com instruções de instalação, dependências e execução. 

Recomenda-se o uso de ambientes virtuais:
```bash
python -m venv venv
source venv/bin/activate  # Linux/macOS
venv\Scripts\activate     # Windows

pip install -r requirements.txt
