# Projeto DBT-Core para Data Warehouse

Este projeto utiliza DBT (Data Build Tool) para gerenciar e transformar dados de um Data Warehouse (DW) de commodities. O objetivo é criar um pipeline de dados robusto e eficiente que trata e organiza os dados de commodities e suas movimentações para análise.

## Estrutura do Projeto

```mermaid
graph TD
    A[Início] --> B[Extrair Dados das Commodities]
    B --> C[Transformar Dados das Commodities]
    C --> D[Carregar Dados no PostgreSQL]
    D --> E[Fim]

    subgraph Extrair
        B1[Buscar Dados de Cada Commodity]
        B2[Adicionar Dados na Lista]
    end

    subgraph Transformar
        C1[Concatenar Todos os Dados]
        C2[Preparar DataFrame]
    end

    subgraph Carregar
        D1[Salvar DataFrame no PostgreSQL]
    end

    B --> B1
    B1 --> B2
    B2 --> C
    C --> C1
    C1 --> C2
    C2 --> D
    D --> D1
```