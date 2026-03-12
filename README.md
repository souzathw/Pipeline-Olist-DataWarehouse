# Data Engineering Pipeline --- Olist Data Warehouse

## Visão Geral

Este projeto implementa um pipeline completo de Engenharia de Dados
utilizando tecnologias modernas de processamento, armazenamento,
transformação e visualização de dados.

O objetivo é construir um **Data Warehouse analítico** com base no
dataset público da **Olist**, permitindo explorar métricas de negócio
relacionadas à satisfação do cliente, comportamento de pedidos,
pagamentos e impacto do atraso na entrega nas avaliações.

O pipeline foi desenvolvido seguindo boas práticas de arquitetura de
dados, com separação por camadas, modelagem dimensional, execução
incremental por lote e orquestração automatizada.

------------------------------------------------------------------------

## Arquitetura do Projeto

O fluxo completo do projeto segue a arquitetura abaixo:

    Dataset Olist
          │
          ▼
    Apache Spark (ETL - Bronze Layer)
          │
          ▼
    Amazon S3 (Data Lake - Bronze)
          │
          ▼
    Amazon Redshift (STG Layer)
          │
          ▼
    dbt Transformations
          │
          ▼
    Star Schema (Facts + Dimensions)
          │
          ▼
    Data Marts Analíticos
          │
          ▼
    Looker Studio Dashboards

------------------------------------------------------------------------

## Stack Tecnológica

As principais tecnologias utilizadas neste projeto foram:

-   Python
-   Apache Spark
-   Docker
-   Amazon S3
-   Amazon Redshift
-   dbt (Data Build Tool)
-   Apache Airflow
-   SQL
-   Looker Studio

------------------------------------------------------------------------

## Estrutura das Camadas

### 1. Bronze Layer

A camada Bronze é responsável pela ingestão e armazenamento inicial dos
dados.

Nessa etapa:

-   os dados são processados com **Apache Spark**
-   os arquivos são gravados em **formato Parquet**
-   o armazenamento é feito no **Amazon S3**
-   os dados são particionados por `ingestion_date`

Exemplo de estrutura:

    s3://bucket/bronze/olist/orders/ingestion_date=YYYY-MM-DD/

Essa camada representa a base bruta tratada, pronta para carga no Data
Warehouse.

------------------------------------------------------------------------

### 2. STG Layer

Após a ingestão no S3, os dados são carregados no **Amazon Redshift**
por meio de `COPY FROM S3`, compondo a camada de staging no schema
`stg`.

Exemplos de tabelas:

    stg.orders
    stg.order_reviews
    stg.order_payments
    stg.order_items
    stg.customers
    stg.sellers
    stg.products

Essa camada mantém os dados com transformação mínima, servindo como base
confiável para o dbt.

------------------------------------------------------------------------

### 3. Camada Analítica com dbt

O **dbt** é utilizado para transformar os dados da camada `stg` em
estruturas analíticas organizadas.

A estrutura dos modelos foi organizada da seguinte forma:

    models/
    ├── staging/
    ├── dimensions/
    ├── facts/
    └── marts/

------------------------------------------------------------------------

## Modelagem Dimensional

O projeto segue o padrão **Star Schema**, separando dimensões e fatos
para facilitar análises e consumo por ferramentas de BI.

### Dimensões

    dim_customer
    dim_seller
    dim_product
    dim_date

### Fatos

    fact_orders
    fact_order_reviews
    fact_order_payments

------------------------------------------------------------------------

## Data Marts

Foram criados marts analíticos com foco em responder perguntas
específicas de negócio.

### mart_nps_proxy

Mart responsável por representar a satisfação do cliente a partir das
avaliações dos pedidos.

Principais campos:

    total_reviews
    promoters
    passives
    detractors
    pct_promoters
    pct_detractors
    nps_proxy_score

Esse mart funciona como uma aproximação de NPS com base no
`review_score`.

------------------------------------------------------------------------

### mart_delay_vs_review_score

Mart responsável por analisar o impacto do atraso na entrega sobre a
nota dada pelo cliente.

Principais campos:

    delay_days
    avg_score
    n_reviews

Esse mart permite identificar a relação entre performance logística e
experiência do cliente.

------------------------------------------------------------------------

### mart_daily_orders

Mart utilizado para análise do volume diário de pedidos.

Principais campos:

    order_date
    orders

------------------------------------------------------------------------

## Incremental por Lote

Uma das principais características do projeto é a execução incremental
por `ingestion_date`.

Isso permite:

-   reprocessar lotes específicos
-   evitar rebuild total desnecessário
-   manter o pipeline mais eficiente
-   seguir um padrão mais próximo de ambientes reais de engenharia de
    dados

Exemplo de execução:

    dbt build --vars '{ingestion_date: "2026-02-04"}'

------------------------------------------------------------------------

## Orquestração com Airflow

O pipeline é orquestrado com **Apache Airflow**, permitindo automatizar
a execução das etapas de transformação.

Fluxo orquestrado:

    Carga Bronze
       ↓
    Carga STG no Redshift
       ↓
    dbt build incremental

------------------------------------------------------------------------

## Dashboards

Os dados transformados são consumidos no **Looker Studio**, onde foram
construídos dashboards analíticos.

### Customer Satisfaction

Utiliza o mart `mart_nps_proxy`:

-   total de reviews
-   promoters
-   passives
-   detractors
-   NPS proxy score

### Delivery Delay vs Review Score

Utiliza o mart `mart_delay_vs_review_score`:

-   relação entre dias de atraso e média das notas
-   volume de reviews por faixa de atraso

------------------------------------------------------------------------

## Qualidade de Dados

Foram implementados testes no **dbt** para garantir a confiabilidade dos
dados.

Exemplos:

-   `not_null`
-   `unique`
-   `relationships`
-   `accepted_values`

Validação importante:

    review_score ∈ [1,2,3,4,5]

------------------------------------------------------------------------

## Como Executar

Rodar dbt:

    dbt build

Rodar incremental por lote:

    dbt build --vars '{ingestion_date: "YYYY-MM-DD"}'

Gerar documentação dbt:

    dbt docs generate
    dbt docs serve

------------------------------------------------------------------------

## Objetivo do Projeto

Demonstrar a construção de um pipeline moderno de Engenharia de Dados
cobrindo:

-   ingestão
-   processamento
-   armazenamento
-   transformação
-   modelagem dimensional
-   orquestração
-   visualização analítica

------------------------------------------------------------------------

## Autor

**Thiago Souza**