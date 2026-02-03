# Pipeline Olist Datawarehouse

Pipeline **fim-a-fim** de Engenharia de Dados (Extract → Transform → Load → Modelagem → BI) usando **Apache Airflow**, **PySpark**, **Amazon Redshift**, **dbt** e **Looker Studio**, com **versionamento no GitHub**.

> **Problema de negócio:** 
> - Quais regiões têm **maior atraso de entrega**?
> - Quais categorias/sellers geram **mais receita (GMV)** e **pior satisfação**?
> - Como **frete** e **prazo** impactam **reviews**?
> - Qual o desempenho por **UF/cidade/categoria** ao longo do tempo?

---

## 1) Arquitetura do projeto (visão geral)

**Fonte (Olist CSV)** → **S3 (raw)** → **Spark (bronze/parquet)** → **Redshift (staging)** → **dbt (marts star schema)** → **Looker Studio (dashboards)**

### Camadas de dados
- **raw (S3)**: CSV original (imutável), particionado por data de ingestão
- **bronze (S3)**: Parquet limpo/padronizado via PySpark
- **staging (Redshift)**: tabelas espelho para carga (COPY)
- **analytics (Redshift)**: fatos e dimensões finais (dbt)

---

## 2) Stack / Ferramentas

- **Orquestração:** Apache Airflow (Docker Compose local)
- **Processamento:** PySpark (jobs versionados no repo)
- **Storage (landing/bronze):** Amazon S3
- **Data Warehouse:** Amazon Redshift (preferência: Serverless)
- **Modelagem / Qualidade:** dbt (docs + tests)
- **BI:** Looker Studio conectado ao Redshift
- **Versionamento/CI:** GitHub + GitHub Actions

---

## 3) Dataset (fonte real)

- **Brazilian E-Commerce Public Dataset by Olist (Kaggle)**
  - Você pode baixar do Kaggle e manter uma cópia versionada no S3/raw para reprodutibilidade.
  - O pipeline é desenhado para ingestão via arquivos CSV (HTTP/Local → S3).

---

## 4) KPIs e entregáveis

### KPIs (mínimo)
- GMV e pedidos por mês/UF/categoria
- Prazo de entrega (lead time) por UF/cidade/seller
- % de atrasos (quando aplicável)
- Review score médio por categoria/seller
- Frete médio por região/categoria
- Mix de pagamento (cartão/boleto) e parcelas

### Entregáveis
- Redshift com schema **analytics** (dimensões + fatos)
- dbt com **docs** e **tests** rodando
- DAGs Airflow executando o pipeline
- Dashboard Looker Studio consumindo tabelas finais
- Repositório GitHub com README e estrutura reproduzível



## 5) Roadmap de evolução (depois do MVP)
- Incremental loads (simular CDC)
- Data Quality com Great Expectations/Soda
- Observabilidade (logs, métricas, alertas)
- Otimização Redshift (sort/dist keys onde fizer sentido)
- Projeto separado: Data Lake puro → depois Lakehouse


