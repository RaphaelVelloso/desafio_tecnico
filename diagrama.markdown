# 1. Diagrama de Arquitetura

```mermaid
flowchart TD
    subgraph SOURCES["Fontes de Dados"]
        S1["sensores_iot.json<br/>(Sensoriamento / IoT)"]
        S2["producao_moinhos.csv<br/>(Sistemas Fabris)"]
        S3["cadastro_fornecedores.csv<br/>(Master Data)"]
        S4["eventos_sap.csv<br/>(ERP Legado SAP ECC)"]
    end

    subgraph BRONZE["Camada Bronze (Raw Ingestion / Append-Only)"]
        B1["bronze.sensores_iot<br/>(Delta Lake - Streaming)"]
        B2["bronze.producao_moinhos<br/>(Delta Lake - Auto Loader Batch)"]
        B3["bronze.cadastro_fornecedores<br/>(Delta Lake - Batch)"]
        B4["bronze.eventos_sap<br/>(Delta Lake - Scheduled Batch)"]
    end

    subgraph SILVER["Camada Silver (Cleansed, Deduplicated & Conformed)"]
        SV1["silver.sensores_iot<br/>(Cleaned & Watermarked)"]
        SV2["silver.producao_moinhos<br/>(Harmonized Schema & Timestamps)"]
        SV3["silver.dim_fornecedores<br/>(SCD Tipo 2 / Historical Tracking)"]
        SV4["silver.eventos_sap<br/>(Validated Financial Events)"]
    end

    subgraph GOLD["Camada Gold (Business Aggregations & Analytical Marts)"]
        G1["gold.dash_operacao_sensores<br/>(Near Real-Time Metrics)"]
        G2["gold.produtividade_plantas<br/>(Daily / Monthly Aggregates)"]
        G3["gold.relatorios_financeiros_sap<br/>(Monthly Financial Closure)"]
        G4["gold.feature_store_data_science<br/>(IoT & Operational Features)"]
    end

    subgraph GOVERNANCE["Governança de Acesso - Unity Catalog"]
        UC1["operacoes_catalog<br/>(Operações & Planta)"]
        UC2["financeiro_catalog<br/>(Financeiro & Auditoria)"]
        UC3["datascience_catalog<br/>(Data Science & ML)"]
    end

    %% Ingestão
    S1 -->|"Structured Streaming<br/>(Trigger AvailableNow/ProcessingTime)"| B1
    S2 -->|"Databricks Auto Loader<br/>(CloudFiles - Incremental Batch)"| B2
    S3 -->|"Auto Loader / Batch Job"| B3
    S4 -->|"Databricks Workflow<br/>(Batch Cron Agendado)"| B4

    %% Bronze para Silver
    B1 -->|"Stream-to-Stream<br/>Watermark + dropDuplicates"| SV1
    B2 -->|"PySpark ETL / Merge<br/>coalesce(planta_id, id_planta)"| SV2
    B3 -->|"Delta MERGE INTO<br/>(SCD Type 2 Pattern)"| SV3
    B4 -->|"Data Quality Enforcement<br/>& Foreign Key Auditing"| SV4

    %% Silver para Gold
    SV1 & SV2 --> G1
    SV2 --> G2
    SV3 & SV4 --> G3
    SV1 & SV2 & SV3 --> G4

    %% Unity Catalog
    G1 & G2 --> UC1
    G3 --> UC2
    G4 & SV1 & SV2 --> UC3