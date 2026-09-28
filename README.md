# desafio_tecnico

# Projeto NioMetal S.A. - Desafio Técnico

## Arquitetura Medallion — Plataforma NioMetal S.A.

| Camada | Fonte / Dataset | SLA / Frequência | Estratégia de Ingestão | Formato & Particionamento | Tratamento & Schema Evolution | Governança (Unity Catalog) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Bronze** (Raw) | `sensores_iot.json` | Near Real-Time | Streaming via Auto Loader (`cloudFiles`) | Delta Lake<br/>*Sem particionamento* | Ingestão append-only dos JSONs brutos com `schemaEvolutionMode = "addNewColumns"`. | `datascience_catalog` (Read)<br/>`operacoes_catalog` (Read) |
| **Bronze** (Raw) | `producao_moinhos.csv` | Diário / Intra-diário | Incremental Batch via Auto Loader | Delta Lake<br/>*Sem particionamento* | Suporta alteração de schema (`planta_id` / `id_planta`) usando `mergeSchema = true`. | `operacoes_catalog` (Read) |
| **Bronze** (Raw) | `cadastro_fornecedores.csv` | Diário / Semanal | Batch (Snapshot Incremental) | Delta Lake<br/>*Sem particionamento* | Ingestão full ou incremental do cadastro mestre bruto. | `financeiro_catalog` (Read) |
| **Bronze** (Raw) | `eventos_sap.csv` | Mensal (Fechamento) | Scheduled Batch via Workflows | Delta Lake<br/>*Sem particionamento* | Ingestão dos lançamentos brutos vindos do ERP legado (SAP ECC-like). | `financeiro_catalog` (Read) |
| **Silver** (Conformed) | `sensores_iot` | Minutos (Streaming) | Stream-to-Stream Processing | Delta Lake<br/>Partição: `data` (`YYYY-MM-DD`)<br/>*Z-Order / Liquid Clustering por `sensor_id`, `planta_id`* | Limpeza, parsing do JSON, deduplicação com `watermark` + `dropDuplicates(["sensor_id", "timestamp"])`. | `operacoes_catalog.silver`<br/>`datascience_catalog.silver` |
| **Silver** (Conformed) | `producao_moinhos` | Diário / Intra-diário | Batch Incremental / Append | Delta Lake<br/>Partição: `ano_mes` | Filtro de nulos em `toneladas_produzidas`, conversão de fuso para UTC, e harmonização de schema via `coalesce(planta_id, id_planta)`. | `operacoes_catalog.silver`<br/>`datascience_catalog.silver` |
| **Silver** (Conformed) | `dim_fornecedores` | Diário / Semanal | Batch MERGE INTO | Delta Lake<br/>*Sem particionamento* | Implementação de **SCD Tipo 2** para preservação do histórico de alterações (`is_current`, `start_date`, `end_date`). | `financeiro_catalog.silver` |
| **Silver** (Conformed) | `eventos_sap` | Mensal | Batch Incremental | Delta Lake<br/>Partição: `ano_mes` | Validação de integridade referencial com fornecedores, padronização de chaves (`bukrs`, `belnr`, `matnr`). | `financeiro_catalog.silver` |
| **Gold** (Business) | `dash_operacao_sensores` | Near Real-Time | Streaming / Batch Agregado | Delta Lake<br/>*Liquid Clustering: `planta_id`* | Métricas consolidadas de telemetria e alertas operacionais para a planta. | `operacoes_catalog.gold` (Full) |
| **Gold** (Business) | `produtividade_plantas` | Diário / Mensal | Batch Agregado | Delta Lake<br/>*Liquid Clustering: `planta_id`, `mes`* | Indicadores consolidados de toneladas produzidas por moinho/planta para tomada de decisão. | `operacoes_catalog.gold` (Full) |
| **Gold** (Business) | `relatorios_financeiros_sap` | Mensal | Scheduled Batch | Delta Lake<br/>Partição: `ano_mes` | Relatórios consolidados de fechamento contábil e auditoria financeira. | `financeiro_catalog.gold` (Full) |
| **Gold** (Business) | `feature_store_ds` | Semanal / Sob Demanda | Batch ETL | Delta Lake<br/>*Liquid Clustering: `sensor_id`, `planta_id`* | Matriz de features consolidadas para modelos preditivos e Machine Learning. | `datascience_catalog.feature_store` |

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