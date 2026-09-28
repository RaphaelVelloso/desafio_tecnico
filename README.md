# desafio_tecnico

# Projeto NioMetal S.A. - Desafio Técnico

## Parte 1 - Arquitetura Medallion — Plataforma NioMetal S.A.

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

---

# Parte 2 — Pipeline PySpark

## 1. Problemas Técnicos Identificados no Código Original

1. **Gargalo de Memória e Risco de Out-Of-Memory (`df.collect()`)**:  
   O método `collect()` traz todos os dados distribuídos do cluster para a memória do *Driver Node*. Em volumes reais de produção, isso gera um alto gargalo de I/O de rede e causa exceções de *Out Of Memory* (OOM), anulando o poder de processamento distribuído do Spark.
2. **Processamento Iterativo Não Distribuído (Loop `for` em Python)**:  
   Iterar linha a linha (`for linha in dados`) força a execução sequencial na CPU do *Driver Node*. As operações do PySpark devem ser aplicadas de forma vetorial e distribuída entre os *Executors*.
3. **Ausência de Enforcement de Schema e Leitura Lenta**:  
   A leitura sem a definição de um `schema` explícito exige que o Spark infira os tipos de dados ou leia tudo como *String*, tornando a ingestão lenta e propensa a falhas de tipagem na conversão.
4. **Operação Não Idempotente (`mode('overwrite')`)**:  
   Sobreescrever a tabela Silver inteira a cada execução apaga o histórico de dados, causa *downtime* para os consumidores da camada Gold/Dashboards durante a carga e gera custo computacional desnecessário.
5. **Incapacidade de Tratar Mudança de Schema**:  
   O código original ignora a transição da coluna `planta_id` para `id_planta` ao longo do arquivo, o que lança erros de chave (`KeyError`) ao tentar acessar `linha['toneladas_produzidas']`.

   ## 2. Pipeline Refatorado de Produção (`producao_moinhos.csv`)

   [Codigo producao moinhos](https://github.com/RaphaelVelloso/desafio_tecnico/blob/main/processo_moinhos.py)

   ## 3. Dimensão de Histórico SCD Tipo 2

   

   ```mermaid
flowchart TD
    A["1. Lê fornecedores do RAW<br/>(csv / landing path)"] --> B["2. Calcula HASH SHA-256<br/>dos atributos (endereço, status, dados bancários)"]
    
    B --> C{"3. A tabela Silver<br/>já existe?"}
    
    C -->|Não| D["4. Cria primeira versão (Bootstrap)<br/>• is_current = true<br/>• start_date = data_atual<br/>• end_date = null"]
    
    C -->|Sim| E["5. Compara com registros atuais<br/>(is_current = true)"]
    
    E --> F{"6. Detecta alterações<br/>pelo HASH?"}
    
    F -->|Não| G["Nenhuma ação necessária<br/>(Manter registro atual)"]
    
    F -->|Sim| H["Duplica chaves de Join<br/>(Null Key Trick / Staging)"]
    
    H --> I["7. Executa Delta MERGE INTO:<br/>Inativa a versão antiga<br/>• is_current = false<br/>• end_date = data_atual"]
    
    I --> J["8. Insere a nova versão do registro<br/>• is_current = true<br/>• start_date = data_atual<br/>• end_date = null"]
    
    D --> K[("9. Mantém histórico preservado<br/>no Delta Lake (SCD Tipo 2)")]
    G --> K
    J --> K