# desafio_tecnico

# Projeto NioMetal S.A. - Desafio Técnico

# Índice

- [Parte 1 — Arquitetura Medallion — Plataforma NioMetal S.A.](#parte-1--arquitetura-medallion--plataforma-niometal-sa)
- [Parte 2 — Pipeline PySpark](#parte-2--pipeline-pyspark)
- [Parte 3 — SQL Avançado](#parte-3--sql-avançado)
- [Parte 4 — Troubleshooting e Performance](#parte-4--troubleshooting-e-performance)
- [Parte 5 — Integração e nuvem](#parte-5--integração-e-nuvem)
- [Parte 6 — Uso Crítico de Ferramentas de IA](#parte-6--uso-crítico-de-ferramentas-de-ia)
- [Documentação](#documentação)

# Parte 1 — Arquitetura Medallion — Plataforma NioMetal S.A.

<a href="https://github.com/RaphaelVelloso/desafio_tecnico/blob/main/src/parte-1/diagrama_arquitetura.md" target="_blank">Diagrama Arquitetura</a>

```mermaid
flowchart LR
    subgraph SRC["Fontes"]
        S1["sensores_iot.json<br/>JSON aninhado, fluxo continuo"]
        S2["producao_moinhos.csv<br/>lotes diarios / intra-diarios"]
        S3["cadastro_fornecedores.csv<br/>snapshot periodico"]
        S4["eventos_sap.csv<br/>extracao do ERP SAP ECC"]
    end

    LZ[("Landing Zone<br/>External Location<br/>arquivos imutaveis")]

    subgraph BRONZE["BRONZE - append-only, fonte da verdade"]
        B1["prod_operacoes.bronze.sensores_iot"]
        B2["prod_operacoes.bronze.producao_moinhos"]
        B3["prod_financeiro.bronze.cadastro_fornecedores"]
        B4["prod_financeiro.bronze.eventos_sap"]
    end

    subgraph SILVER["SILVER - limpo, tipado, deduplicado"]
        SV1["prod_operacoes.silver.sensores_iot"]
        SV2["prod_operacoes.silver.producao"]
        SV3["prod_financeiro.silver.dim_fornecedores<br/>SCD Tipo 2"]
        SV4["prod_financeiro.silver.eventos_sap"]
        Q[("quarentena<br/>registros rejeitados com motivo")]
    end

    subgraph GOLD["GOLD - visoes de negocio"]
        G1["prod_operacoes.gold.dash_operacao_sensores"]
        G2["prod_operacoes.gold.produtividade_plantas"]
        G3["prod_financeiro.gold.relatorios_financeiros_sap"]
        G4["prod_datascience.feature_store.features_sensores_producao"]
    end

    subgraph CONS["Consumo"]
        C1["Dashboards operacionais"]
        C2["Relatorios financeiros"]
        C3["Data Science / ML"]
    end

    S1 --> LZ
    S2 --> LZ
    S3 --> LZ
    S4 --> LZ

    LZ -->|"Auto Loader<br/>streaming, trigger 1 min"| B1
    LZ -->|"Auto Loader<br/>availableNow"| B2
    LZ -->|"snapshot diario ou semanal"| B3
    LZ -->|"carga diaria + fechamento mensal"| B4

    B1 -->|"dropDuplicatesWithinWatermark"| SV1
    B1 -.->|"reconciliacao diaria<br/>MERGE insert-only"| SV1
    B2 -->|"UTC, coalesce planta, tipos"| SV2
    B3 -->|"AUTO CDC ou MERGE unico<br/>SCD Tipo 2"| SV3
    B4 -->|"chaves normalizadas + FK"| SV4
    SV3 -->|"lookup de fornecedor"| SV4
    B2 -.->|"rejeitados"| Q
    B4 -.->|"orfaos sinalizados"| Q

    SV1 -->|"streaming, janelas 1 a 5 min"| G1
    SV2 --> G2
    SV3 --> G3
    SV4 --> G3
    SV1 --> G4
    SV2 --> G4

    G1 --> C1
    G2 --> C1
    G3 --> C2
    G4 --> C3
```

**Princípios que orientam o desenho:**

1. **Bronze é imutável e é a fonte da verdade.** Tudo que está em Silver e Gold pode ser reconstruído a partir dele, e isso viabiliza o reprocessamento histórico (seção 5).
2. **Silver aplica regras de negócio e qualidade.** Ele garante tipos, fuso horário, deduplicação e chaves normalizadas, e registros rejeitados vão para quarentena com motivo, sem descarte silencioso.
3. **Gold é o contrato com o consumidor.** Consumidores leem visões estáveis, e as tabelas físicas por trás podem ser versionadas.
4. **A propriedade é por domínio.** Operações e Financeiro são donos de seus dados, e Data Science acessa por *grants*, sem copiar dados.


## 2. Estratégia de ingestão: batch vs. streaming

| Fonte | SLA | Modo | Por que essa escolha | Alternativa descartada |
| :-- | :-- | :-- | :-- | :-- |
| `sensores_iot.json` | Minutos | **Streaming** (Auto Loader, `trigger(processingTime="1 minute")`) | O fluxo é contínuo e o SLA é de minutos. O Auto Loader escala a descoberta de arquivos (file notification ou listagem incremental, conforme o volume) e o checkpoint dá semântica exactly-once na escrita em Delta. | `availableNow` agendado a cada N minutos é mais barato, mas a latência passa a ser o intervalo do agendamento. Serve se o SLA real for de 5 a 10 min. |
| `producao_moinhos.csv` | Diário / intra-diário | **Batch incremental** (Auto Loader `availableNow`) | Os dados chegam em lotes de arquivo. O checkpoint do Auto Loader processa só arquivos novos e o cluster fica ligado apenas durante a carga. | Ler o diretório inteiro a cada execução reprocessa o histórico. Streaming contínuo mantém cluster ligado sem ganho de SLA. |
| `cadastro_fornecedores.csv` | Diário / semanal | **Batch agendado** de snapshots | O volume é baixo e as mudanças são raras. O histórico é construído na Silver via SCD2. | Streaming não traz ganho para uma dimensão que muda esporadicamente. |
| `eventos_sap.csv` | Mensal | **Batch agendado**: carga diária incremental + fechamento mensal | O relatório é mensal, mas a carga diária evita um pico no fechamento e antecipa a detecção de fornecedores órfãos. | Streaming: a fonte é uma extração em lote de um ERP legado. |

> **Sobre `AvailableNow` e `ProcessingTime`:** são modos diferentes. `processingTime` mantém uma query contínua (baixa latência). `availableNow` processa o que houver e encerra (batch incremental). O desenho usa cada um onde o SLA pede.

### 2.1 sensores_iot

**Bronze** (`bronze.sensores_iot`):
- Append-only do JSON bruto com metadados (`_ingestion_ts`, `_source_file` via `_metadata.file_path`).
- A coluna `_rescued_data` preserva o que não casa com o schema.
- Usar `cloudFiles.schemaHints` só para tipos críticos, e não um `.schema()` completo. Com schema explícito completo o Auto Loader não infere nem evolui.

**Silver** (`silver.sensores_iot`) com **deduplicação em duas camadas**:
- **Caminho quase em tempo real:** `withWatermark("event_ts", "2 hours")` + `dropDuplicatesWithinWatermark(["sensor_id", "event_ts"])`. O estado é limitado pelo watermark. A variante `dropDuplicatesWithinWatermark` (Spark 3.5+ / DBR 13.3+) não exige o event time na lista de colunas, e sem ele o estado de um `dropDuplicates` comum nunca expiraria.
- **Reconciliação diária a partir do Bronze:** um `MERGE` *insert-only* por `(sensor_id, event_ts)` recupera eventos que chegaram depois do watermark. Isso evita perda silenciosa.
- **Por que os dois:** o stream entrega latência baixa. O Bronze guarda tudo, então a reconciliação garante completude sem precisar de um watermark gigante (que faria o estado crescer).

**Gold:** `dash_operacao_sensores` é uma agregação em streaming com janelas de 1 a 5 min, lendo a Silver como stream. Correções históricas propagam via Change Data Feed ou reprocessamento da janela.

### 2.2 producao_moinhos

**Bronze:** o Auto Loader lê o CSV como string (comportamento padrão para formatos texto), com `schemaEvolutionMode=addNewColumns`. Assim a mudança de schema não derruba a carga por tipagem.

**Silver:**
- Converte `data_producao` para UTC. A regra é: offset ou `Z` no valor indica UTC, e sem offset vale o fuso da planta (tabela de fusos por planta).
- Aplica `planta_id = coalesce(planta_id, id_planta)` e faz os *casts* de tipo.
- Envia nulos em `toneladas_produzidas` e linhas malformadas para a **quarentena**, com motivo e arquivo de origem. Nulo não é zero, então não é convertido nem descartado.
- Deduplica pela chave de negócio `(planta_id, moinho_id, data_producao)`, e vence a última ingestão. O `MERGE` é feito em `foreachBatch`, com deduplicação prévia dentro do lote, pois o `MERGE` falha com chaves duplicadas na origem.

### 2.3 cadastro_fornecedores

**Bronze:** guarda cada snapshot com `snapshot_date`.

**Silver `dim_fornecedores`** (SCD Tipo 2):
- Usa `AUTO CDC ... STORED AS SCD TYPE 2` (antigo `APPLY CHANGES INTO`, em Lakeflow Declarative Pipelines) com `SEQUENCE BY` a data de atualização da fonte. A alternativa é um único `MERGE` com chave nula para inserir a nova versão e fechar a antiga na mesma transação.
- `valid_from`/`valid_to` usam a **vigência de negócio** (data da alteração na fonte), não a data da carga. Isso preserva joins point-in-time com eventos históricos.
- A coluna `dados_bancarios` recebe *column mask* (seção 7).

### 2.4 eventos_sap

**Bronze:** mantém os lançamentos brutos, com metadados de lote.

**Silver:**
- **Chave do documento:** `BUKRS + BELNR + GJAHR`. Apenas `BELNR` não é único.
- **Normalização de chaves:** `MATNR` (18 caracteres no ECC) e `LIFNR` seguem a conversão ALPHA do SAP. Valores numéricos recebem `lpad` com zeros, e valores alfanuméricos ficam como estão. `MATNR` é armazenado como string com espaço para 40 caracteres, para não travar uma futura migração para S/4HANA.
- **Fornecedor órfão:** o evento **permanece na Silver** com `fornecedor_valido = false` e é registrado na quarentena/tabela de DQ. Dados financeiros não podem sumir, porque o total precisa reconciliar com o SAP. O relatório Gold mostra "fornecedor não cadastrado" como linha separada. Quando o fornecedor chegar ao cadastro, os eventos são revalidados.

---

# 3. Formato de armazenamento e particionamento

**Formato:** Delta Lake em todas as camadas, como tabelas gerenciadas do Unity Catalog. O Delta dá transações ACID (leitores nunca veem estado parcial), `MERGE` idempotente, time travel e Change Data Feed. O Unity Catalog com *predictive optimization* automatiza `OPTIMIZE` e `VACUUM` quando habilitada.

**Particionamento:** usar **Liquid Clustering** em vez de partições. O Liquid Clustering **não pode ser combinado com partições nem com `ZORDER` na mesma tabela**. Ele também permite trocar as chaves de clustering sem reescrever a tabela. Partições fixas só se justificariam em tabelas com volume da ordem de 1 TB ou mais e partições com pelo menos 1 GB, o que não é o caso de `eventos_sap` ou `producao`.

| Tabela | Clustering | Motivo |
| :-- | :-- | :-- |
| `bronze.*` | Sem clustering, ou `ingestion_date` | Escrita append e leitura por faixa de ingestão (reprocessamento). |
| `silver.sensores_iot` | `event_date, planta_id, sensor_id` | Consultas por período, planta e sensor. |
| `silver.producao` | `data_producao_date, planta_id` | Consultas por período e planta. Volume moderado, então nada de partição. |
| `silver.dim_fornecedores` | `fornecedor_id` | Tabela pequena, acessada por chave. |
| `silver.eventos_sap` | `ano_mes, bukrs` | Relatórios mensais por empresa. |
| `gold.*` | Chaves de consulta do dashboard/relatório | Otimizado por consumidor. |

**Propriedades recomendadas (Silver):**

```sql
CREATE TABLE prod_operacoes.silver.sensores_iot (
  sensor_id STRING NOT NULL,
  planta_id STRING,
  event_ts TIMESTAMP NOT NULL,
  event_date DATE,
  temperatura DOUBLE,
  vibracao DOUBLE,
  consumo_eletrico DOUBLE,
  ingestion_ts TIMESTAMP,
  source_file STRING
)
CLUSTER BY (event_date, planta_id, sensor_id)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.columnMapping.mode' = 'name',
  'delta.deletedFileRetentionDuration' = 'interval 30 days'
);
```

- `NOT NULL` e `CHECK` constraints dão enforcement real no Delta. O `nullable=False` de um schema Spark é ignorado em fontes de arquivo.
- Change Data Feed permite propagar correções para o Gold.
- Column mapping habilita renomear ou remover colunas sem reescrever dados, e exige upgrade de protocolo (consumidores precisam de leitores compatíveis).
- Ampliar `deletedFileRetentionDuration` (padrão de 7 dias) amplia a janela de time travel e de `RESTORE`.

---

## 4. Schema evolution (`planta_id` → `id_planta`)

1. **Bronze absorve a mudança.** O Auto Loader usa `schemaLocation` e `addNewColumns`, com todas as colunas como string. Quando `id_planta` aparece, o stream **falha uma vez** (`UnknownFieldException`) e, ao reiniciar, segue com o schema evoluído. Por isso o Job precisa de **retry** (pelo menos 1), para não exigir intervenção manual. Tipos incompatíveis vão para `_rescued_data`.
2. **Silver normaliza.** `planta_id = coalesce(planta_id, id_planta)` e depois descarta `id_planta`. O Bronze permanece fiel à origem, e a Silver mantém um contrato único, então os consumidores nunca veem a mudança.
3. **Mudanças controladas do nosso lado** (renomear uma coluna da Silver, por exemplo) usam *column mapping* (`ALTER TABLE ... RENAME COLUMN`), que é uma operação de metadados.
4. **Detecção:** todo evento de evolução de schema gera alerta, e um teste de contrato no CI valida o schema esperado da Silver.

---

## 5. Reprocessamento histórico sem downtime

**Base técnica:**
- O Bronze é imutável e a Silver é reconstruível de forma determinística e idempotente (`MERGE` por chave de negócio).
- O Delta dá isolamento por snapshot: quem está lendo continua vendo a versão anterior até o commit.
- Consumidores (BI, Data Science, Financeiro) leem **views estáveis**, e as tabelas físicas por trás podem ser versionadas.

```mermaid
flowchart LR
    BR[("Bronze imutavel<br/>fonte da verdade")] --> JOB["Job de backfill<br/>compute isolado<br/>mesmo codigo, parametrizado por periodo"]
    JOB --> V2["tabela fisica v2 - shadow"]
    LIVE["Stream paralelo em v2<br/>startingVersion = ponto de corte"] --> V2
    V2 --> VAL{"Validacao<br/>contagens, somas, DQ<br/>comparacao com v1"}
    VAL -->|"reprovado"| FIX["Corrige e refaz<br/>consumidores nao afetados"]
    VAL -->|"aprovado"| SW["CREATE OR REPLACE VIEW<br/>troca atomica da view de servico"]
    SW --> CONS["Consumidores<br/>leem sempre a view"]
    SW -.->|"rollback: reaponta a view"| V1["tabela fisica v1<br/>mantida por N dias"]
```

| Cenário | Estratégia | Por que não causa downtime |
| :-- | :-- | :-- |
| **A. Correção de uma janela** (poucos dias) | `MERGE` ou `replaceWhere` **na própria tabela**, lendo do Bronze. | Commit atômico com isolamento por snapshot. Downstream em streaming deve usar Change Data Feed ou `skipChangeCommits`, porque commits de overwrite/update quebram uma leitura em streaming que espera só appends. |
| **B. Mudança de lógica com reconstrução total** | *Blue/green*: constrói a tabela `v2` em paralelo e valida contra `v1`. Depois faz `CREATE OR REPLACE VIEW` para apontar a view de serviço para `v2`. | A troca da view é uma operação de metadados atômica. Os consumidores nunca deixam de ter dados. |
| **C. Idem, com stream no meio da cadeia** (Silver → Gold em streaming) | Reconstrói Silver `v2` e Gold `v2` **em paralelo**, cada um com seu checkpoint. Um stream ao vivo escreve em `v2` a partir do ponto de corte (`startingVersion` ou `startingTimestamp` do Bronze). A troca de view acontece na **borda de consumo (Gold)**. | Streaming não lê de views, então o swap fica na borda e não entre Silver e Gold. |
| **D. Rollback** | Reapontar a view para `v1`, ou `RESTORE TABLE ... TO VERSION AS OF`. | O rollback via time travel só funciona dentro da retenção configurada (`deletedFileRetentionDuration`, `logRetentionDuration`). |

**Cuidados:**
- **Compute isolado:** o backfill roda em cluster/serverless próprio para não competir com os workloads de SLA.
- **Validação antes do swap:** comparar contagem de linhas, somas de `toneladas_produzidas` e `valor`, e resultados de DQ entre `v1` e `v2`.
- **Retenção da landing zone:** os arquivos brutos devem ficar retidos por pelo menos o horizonte de reprocessamento aceito (política de lifecycle).
- **Sem reset do checkpoint do stream ao vivo:** o backfill usa checkpoint próprio.

---

## 6. Volumetria crescente

- **Ingestão:** o Auto Loader (com file notification em alto volume) escala a descoberta de arquivos sem listar o diretório inteiro.
- **Layout:** Liquid Clustering e *predictive optimization* mantêm o layout sem particionar demais. Otimizar a escrita (`optimizeWrite`, compactação automática) reduz small files.
- **Compute:** compute separado por workload (streaming de sensores, batch de produção, backfill). O batch usa `availableNow` para pagar só pelo tempo de execução.
- **Custo de armazenamento:** política de lifecycle move o raw antigo para camadas mais baratas.
- **Monitoramento:** acompanhar número de arquivos e tamanho médio por tabela para detectar degradação de layout.

---

## 7. Governança de acesso (Unity Catalog)

**Estrutura:**
- **Um metastore** por região, com workspaces por ambiente.
- **Catálogo por domínio e ambiente:** `prod_operacoes`, `prod_financeiro`, `prod_datascience` (e equivalentes `dev_` e `stg_`).
- **Schemas por camada:** `bronze`, `silver`, `gold`, mais `quarentena` e `governance` (funções de máscara e filtros).
- **Sem cópia de dados entre domínios.** Data Science recebe *grants* sobre Silver e Gold de Operações, em vez de ter uma segunda cópia da Silver.
- **Grupos sincronizados do IdP:** `grp_engenharia_dados`, `grp_operacoes`, `grp_financeiro`, `grp_datascience`. Os jobs rodam como **service principal**, não como usuário pessoal.
- **Ownership por grupo**, não por pessoa.

**Matriz de acesso (mínimo privilégio):**

| Schema | Engenharia (service principal) | Operações | Financeiro | Data Science |
| :-- | :-- | :-- | :-- | :-- |
| `prod_operacoes.bronze` | MODIFY | — | — | — |
| `prod_operacoes.silver` | MODIFY | SELECT | — | SELECT |
| `prod_operacoes.gold` | MODIFY | SELECT | — | SELECT |
| `prod_financeiro.bronze` | MODIFY | — | — | — |
| `prod_financeiro.silver` | MODIFY | — | SELECT | — (via view sem dados sensíveis, sob aprovação) |
| `prod_financeiro.gold` | MODIFY | — | SELECT | — |
| `prod_datascience.*` | — | SELECT nos resultados publicados | — | ALL PRIVILEGES |

**Por que o Bronze fica fechado:** ele contém dados brutos, com duplicatas, fusos misturados e valores não validados. Expô-lo a usuários de negócio gera análises erradas e amplia a superfície de dados sensíveis.

**Segurança fina:**

```sql
-- Column mask: dados bancarios so aparecem para auditoria financeira
CREATE OR REPLACE FUNCTION prod_financeiro.governance.mask_dados_bancarios(v STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('grp_financeiro_auditoria') THEN v ELSE '****' END;

ALTER TABLE prod_financeiro.silver.dim_fornecedores
  ALTER COLUMN dados_bancarios SET MASK prod_financeiro.governance.mask_dados_bancarios;

-- Row filter: usuario operacional enxerga apenas as plantas do seu grupo
CREATE OR REPLACE FUNCTION prod_operacoes.governance.filtro_planta(p STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('grp_operacoes_global')
    OR is_account_group_member(CONCAT('grp_planta_', p));

ALTER TABLE prod_operacoes.silver.producao SET ROW FILTER prod_operacoes.governance.filtro_planta ON (planta_id);
```

- **Armazenamento:** o acesso a arquivos é feito só por *external locations* e *storage credentials* do Unity Catalog. Nenhum usuário acessa o storage diretamente, e a landing zone é lida apenas pelo service principal de ingestão.
- **Classificação:** tags de sensibilidade (`dados_bancarios`, `financeiro`) apoiam auditoria e políticas.
- **Auditoria e linhagem:** as system tables (`system.access.audit`, `system.access.table_lineage`, `system.access.column_lineage`) e o histórico do Delta sustentam a auditoria da dimensão de fornecedores, que o enunciado destaca.

---

## 8. Qualidade de dados e observabilidade

- **Regras de qualidade** (nulos, faixas, chaves, integridade referencial) declaradas como *expectations* (Lakeflow Declarative Pipelines) ou em módulo reutilizável, com política por regra: `warn`, `drop para quarentena` ou `fail`.
- **Quarentena:** cada registro rejeitado guarda o payload, o motivo, o arquivo de origem e o timestamp, o que permite análise e reprocessamento.
- **Frescor e SLA:** alerta quando `max(ingestion_ts)` de uma tabela ultrapassa o SLA da fonte. Job com falha, duração acima do esperado e evento de evolução de schema também alertam.
- **Orquestração:** Databricks Workflows (Lakeflow Jobs) com retries, dependências e notificações.

---

<small><a href="#indice">⬆️ Voltar ao topo</a></small>

---

# Parte 2 — Pipeline PySpark

## 1. Problemas Técnicos Identificados no Código Original

   | Arquivo | Conteúdo |
   | :-- | :-- |
   | `common.py` | Logging estruturado, contrato de schema, retry de evolução de schema |
   | `pipeline_producao_moinhos.py` | Landing → Bronze → Silver de produção, com quarentena |
   | `scd2_fornecedores.py` | Dimensão de fornecedores em SCD Tipo 2 |
   | `dedup_sensores_iot.py` | Deduplicação dos sensores (stream + reconciliação) |

   | # | Problema | Causa e impacto | Como o código novo resolve |
   | :-- | :-- | :-- | :-- |
   | 1 | `df.collect()` | Traz **todas** as linhas para a memória do driver. Com volume real causa OOM e anula o processamento distribuído. | Nenhuma ação traz dados ao driver; tudo é DataFrame/streaming. |
   | 2 | Loop `for` em Python para filtrar | Executa linha a linha em um único processo. O que um `filter` distribuído faz em paralelo vira trabalho sequencial. | Filtros e validações são expressões Spark (`F.when`, `F.filter`). |
   | 3 | `spark.createDataFrame(resultado)` | Reenvia os dados do driver ao cluster e **reinfere o schema** a partir de objetos Python. Com lista vazia falha ("can not infer schema from empty dataset"). | O DataFrame nunca sai do Spark; os tipos vêm de `try_cast` explícito. |
   | 4 | Leitura sem schema | `csv(header=True)` sem `inferSchema` lê **tudo como string**, então `toneladas_produzidas` nunca é validada como número. Os nomes de coluna vêm de um único header. | Bronze em string (não falha por tipagem); Silver com tipos via `try_cast`, contrato de colunas e constraints Delta (ver seção 2). |
   | 5 | `mode('overwrite')` a cada execução | Reescreve a tabela inteira (custo crescente) e **não é incremental**. Se o raw for rotacionado, o histórico some. Commits de overwrite também quebram consumidores em streaming que esperam só appends. Obs.: o overwrite do Delta é atômico, então não causa downtime. | Auto Loader processa só arquivos novos; MERGE idempotente por chave de negócio. |
   | 6 | Sem deduplicação | O dataset traz linhas duplicadas e horários em fusos diferentes; ambos passam direto para a Silver. | Normalização para UTC e deduplicação por `(planta, moinho, instante UTC)`. |
   | 7 | Descarte silencioso de nulos | Registros sem `toneladas_produzidas` somem sem rastro: sem contagem, sem motivo, sem como reprocessar. | Quarentena com payload, motivos, arquivo de origem e timestamp. |
   | 8 | Sem tratamento de erros nem logging | Falha de leitura ou escrita não deixa evidência e não há métrica para alertar. | Log JSON por micro-batch e exceção propagada (o Job falha e aciona retry/alerta). |
   | 9 | Caminhos `/mnt/...` hardcoded e escrita por path | Montagens DBFS não são governadas pelo Unity Catalog (sem lineage, sem permissões finas) e o código não é promovível entre ambientes. | Volumes/tabelas do Unity Catalog e parâmetros do Job. |
   | 10 | Raw → Silver direto, sem Bronze | Sem uma cópia imutável do dado original, corrigir uma regra exige reler o raw (que pode ter sido rotacionado). | Landing → Bronze append-only → Silver. |
   | 11 | `carregar_fornecedores` com `overwrite` | Destrói o histórico de mudanças (endereço, status, dados bancários), que o enunciado diz ser necessário para auditoria. | SCD Tipo 2 (seção 3). |
   | 12 | Funções sem parâmetros e dependentes de `spark` global | Não é testável nem reutilizável. | `Config` e argumentos do Job; funções puras que recebem `spark`/DataFrame. |




## 2. Pipeline Refatorado de Produção (`producao_moinhos.csv`)

```mermaid
flowchart LR
    L[("Landing<br/>CSV")] -->|"Auto Loader<br/>availableNow"| B["Bronze<br/>append-only, tudo string"]
    B -->|"stream + foreachBatch"| N["Normaliza<br/>planta_id, UTC, tipos"]
    N -->|"valido"| D["Deduplica<br/>planta + moinho + instante UTC"]
    N -->|"rejeitado"| Q[("Quarentena<br/>payload + motivos")]
    D -->|"MERGE idempotente"| S["Silver producao"]
```
### Como cada requisito do enunciado é atendido

| Requisito | Implementação | Por quê |
| :-- | :-- | :-- |
| **Leitura escalável (sem `collect()`)** | Auto Loader no Bronze; Delta como fonte de streaming no Silver. | O processamento fica distribuído e o volume de dados não é limitado pela memória do driver. |
| **Enforcement de schema** | (1) `assert_required_columns` falha rápido se o Bronze perder colunas essenciais; (2) `try_cast` para `TIMESTAMP` e `DECIMAL(18,3)` — valor inválido vira `NULL` e a linha vai para a quarentena; (3) `NOT NULL` e `CHECK (toneladas_produzidas >= 0)` na tabela Silver. | Não usei `spark.read.schema(...)` em CSV porque, com `enforceSchema=true` (padrão), o Spark ignora os nomes do header e mapeia por **posição**: a mudança `planta_id` → `id_planta` (ou qualquer reordenação) passaria em silêncio. O `nullable=False` de um schema Spark também é ignorado em fontes de arquivo; o enforcement real está nas constraints do Delta. |
| **Tratamento de erros e logging** | `log_event` em JSON por micro-batch (lidos, rejeitados, duplicados removidos, segundos); `logger.exception` + `raise`. | Propagar o erro faz o Job falhar, acionar retry e alerta. O checkpoint garante que nada se perde, e o MERGE é idempotente, então reprocessar é seguro. |
| **Carga incremental** | Checkpoint do Auto Loader (arquivos) e do stream Delta (offsets do Bronze). | Cada execução processa só o que é novo. |
| **Idempotência** | MERGE por `(planta_id, moinho_id, data_producao_utc)`; `whenMatchedUpdateAll` só se `s._ingestion_ts > t._ingestion_ts`; escrita idempotente da quarentena (`txnAppId` + `txnVersion`). | Reexecutar o mesmo lote ou reprocessar um arquivo não duplica linhas nem sobrescreve dado mais novo por um mais antigo. |

### Decisões e trade-offs

- **`foreachBatch` + MERGE** em vez de um MERGE em cima da leitura completa do Bronze. O checkpoint entrega só o novo, e o MERGE por chave torna a reexecução de um lote segura (o `foreachBatch` tem semântica at-least-once).
- **Sessão em UTC** (`spark.sql.session.timeZone = UTC`): o parse de strings sem offset passa a ser "literal", e a conversão de horário local para UTC fica explícita em `to_utc_timestamp`. Assim o resultado não depende da configuração do cluster.
- **Deduplicar depois de normalizar para UTC:** a mesma leitura escrita em UTC num arquivo e em horário local em outro só é reconhecida como duplicata quando ambas estão no mesmo fuso.
- **Custo:** `persist()` do lote e algumas ações (`count`) por micro-batch. É aceitável para observabilidade e evita recomputar a normalização.
- **Limitação conhecida:** `txnVersion` usa o `batch_id`, que reinicia se o checkpoint for recriado. Após recriar o checkpoint, use um novo `txnAppId`.

### Premissas (ver registro de suposições no README)

- A mudança `planta_id` → `id_planta` ocorre **entre arquivos**.
- Timestamp com offset ou `Z` é UTC; sem offset é horário local da planta (padrão `America/Sao_Paulo`, configurável por planta).
- Ponto decimal `.` em `toneladas_produzidas`.
- A chave de negócio é `(planta_id, moinho_id, instante)`; a coluna de data se chama `data_producao`.

---

## 3. Dimensão de fornecedores — SCD Tipo 2

### Modelo

| Coluna | Papel |
| :-- | :-- |
| `fornecedor_id` | Chave de negócio |
| `nome`, `endereco`, `status_contratual`, `dados_bancarios` | Atributos versionados |
| `attribute_hash` | SHA-256 do JSON dos atributos (detecta mudança sem comparar coluna a coluna) |
| `valid_from` / `valid_to` | Vigência em intervalo **semiaberto** `[valid_from, valid_to)`; `valid_to` é `NULL` na versão corrente |
| `is_current` | Facilita consultas à versão atual |

Exemplo: o fornecedor F1 muda de endereço em 2026-03-01.

| fornecedor_id | endereco | valid_from | valid_to | is_current |
| :-- | :-- | :-- | :-- | :-- |
| F1 | Rua A, 10 | 2025-01-10 | 2026-03-01 | false |
| F1 | Rua B, 20 | 2026-03-01 | NULL | true |

### Algoritmo (um único MERGE)

1. Limpa os atributos (`trim`, vazio vira `NULL`) e calcula `attribute_hash`.
2. Mantém **uma linha por fornecedor** no snapshot (a mais recente).
3. Identifica os fornecedores **alterados**: existem como corrente, o hash difere e a vigência da fonte é maior que a `valid_from` atual.
4. Monta o lote do MERGE: todos os fornecedores com `merge_key = fornecedor_id` **mais** uma segunda cópia dos alterados com `merge_key = NULL`.
5. `MERGE ... ON t.fornecedor_id = s.merge_key AND t.is_current = true`:
   - **Casou e mudou:** fecha a versão (`valid_to = vigência nova`, `is_current = false`).
   - **Não casou:** insere. Isso cobre fornecedor novo (`merge_key = id`) e a nova versão dos alterados (`merge_key = NULL`, que nunca casa).

**Por que um único MERGE?** Fechar e inserir na mesma transação Delta evita o estado "fornecedor sem versão corrente" que uma falha entre dois comandos deixaria, e os intervalos ficam contíguos porque a `valid_to` antiga é igual à `valid_from` nova.

**Por que o hash usa `to_json(struct(...))`?** `concat_ws` ignora NULLs, então `("A", NULL, "B")` e `("A", "B", NULL)` gerariam o mesmo texto e a mudança passaria despercebida. O JSON preserva o nome de cada campo. O sal opcional (`--salt`, vindo de secret scope) dificulta reverter o hash por força bruta.

**Idempotência:** reexecutar o mesmo snapshot não muda nada, porque o hash é igual ao da versão corrente. Um snapshot atrasado não regride a dimensão, por causa da guarda `vigencia_ts > valid_from`.

**Ordem:** se um micro-batch tiver mais de um snapshot, eles são aplicados em ordem cronológica, um MERGE por snapshot, para preservar as versões intermediárias.

### Validações (rodar após cada carga)

```sql
-- 1. Exatamente uma versão corrente por fornecedor (deve retornar 0 linhas)
SELECT fornecedor_id
FROM prod_financeiro.silver.dim_fornecedores
GROUP BY fornecedor_id
HAVING SUM(CAST(is_current AS INT)) <> 1;

-- 2. Sem sobreposição nem buraco entre versões (deve retornar 0 linhas)
SELECT fornecedor_id, valid_from, valid_to, prox_from
FROM (
  SELECT fornecedor_id, valid_from, valid_to,
         LEAD(valid_from) OVER (PARTITION BY fornecedor_id ORDER BY valid_from) AS prox_from
  FROM prod_financeiro.silver.dim_fornecedores
)
WHERE prox_from IS NOT NULL AND valid_to <> prox_from;
```

### Limitações e decisões em aberto

- **Remoções:** um fornecedor que some do snapshot não é encerrado. Tratar isso exige decidir se o arquivo é um retrato completo e como representar "inativo" (extensão possível: `whenNotMatchedBySourceUpdate`).
- **Vigência de negócio:** vem da coluna `data_atualizacao`. Sem ela, usa a data do snapshot, o que perde precisão se houver várias mudanças entre snapshots.
- **Chave substituta (surrogate key):** não incluí uma coluna `IDENTITY`, pois ela restringe transações concorrentes. Se o modelo dimensional exigir, pode ser adicionada.
- **`dados_bancarios`:** fica em claro na Silver e é protegido por *column mask* do Unity Catalog (ver Parte 1).

---

## 4. Deduplicação das leituras dos sensores (fora de ordem)

### O problema

As leituras chegam fora de ordem (*late-arriving*) e algumas se repetem por falha de rede. Deduplicar não exige ordenar os eventos, exige **reconhecer que dois registros são a mesma leitura** sem guardar estado para sempre.

### Estratégia

1. **Identidade da leitura:** `(sensor_id, event_ts)` ou, se a fonte trouxer um identificador, `(sensor_id, event_id)`. O `event_ts` é o **event time** (quando a leitura ocorreu), não o horário de chegada.
2. **Watermark sobre o event time:** `withWatermark("event_ts", "2 hours")` limita o estado do Spark. O Spark só guarda chaves dentro da janela de atraso aceita.
3. **`dropDuplicatesWithinWatermark`:** remove duplicatas dentro do watermark e expira o estado junto com ele. Um `dropDuplicates` comum que **não** inclua o event time na chave manteria o estado para sempre (crescimento ilimitado); com o event time na chave o estado é limpo, e a variante `WithinWatermark` dispensa essa exigência.
4. **Bronze guarda tudo:** o stream descarta leituras mais atrasadas que o watermark, o que seria perda silenciosa. Por isso o Bronze é append-only e completo.
5. **Reconciliação periódica:** um `MERGE` *insert-only* a partir do Bronze insere na Silver o que faltar (leituras atrasadas demais para o stream), sem alterar o que já existe.

```mermaid
flowchart LR
    B[("Bronze<br/>tudo, append-only")] -->|"stream 1 min<br/>watermark + dropDuplicatesWithinWatermark"| S["Silver sensores"]
    B -->|"reconciliacao periodica<br/>MERGE insert-only"| S
```

### Alternativas consideradas

| Alternativa | Vantagem | Desvantagem | Decisão |
| :-- | :-- | :-- | :-- |
| `dropDuplicates` sem watermark | Simples | Estado cresce sem limite; o job degrada e eventualmente falha | Descartada |
| Watermark grande (dias) | Cobre quase todo atraso | Estado enorme, alto custo e checkpoint pesado | Descartada |
| Só MERGE em cada micro-batch | Sem estado; aceita qualquer atraso | MERGE a cada minuto é caro e concorre com o stream | Usado só na reconciliação |
| **Stream com watermark + reconciliação** | Baixa latência **e** completude | Duas rotas para manter | **Escolhida** |

### Como dimensionar o watermark

Meça o atraso real no Bronze e escolha um percentil alto (p99) como ponto de partida:

```sql
SELECT percentile_approx(
         unix_timestamp(_ingestion_ts) - unix_timestamp(CAST(`timestamp` AS TIMESTAMP)),
         array(0.5, 0.95, 0.99, 0.999)) AS atraso_segundos
FROM prod_operacoes.bronze.sensores_iot;
```

O valor de 2 h no código é **provisório** até essa medição. Se o p99 for de poucos minutos, um watermark de 15 a 30 minutos reduz estado e custo.

### Como monitorar

No `lastProgress` do stream, acompanhe `stateOperators[0].numRowsTotal` (o estado deve se estabilizar) e `numRowsDroppedByWatermark` (leituras que a reconciliação terá de recuperar).

### Limitações

- Duas leituras com a mesma chave e **valores diferentes** (falha do sensor) ficam com a primeira ingerida. Se isso importar, a regra de desempate precisa ser definida com o negócio.
- Linhas sem `sensor_id` ou sem timestamp permanecem no Bronze e não chegam à Silver. Em produção, devem gerar métrica e alerta.
- A reconciliação e o stream escrevem na mesma tabela; conflitos de concorrência são esperados e tratados com nova tentativa.

---

# Parte 3 — SQL Avançado

## 1. Moinhos com Maior Queda Percentual de Produção Mês a Mês (Últimos 6 Meses)
   Esta consulta calcula a produção consolidada por mês/moinho, busca o valor do mês anterior através da função de janela LAG(), calcula a variação percentual e identifica os 3 moinhos com a maior queda percentual no período.

## 2. Detecção de Anomalias de Produção (Média Móvel e Desvio Padrão de 7 Dias)
   Esta consulta analisa a série temporal diária por moinho e calcula a média móvel e o desvio padrão dos últimos 7 dias (sem incluir o próprio dia do evento, evitando contaminação do cálculo pelo pico de anomalia).

## 3. Qualidade de Dados: Violação de Integridade Referencial (eventos_sap vs cadastro_fornecedores)
   Esta consulta identifica lançamentos na tabela financeira (silver.eventos_sap) cujos fornecedores não existem na dimensão ativa de fornecedores (silver.cadastro_fornecedores), consolidando a contagem de registros e a volumetria financeira afetada agrupadas por mês de ocorrência.

   <a href="https://github.com/RaphaelVelloso/desafio_tecnico/blob/main/src/parte-3/silver.eventos_sap.sql" target="_blank">Codigo SQL para os 3 topicos</a>

<small><a href="#indice">⬆️ Voltar ao topo</a></small>

---

# Parte 4 — Troubleshooting e Performance

   Ao investigar uma degradação severa sem alteração de código, a investigação deve ir do nível macro (infra/recursos) para o nível micro (execução de DAG/código)

## 1. Análise das Métricas Globais do Cluster
   * Verifique se o autoscaling escalou até o limite máximo (8 workers).

   * Cheque se houve degradação na rede, I/O de armazenamento ou gargalo de CPU/Memória nos nós.

## 2. Executors - Spark UI
   * Identificar se os executores estão gastando muito tempo em GC Pauses.

   * Verificar se há Spill de memória/disco elevados, o que indica que os dados não cabem na RAM durante as operações wide

## 3. Jobs / Stages - Spark UI
   * Localizar qual Stage exato está consumindo a maior parte das 4 horas.

   * Analisar o gráfico de barras de duração das tasks dentro do Stage:

      * Diagnóstico de Data Skew (uma ou poucas tarefas processando quase tudo)

      * Falta de paralelismo, problema de I/O ou estouro de memória no Driver/Executors.

## 4. Análise do Plano de Execução

   * Verificar se o Spark tentou realizar o Broadcast Join com uma tabela que ficou grande demais, forçando troca para SortMergeJoin ou causando Driver Out-Of-Memory (OOM)

## 5. Hipoteses para a degradação
### 5.1. Falha no Broadcast Join

   * O enunciado menciona que a tabela cadastro_fornecedores cresceu bastante.

   * Se a tabela superou o limite de broadcast (padrão de 10 MB), o Spark altera a estratégia para SortMergeJoin ou Shuffle Hash Join. Isso introduz uma etapa massiva de Shuffle que não existia anteriormente.

   * Se o otimizador (ou o código) continuou forçando o broadcast() de uma tabela que ficou gigante, a tabela inteira foi enviada do Driver para todos os Executors, causando uso excessivo de memória, picos brutais de GC e gravação excessiva em disco.

### 5.2. Data Skew nas Transformações Wide

   * Raciocínio: O dataset de sensores costuma crescer de forma desigual (ex.: um sensor com defeito enviando milhões de eventos, ou concentração de eventos em um único timestamp ou fornecedor_id).

   * Como o pipeline faz múltiplas transformações wide (operações com Shuffle como groupBy, join ou dropDuplicates), dados agrupados pela mesma chave serão enviados para a mesma partição.

   * Se 99% das tarefas terminam em segundos e 1 tarefa fica travada por horas processando a chave desproporcional, o job inteiro fica retido aguardando essa tarefa.

## 6. Otimizações Concretas e Ações de Mitigação

### 6.1. Problema: Degradação de I/O de armazenamento ou rede (Storage/Network Throttle)

   * Converter arquivos raw para Delta/Parquet: Se o sensores_iot.json é lido em formato texto cru, usar o Auto Loader (cloudFiles) para ingerir em Delta Lake com suporte a file notification.

   * Compactação de arquivos pequenos (Small Files Problem): Executar um OPTIMIZE na tabela de fornecedores para juntar múltiplos arquivos pequenos em arquivos maiores de 1GB, reduzindo as chamadas de API ao storage.

### 6.2. Problema: Degradação de I/O de armazenamento ou rede (Storage/Network Throttle)

   * Reduzir o uso da Heap para cache: Reajustar a fração de memória usada para execução e armazenamento via spark.memory.fraction ou evitar usar .cache() em grandes conjuntos de dados desnecessariamente

### 6.3. Problema: Data Skew 

   * Mitigação Automática com AQE Skew Join: Habilitar o tratamento automático de assimetria do Spark:

      Exemplos de configurações:
      * Habilita a execução adaptativa de consultas:
         spark.conf.set("spark.sql.adaptive.enabled", "true")

      * Converte dinamicamente SortMergeJoin em BroadcastJoin se a tabela filtrada for pequena:
         spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold.enabled", "true")

      * Trata DATA SKEW automaticamente dividindo partições grandes em sub-partições:
         spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
         spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
         spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

      * Consolida partições pequenas de Shuffle automaticamente:
         spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

   * Técnica Manual de Salting: Se a chave do groupBy ou join estiver muito concentrada (ex.: um único sensor_id tem 80% das leituras), acrescente uma coluna de "sal" aleatório para quebrar o dado em múltiplas partições antes da operação wide.

### 6.4. Estratégia de Particionamento, Reparticionamento e Caching

   * Evitar repartition() desnecessário: Se o pipeline faz repartition() sem necessidade antes das transformações wide, substitua por coalesce() quando apenas reduzir partições.

   * Ajuste de Partições de Shuffle (spark.sql.shuffle.partitions): O valor padrão (200) pode ser pequeno para o novo volume. Com AQE habilitado, pode-se definir um número inicial maior (ex.: 800), e o Spark reduzirá automaticamente se necessário.

   * Uso consciente de Caching: Se a tabela cadastro_fornecedores ou o dataset intermediário de sensores for reutilizado múltiplas vezes no mesmo DAG, faça o .persist(StorageLevel.MEMORY_AND_DISK) e lembre-se de dar .unpersist() ao final do job.

<small><a href="#indice">⬆️ Voltar ao topo</a></small>

---

# Parte 5 — Integração e nuvem

## 1. Autenticação e Gestão de Segredos

Para garantir segurança total e conformidade com boas práticas, nenhuma credencial ou chave privada fica salva no código ou em variáveis de ambiente abertas

* Gestão de Segredos (AWS Secrets Manager / Azure Key Vault):

   * Credenciais de origem (chaves SSH, tokens API REST, senhas SFTP) são armazenadas no AWS Secrets Manager ou no Azure Key Vault, aproveitando a infraestrutura onde o pipeline de borda é executado.

   * A rotação de chaves e tokens ocorre de forma automatizada via rotinas nativas de cada serviço de segredos

* Acesso por Identity Federation (IAM Roles / Managed Identities):

   * O motor de execução não utiliza usuários ou chaves de API estáticas.

   * Por meio de Federated Identity Credential / Workload Identity (conectando uma AWS IAM Role a uma Azure Managed Identity), os componentes de computação autenticam-se entre as nuvens com tokens temporários de curta duração (Princípio do Menor Privilégio)

   * Em tempo de execução, a função de ingestão recupera o segredo do cofre via SDK e estabelece a conexão segura com o fornecedor externo.

## 2. Estratégia de Retry e Idempotência

Falhas de rede ou interrupções parciais de download não podem corromper o ambiente nem gerar duplicidades.

### 2.1. Mecanismo de Retry

   * Backoff Exponencial com Jitter: Se a conexão SFTP/REST falhar ou o servidor de origem estiver indisponível, a execução realiza até 3 a 5 tentativas, aumentando progressivamente o tempo de espera (ex.: 1min, 4min, 16min) com uma variação aleatória (jitter) para evitar sobrecarregar o servidor remoto.

   * Se todas as tentativas falharem, a mensagem de controle é enviada para uma fila de falhas, acionando o time de engenharia sem interromper o restante da esteira de dados.

### 2.2. Idempotência e Tratamento de Falhas Parciais

   * Aterrissagem Atômica e Carga Silver Idempotente:
      * Os arquivos do fornecedor são baixados para o Data Lake em estrutura de partições lógicas:
   
      * O download é realizado primeiramente em arquivo temporário (.tmp). O arquivo só é renomeado para a extensão final após a validação bem-sucedida do MD5 Checksum.
   
   * Processamento Idempotente no Delta Lake:

      * O motor de processamento lê a camada Raw e grava na camada Silver via MERGE INTO no Delta Lake, ou substituição atômica de partição (mode("overwrite") com replaceWhere = "data_ingestao = 'YYYY-MM-DD'").
      
      * Reprocessar o mesmo arquivo N vezes gera rigorosamente o mesmo resultado final na tabela, sem duplicar registros.

## 3. Observabilidade e Monitoramento Cruzado

O monitoramento passivo garante que o time de dados seja notificado proativamente sobre falhas ou atrasos na entrega.

   * Alertas Dinâmicos de Falha:

      * Logs estruturados em formato JSON gerados na ingestão são transmitidos em tempo real para o Amazon CloudWatch Logs e Azure Log Analytics.

      * Exceções e erros não tratados disparam alarmes (CloudWatch Alarms / Azure Monitor Alerts) que roteiam notificações unificadas via Amazon SNS / Azure Action Groups diretamente para canais do Slack, Microsoft Teams ou ferramentas de Pager.
   
   * Detecção de Atraso

      * Um gatilho agendado (EventBridge Scheduled Rule ou Azure Logic Apps Trigger) executa no horário limite (ex.: 07:15 AM).

      * A rotina consulta diretamente os metadados do storage. Se nenhum novo arquivo for detectado na partição do dia, é emitida uma notificação "Arquivo do Fornecedor Não Entregue", por exemplo.

## 4. Controle e Otimização de Custos (FinOps)

Para evitar gastos computacionais desnecessários em dias sem arquivos novos ou finais de semana:

   * Ingestão Serverless sem Custo Ocioso:

      * O download inicial utiliza event trigger.

      * Em dias sem arquivo novo ou finais de semana, a execução é encerrada em segundos, mantendo o custo computacional em centavos de dólar por mês.
   
   * Orquestração Orientada a Eventos no Lakehouse:

      * O salvamento do arquivo na Landing Zone dispara uma notificação de evento (S3 Event -> SQS ou Azure Event Grid -> Queue).

      * O processamento no Lakehouse é acionado de forma reativa através da opção Trigger.AvailableNow (ou Trigger.Once). O cluster de processamento sobe sob demanda, processa o dado da camada Raw para a Silver e desliga automaticamente por inatividade.

   * Gestão de Ciclo de Vida do Storage:

      * Políticas integradas de retenção movem os dados da camada Raw para armazenamento de arquivamento após 30 dias, programando o expurgo definitivo após 1 ano.

<small><a href="#indice">⬆️ Voltar ao topo</a></small>

---

# Parte 6 — Uso Crítico de Ferramentas de IA

## Escolha pelo menos uma das Partes 2 a 5 e use uma ferramenta de IA (Copilot, ChatGPT, Databricks Assistant/Genie ou similar) para gerar uma primeira versão da solução antes de você revisar e ajustar.
   * IA — ChatGPT

## Crie um arquivo AI_USAGE.md contendo: o(s) prompt(s) principais usados; um resumo do que a IA sugeriu; o que você manteve, o que você mudou e por quê; e qualquer erro ou suposição incorreta que a IA tenha introduzido.

   * <a href="https://github.com/RaphaelVelloso/desafio_tecnico/blob/main/AI_USAGE.md" target="_blank">AI_USAGE</a>

   A parte escolhida para o desenvolvimento das competências foi a 2 dentro do desafio.

   O prompt inicial está dentro do arquivo.

   Partindo do começo, eu manteria parcialmente as escolhas feitas pelo Chat. Eu não colocaria Data Quality e observabilidade diretamente dentro do código do pipeline, pois isso dificultaria a escalabilidade e poderia gerar duplicação de lógica entre diferentes datasets. Para Data Quality, utilizaria uma camada ou framework reutilizável, com regras, métricas e tratamento dos registros inválidos.

   Para observabilidade, utilizaria uma solução externa de monitoramento e logging, permitindo acompanhar execução, volumetria, falhas e SLA sem acoplar essas responsabilidades ao código.

   Eu manteria a quarentena mencionada, pois os registros inválidos não deveriam ser simplesmente descartados. Seria importante preservá-los com informações de rastreabilidade, como arquivo de origem, timestamp e motivo da inconsistência, permitindo análises e possíveis reprocessamentos.

   Por fim, manteria testes e versionamento como parte do processo de desenvolvimento e CI/CD, e não necessariamente como responsabilidades do código de ingestão.

## Responda de forma dissertativa (10-15 linhas): descreva uma situação — neste desafio ou na sua experiência — em que uma sugestão de IA parecia correta à primeira vista, mas continha um erro sutil (técnico ou de lógica). Como você percebeu o erro e o que isso te ensinou sobre validar código gerado por IA?

Em um projeto recente, no qual construí um lineage técnico para migração de plataformas, utilizei uma REST API da Microsoft para capturar informações sobre datasets, dataflows, relatórios, colunas e seus relacionamentos, com o objetivo de mapear o fluxo de dados end-to-end, ou seja, da origem ate o consumo.
Para esse desenvolvimento, precisei trabalhar com grafos para representar os relacionamentos up e downstream e utilizei IA generativa como suporte na construção de algumas soluções.
O código gerado parecia estar correto e o adaptei ao meu contexto, mas, durante as validações, identifiquei um problema sutil na cardinalidade dos relacionamentos.
Alguns relacionamentos estavam sendo tratadas como muitos-para-muitos, gerando um produto cartesiano e, consequentemente, uma volumetria de dados muito maior que a esperada.
Também identifiquei problemas em cenários com ciclos dentro desses downstream datasets e dataflows,pois um consumia dado de outro e esse segundo consumia dado de um terceiro, que por sua vez consumia dado do primeiro.
Percebi os erros ao comparar a cardinalidade e a volumetria esperadas com os resultados gerados e ao validar manualmente alguns caminhos do grafo.
Isso me ensinou que um código gerado por IA pode estar sintaticamente correto e produzir resultados aparentemente plausíveis, mas ainda conter erros de lógica.
A principal lição foi que a IA deve ser utilizada como ferramenta de apoio, e não como substituta da validação técnica.
Passei a validar não apenas se o código executa, mas também suas premissas, cardinalidade, casos de borda, volumetria e resultados esperados.
Também entendi que prompts mais precisos ajudam a reduzir ambiguidades, mas não substituem o conhecimento técnico e a validação do resultado.

<small><a href="#indice">⬆️ Voltar ao topo</a></small>

---

# Documentação

## Apache Spark & Databricks Architecture

   * [Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
   * [Tuning Spark](https://spark.apache.org/docs/latest/tuning.html)
   * [Adaptive query execution](https://docs.databricks.com/aws/en/optimizations/aqe)
   * [Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader)
   * [Delta Lake](https://docs.databricks.com/aws/en/delta)
   * [Optimize data](https://docs.databricks.com/aws/en/tables/operations/optimize)

## Segurança, Gestão de Segredos
   * [Azure Key Vault](https://learn.microsoft.com/en-us/azure/key-vault/general/overview)
   * [Azure resources](https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/overview)
   * [IAM Roles e Políticas de Menor Privilégio](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html)

## Padrões de Ingestão, Eventos e Armazenamento
   * [Azure Data Factory / Synapse Pipelines](https://learn.microsoft.com/en-us/azure/data-factory/connector-sftp?tabs=data-factory)
   * [Event Grid](https://learn.microsoft.com/en-us/azure/event-grid/event-schema-blob-storage?tabs=cloud-event-schema)
   * [MERGE INTO e Gravação Idempotente](https://docs.delta.io/delta-update/)

## Observabilidade, Monitoramento e Alertas
   * [Azure Monitor](https://learn.microsoft.com/en-us/azure/azure-monitor/fundamentals/overview)
   * [Alertas de Métrica e Regras de Agendamento no Azure Monitor](https://learn.microsoft.com/en-us/azure/azure-monitor/alerts/alerts-overview)
   * [EventBridge](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-what-is.html)

<small><a href="#indice">⬆️ Voltar ao topo</a></small>
