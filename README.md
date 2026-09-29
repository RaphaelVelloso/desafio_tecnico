# desafio_tecnico

# Projeto NioMetal S.A. - Desafio Técnico

## Parte 1 — Arquitetura Medallion — Plataforma NioMetal S.A.

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

   [Diagrama controle cadastro fornecedor](https://github.com/RaphaelVelloso/desafio_tecnico/blob/main/diagrama_cadastro_fornecedor.markdown)

   [Codigo para cadastro de fornecedor](https://github.com/RaphaelVelloso/desafio_tecnico/blob/main/cadastro_fornecedores.py)

## 4. Estratégia de Deduplicação de Dados Fora de Ordem

### Structured Streaming com Watermarking

   A estratégia ideal em Spark/Databricks para resolver este problema em tempo real (ou em micro-batches contínuos) baseia-se na combinação de dois conceitos: Watermarking e Deduplicação de Estado (Stateful Deduplication).

   [Diagrama watermark](https://github.com/RaphaelVelloso/desafio_tecnico/blob/main/diagrama_iot.md)

### Como Funciona a Mecânica Interna:
#### 1. Janela de Watermark (Tolerância ao Atraso):
   O Watermark estabelece o limite de tempo que o engine do Spark aceita esperar por dados atrasados em relação ao maior timestamp visto até ao momento.
   Exemplo: Com .withWatermark("timestamp", "2 hours"), se o Spark já processou um evento de 14:00, eventos com timestamp anterior a 12:00 serão descartados se chegarem depois.

#### 2. Gerenciamento de Estado (RocksDB/State Store):
   O Spark mantém um registo temporário das chaves únicas de dedup (sensor_id + timestamp) na memória/disco do executor. Quando um evento duplicado chega dentro da janela de 2 horas, o Spark compara-o com o estado e descarta-o.

#### 3. Limpeza Automática de Estado (Garbage Collection):
   Assim que o tempo do Watermark avança, o Spark limpa o estado das chaves mais antigas do que a janela definida, garantindo que a memória não estoure (Out Of Memory), mesmo que o stream rode indefinidamente.

   [Exemplo simplificado deduplicacao](https://github.com/RaphaelVelloso/desafio_tecnico/blob/main/sensores_iot.py)

---

# Parte 3 — SQL avançado

## 1. Moinhos com Maior Queda Percentual de Produção Mês a Mês (Últimos 6 Meses)
   Esta consulta calcula a produção consolidada por mês/moinho, busca o valor do mês anterior através da função de janela LAG(), calcula a variação percentual e identifica os 3 moinhos com a maior queda percentual no período.

## 2. Detecção de Anomalias de Produção (Média Móvel e Desvio Padrão de 7 Dias)
   Esta consulta analisa a série temporal diária por moinho e calcula a média móvel e o desvio padrão dos últimos 7 dias (sem incluir o próprio dia do evento, evitando contaminação do cálculo pelo pico de anomalia).

## 3. Qualidade de Dados: Violação de Integridade Referencial (eventos_sap vs cadastro_fornecedores)
   Esta consulta identifica lançamentos na tabela financeira (silver.eventos_sap) cujos fornecedores não existem na dimensão ativa de fornecedores (silver.cadastro_fornecedores), consolidando a contagem de registros e a volumetria financeira afetada agrupadas por mês de ocorrência.

   [Codigo SQL para os 3 topicos](https://github.com/RaphaelVelloso/desafio_tecnico/blob/main/silver.eventos_sap.sql)

---

# Parte 4 — Troubleshooting e performance

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
### 5.1 Falha no Broadcast Join

   * O enunciado menciona que a tabela cadastro_fornecedores cresceu bastante.

   * Se a tabela superou o limite de broadcast (padrão de 10 MB), o Spark altera a estratégia para SortMergeJoin ou Shuffle Hash Join. Isso introduz uma etapa massiva de Shuffle que não existia anteriormente.

   * Se o otimizador (ou o código) continuou forçando o broadcast() de uma tabela que ficou gigante, a tabela inteira foi enviada do Driver para todos os Executors, causando uso excessivo de memória, picos brutais de GC e gravação excessiva em disco.

### 5.2 Data Skew nas Transformações Wide

   * Raciocínio: O dataset de sensores costuma crescer de forma desigual (ex.: um sensor com defeito enviando milhões de eventos, ou concentração de eventos em um único timestamp ou fornecedor_id).

   * Como o pipeline faz múltiplas transformações wide (operações com Shuffle como groupBy, join ou dropDuplicates), dados agrupados pela mesma chave serão enviados para a mesma partição.

   * Se 99% das tarefas terminam em segundos e 1 tarefa fica travada por horas processando a chave desproporcional, o job inteiro fica retido aguardando essa tarefa.

## 6. Otimizações Concretas e Ações de Mitigação

### 6.1 Problema: Degradação de I/O de armazenamento ou rede (Storage/Network Throttle)

   * Converter arquivos raw para Delta/Parquet: Se o sensores_iot.json é lido em formato texto cru, usar o Auto Loader (cloudFiles) para ingerir em Delta Lake com suporte a file notification.

   * Compactação de arquivos pequenos (Small Files Problem): Executar um OPTIMIZE na tabela de fornecedores para juntar múltiplos arquivos pequenos em arquivos maiores de 1GB, reduzindo as chamadas de API ao storage.

### 6.2 Problema: Degradação de I/O de armazenamento ou rede (Storage/Network Throttle)

   * Reduzir o uso da Heap para cache: Reajustar a fração de memória usada para execução e armazenamento via spark.memory.fraction ou evitar usar .cache() em grandes conjuntos de dados desnecessariamente

### 6.3 Problema: Data Skew 

   * Mitigação Automática com AQE Skew Join: Habilitar o tratamento automático de assimetria do Spark:

      Exemplos de configurações:
      * Habilita a execução adaptativa de consultas
         spark.conf.set("spark.sql.adaptive.enabled", "true")

      * Converte dinamicamente SortMergeJoin em BroadcastJoin se a tabela filtrada for pequena
         spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold.enabled", "true")

      * Trata DATA SKEW automaticamente dividindo partições grandes em sub-partições
         spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
         spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
         spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

      * Consolida partições pequenas de Shuffle automaticamente
         spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

   * Técnica Manual de Salting: Se a chave do groupBy ou join estiver muito concentrada (ex.: um único sensor_id tem 80% das leituras), acrescente uma coluna de "sal" aleatório para quebrar o dado em múltiplas partições antes da operação wide.

### 6.4 Estratégia de Particionamento, Reparticionamento e Caching

   * Evitar repartition() desnecessário: Se o pipeline faz repartition() sem necessidade antes das transformações wide, substitua por coalesce() quando apenas reduzir partições.

   * Ajuste de Partições de Shuffle (spark.sql.shuffle.partitions): O valor padrão (200) pode ser pequeno para o novo volume. Com AQE habilitado, pode-se definir um número inicial maior (ex.: 800), e o Spark reduzirá automaticamente se necessário.

   * Uso consciente de Caching: Se a tabela cadastro_fornecedores ou o dataset intermediário de sensores for reutilizado múltiplas vezes no mesmo DAG, faça o .persist(StorageLevel.MEMORY_AND_DISK) e lembre-se de dar .unpersist() ao final do job.