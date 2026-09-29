# Metodologia 

## 1. Ferramentas usadas

| Ferramenta | Para quê | Onde |
| :-- | :-- | :-- |
| **ChatGPT** | Primeira versão do código da Parte 2 (`refatoracao_parte2_gpt.py`, `implementacao_scd2_gpt.py`, `deduplicacao_gpt.py`) e tabela de problemas do pipeline original | Parte 2 |
| **Claude** (conversa de revisão e reescrita) | Revisão crítica da entrega e proposta de nova versão das Partes 1 a 4 (arquitetura, pipelines PySpark, SQL, troubleshooting) | Partes 1 a 4 |

## 2. Parte escolhida para a primeira versão gerada por IA

**Parte 2 (pipeline PySpark).** Fluxo seguido: (1) a IA gerou uma primeira versão; (2) eu revisei; (3) uma segunda ferramenta fez uma revisão crítica; (4) as diferenças foram registradas na seção 5 e 6.

## 3. Prompts principais

### 3.1 ChatGPT

**Prompt de contexto** (`[PREENCHER: confirme se este foi o prompt enviado ao ChatGPT]`):

```text
Atue como um Engenheiro de Dados Principal / Arquiteto de Dados Specialist em Databricks, Apache Spark, Delta Lake e Soluções em Nuvem (AWS & Azure).

Estou atuando empresa, a NioMetal S.A. Preciso que você contrua junto comigo algumas solucoes para determinados problemas que estou enfrentando.

Temos que ser rigorosos com o detalhamento das resposta explicando o motivo e a causa das possiveis solucoes

### CONTEXTO E CENÁRIO DA NIOMETAL S.A.
Plataforma Lakehouse em Databricks / Unity Catalog gerenciando 4 fontes principais:
1. producao_moinhos (Batch/Bronze)
2. sensores_iot.json (Streaming/Semiestruturado)
3. cadastro_fornecedores (Dimensão/SCD Tipo 2)
4. eventos_sap (Financeiro/Relacional)
```

**Prompt da tabela de problemas:**

```text
Crie um resumo de uma forma tabelar para que eu consiga documentar dentro do README do meu repositorio o problema trazendo as possiveis solucoes e os motivos pelas quais foram sugeridas. A tabela teremos a primeira coluna com o problema, a segunda, a solucao e o terceiro o motivo da solucao
```

| Arquivo | O que sugeriu |
| :-- | :-- |
| `refatoracao_parte2_gpt.py` | Leitura do diretório inteiro com schema explícito (`StructType`) e `FAILFAST`; filtro de nulos e negativos; MERGE por `id_producao` (`whenMatchedUpdateAll` / `whenNotMatchedInsertAll`); criação da tabela por `overwrite` de DataFrame vazio; logging com `try/except` em cada etapa |
| `implementacao_scd2_gpt.py` | Schema explícito, `dropDuplicates` pela chave, hash SHA-256 dos atributos com `coalesce`, e SCD2 em **dois passos**: um MERGE que fecha as versões antigas e um `append` que insere as novas |
| `deduplicacao_gpt.py` | Stream Auto Loader com schema (`sensor_id`, `event_id`, `event_timestamp`, `temperature`, `pressure`), watermark de 30 minutos e `dropDuplicates(["sensor_id", "event_id"])`, com escrita Delta e checkpoint |
| Tabela de problemas (README/AI_USAGE) | 11 problemas do código original com impacto (`collect()`, loop Python, `overwrite`, ausência de idempotência, SCD2 como overwrite, `/mnt` hardcoded, sem schema, sem data quality, sem observabilidade, sem quarentena, sem testes) |

## 4. Erros e suposições incorretas introduzidos pela IA

A coluna **"Verificado em execução?"** deve ser preenchida por você **depois de reproduzir** o comportamento. A coluna "Como verificar" indica um teste rápido para cada item.

### 4.1 Nos artefatos do ChatGPT

| # | Arquivo | Erro ou suposição incorreta | Por que parecia correto | Como verificar | Verificado em execução? |
| :-- | :-- | :-- | :-- | :-- | :-- |
| 1 | `deduplicacao_gpt.py` | `dropDuplicates(["sensor_id","event_id"])` com watermark, mas **sem o event time entre as colunas**. O Spark não consegue expirar o estado; o watermark existe e não limpa as chaves, então o estado cresce indefinidamente | A combinação watermark + `dropDuplicates` é o padrão de deduplicação em streaming, e o job roda e devolve resultado correto em amostras pequenas | Rodar o stream com dados sintéticos contínuos e acompanhar `stateOperators[0].numRowsTotal` no `lastProgress`, com e sem o event time na chave (ou com `dropDuplicatesWithinWatermark`) | ☐ Sim ☐ Não — `[PREENCHER: resultado]` |
| 2 | `deduplicacao_gpt.py` | Schema inventado (`event_id`, `pressure`) e ausência de vibração e consumo elétrico, o que **contradiz o enunciado**; watermark de 30 min sem justificativa; `input_file_name()` sem suporte no Unity Catalog | O código é coerente em si mesmo | Comparar o schema com o enunciado (temperatura, vibração, consumo elétrico, aninhado) | ☐ Sim ☐ Não |
| 3 | `implementacao_scd2_gpt.py` | SCD2 em **dois passos** (MERGE fecha + `append` insere): uma falha entre os dois deixa o fornecedor sem versão corrente; `current_timestamp()` avaliado em duas queries, então `valid_to` da versão antiga ≠ `valid_from` da nova; `dropDuplicates([chave])` mantém uma linha arbitrária; vigência = hora da ingestão, não a do negócio | Cada passo, isolado, está correto, e o código roda de ponta a ponta no caminho feliz | Forçar uma exceção entre o MERGE e o `append` e inspecionar a dimensão; comparar `valid_to` e `valid_from` das versões | ☐ Sim ☐ Não |
| 4 | `refatoracao_parte2_gpt.py` | **Não é incremental**: relê o diretório inteiro a cada execução (o MERGE dá idempotência, não incrementalidade) | O MERGE "parece" tornar a carga incremental | Executar duas vezes e conferir na Spark UI que os mesmos arquivos são lidos de novo | ☐ Sim ☐ Não |
| 5 | `refatoracao_parte2_gpt.py` | Schema explícito em CSV com `enforceSchema=true` (padrão): o Spark mapeia por **posição** e ignora os nomes do header, então a troca `planta_id` → `id_planta` (ou reordenação) passa em silêncio; o schema nem inclui planta | Um schema explícito parece a forma mais segura de garantir os tipos | Ler dois CSVs com colunas em ordem diferente usando o mesmo schema e comparar os valores | ☐ Sim ☐ Não |
| 6 | `refatoracao_parte2_gpt.py` | `TimestampType` + `FAILFAST` não resolvem fusos mistos (UTC e local); o MERGE **falha** com chaves duplicadas no lote (o dataset tem duplicatas); `whenMatchedUpdateAll` sem condição reescreve tudo e pode sobrescrever dado novo por antigo; `nullable=False` é ignorado em arquivos | Cada escolha é uma boa prática isolada | Rodar com um lote contendo chaves duplicadas e um timestamp em fuso diferente | ☐ Sim ☐ Não |
| 7 | `refatoracao_parte2_gpt.py` | Descarta nulos em silêncio, **contradizendo o próprio diagnóstico** ("ausência de quarantine"); mantém `/mnt` hardcoded, que a tabela de problemas critica; chave `id_producao` inventada; execução no import | A lista de problemas e o código foram gerados em respostas diferentes | Comparar a tabela de problemas com o código | ☐ Sim ☐ Não |

### 4.2 Em outros arquivos e textos da entrega

`[PREENCHER: ferramenta que gerou estes arquivos/textos, ou "escritos por mim"]`

| # | Arquivo | Erro ou imprecisão | Como verificar | Verificado em execução? |
| :-- | :-- | :-- | :-- | :-- |
| 8 | `cadastro_fornecedores.py`, `processo_moinhos.py` | `whenMatchedAnd` e `updateExpr` não existem no merge builder Python do Delta (o correto é `whenMatchedUpdate(condition=..., set=...)`); `whenNotMatchedInsert` recebe um dict no lugar do argumento `condition` | `[m for m in dir(DeltaMergeBuilder) if m.startswith("when")]` e rodar a função | ☐ Sim ☐ Não |
| 9 | `cadastro_fornecedores.py` | `concat_ws` ignora NULLs: `("A", NULL, "B")` e `("A", "B", NULL)` geram o mesmo hash e a mudança não é detectada | `SELECT sha2(concat_ws('\|\|','A',NULL,'B'),256) = sha2(concat_ws('\|\|','A','B',NULL),256)` deve retornar `true` | ☐ Sim ☐ Não |
| 10 | `processo_moinhos.py` | O arquivo contém o SCD2 de fornecedores em vez do pipeline de produção e não importa `SparkSession` (usado na anotação de tipo) | Importar o módulo | ☐ Sim ☐ Não |
| 11 | README (Parte 2 e Parte 4) | "leitura lenta" sem schema (sem `inferSchema` não há custo de inferência); `KeyError` em `planta_id` (o código original nunca acessa essa coluna); `overwrite` causa "downtime" (o overwrite do Delta é atômico); configuração `spark.sql.adaptive.autoBroadcastJoinThreshold.enabled` inexistente; configurações padrão do AQE apresentadas como otimização | Conferir a documentação do Spark/Delta | ☐ Sim ☐ Não |

### 4.3 Erros da IA na nova versão (Claude), detectados durante o trabalho

| # | Erro | Como foi detectado | Correção |
| :-- | :-- | :-- | :-- |
| 12 | Nos testes da Parte 3, o mês esperado do dado sintético estava rotulado errado (junho em vez de maio); a **consulta** estava correta, o **teste** não | O teste falhou ao ser executado, e a comparação com a construção dos dados mostrou o erro de indexação | Expectativa corrigida no teste |
| 13 | Na Parte 2, `F.Column` foi usado como anotação de tipo em vez de importar `Column` de `pyspark.sql` | Revisão do código antes de entregar | Import corrigido |

### 4.4 Riscos e suposições **não verificados** na nova versão

- O código das Partes 2 e 3 **não foi executado no Spark** (só verificação de sintaxe Python e teste lógico em SQLite para a Parte 3). `[PREENCHER: atualize após executar]`
- Comportamento do Auto Loader com CSVs cujo header muda entre arquivos (`planta_id` vs `id_planta`).
- Escrita idempotente (`txnAppId` / `txnVersion`) dentro do `foreachBatch`.
- Conflitos de concorrência entre o stream da Silver de sensores e o MERGE de reconciliação.
- Valores padrão de configuração citados na Parte 4 podem variar por versão do Runtime.

## 5. O que mantive, o que mudei e por quê

A coluna **"Decisão final"** é sua. As propostas vêm da revisão e devem ser confirmadas depois de executar o código.

| Tema | ChatGPT propôs | Versão proposta na revisão | Por quê | Decisão final |
| :-- | :-- | :-- | :-- | :-- |
| Estrutura em funções (ler, tratar, carregar) | Funções separadas por etapa | **Manter**, com `Config` e argumentos | Separação clara e testável | `[PREENCHER]` |
| Schema | `StructType` explícito com `FAILFAST` | Bronze em string + `try_cast` + constraints Delta | Evitar mapeamento posicional e falha total por um valor inválido | `[PREENCHER]` |
| Carga | MERGE por `id_producao` sobre a leitura completa | Auto Loader + MERGE por chave de negócio, com dedup no lote e condição por ingestão | Incrementalidade real e idempotência | `[PREENCHER]` |
| Nulos | Descartados em silêncio | Quarentena com motivo | Concordo com o README ("manteria a quarentena") | `[PREENCHER]` |
| Data quality e logging | Dentro do código do pipeline | **Divergência:** o README diz que DQ e observabilidade ficariam fora do pipeline, em camada reutilizável; a nova versão os mantém no código (com `common.py` compartilhado) | Decidir: mover as regras para um módulo/framework de qualidade ou ajustar o texto do README | `[PREENCHER]` |
| SCD2 | Dois passos | MERGE único (chave nula) | Atomicidade e vigência contígua | `[PREENCHER]` |
| Deduplicação de sensores | `dropDuplicates` por `event_id` com watermark de 30 min | `dropDuplicatesWithinWatermark` + reconciliação a partir do Bronze | Estado limitado e sem perda de dado atrasado | `[PREENCHER]` |
| Watermark | 30 min (GPT) / 2 h (versão anterior) | Provisório: dimensionar pelo p99 do atraso medido no Bronze | Sem medição, qualquer valor é chute | `[PREENCHER]` |

## 6. Como validei

| Verificação | Feita? | Evidência |
| :-- | :-- | :-- |
| Sintaxe Python dos arquivos da Parte 2 | Sim (`py_compile`, sem erros) | Executado na revisão |
| Lógica das consultas da Parte 3 (ranking, janelas, anti-join) | Sim, em SQLite equivalente com dados sintéticos e uma referência independente em Python | Executado na revisão; a sintaxe Spark não foi testada |
| Execução do pipeline de produção (Parte 2) no Spark | `[PREENCHER]` | `[PREENCHER: contagens, quarentena]` |
| Execução do SCD2 (Parte 2) e consultas de validação | `[PREENCHER]` | `[PREENCHER]` |
| Stream de deduplicação e `numRowsTotal` (Parte 2) | `[PREENCHER]` | `[PREENCHER]` |
| Execução das 3 consultas SQL no Spark (Parte 3) | `[PREENCHER]` | `[PREENCHER]` |

## 7. Aprendizados

- O código de IA que compila e passa em uma amostra pequena pode falhar em **propriedades operacionais** (estado que cresce, atomicidade, concorrência, incrementalidade) que só aparecem em produção; testar essas propriedades é parte da revisão.
- A IA foi coerente **dentro de cada resposta**, mas as respostas se contradiziam entre si (diagnóstico vs. código, schemas e nomes de coluna diferentes); a consistência entre artefatos precisa ser verificada por quem dirige.
- Um segundo revisor (humano ou outra IA) encontra erros que o autor não vê, e o que ele afirma também precisa ser reproduzido antes de virar conclusão.

---