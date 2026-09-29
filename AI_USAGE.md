A IA foi utilizada no desenvolvimento do meu desafio para me auxiliar em pesquisas de documentacoes, organizacao do texto, elaboracao de raciocinio e validacao de conteudo.

O prompt usado foi: 

Atue como um Engenheiro de Dados Principal / Arquiteto de Dados Specialist em Databricks, Apache Spark, Delta Lake e Soluções em Nuvem (AWS & Azure). 

Estou atuando empresa, a NioMetal S.A. Preciso que você contrua junto comigo algumas solucoes para determinados problemas que estou enfrentando.

Temos que ser rigorosos com o detalhamento das resposta explicando o motivo e a causa das possiveis solucoes

---

### CONTEXTO E CENÁRIO DA NIOMETAL S.A.
Plataforma Lakehouse em Databricks / Unity Catalog gerenciando 4 fontes principais:
1. producao_moinhos (Batch/Bronze)
2. sensores_iot.json (Streaming/Semiestruturado)
3. cadastro_fornecedores (Dimensão/SCD Tipo 2)
4. eventos_sap (Financeiro/Relacional)

---

## Resumo Técnico — Problemas e Soluções

| Problema                                                   | Solução                                                         | Motivo                                                                                   |
| ---------------------------------------------------------- | --------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| Uso de `collect()`                                         | Utilizar transformações distribuídas com Spark                  | Evitar transferência de grandes volumes para o Driver e possíveis erros de Out Of Memory |
| Schema inferido                                            | Definir `StructType` explicitamente                             | Garantir contrato de dados, tipos corretos e maior previsibilidade                       |
| Uso de `overwrite`                                         | Utilizar `MERGE` com Delta Lake                                 | Evitar sobrescrita do histórico e permitir processamento incremental                     |
| Reprocessamento do histórico                               | Utilizar Auto Loader + checkpoints                              | Processar apenas novos arquivos e reduzir I/O, tempo e custo                             |
| Ausência de idempotência                                   | Utilizar chaves de negócio, deduplicação e `MERGE`              | Permitir reexecuções sem gerar registros duplicados                                      |
| Dados inválidos                                            | Implementar validações e uma camada de registros rejeitados     | Evitar que dados inconsistentes avancem para as camadas seguintes sem rastreabilidade    |
| Ausência de tratamento de erros                            | Implementar `try/except`, logging estruturado e `raise`         | Permitir diagnóstico, monitoramento e identificação correta de falhas                    |
| Ausência de observabilidade                                | Registrar métricas de ingestão e execução                       | Monitorar volume, erros, arquivos processados e duração dos jobs                         |
| `createDataFrame()` após `collect()`                       | Manter todo o processamento dentro do Spark                     | Evitar conversões desnecessárias entre objetos Python e DataFrames Spark                 |
| `cadastro_fornecedores` com `overwrite`                    | Implementar SCD Type 2 com Delta Lake                           | Preservar versões anteriores dos fornecedores e permitir análises históricas             |
| Alterações de fornecedores sem histórico                   | Utilizar `valid_from`, `valid_to`, `is_current` e `record_hash` | Identificar versões e determinar qual registro está atualmente vigente                   |
| Eventos IoT fora de ordem                                  | Utilizar `event_time` + watermark                               | Permitir tratamento de eventos atrasados sem depender da ordem de chegada                |
| Duplicidade em sensores IoT                                | Utilizar `event_id` + `dropDuplicates()`                        | Garantir que o mesmo evento não seja processado múltiplas vezes                          |
| Ausência de `event_id` no IoT                              | Criar chave determinística baseada nos atributos do evento      | Permitir deduplicação quando a origem não fornece um identificador único                 |
| Uso de paths `/mnt` como principal mecanismo de governança | Utilizar tabelas Delta governadas pelo Unity Catalog            | Centralizar controle de acesso, governança, lineage e descoberta dos dados               |

### Arquitetura resultante

```text
                           FONTES
                              │
            ┌─────────────────┼─────────────────┐
            │                 │                 │
            ▼                 ▼                 ▼
       PRODUÇÃO             IoT               SAP
         Batch            Streaming         Relacional
            │                 │                 │
            └─────────────────┼─────────────────┘
                              ▼
                         ┌─────────┐
                         │ BRONZE  │
                         └────┬────┘
                              │
                    Raw + Metadata
                              │
                              ▼
                         ┌─────────┐
                         │ SILVER  │
                         └────┬────┘
                              │
             ┌────────────────┼────────────────┐
             │                │                │
             ▼                ▼                ▼
        Validação       Deduplicação        SCD Type 2
        Incremental      Event Time          Histórico
             │                │                │
             └────────────────┼────────────────┘
                              ▼
                         ┌─────────┐
                         │  GOLD   │
                         └────┬────┘
                              │
                              ▼
                    BI / Analytics / ML
```

### Princípios adotados

* **Processamento distribuído:** evitar operações que tragam grandes volumes para o Driver.
* **Schema explícito:** estabelecer contratos de dados na ingestão.
* **Incrementalidade:** processar somente dados novos ou alterados.
* **Idempotência:** permitir reexecução sem duplicação.
* **Delta Lake:** utilizar ACID transactions, `MERGE` e histórico.
* **SCD Type 2:** preservar alterações históricas das dimensões.
* **Event Time:** considerar o momento em que o evento ocorreu, e não somente o momento em que foi processado.
* **Watermark:** controlar eventos atrasados em pipelines de streaming.
* **Data Quality:** validar e rastrear registros inválidos.
* **Observabilidade:** registrar métricas, erros e informações de execução.
* **Governança:** utilizar Unity Catalog para controle de acesso, descoberta e lineage.
* **Reprocessabilidade:** manter dados suficientes para recuperar ou reprocessar uma etapa sem depender novamente da fonte original.
