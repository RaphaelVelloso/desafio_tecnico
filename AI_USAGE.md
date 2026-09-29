# Metodologia 

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

## Resumo Técnico — Problemas e Soluções para a PARTE 2

Parte do prompt: Crie um resumo de uma forma tabelar para que eu consiga documentar dentro do README do meu repositorio o problema trazendo as possiveis solucoes e os motivos pelas quais foram sugeridas. A tabela teremos a primeira coluna com o problema, a segunda, a solucao e o terceiro o motivo da solucao

## Diagnóstico e Soluções do Pipeline

### Liste todos os problemas técnicos que você identifica neste código (pelo menos 4).

| Prioridade técnica | Problema                           | Impacto                                                                      |
| ------------------ | ---------------------------------- | ---------------------------------------------------------------------------- |
| **Crítico**        | `df.collect()`                     | Pode causar OOM no Driver e inviabilizar o processamento de grandes volumes. |
| **Crítico**        | Loop Python sobre os registros     | Elimina grande parte do benefício do processamento distribuído do Spark.     |
| **Crítico**        | `mode("overwrite")`                | Pode causar perda dos dados existentes em caso de execução incorreta.        |
| **Crítico**        | Ausência de idempotência           | Reexecuções podem gerar inconsistências ou duplicidades.                     |
| **Crítico**        | SCD2 implementado como `overwrite` | Destrói o histórico da dimensão de fornecedores.                             |
| **Alto**           | Caminhos `/mnt/...` hardcoded      | Prejudica governança, portabilidade e promoção entre ambientes.              |
| **Alto**           | Ausência de schema explícito       | Pode provocar problemas de tipagem e inconsistências na ingestão.            |
| **Alto**           | Ausência de Data Quality           | Dados inválidos podem chegar às camadas analíticas.                          |
| **Alto**           | Ausência de observabilidade        | Dificulta identificar causa, impacto e duração das falhas.                   |
| **Médio/Alto**     | Ausência de quarantine             | Dados inválidos são descartados sem rastreabilidade.                         |
| **Médio/Alto**     | Ausência de testes/versionamento   | Aumenta risco de regressões e dificulta manutenção.                          |

### Reescreva o pipeline de produção aplicando boas práticas: leitura escalável (sem collect()), enforcement de schema, tratamento de erros/logging, e carga incremental/idempotente (não reprocessar o histórico inteiro acada execução).

<a href="https://github.com/RaphaelVelloso/desafio_tecnico/blob/main/src/parte-6/refatoracao_parte2_gpt.py" target="_blank">Refatoracao do codigo apresentado</a>

### Implemente o tratamento de cadastro_fornecedores como uma dimensão de histórico (SCD Tipo 2), preservando as versões anteriores dos registros.

<a href="https://github.com/RaphaelVelloso/desafio_tecnico/blob/main/src/parte-6/implementacao_scd2_gpt.py" target="_blank">Codigo cadastro_fornecedores GPT</a>

Achei bem interessante a maneira como o codigo foi desenvolvido e muito mais simples do que pensei anteriormente

### Explique — em texto ou código — a estratégia que você usaria para deduplicar as leituras de sensores_iot.json considerando que elas chegam fora de ordem.

<a href="https://github.com/RaphaelVelloso/desafio_tecnico/blob/main/src/parte-6/deduplicacao_gpt.py" target="_blank">Deduplicacao GPT</a>

Para sensores_iot.json, eu trataria a deduplicação como um problema de event time + chave do evento + dados chegando fora de ordem, e não simplesmente como um dropDuplicates().

Identificar unicamente o evento pelo sensor_id + event_id (ou sensor_id + event_timestamp, caso não exista event_id), utilizar o event_timestamp como event time, aplicar watermark para limitar o estado mantido pelo Spark e persistir os dados em Delta.