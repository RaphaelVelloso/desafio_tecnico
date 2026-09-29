-- =====================================================================================
-- Parte 3 / Consulta 3 - Qualidade de dados: eventos_sap cujo fornecedor_id nao existe no
--                        cadastro de fornecedores (violacao de integridade referencial),
--                        agrupado por mes de ocorrencia.
--
-- Tabelas: silver.eventos_sap (bukrs, belnr, fornecedor_id, valor, data)
--          silver.dim_fornecedores  (dimensao SCD Tipo 2: fornecedor_id, valid_from, valid_to, is_current)
--          -> e o "cadastro_fornecedores" do enunciado, ja com historico de versoes.
--
-- Decisoes:
--   * "Existe" = a CHAVE aparece em QUALQUER versao da dimensao. Filtrar is_current = true
--     mediria outra coisa (estado de hoje) e trataria como orfao um fornecedor que so tem
--     versoes encerradas.
--   * NOT EXISTS (anti-join): tem semantica clara com NULL. NOT IN retornaria zero linhas se a
--     subconsulta contivesse um NULL.
--   * FK nula NAO e violacao (nao referencia ninguem); e reportada a parte (3b).
--   * A chave e normalizada dos dois lados (conversao ALPHA do SAP: numericos ignoram zeros a
--     esquerda), para que '0000012345' vs '12345' nao apareca como fornecedor inexistente.
--     A Silver ja normaliza; aqui e uma defesa para nao gerar falso positivo.
-- =====================================================================================

-- ---- 3a. Violacoes de integridade referencial (fornecedor inexistente) -----------------------
WITH fornecedores_conhecidos AS (
    SELECT DISTINCT
           CASE WHEN trim(fornecedor_id) RLIKE '^[0-9]+$'
                THEN regexp_replace(trim(fornecedor_id), '^0+', '')
                ELSE upper(trim(fornecedor_id)) END AS chave
    FROM silver.dim_fornecedores
),
eventos AS (
    SELECT bukrs,
           belnr,
           valor,
           fornecedor_id,
           data,
           CASE WHEN trim(fornecedor_id) RLIKE '^[0-9]+$'
                THEN regexp_replace(trim(fornecedor_id), '^0+', '')
                ELSE upper(trim(fornecedor_id)) END AS chave_fornecedor
    FROM silver.eventos_sap
    WHERE fornecedor_id IS NOT NULL AND trim(fornecedor_id) <> ''
)
SELECT trunc(e.data, 'MM')                                        AS mes_ocorrencia,
       COUNT(*)                                                    AS registros_orfaos,
       COUNT(DISTINCT e.bukrs, e.belnr)                            AS documentos_afetados,   -- BELNR so e unico por empresa (e ano fiscal)
       COUNT(DISTINCT e.chave_fornecedor)                          AS fornecedores_inexistentes,
       SUM(e.valor)                                                AS valor_total_afetado,
       slice(sort_array(collect_set(e.fornecedor_id)), 1, 5)       AS exemplos_fornecedor_id
FROM eventos e
WHERE NOT EXISTS (
    SELECT 1
    FROM fornecedores_conhecidos f
    WHERE f.chave = e.chave_fornecedor
)
GROUP BY trunc(e.data, 'MM')
ORDER BY mes_ocorrencia;


-- ---- 3b. Informativo: eventos SEM fornecedor informado (nao e violacao de FK) ----------------
SELECT trunc(data, 'MM')  AS mes_ocorrencia,
       COUNT(*)           AS registros_sem_fornecedor,
       SUM(valor)         AS valor_total
FROM silver.eventos_sap
WHERE fornecedor_id IS NULL OR trim(fornecedor_id) = ''
GROUP BY trunc(data, 'MM')
ORDER BY mes_ocorrencia;


-- ---- 3c. Opcional (validade temporal): fornecedor existe, mas nao estava vigente na data do evento --
-- Atencao: depende de valid_from refletir a vigencia de NEGOCIO. Se a dimensao foi carregada com a
-- data do snapshot como vigencia, todo evento anterior ao primeiro snapshot sera sinalizado.
WITH versoes AS (
    SELECT CASE WHEN trim(fornecedor_id) RLIKE '^[0-9]+$'
                THEN regexp_replace(trim(fornecedor_id), '^0+', '')
                ELSE upper(trim(fornecedor_id)) END AS chave,
           CAST(valid_from AS DATE)                 AS vigencia_ini,   -- intervalo semiaberto [ini, fim)
           CAST(valid_to   AS DATE)                 AS vigencia_fim
    FROM silver.dim_fornecedores
),
eventos AS (
    SELECT bukrs, belnr, valor, data,
           CASE WHEN trim(fornecedor_id) RLIKE '^[0-9]+$'
                THEN regexp_replace(trim(fornecedor_id), '^0+', '')
                ELSE upper(trim(fornecedor_id)) END AS chave_fornecedor
    FROM silver.eventos_sap
    WHERE fornecedor_id IS NOT NULL AND trim(fornecedor_id) <> ''
)
SELECT trunc(e.data, 'MM')            AS mes_ocorrencia,
       COUNT(*)                        AS registros_fora_da_vigencia,
       SUM(e.valor)                    AS valor_total_afetado
FROM eventos e
WHERE EXISTS (SELECT 1 FROM versoes v WHERE v.chave = e.chave_fornecedor)
  AND NOT EXISTS (
        SELECT 1
        FROM versoes v
        WHERE v.chave = e.chave_fornecedor
          AND e.data >= v.vigencia_ini
          AND (v.vigencia_fim IS NULL OR e.data < v.vigencia_fim)
  )
GROUP BY trunc(e.data, 'MM')
ORDER BY mes_ocorrencia;