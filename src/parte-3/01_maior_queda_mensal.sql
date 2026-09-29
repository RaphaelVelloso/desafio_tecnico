-- =====================================================================================
-- Parte 3 / Consulta 1 - Os 3 moinhos com maior queda percentual de producao mes a mes,
--                        considerando os ultimos 6 meses COMPLETOS.
--
-- Tabela: silver.producao (planta_id, moinho_id, data DATE, toneladas_produzidas)
--
-- Interpretacao: para cada moinho, pega-se a PIOR queda mensal ocorrida na janela; depois
-- ranqueiam-se os moinhos por essa queda e retornam-se os 3 primeiros.
-- Um moinho e identificado por (planta_id, moinho_id): o mesmo moinho_id pode existir em plantas
-- diferentes.
-- =====================================================================================
WITH parametros AS (
    -- O mes corrente esta incompleto: compara-lo com um mes cheio geraria "quedas" falsas.
    -- Por isso a janela termina no ultimo mes completo.
    SELECT trunc(current_date(), 'MM') AS mes_corrente
),
calendario AS (
    -- 7 meses completos: 1 mes-base (para o LAG do primeiro mes avaliado) + 6 meses avaliados.
    SELECT explode(
               sequence(add_months(mes_corrente, -7), add_months(mes_corrente, -1), INTERVAL 1 MONTH)
           ) AS mes
    FROM parametros
),
moinhos AS (
    SELECT DISTINCT p.planta_id, p.moinho_id
    FROM silver.producao p
    CROSS JOIN parametros
    WHERE p.data >= add_months(mes_corrente, -7) AND p.data < mes_corrente
),
producao_mensal AS (
    SELECT p.planta_id,
           p.moinho_id,
           trunc(p.data, 'MM') AS mes,
           CAST(SUM(p.toneladas_produzidas) AS DOUBLE) AS toneladas
    FROM silver.producao p
    CROSS JOIN parametros
    WHERE p.data >= add_months(mes_corrente, -7) AND p.data < mes_corrente
    GROUP BY p.planta_id, p.moinho_id, trunc(p.data, 'MM')
),
serie AS (
    -- Serie mensal SEM buracos: um moinho sem nenhum registro em um mes conta como producao 0
    -- (parado). Sem este calendario, o LAG compararia com o ultimo mes que tem linha e
    -- esconderia a parada. Mes com registros, mas todos nulos, permanece NULL (desconhecido).
    SELECT m.planta_id,
           m.moinho_id,
           c.mes,
           CASE WHEN pm.mes IS NULL THEN CAST(0 AS DOUBLE) ELSE pm.toneladas END AS toneladas
    FROM moinhos m
    CROSS JOIN calendario c
    LEFT JOIN producao_mensal pm
           ON pm.planta_id = m.planta_id
          AND pm.moinho_id = m.moinho_id
          AND pm.mes = c.mes
),
variacao AS (
    SELECT planta_id,
           moinho_id,
           mes,
           toneladas,
           LAG(toneladas) OVER (PARTITION BY planta_id, moinho_id ORDER BY mes) AS toneladas_mes_anterior
    FROM serie
),
quedas AS (
    SELECT v.planta_id,
           v.moinho_id,
           v.mes,
           v.toneladas_mes_anterior,
           v.toneladas,
           (v.toneladas_mes_anterior - v.toneladas) / v.toneladas_mes_anterior * 100 AS queda_pct
    FROM variacao v
    CROSS JOIN parametros
    WHERE v.mes >= add_months(mes_corrente, -6)          -- so os 6 meses avaliados (o mes-base fica de fora)
      AND v.toneladas_mes_anterior > 0                   -- evita divisao por zero
      AND v.toneladas IS NOT NULL                        -- mes desconhecido nao e queda
      AND v.toneladas < v.toneladas_mes_anterior         -- so quedas
),
pior_mes_por_moinho AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY planta_id, moinho_id
               ORDER BY queda_pct DESC, mes DESC
           ) AS rn_moinho
    FROM quedas
),
ranking AS (
    -- ROW_NUMBER com desempate deterministico: retorna exatamente 3 moinhos.
    -- Para incluir empates no 3o lugar, troque por RANK().
    SELECT *,
           ROW_NUMBER() OVER (ORDER BY queda_pct DESC, planta_id, moinho_id) AS posicao
    FROM pior_mes_por_moinho
    WHERE rn_moinho = 1
)
SELECT posicao,
       planta_id,
       moinho_id,
       mes                          AS mes_da_queda,
       toneladas_mes_anterior,
       toneladas                    AS toneladas_no_mes,
       ROUND(queda_pct, 2)          AS queda_pct
FROM ranking
WHERE posicao <= 3
ORDER BY posicao;
