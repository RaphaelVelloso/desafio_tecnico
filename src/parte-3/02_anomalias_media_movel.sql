-- =====================================================================================
-- Parte 3 / Consulta 2 - Deteccao de anomalias: dias em que a producao de um moinho se
--                        afasta mais de 2 desvios-padrao da media movel dos 7 dias anteriores.
--
-- Tabela: silver.producao (planta_id, moinho_id, data DATE, toneladas_produzidas)
-- A tabela so tem producao; para "consumo", aplique o mesmo padrao trocando a metrica/tabela.
--
-- Decisoes:
--   * A janela EXCLUI o proprio dia: se o pico entrasse na media e no desvio, ele "esconderia" a si mesmo.
--   * A janela e em DIAS CORRIDOS (RANGE sobre o numero do dia), nao em "7 linhas": se faltarem
--     dias na serie, ROWS abrangeria mais de 7 dias de calendario e distorceria a media.
--   * Anomalia nos dois sentidos (ACIMA e ABAIXO): uma queda brusca de producao e tao relevante
--     quanto um pico. Para a leitura literal "ultrapassa", filtre tipo_anomalia = 'ACIMA'.
--   * Guardas: minimo de dias na janela e desvio > 0 (desvio 0 tornaria qualquer variacao
--     minima "anomala"; com poucos pontos o desvio e apenas ruido).
-- =====================================================================================
WITH parametros AS (
    SELECT CAST(2.0 AS DOUBLE) AS n_desvios,
           5                   AS min_dias_janela
),
producao_diaria AS (
    -- Consolida varios registros no mesmo dia (turnos, reprocessamentos ja deduplicados na Silver).
    SELECT planta_id,
           moinho_id,
           data,
           datediff(data, DATE'1970-01-01')            AS dia_num,
           CAST(SUM(toneladas_produzidas) AS DOUBLE)   AS total_dia
    FROM silver.producao
    GROUP BY planta_id, moinho_id, data
),
estatisticas AS (
    SELECT planta_id,
           moinho_id,
           data,
           total_dia,
           AVG(total_dia)         OVER w AS media_7d,
           STDDEV_SAMP(total_dia) OVER w AS desvio_7d,
           COUNT(total_dia)       OVER w AS dias_na_janela
    FROM producao_diaria
    WINDOW w AS (
        PARTITION BY planta_id, moinho_id
        ORDER BY dia_num
        RANGE BETWEEN 7 PRECEDING AND 1 PRECEDING
    )
)
SELECT e.planta_id,
       e.moinho_id,
       e.data,
       ROUND(e.total_dia, 2)                                        AS producao_dia,
       ROUND(e.media_7d, 2)                                         AS media_7d,
       ROUND(e.desvio_7d, 2)                                        AS desvio_7d,
       ROUND(e.media_7d - p.n_desvios * e.desvio_7d, 2)             AS limite_inferior,
       ROUND(e.media_7d + p.n_desvios * e.desvio_7d, 2)             AS limite_superior,
       ROUND((e.total_dia - e.media_7d) / e.desvio_7d, 2)           AS z_score,
       CASE WHEN e.total_dia > e.media_7d THEN 'ACIMA' ELSE 'ABAIXO' END AS tipo_anomalia
FROM estatisticas e
CROSS JOIN parametros p
WHERE e.dias_na_janela >= p.min_dias_janela
  AND e.desvio_7d > 0
  AND ABS(e.total_dia - e.media_7d) > p.n_desvios * e.desvio_7d
ORDER BY e.data DESC, e.planta_id, e.moinho_id;
