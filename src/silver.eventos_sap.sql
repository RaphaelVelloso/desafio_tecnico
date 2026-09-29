-- Moinhos com Maior Queda Percentual de Produção Mês a Mês (Últimos 6 Meses)

WITH producao_mensal AS (
    -- 1. Consolida a produção por moinho e por mês nos últimos 6 meses
    SELECT 
        moinho_id,
        DATE_TRUNC('month', CAST(data AS DATE)) AS mes,
        SUM(toneladas_produzidas) AS total_produzido
    FROM silver.producao
    WHERE CAST(data AS DATE) >= ADD_MONTHS(CURRENT_DATE(), -6)
    GROUP BY moinho_id, DATE_TRUNC('month', CAST(data AS DATE))
),
comparativo_mensal AS (
    -- 2. Obtém a produção do mês anterior usando a função de janela LAG()
    SELECT 
        moinho_id,
        mes,
        total_produzido,
        LAG(total_produzido) OVER (
            PARTITION BY moinho_id 
            ORDER BY mes
        ) AS produzido_mes_anterior
    FROM producao_mensal
),
variacao_percentual AS (
    -- 3. Calcula a variação percentual mês a mês
    SELECT 
        moinho_id,
        mes,
        total_produzido,
        produzido_mes_anterior,
        ((total_produzido - produzido_mes_anterior) / produzido_mes_anterior) * 100 AS variacao_pct
    FROM comparativo_mensal
    WHERE produzido_mes_anterior IS NOT NULL AND produzido_mes_anterior > 0
)
-- 4. Seleciona a maior queda percentual (menor valor de variação) por moinho e obtém o TOP 3
SELECT 
    moinho_id,
    MIN(variacao_pct) AS maior_queda_percentual
FROM variacao_percentual
GROUP BY moinho_id
ORDER BY maior_queda_percentual ASC
LIMIT 3;

-- Detecção de Anomalias de Produção (Média Móvel e Desvio Padrão de 7 Dias)

WITH producao_diaria AS (
    -- 1. Agrupa a produção diária por moinho caso haja múltiplos registros no mesmo dia
    SELECT 
        moinho_id,
        CAST(data AS DATE) AS data,
        SUM(toneladas_produzidas) AS total_dia
    FROM silver.producao
    GROUP BY moinho_id, CAST(data AS DATE)
),
estatisticas_móveis AS (
    -- 2. Calcula a Média Móvel e o Desvio Padrão dos últimos 7 dias (excluindo o dia atual)
    SELECT 
        moinho_id,
        data,
        total_dia,
        AVG(total_dia) OVER (
            PARTITION BY moinho_id 
            ORDER BY data 
            RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND INTERVAL 1 DAY PRECEDING
        ) AS media_movel_7d,
        STDDEV(total_dia) OVER (
            PARTITION BY moinho_id 
            ORDER BY data 
            RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND INTERVAL 1 DAY PRECEDING
        ) AS stddev_7d
    FROM producao_diaria
)
-- 3. Filtra apenas as ocorrências em que a produção ultrapassa Média + (2 * Desvio Padrão)
SELECT 
    moinho_id,
    data,
    total_dia AS producao_anomala,
    ROUND(media_movel_7d, 2) AS media_movel_7d,
    ROUND(stddev_7d, 2) AS stddev_7d,
    ROUND(media_movel_7d + (2 * stddev_7d), 2) AS limite_superior
FROM estatisticas_móveis
WHERE media_movel_7d IS NOT NULL 
  AND stddev_7d IS NOT NULL
  AND total_dia > (media_movel_7d + (2 * stddev_7d))
ORDER BY data DESC, moinho_id;

-- Qualidade de Dados: Violação de Integridade Referencial
SELECT 
    DATE_TRUNC('month', CAST(s.data AS DATE)) AS mes_ocorrencia,
    COUNT(s.belnr) AS total_registros_orfaos,
    COUNT(DISTINCT s.fornecedor_id) AS total_fornecedores_inexistentes,
    SUM(s.valor) AS valor_total_orfao
FROM silver.eventos_sap s
LEFT JOIN silver.cadastro_fornecedores f 
    ON s.fornecedor_id = f.fornecedor_id 
    -- Se a tabela de fornecedores for SCD Tipo 2, consideramos o registro ativo no momento
    AND f.is_current = true
WHERE f.fornecedor_id IS NULL
GROUP BY DATE_TRUNC('month', CAST(s.data AS DATE))
ORDER BY mes_ocorrencia DESC;