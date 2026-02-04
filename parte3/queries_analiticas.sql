-- ============================================
-- QUERIES ANALÍTICAS - PARTE 3.4
-- ============================================

USE ans_despesas;

-- ============================================
-- QUERY 1: Top 5 operadoras com maior crescimento percentual
-- ============================================
-- IMPORTANTE: Dados da ANS são ACUMULADOS no ano (1T, 2T, 3T)
-- Por isso, para calcular crescimento real, precisamos comparar trimestres consecutivos
-- Crescimento = (Valor_3T - Valor_2T) / (Valor_2T - Valor_1T) 
-- Ou seja: despesas do último trimestre vs despesas do penúltimo

-- Desafio: Operadoras podem não ter dados em todos os trimestres
-- Solução: Calcular crescimento entre trimestres consecutivos disponíveis
-- Trade-off: Exclui operadoras que iniciaram/encerraram operações no meio do período

WITH trimestres_ordenados AS (
    SELECT 
        cnpj,
        razao_social,
        ano,
        trimestre,
        valor_despesas,
        ROW_NUMBER() OVER (PARTITION BY cnpj ORDER BY ano, trimestre) AS num_trimestre,
        LAG(valor_despesas, 1) OVER (PARTITION BY cnpj ORDER BY ano, trimestre) AS acumulado_anterior,
        LAG(valor_despesas, 2) OVER (PARTITION BY cnpj ORDER BY ano, trimestre) AS acumulado_2_atras
    FROM despesas_consolidadas
    WHERE valor_despesas > 0
),
crescimentos AS (
    SELECT 
        cnpj,
        razao_social,
        ano,
        trimestre,
        valor_despesas AS acumulado_atual,
        acumulado_anterior,
        -- Despesa do período atual (diferença entre acumulados)
        (valor_despesas - COALESCE(acumulado_anterior, 0)) AS despesa_periodo_atual,
        -- Despesa do período anterior (diferença entre acumulados anteriores)
        (COALESCE(acumulado_anterior, 0) - COALESCE(acumulado_2_atras, 0)) AS despesa_periodo_anterior,
        -- Crescimento percentual comparando despesas de períodos (não acumulados)
        CASE 
            WHEN (COALESCE(acumulado_anterior, 0) - COALESCE(acumulado_2_atras, 0)) > 0
            THEN (
                ((valor_despesas - COALESCE(acumulado_anterior, 0)) - (COALESCE(acumulado_anterior, 0) - COALESCE(acumulado_2_atras, 0)))
                / (COALESCE(acumulado_anterior, 0) - COALESCE(acumulado_2_atras, 0))
            ) * 100
            ELSE NULL
        END AS crescimento_percentual
    FROM trimestres_ordenados
    WHERE num_trimestre > 1  -- Apenas trimestres que têm um anterior para comparar
)
SELECT 
    cnpj,
    razao_social,
    CONCAT(ano, '-Q', trimestre) AS trimestre_atual,
    ROUND(despesa_periodo_atual, 2) AS despesa_do_trimestre,
    ROUND(crescimento_percentual, 2) AS crescimento_vs_trimestre_anterior
FROM crescimentos
WHERE crescimento_percentual IS NOT NULL
  AND despesa_periodo_anterior > 0  -- Garantir que existe período anterior válido
ORDER BY crescimento_percentual DESC
LIMIT 5;

-- ============================================
-- QUERY 2: Distribuição de despesas por UF
-- ============================================

SELECT 
    o.uf,
    COUNT(DISTINCT d.cnpj) AS total_operadoras,
    SUM(d.valor_despesas) AS despesas_totais,
    ROUND(AVG(d.valor_despesas), 2) AS media_por_operadora,
    ROUND(SUM(d.valor_despesas) / COUNT(DISTINCT d.cnpj), 2) AS media_real_por_operadora
FROM despesas_consolidadas d
INNER JOIN operadoras o ON d.cnpj = o.cnpj
WHERE o.uf IS NOT NULL
GROUP BY o.uf
ORDER BY despesas_totais DESC
LIMIT 5;

-- ============================================
-- QUERY 3: Operadoras com despesas acima da média em 2+ trimestres
-- ============================================

WITH media_geral AS (
    -- Calcular média de despesas por trimestre
    SELECT 
        ano,
        trimestre,
        AVG(valor_despesas) AS media_trimestre
    FROM despesas_consolidadas
    GROUP BY ano, trimestre
),
operadoras_acima_media AS (
    -- Identificar operadoras acima da média em cada trimestre
    SELECT 
        d.cnpj,
        d.razao_social,
        d.ano,
        d.trimestre,
        d.valor_despesas,
        m.media_trimestre,
        CASE 
            WHEN d.valor_despesas > m.media_trimestre THEN 1 
            ELSE 0 
        END AS acima_media
    FROM despesas_consolidadas d
    INNER JOIN media_geral m 
        ON d.ano = m.ano AND d.trimestre = m.trimestre
)
SELECT 
    COUNT(DISTINCT cnpj) AS total_operadoras,
    'Operadoras com despesas acima da média em 2+ trimestres' AS descricao
FROM (
    SELECT 
        cnpj,
        razao_social,
        SUM(acima_media) AS trimestres_acima_media
    FROM operadoras_acima_media
    GROUP BY cnpj, razao_social
    HAVING SUM(acima_media) >= 2
) AS resultado;

WITH media_geral AS (
    SELECT 
        ano,
        trimestre,
        AVG(valor_despesas) AS media_trimestre
    FROM despesas_consolidadas
    GROUP BY ano, trimestre
),
operadoras_acima_media AS (
    SELECT 
        d.cnpj,
        d.razao_social,
        d.ano,
        d.trimestre,
        d.valor_despesas,
        m.media_trimestre,
        CASE 
            WHEN d.valor_despesas > m.media_trimestre THEN 1 
            ELSE 0 
        END AS acima_media
    FROM despesas_consolidadas d
    INNER JOIN media_geral m 
        ON d.ano = m.ano AND d.trimestre = m.trimestre
)
SELECT 
    cnpj,
    razao_social,
    SUM(acima_media) AS trimestres_acima_media,
    COUNT(*) AS total_trimestres,
    ROUND(AVG(valor_despesas), 2) AS media_despesas
FROM operadoras_acima_media
GROUP BY cnpj, razao_social
HAVING SUM(acima_media) >= 2
ORDER BY trimestres_acima_media DESC, media_despesas DESC;
