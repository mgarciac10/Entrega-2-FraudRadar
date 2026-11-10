-- Q7: tasa de falsos positivos entre alertas revisadas por regla.
-- Las alertas abiertas no entran al denominador porque aún no hay decisión del analista.
SELECT
    r.rule AS regla,
    COUNT_IF(a.status IN ('confirmada', 'descartada')) AS alertas_revisadas,
    COUNT_IF(a.status = 'confirmada') AS confirmadas,
    COUNT_IF(a.status = 'descartada') AS falsos_positivos,
    ROUND(
        100.0 * COUNT_IF(a.status = 'descartada')
        / NULLIF(COUNT_IF(a.status IN ('confirmada', 'descartada')), 0),
        2
    ) AS tasa_falsos_positivos_pct
FROM fraudradar.dim_rule r
LEFT JOIN fraudradar.fact_alert a ON a.rule_key = r.rule_key
GROUP BY r.rule
ORDER BY tasa_falsos_positivos_pct DESC NULLS LAST;
