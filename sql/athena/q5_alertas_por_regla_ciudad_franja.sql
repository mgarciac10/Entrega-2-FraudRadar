-- Q5: alertas y monto en riesgo por regla, ciudad y franja horaria.
SELECT
    r.rule AS regla,
    COALESCE(l.city, 'Desconocida') AS ciudad,
    t.time_band AS franja_horaria,
    COUNT(*) AS alertas,
    SUM(a.amount_at_risk_cop) AS monto_en_riesgo_cop
FROM fraudradar.fact_alert a
JOIN fraudradar.dim_rule r ON a.rule_key = r.rule_key
JOIN fraudradar.dim_time t ON a.time_key = t.time_key
LEFT JOIN fraudradar.dim_location l ON a.location_key = l.location_key
GROUP BY r.rule, COALESCE(l.city, 'Desconocida'), t.time_band
ORDER BY monto_en_riesgo_cop DESC;
