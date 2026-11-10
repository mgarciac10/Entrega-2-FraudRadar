-- Q9: precisión de las alertas y recall de los ataques por regla, con las etiquetas del productor.
--
-- Los ataques se solapan a propósito: un monto acumulado grande también es un monto atípico. Por eso
-- hay dos precisiones:
--   precision            alertas de la regla que caen sobre fraude real, de CUALQUIER patrón
--                        (lo que le importa al analista: ¿la alerta era fraude?)
--   precision_patron     alertas de la regla que caen sobre un ataque de SU patrón
--                        (¿la regla detecta el ataque para el que fue diseñada?)
-- Y dos recall:
--   recall               ataques del patrón detectados por su propia regla
--   recall_cualquier     ataques del patrón detectados por alguna regla (lo que mide Q8)
WITH alert_links AS (
    SELECT a.alert_id, r.rule, t.attack_id, t.attack_pattern
    FROM fraudradar.fact_alert a
    JOIN fraudradar.dim_rule r ON a.rule_key = r.rule_key
    CROSS JOIN UNNEST(a.transaction_ids) AS linked(transaction_id)
    JOIN fraudradar.fact_transaction t
      ON t.transaction_id = linked.transaction_id
     AND t.is_fraud_injected
     AND t.attack_id IS NOT NULL
),
alerts_by_rule AS (
    SELECT r.rule, COUNT(DISTINCT a.alert_id) AS total_alerts
    FROM fraudradar.fact_alert a
    JOIN fraudradar.dim_rule r ON a.rule_key = r.rule_key
    GROUP BY r.rule
),
precision_by_rule AS (
    SELECT rule,
           COUNT(DISTINCT alert_id) AS alerts_on_fraud,
           COUNT(DISTINCT CASE WHEN attack_pattern = rule THEN alert_id END) AS alerts_on_own_pattern
    FROM alert_links
    GROUP BY rule
),
expected AS (
    SELECT attack_pattern AS rule, COUNT(DISTINCT attack_id) AS expected_attacks
    FROM fraudradar.fact_transaction
    WHERE is_fraud_injected AND attack_id IS NOT NULL AND attack_pattern IS NOT NULL
    GROUP BY attack_pattern
),
recall_by_pattern AS (
    SELECT attack_pattern AS rule,
           COUNT(DISTINCT CASE WHEN rule = attack_pattern THEN attack_id END) AS detected_by_own_rule,
           COUNT(DISTINCT attack_id) AS detected_by_any_rule
    FROM alert_links
    GROUP BY attack_pattern
)
SELECT
    d.rule AS regla,
    COALESCE(a.total_alerts, 0) AS alertas,
    COALESCE(p.alerts_on_fraud, 0) AS alertas_sobre_fraude,
    COALESCE(a.total_alerts, 0) - COALESCE(p.alerts_on_fraud, 0) AS falsos_positivos,
    ROUND(1.0 * COALESCE(p.alerts_on_fraud, 0) / NULLIF(a.total_alerts, 0), 4) AS precision,
    ROUND(1.0 * COALESCE(p.alerts_on_own_pattern, 0) / NULLIF(a.total_alerts, 0), 4) AS precision_patron,
    COALESCE(e.expected_attacks, 0) AS ataques_del_patron,
    COALESCE(e.expected_attacks, 0) - COALESCE(r.detected_by_own_rule, 0) AS ataques_no_detectados,
    ROUND(1.0 * COALESCE(r.detected_by_own_rule, 0) / NULLIF(e.expected_attacks, 0), 4) AS recall,
    ROUND(1.0 * COALESCE(r.detected_by_any_rule, 0) / NULLIF(e.expected_attacks, 0), 4) AS recall_cualquier
FROM fraudradar.dim_rule d
LEFT JOIN alerts_by_rule a ON a.rule = d.rule
LEFT JOIN precision_by_rule p ON p.rule = d.rule
LEFT JOIN expected e ON e.rule = d.rule
LEFT JOIN recall_by_pattern r ON r.rule = d.rule
ORDER BY d.rule;
