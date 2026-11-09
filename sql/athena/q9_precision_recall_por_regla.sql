-- Q9: precisión de alertas y recall de ataques, según las etiquetas sintéticas.
-- Un TP es una alerta que referencia una transacción de un ataque de la regla correcta.
-- Recall cuenta ataques distintos detectados por su regla esperada.
WITH expected AS (
    SELECT attack_pattern AS rule, COUNT(DISTINCT attack_id) AS expected_attacks
    FROM fraudradar.fact_transaction
    WHERE is_fraud_injected AND attack_id IS NOT NULL AND attack_pattern IS NOT NULL
    GROUP BY attack_pattern
),
matched AS (
    SELECT DISTINCT a.alert_id, r.rule, t.attack_id
    FROM fraudradar.fact_alert a
    JOIN fraudradar.dim_rule r ON a.rule_key = r.rule_key
    CROSS JOIN UNNEST(a.transaction_ids) AS linked(transaction_id)
    JOIN fraudradar.fact_transaction t
      ON t.transaction_id = linked.transaction_id
     AND t.is_fraud_injected
     AND t.attack_id IS NOT NULL
     AND t.attack_pattern = r.rule
),
matched_by_rule AS (
    SELECT rule,
           COUNT(DISTINCT alert_id) AS true_positive_alerts,
           COUNT(DISTINCT attack_id) AS detected_attacks
    FROM matched
    GROUP BY rule
),
alerts_by_rule AS (
    SELECT r.rule, COUNT(DISTINCT a.alert_id) AS total_alerts
    FROM fraudradar.fact_alert a
    JOIN fraudradar.dim_rule r ON a.rule_key = r.rule_key
    GROUP BY r.rule
),
rules AS (
    SELECT rule FROM fraudradar.dim_rule
)
SELECT
    r.rule,
    COALESCE(m.true_positive_alerts, 0) AS true_positive_alerts,
    COALESCE(a.total_alerts, 0) - COALESCE(m.true_positive_alerts, 0) AS false_positive_alerts,
    COALESCE(e.expected_attacks, 0) - COALESCE(m.detected_attacks, 0) AS false_negative_attacks,
    ROUND(
        1.0 * COALESCE(m.true_positive_alerts, 0) / NULLIF(COALESCE(a.total_alerts, 0), 0),
        4
    ) AS precision_score,
    ROUND(
        1.0 * COALESCE(m.detected_attacks, 0) / NULLIF(COALESCE(e.expected_attacks, 0), 0),
        4
    ) AS recall_score
FROM rules r
LEFT JOIN expected e ON r.rule = e.rule
LEFT JOIN matched_by_rule m ON r.rule = m.rule
LEFT JOIN alerts_by_rule a ON r.rule = a.rule
ORDER BY r.rule;
