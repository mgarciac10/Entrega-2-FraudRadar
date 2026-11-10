-- Q8: monto fraudulento ocurrido después de la primera alerta frente al reporte diario.
-- El reporte diario sale a la medianoche de Colombia (UTC-5, sin horario de verano). event_ts está
-- en UTC: se pasa a hora local, se trunca al día y se suma un día y las 5 h para volver a UTC.
WITH attacks AS (
    SELECT
        attack_id,
        attack_pattern,
        MIN(event_ts) AS attack_start,
        DATE_TRUNC('day', MIN(event_ts) - INTERVAL '5' HOUR) + INTERVAL '1' DAY + INTERVAL '5' HOUR
            AS daily_report_ts,
        SUM(amount_cop) AS total_fraud_cop
    FROM fraudradar.fact_transaction
    WHERE is_fraud_injected AND attack_id IS NOT NULL
    GROUP BY attack_id, attack_pattern
),
first_alert AS (
    SELECT
        t.attack_id,
        MIN(a.trigger_event_ts) AS first_alert_ts
    FROM fraudradar.fact_alert a
    CROSS JOIN UNNEST(a.transaction_ids) AS linked(transaction_id)
    JOIN fraudradar.fact_transaction t
      ON t.transaction_id = linked.transaction_id
     AND t.is_fraud_injected
     AND t.attack_id IS NOT NULL
    GROUP BY t.attack_id
),
by_attack AS (
    SELECT
        x.attack_id,
        x.attack_pattern,
        x.attack_start,
        x.daily_report_ts,
        f.first_alert_ts,
        SUM(t.amount_cop) AS total_fraud_cop,
        SUM(CASE WHEN f.first_alert_ts IS NOT NULL
                       AND t.event_ts > f.first_alert_ts
                 THEN t.amount_cop ELSE 0 END) AS fraudradar_evitable_cop,
        SUM(CASE WHEN t.event_ts > x.daily_report_ts
                 THEN t.amount_cop ELSE 0 END) AS daily_report_evitable_cop
    FROM attacks x
    JOIN fraudradar.fact_transaction t
      ON t.attack_id = x.attack_id AND t.is_fraud_injected
    LEFT JOIN first_alert f ON f.attack_id = x.attack_id
    GROUP BY x.attack_id, x.attack_pattern, x.attack_start, x.daily_report_ts, f.first_alert_ts
)
SELECT
    attack_id,
    attack_pattern AS regla_esperada,
    attack_start,
    first_alert_ts,
    daily_report_ts,
    total_fraud_cop,
    fraudradar_evitable_cop,
    total_fraud_cop - fraudradar_evitable_cop AS fraudradar_no_evitable_cop,
    daily_report_evitable_cop,
    total_fraud_cop - daily_report_evitable_cop AS daily_report_no_evitable_cop
FROM by_attack
ORDER BY attack_start, attack_id;
