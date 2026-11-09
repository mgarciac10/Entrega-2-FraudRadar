-- Q6: canales y categorías de comercio que concentran el monto sospechoso.
WITH flagged AS (
    SELECT *
    FROM fraudradar.fact_transaction
    WHERE is_flagged
),
by_channel AS (
    SELECT
        'canal' AS tipo,
        c.channel AS dimension_name,
        SUM(f.amount_cop) AS monto_sospechoso_cop,
        COUNT(*) AS transacciones
    FROM flagged f
    JOIN fraudradar.dim_channel c ON f.channel_key = c.channel_key
    GROUP BY c.channel
),
by_category AS (
    SELECT
        'categoria_comercio' AS tipo,
        COALESCE(m.merchant_category, 'sin_comercio') AS dimension_name,
        SUM(f.amount_cop) AS monto_sospechoso_cop,
        COUNT(*) AS transacciones
    FROM flagged f
    LEFT JOIN fraudradar.dim_merchant m ON f.merchant_key = m.merchant_key
    GROUP BY COALESCE(m.merchant_category, 'sin_comercio')
)
SELECT tipo, dimension_name, monto_sospechoso_cop, transacciones,
       ROW_NUMBER() OVER (PARTITION BY tipo ORDER BY monto_sospechoso_cop DESC) AS posicion
FROM (
    SELECT * FROM by_channel
    UNION ALL
    SELECT * FROM by_category
)
ORDER BY tipo, posicion;
