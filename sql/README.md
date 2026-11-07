# sql

**Responsable:** Luis Moreno · **Par:** Juan José Gómez

`athena/`: DDL para registrar las tablas Delta de Gold en el Glue Data Catalog y las queries de negocio. Una de estas queries se corre en vivo en la demo.

| Query | Pregunta de negocio |
|---|---|
| `q5_alertas_por_regla_ciudad_franja.sql` | ¿Cuántas alertas y cuánto monto en riesgo hubo por regla, ciudad y franja horaria? |
| `q6_canal_comercio_sospechoso.sql` | ¿Qué canal y qué categoría de comercio concentran el mayor monto sospechoso? |
| `q7_falsos_positivos_por_regla.sql` | ¿Qué porcentaje de alertas de cada regla fue confirmado como fraude? |
| `q8_fraude_evitable.sql` | De cada ataque inyectado, ¿cuánto monto ocurrió antes de la primera alerta (no evitable) y cuánto después (evitable si el analista reacciona)? Compara contra el reporte diario actual |
| `q9_precision_recall_por_regla.sql` | Precisión y recall exactos por regla, cruzando `is_fraud_injected` con las alertas |

**Q8 es la métrica de valor del proyecto.** FraudRadar no detiene la primera transacción de un ataque; Q8 mide cuánto del fraude posterior sí se evita y lo compara con el escenario del reporte diario (donde se evitaría casi nada). Se muestra en la demo junto a la latencia evento → alerta.

Definición: para cada `attack_id`, `monto_no_evitable` = suma de `amount_cop` de las transacciones fraudulentas con `event_ts` ≤ el primer `trigger_event_ts` de sus alertas, y `monto_evitable` = el resto. Escenario base: la primera revisión diaria posterior al inicio del ataque.
