# sql

**Responsable:** Luis Moreno · **Par:** Juan José Gómez

`athena/`: DDL para registrar las tablas Delta de Gold en el Glue Data Catalog y las queries de negocio. Una de estas queries se corre en vivo en la demo.

| Query | Pregunta de negocio |
|---|---|
| `q5_alertas_por_regla_ciudad_franja.sql` | ¿Cuántas alertas y cuánto monto en riesgo hubo por regla, ciudad y franja horaria? |
| `q6_canal_comercio_sospechoso.sql` | ¿Qué canal y qué categoría de comercio concentran el mayor monto sospechoso? |
| `q7_falsos_positivos_por_regla.sql` | ¿Qué porcentaje de alertas de cada regla fue confirmado como fraude? |
