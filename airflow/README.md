# airflow

**Responsable:** Luis Moreno · **Par:** Juan José Gómez

Airflow no transporta datos: dispara los jobs batch de `spark/batch/` según sus dependencias. Los jobs de streaming no dependen de Airflow (corren continuos en Docker).

| DAG | Frecuencia | Tareas |
|---|---|---|
| `lakehouse_15min` | cada 15 min | `check_bronze_freshness` → `bronze_to_silver` → [`extract_alert_status`, `build_dimensions`] → `silver_to_gold` → `data_quality_checks` |
| `daily_batch` | diario | `ingest_catalogs` → `build_user_profile` → `optimize_and_vacuum` |

Configuración común: 2 reintentos con 2 min de espera, `max_active_runs=1`, `catchup=False`. Si una tarea falla, las siguientes quedan en `upstream_failed` y Gold no se construye con datos incompletos.
