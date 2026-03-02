## Cambios aplicados: Índices y LowCardinality en ClickHouse

Fecha: hoy

### Resumen

- Se añadieron índices secundarios (data skipping indexes) para acelerar filtros y JOINs por `session_id` y `agent_session_id`.
- Se convirtieron columnas clave a `LowCardinality(String)` para reducir I/O y memoria en JOINs/agrupaciones.
- Se ajustaron DELETEs del ETL para borrar por partición (`workday`) y así acelerar idempotencia.

### 1) Índices aplicados

- Tipo: data skipping index `TYPE set(100) GRANULARITY 4)`
- Objetivo: acelerar búsquedas/joins que filtran por `session_id` y `agent_session_id`.

Índices creados:

- `contractor_activity_15s(session_id)` → `idx_session_id`
- `events_raw(session_id)` → `idx_session_id`
- `session_summary(session_id)` → `idx_session_id`
- `events_raw(agent_session_id)` → `idx_agent_session_id`

Implementación (script ejecutado):

- Script: `scripts/add-skip-indexes.ts`
- Acciones del script:
  - `ALTER TABLE ... ADD INDEX IF NOT EXISTS ...`
  - `ALTER TABLE ... MATERIALIZE INDEX ...` (para datos ya existentes)

Ejemplo SQL (referencia):

```sql
ALTER TABLE contractor_activity_15s
ADD INDEX IF NOT EXISTS idx_session_id session_id TYPE set(100) GRANULARITY 4;
ALTER TABLE contractor_activity_15s MATERIALIZE INDEX idx_session_id;
```

Por qué:

- `processActivityToSessionSummary` y consultas puntuales por sesión dependen de `session_id`. Sin índice, ClickHouse puede escanear tablas completas.
- Los índices de tipo `set()` permiten saltar granulos que no contienen el valor buscado.

### 2) LowCardinality aplicado

Objetivo:

- Reducir el tamaño en memoria y el costo de comparaciones/agrupaciones para columnas con cardinalidad baja/media y muy repetitivas.

Script ejecutado:

- `scripts/apply-lowcardinality.ts`

Resultado por tabla/columna (estado final relevante):

- `events_raw`
  - `contractor_id`: LowCardinality(String) ✔
  - `agent_id`: LowCardinality(Nullable(String)) ✔
  - `session_id`: sin cambios (parte de índice `idx_session_id`) ↺
  - `agent_session_id`: sin cambios (parte de índice `idx_agent_session_id`) ↺

- `contractor_activity_15s`
  - `contractor_id`: LowCardinality(String) ✔
  - `agent_id`: LowCardinality(Nullable(String)) ✔
  - `agent_session_id`: LowCardinality(Nullable(String)) ✔
  - `session_id`: sin cambios (parte de índice `idx_session_id`) ↺

- `contractor_daily_metrics`
  - `contractor_id`: LowCardinality(String) ✔

- `session_summary`
  - `contractor_id`: LowCardinality(String) ✔
  - `session_id`: sin cambios (parte de índice `idx_session_id`) ↺

- `app_usage_summary`
  - `contractor_id`: LowCardinality(String) ✔
  - `app_name`: LowCardinality(String) ✔

- `apps_dimension`
  - `app_name`: LowCardinality(String) ✔
  - `category`: LowCardinality(String) ✔

- `domains_dimension`
  - `domain`: LowCardinality(String) ✔
  - `category`: LowCardinality(String) ✔

Notas:

- ClickHouse no permite `Nullable(LowCardinality(String))`. Se usó `LowCardinality(Nullable(String))` cuando la columna puede ser nula.
- Columnas que forman parte de un índice no se modificaron de tipo para no invalidar dicho índice.

### 3) Ajustes en ETL (para aprovechar particiones e índices)

- `EtlService.processEventsToActivity`:
  - Antes: `ALTER TABLE ... DELETE ... beat_timestamp BETWEEN ...`
  - Ahora: `ALTER TABLE ... DELETE WHERE workday BETWEEN ...` (usa partition key, más rápido)

- `EtlService.processEventsToAppUsage`:
  - Antes: `DELETE ... WHERE (contractor_id, workday, app_name) IN (subquery)`
  - Ahora: `DELETE WHERE workday BETWEEN ...` (borra por partición; luego `INSERT SELECT`)

- `EtlService.processActivityToSessionSummary`:
  - Subconsultas de `events_raw` filtran por `session_id` presente en `contractor_activity_15s` (o por `sessionId` directo), habilitando el uso del índice y evitando escaneos completos.

### 4) Cómo verificar

- Índices creados:

```sql
SELECT table, name, type, expr
FROM system.data_skipping_indices
WHERE database = currentDatabase();
```

- Tipos LowCardinality:

```sql
SELECT table, name, type
FROM system.columns
WHERE database = currentDatabase()
  AND table IN (
    'events_raw','contractor_activity_15s','contractor_daily_metrics',
    'session_summary','app_usage_summary','apps_dimension','domains_dimension'
  )
ORDER BY table, name;
```

- Beneficio en queries:

```sql
SELECT query, query_duration_ms, read_rows, read_bytes
FROM system.query_log
WHERE type = 'QueryFinish'
  AND event_time > now() - INTERVAL 1 HOUR
ORDER BY query_duration_ms DESC
LIMIT 20;
```

### 5) Operación y repetición

- Índices y tipos son cambios de esquema persistentes (one‑time).
- No es necesario re-ejecutarlos en cada ETL; sólo si recreás tablas o removés índices.
- Ante un backfill grande, no es necesario desmaterializar índices; sólo considerar pausar MVs si las usás para evitar doble procesamiento.

### 6) Rollback (opcional)

- Quitar índices:

```sql
ALTER TABLE contractor_activity_15s DROP INDEX IF EXISTS idx_session_id;
ALTER TABLE events_raw DROP INDEX IF EXISTS idx_session_id;
ALTER TABLE session_summary DROP INDEX IF EXISTS idx_session_id;
ALTER TABLE events_raw DROP INDEX IF EXISTS idx_agent_session_id;
```

- Revertir tipos (ejemplo):

```sql
ALTER TABLE app_usage_summary MODIFY COLUMN contractor_id String;
ALTER TABLE app_usage_summary MODIFY COLUMN app_name String;
-- Repetir para otras columnas si fuese necesario
```

### 7) Scripts y referencias

- `scripts/add-skip-indexes.ts`: crea y materializa índices.
- `scripts/apply-lowcardinality.ts`: aplica LowCardinality a columnas clave.
- `docs/INDEX_OPTIMIZATION.md`: guía detallada de optimización y trade‑offs.
