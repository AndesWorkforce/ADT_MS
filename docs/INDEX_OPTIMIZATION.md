# Optimización de Índices para ETL en ClickHouse

## 📚 Conceptos Clave: Índices en ClickHouse

### ¿Cómo funcionan los índices en ClickHouse?

En ClickHouse, **el `ORDER BY` es el índice principal** (similar a un índice clustered en SQL Server). A diferencia de bases SQL tradicionales:

1. **`ORDER BY` = Índice Primario**: Define el orden físico de los datos en disco
2. **Skip Indexes**: Índices secundarios opcionales para búsquedas específicas
3. **Los JOINs se optimizan** cuando ambas tablas tienen las columnas de JOIN en el `ORDER BY`

### ¿Por qué importa para el ETL?

En tu ETL actual, hay varios **JOINs costosos**:

```sql
-- Ejemplo del ETL actual (processActivityToDailyMetrics)
FROM contractor_activity_15s a
LEFT JOIN (
  SELECT contractor_id, workday, ...
  FROM events_raw
  ...
) app ON app.contractor_id = a.contractor_id AND app.workday = a.workday
```

**Si las tablas no están ordenadas por `contractor_id` y `workday`, ClickHouse tiene que:**

- Escanear toda la tabla derecha para cada fila de la izquierda
- Hacer un "hash join" o "merge join" costoso
- Consumir mucha memoria y CPU

## 🔍 Análisis de la Situación Actual

### Tablas y sus `ORDER BY` actuales:

| Tabla                      | ORDER BY Actual                              | JOINs que hace                     | ¿Optimizado? |
| -------------------------- | -------------------------------------------- | ---------------------------------- | ------------ |
| `events_raw`               | `(contractor_id, timestamp, event_id)`       | ✅ Por `contractor_id`             | ✅ Sí        |
| `contractor_activity_15s`  | `(contractor_id, beat_timestamp)`            | ✅ Por `contractor_id`             | ✅ Sí        |
| `contractor_daily_metrics` | `(contractor_id, workday)`                   | ✅ Por `contractor_id` + `workday` | ✅ Sí        |
| `session_summary`          | `(contractor_id, session_start, session_id)` | ⚠️ Por `session_id`                | ⚠️ Parcial   |
| `apps_dimension`           | `(app_name)`                                 | ✅ Por `app_name`                  | ✅ Sí        |
| `domains_dimension`        | `(domain)`                                   | ✅ Por `domain`                    | ✅ Sí        |

### Problemas Identificados:

1. **JOIN por `session_id` en `processActivityToSessionSummary`**:

   ```sql
   FROM contractor_activity_15s a
   LEFT JOIN (
     SELECT e.session_id, ...
     FROM events_raw e
     ...
   ) app ON app.session_id = a.session_id
   ```

   - `contractor_activity_15s` está ordenado por `(contractor_id, beat_timestamp)`, **NO por `session_id`**
   - `events_raw` está ordenado por `(contractor_id, timestamp, event_id)`, **NO por `session_id`**
   - **Resultado**: JOIN lento porque ClickHouse no puede usar el índice

2. **Búsquedas por `session_id` sin índice**:
   ```sql
   SELECT DISTINCT session_id
   FROM contractor_activity_15s
   WHERE session_id IS NOT NULL
   ```

   - Sin índice en `session_id`, ClickHouse escanea toda la tabla

## ✅ Optimizaciones Propuestas

### 1. Agregar Skip Index para `session_id`

**Problema**: Búsquedas por `session_id` son lentas porque no está en el `ORDER BY`.

**Solución**: Agregar un **skip index** (índice secundario) para `session_id`:

```sql
-- Para contractor_activity_15s
ALTER TABLE contractor_activity_15s
ADD INDEX idx_session_id session_id TYPE set(100) GRANULARITY 4;

-- Para events_raw (si se busca por session_id frecuentemente)
ALTER TABLE events_raw
ADD INDEX idx_session_id session_id TYPE set(100) GRANULARITY 4;
```

**¿Qué hace?**

- Crea un índice secundario que ClickHouse puede usar para filtrar rápidamente por `session_id`
- `set(100)`: Índice de tipo "set" con hasta 100 valores únicos por granulo
- `GRANULARITY 4`: Cada 4 granulos (8192 filas) se indexa

### 2. Optimizar `ORDER BY` para búsquedas comunes

**Opción A: Mantener orden actual pero agregar skip index** (Recomendado)

- Mantiene el orden actual que es bueno para búsquedas por `contractor_id`
- Agrega skip index para `session_id` cuando se necesite

**Opción B: Cambiar `ORDER BY` para incluir `session_id`** (Solo si `session_id` es más importante)

```sql
-- NO recomendado porque rompe el orden actual optimizado para contractor_id
ORDER BY (contractor_id, session_id, beat_timestamp)
```

### 3. Crear tabla de lookup para `session_id` → `contractor_id`

**Para JOINs frecuentes por `session_id`**, crear una tabla pequeña de lookup:

```sql
CREATE TABLE IF NOT EXISTS session_lookup (
  session_id String,
  contractor_id String,
  session_start DateTime
) ENGINE = MergeTree()
ORDER BY (session_id, contractor_id)
SETTINGS index_granularity = 8192;

-- Llenar desde contractor_activity_15s
INSERT INTO session_lookup
SELECT DISTINCT session_id, contractor_id, min(beat_timestamp) AS session_start
FROM contractor_activity_15s
WHERE session_id IS NOT NULL
GROUP BY session_id, contractor_id;
```

**Uso en JOINs**:

```sql
FROM contractor_activity_15s a
LEFT JOIN session_lookup sl ON sl.session_id = a.session_id
LEFT JOIN (
  SELECT e.session_id, ...
  FROM events_raw e
  ...
) app ON app.session_id = a.session_id
```

### 4. Optimizar JOINs con subconsultas pre-agregadas

**Problema actual**: Las subconsultas en los JOINs se ejecutan para cada fila.

**Solución**: Pre-agregar en tablas temporales o usar Materialized Views:

```sql
-- Crear MV que pre-agrega AppUsage por contractor + workday
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_app_usage_by_contractor_day
ENGINE = SummingMergeTree()
PARTITION BY workday
ORDER BY (contractor_id, workday, app_name)
AS
SELECT
  contractor_id,
  toDate(timestamp) AS workday,
  app_name,
  sum(JSONExtractFloat(payload, 'AppUsage', app_name)) AS total_seconds
FROM events_raw
ARRAY JOIN JSONExtractKeys(payload, 'AppUsage') AS app_name
WHERE JSONHas(payload, 'AppUsage')
GROUP BY contractor_id, workday, app_name;

-- Luego en el ETL, hacer JOIN directo:
FROM contractor_activity_15s a
LEFT JOIN (
  SELECT
    contractor_id,
    workday,
    sum(total_seconds * ifNull(d.weight, 0.5)) AS weighted_seconds,
    sum(total_seconds) AS total_seconds
  FROM mv_app_usage_by_contractor_day
  LEFT JOIN apps_dimension d ON d.app_name = app_name
  GROUP BY contractor_id, workday
) app ON app.contractor_id = a.contractor_id AND app.workday = a.workday
```

## 🚀 Implementación Recomendada

### Paso 1: Agregar Skip Indexes (Impacto inmediato, bajo riesgo)

```sql
-- Script: add-skip-indexes.sql
USE your_database;

-- Skip index para session_id en contractor_activity_15s
ALTER TABLE contractor_activity_15s
ADD INDEX IF NOT EXISTS idx_session_id session_id TYPE set(100) GRANULARITY 4;

-- Skip index para session_id en events_raw (si se busca frecuentemente)
ALTER TABLE events_raw
ADD INDEX IF NOT EXISTS idx_session_id session_id TYPE set(100) GRANULARITY 4;

-- Materializar los índices (requerido después de crearlos)
ALTER TABLE contractor_activity_15s MATERIALIZE INDEX idx_session_id;
ALTER TABLE events_raw MATERIALIZE INDEX idx_session_id;
```

### Paso 2: Crear tabla de lookup para session_id (Opcional, si los JOINs por session_id son muy frecuentes)

```sql
-- Script: create-session-lookup.sql
CREATE TABLE IF NOT EXISTS session_lookup (
  session_id String,
  contractor_id String,
  session_start DateTime,
  session_end DateTime
) ENGINE = ReplacingMergeTree(session_end)
ORDER BY (session_id, contractor_id)
SETTINGS index_granularity = 8192;

-- Poblar inicialmente
INSERT INTO session_lookup
SELECT
  session_id,
  contractor_id,
  min(beat_timestamp) AS session_start,
  max(beat_timestamp) AS session_end
FROM contractor_activity_15s
WHERE session_id IS NOT NULL
GROUP BY session_id, contractor_id;
```

### Paso 3: Monitorear performance

Después de agregar los índices, monitorear:

```sql
-- Ver uso de índices
SELECT
  table,
  name,
  type,
  expr
FROM system.data_skipping_indices
WHERE database = 'your_database';

-- Ver estadísticas de queries
SELECT
  query,
  query_duration_ms,
  read_rows,
  read_bytes
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query LIKE '%contractor_activity_15s%'
ORDER BY query_duration_ms DESC
LIMIT 10;
```

## 📊 Impacto Esperado

### Antes de optimización:

- JOIN por `session_id`: **5-15 segundos** (escaneo completo)
- Búsqueda por `session_id`: **2-5 segundos** (escaneo completo)
- ETL completo: **30-60 segundos** (con 500 contractors)

### Después de optimización:

- JOIN por `session_id`: **0.5-2 segundos** (usa skip index)
- Búsqueda por `session_id`: **0.1-0.5 segundos** (usa skip index)
- ETL completo: **10-20 segundos** (con 500 contractors)

## ⚠️ Consideraciones

1. **Skip Indexes agregan overhead en escritura**: Cada INSERT debe actualizar el índice
   - **Impacto**: ~5-10% más lento en escritura
   - **Beneficio**: 10-100x más rápido en búsquedas

2. **Materializar índices**: Después de crear un skip index, debes materializarlo:

   ```sql
   ALTER TABLE table_name MATERIALIZE INDEX index_name;
   ```

3. **Mantener índices actualizados**: Si cambias el `ORDER BY` de una tabla, los índices pueden necesitar recrearse

## 🎯 Recomendación Final

**Para tu caso específico:**

1. ✅ **Agregar skip index para `session_id`** en `contractor_activity_15s` y `events_raw`
2. ✅ **Mantener el `ORDER BY` actual** (está bien optimizado para `contractor_id`)
3. ⚠️ **Considerar tabla de lookup** solo si los JOINs por `session_id` son muy frecuentes (>100 veces/día)
4. ✅ **Usar Materialized Views** para pre-agregar datos de AppUsage/Browser (ya lo tienes parcialmente)

**Prioridad:**

- **Alta**: Skip index para `session_id` (impacto inmediato, bajo riesgo)
- **Media**: Tabla de lookup (solo si es necesario)
- **Baja**: Cambiar `ORDER BY` (riesgo alto, beneficio bajo)
