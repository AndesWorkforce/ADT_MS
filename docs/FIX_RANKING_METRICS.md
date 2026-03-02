# Corrección de Métricas de Ranking

## 🔍 Problema Identificado

El endpoint `/adt/ranking` estaba devolviendo valores incorrectos:

- `active_percentage` > 100% (ej: 116.66%)
- `productivity_score` > 100 (ej: 256.71)

### Causa Raíz

La tabla `contractor_daily_metrics` usaba el engine `SummingMergeTree()`, que **suma automáticamente** todos los campos numéricos cuando hay múltiples filas con la misma clave de orden (`contractor_id, workday`).

Esto causaba que:

- Si se insertaban métricas duplicadas para el mismo contractor y día, `active_percentage` y `productivity_score` se sumaban incorrectamente.
- Ejemplo: Si se inserta dos veces con `active_percentage = 50`, el resultado final sería `100` (incorrecto).

## ✅ Solución Implementada

### 1. Cambio de Engine

Se cambió el engine de `SummingMergeTree()` a `ReplacingMergeTree(created_at)`:

```sql
ENGINE = ReplacingMergeTree(created_at)
```

**Ventajas:**

- `ReplacingMergeTree` reemplaza filas duplicadas en lugar de sumarlas
- Usa `created_at` como versión: si hay duplicados, mantiene la fila con el `created_at` más reciente
- Los campos calculados (`active_percentage`, `productivity_score`) no se suman incorrectamente

### 2. Agregado de `FINAL` en Queries

Se agregó `FINAL` a las queries que leen de `contractor_daily_metrics`:

```sql
SELECT * FROM contractor_daily_metrics FINAL
WHERE ...
```

**Por qué es necesario:**

- `FINAL` fuerza a ClickHouse a aplicar el merge antes de devolver resultados
- Asegura que se devuelvan los valores correctos (sin duplicados)

### 3. Normalización Adicional en Fórmulas

Se agregó normalización explícita en `calculateAppsScore()` y `calculateBrowserScore()` para asegurar que nunca excedan 100:

```typescript
const score = 100 * (weightedSeconds / totalSeconds);
return Math.min(100, Math.max(0, score));
```

## 📋 Archivos Modificados

1. **`ADT_MS/src/clickhouse/clickhouse.service.ts`**
   - Cambio de engine a `ReplacingMergeTree(created_at)`

2. **`ADT_MS/src/listeners/adt.listener.ts`**
   - Agregado `FINAL` en `getRanking()`
   - Agregado `FINAL` en `getDailyMetrics()`

3. **`ADT_MS/src/etl/transformers/activity-to-daily-metrics.transformer.ts`**
   - Normalización adicional en `calculateAppsScore()`
   - Normalización adicional en `calculateBrowserScore()`

## 🔧 Migración de Datos Existentes

**IMPORTANTE:** Si ya tienes datos en `contractor_daily_metrics` con valores incorrectos, necesitas migrarlos.

### Opción 1: Script de Migración SQL

Ejecuta el script `ADT_MS/scripts/fix-contractor-daily-metrics.sql` que:

1. Detecta duplicados y valores incorrectos
2. Crea una tabla temporal con el engine correcto
3. Recalcula métricas desde `contractor_activity_15s`
4. Copia datos correctos de la tabla original
5. Permite renombrar las tablas

### Opción 2: Reprocesar Datos Afectados

Si prefieres una solución más simple:

1. **Identificar días afectados:**

```sql
SELECT DISTINCT workday
FROM contractor_daily_metrics
WHERE contractor_id IN (
  SELECT contractor_id
  FROM contractor_daily_metrics
  GROUP BY contractor_id, workday
  HAVING COUNT(*) > 1 OR MAX(active_percentage) > 100 OR MAX(productivity_score) > 100
);
```

2. **Eliminar métricas incorrectas:**

```sql
-- Eliminar duplicados (mantener solo el más reciente)
ALTER TABLE contractor_daily_metrics DELETE
WHERE (contractor_id, workday) IN (
  SELECT contractor_id, workday
  FROM (
    SELECT
      contractor_id,
      workday,
      ROW_NUMBER() OVER (PARTITION BY contractor_id, workday ORDER BY created_at DESC) as rn
    FROM contractor_daily_metrics
  )
  WHERE rn > 1
);
```

3. **Forzar merge:**

```sql
OPTIMIZE TABLE contractor_daily_metrics FINAL;
```

4. **Reprocesar días afectados:**

```bash
# Para cada día afectado, ejecutar:
GET /adt/etl/process-daily-metrics?workday=2025-12-01
```

### Opción 3: Recrear Tabla (Solo si no hay muchos datos)

Si tienes pocos datos o puedes permitirte perderlos:

1. **Backup (opcional):**

```sql
CREATE TABLE contractor_daily_metrics_backup AS contractor_daily_metrics;
```

2. **Eliminar tabla:**

```sql
DROP TABLE contractor_daily_metrics;
```

3. **Reiniciar el servicio:** El código creará la tabla automáticamente con el engine correcto.

4. **Reprocesar datos:**

```bash
GET /adt/etl/process-daily-metrics
```

## ✅ Verificación

Después de la migración, verifica que los valores sean correctos:

```sql
-- Verificar que no hay valores > 100
SELECT
  contractor_id,
  workday,
  active_percentage,
  productivity_score,
  CASE
    WHEN active_percentage > 100 THEN 'ERROR: active_percentage > 100'
    WHEN productivity_score > 100 THEN 'ERROR: productivity_score > 100'
    ELSE 'OK'
  END as status
FROM contractor_daily_metrics FINAL
WHERE active_percentage > 100 OR productivity_score > 100
ORDER BY workday DESC
LIMIT 50;

-- Debe devolver 0 filas si todo está correcto
```

## 🧪 Pruebas

### Test 1: Verificar que active_percentage ≤ 100

```sql
SELECT
  MAX(active_percentage) as max_active_percentage,
  COUNT(*) as total_records,
  COUNT(CASE WHEN active_percentage > 100 THEN 1 END) as incorrect_records
FROM contractor_daily_metrics FINAL;
```

**Resultado esperado:**

- `max_active_percentage` ≤ 100
- `incorrect_records` = 0

### Test 2: Verificar que productivity_score ≤ 100

```sql
SELECT
  MAX(productivity_score) as max_productivity_score,
  COUNT(*) as total_records,
  COUNT(CASE WHEN productivity_score > 100 THEN 1 END) as incorrect_records
FROM contractor_daily_metrics FINAL;
```

**Resultado esperado:**

- `max_productivity_score` ≤ 100
- `incorrect_records` = 0

### Test 3: Verificar fórmula de active_percentage

```sql
SELECT
  contractor_id,
  workday,
  total_beats,
  active_beats,
  active_percentage,
  -- Verificar cálculo: active_percentage = (active_beats / total_beats) * 100
  round(100.0 * active_beats / total_beats, 2) as calculated_percentage,
  ABS(active_percentage - round(100.0 * active_beats / total_beats, 2)) as difference
FROM contractor_daily_metrics FINAL
WHERE total_beats > 0
ORDER BY difference DESC
LIMIT 10;
```

**Resultado esperado:**

- `difference` ≈ 0 (pequeñas diferencias por redondeo son aceptables)

## 📝 Notas Adicionales

### ¿Por qué no usar SummingMergeTree?

`SummingMergeTree` es útil cuando quieres sumar métricas agregadas (ej: `total_beats`, `total_keyboard_inputs`), pero **NO** para valores calculados como porcentajes o scores.

### ¿Por qué usar ReplacingMergeTree?

`ReplacingMergeTree` es ideal cuando:

- Tienes una clave única (`contractor_id, workday`)
- Quieres mantener solo la versión más reciente de cada fila
- Los valores calculados no deben sumarse

### Performance de FINAL

`FINAL` puede ser más lento que una query normal porque fuerza el merge. Sin embargo:

- Para tablas pequeñas/medianas (< 1M filas), el impacto es mínimo
- Para tablas grandes, considera usar Materialized Views o caché

## 🚀 Próximos Pasos

1. ✅ Ejecutar migración de datos (si aplica)
2. ✅ Verificar que los valores sean correctos
3. ✅ Monitorear el endpoint `/adt/ranking` para asegurar que devuelve valores correctos
4. ✅ Considerar agregar validaciones en el código para prevenir valores incorrectos en el futuro
