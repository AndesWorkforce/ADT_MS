-- Script de migración para corregir contractor_daily_metrics
-- Problema: SummingMergeTree estaba sumando active_percentage y productivity_score incorrectamente
-- Solución: Cambiar a ReplacingMergeTree y recalcular métricas desde contractor_activity_15s

-- PASO 1: Detectar duplicados y valores incorrectos
-- Ejecutar esto primero para ver qué datos están afectados
SELECT 
  contractor_id,
  workday,
  COUNT(*) as duplicates,
  SUM(total_beats) as total_beats_sum,
  SUM(active_beats) as active_beats_sum,
  MAX(active_percentage) as max_active_percentage,
  MAX(productivity_score) as max_productivity_score,
  -- Verificar si active_percentage > 100 (incorrecto)
  CASE WHEN MAX(active_percentage) > 100 THEN 1 ELSE 0 END as has_incorrect_percentage,
  CASE WHEN MAX(productivity_score) > 100 THEN 1 ELSE 0 END as has_incorrect_score
FROM contractor_daily_metrics
GROUP BY contractor_id, workday
HAVING duplicates > 1 OR MAX(active_percentage) > 100 OR MAX(productivity_score) > 100
ORDER BY duplicates DESC, max_active_percentage DESC
LIMIT 50;

-- PASO 2: Crear tabla temporal con el engine correcto
CREATE TABLE IF NOT EXISTS contractor_daily_metrics_new (
  contractor_id String,
  workday Date,
  total_beats UInt32,
  active_beats UInt32,
  idle_beats UInt32,
  active_percentage Float64,
  total_keyboard_inputs UInt64,
  total_mouse_clicks UInt64,
  avg_keyboard_per_min Float64,
  avg_mouse_per_min Float64,
  total_session_time_seconds UInt64,
  effective_work_seconds UInt64,
  productivity_score Float64,
  created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY workday
ORDER BY (contractor_id, workday)
TTL workday + INTERVAL 730 DAY;

-- PASO 3: Recalcular métricas correctamente desde contractor_activity_15s
-- Esto recalcula active_percentage y productivity_score correctamente
-- NOTA: productivity_score se calcula como 0 aquí porque requiere datos de AppUsage/Browser
-- El ETL deberá reprocesar estos días para calcular el score completo
INSERT INTO contractor_daily_metrics_new
SELECT 
  contractor_id,
  workday,
  COUNT() AS total_beats,
  SUM(1 - is_idle) AS active_beats,
  SUM(is_idle) AS idle_beats,
  -- Recalcular active_percentage correctamente
  round(100.0 * SUM(1 - is_idle) / COUNT(), 2) AS active_percentage,
  SUM(keyboard_count) AS total_keyboard_inputs,
  SUM(mouse_clicks) AS total_mouse_clicks,
  -- avg per minute (cada beat es 15s => 4 beats/min)
  round(SUM(keyboard_count) / (COUNT() / 4.0), 2) AS avg_keyboard_per_min,
  round(SUM(mouse_clicks) / (COUNT() / 4.0), 2) AS avg_mouse_per_min,
  (COUNT() * 15) AS total_session_time_seconds,
  (SUM(1 - is_idle) * 15) AS effective_work_seconds,
  -- productivity_score se recalculará cuando se ejecute el ETL
  0.0 AS productivity_score,
  now() AS created_at
FROM contractor_activity_15s
WHERE workday IN (
  -- Solo recalcular días que tienen problemas
  SELECT DISTINCT workday 
  FROM contractor_daily_metrics
  WHERE contractor_id IN (
    SELECT contractor_id 
    FROM contractor_daily_metrics
    GROUP BY contractor_id, workday
    HAVING COUNT(*) > 1 OR MAX(active_percentage) > 100 OR MAX(productivity_score) > 100
  )
)
GROUP BY contractor_id, workday;

-- PASO 4: Copiar datos correctos de la tabla original (días sin problemas)
INSERT INTO contractor_daily_metrics_new
SELECT *
FROM contractor_daily_metrics FINAL
WHERE (contractor_id, workday) NOT IN (
  SELECT contractor_id, workday
  FROM contractor_daily_metrics
  GROUP BY contractor_id, workday
  HAVING COUNT(*) > 1 OR MAX(active_percentage) > 100 OR MAX(productivity_score) > 100
);

-- PASO 5: Renombrar tablas (EJECUTAR CON PRECAUCIÓN - HACE BACKUP PRIMERO)
-- RENAME TABLE contractor_daily_metrics TO contractor_daily_metrics_old;
-- RENAME TABLE contractor_daily_metrics_new TO contractor_daily_metrics;

-- PASO 6: Reprocesar productivity_score para los días corregidos
-- Ejecutar el ETL para los días afectados:
-- GET /adt/etl/process-daily-metrics?workday=2025-12-01
-- (repetir para cada día afectado)

-- PASO 7: Verificar resultados
SELECT 
  contractor_id,
  workday,
  total_beats,
  active_beats,
  active_percentage,
  productivity_score,
  CASE 
    WHEN active_percentage > 100 THEN 'ERROR: active_percentage > 100'
    WHEN productivity_score > 100 THEN 'ERROR: productivity_score > 100'
    ELSE 'OK'
  END as status
FROM contractor_daily_metrics_new FINAL
WHERE active_percentage > 100 OR productivity_score > 100
ORDER BY workday DESC, active_percentage DESC
LIMIT 50;

