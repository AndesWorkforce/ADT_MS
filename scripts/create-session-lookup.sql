-- Script para crear tabla de lookup que optimiza búsquedas por session_id
-- Esta tabla mantiene un mapeo session_id -> contractor_id para JOINs más rápidos
--
-- USO: Ejecutar después de tener datos en contractor_activity_15s
-- MANTENIMIENTO: Actualizar periódicamente o usar Materialized View

CREATE TABLE IF NOT EXISTS session_lookup (
  session_id String,
  contractor_id String,
  session_start DateTime,
  session_end DateTime,
  created_at DateTime DEFAULT now(),
  updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (session_id, contractor_id)
SETTINGS index_granularity = 8192;

-- Poblar inicialmente desde contractor_activity_15s
-- Esto puede tardar varios minutos si hay muchos datos
INSERT INTO session_lookup
SELECT 
  session_id,
  contractor_id,
  min(beat_timestamp) AS session_start,
  max(beat_timestamp) AS session_end,
  now() AS created_at,
  now() AS updated_at
FROM contractor_activity_15s
WHERE session_id IS NOT NULL
  AND session_id != ''
GROUP BY session_id, contractor_id;

-- Opcional: Crear Materialized View para mantener la tabla actualizada automáticamente
-- Descomentar si quieres que se actualice automáticamente:
/*
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_session_lookup
TO session_lookup AS
SELECT 
  session_id,
  contractor_id,
  min(beat_timestamp) AS session_start,
  max(beat_timestamp) AS session_end,
  now() AS created_at,
  now() AS updated_at
FROM contractor_activity_15s
WHERE session_id IS NOT NULL
  AND session_id != ''
GROUP BY session_id, contractor_id;
*/






