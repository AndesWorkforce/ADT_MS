-- Script para agregar Skip Indexes que optimizan búsquedas por session_id
-- Estos índices mejoran significativamente el rendimiento de JOINs y filtros por session_id
--
-- IMPORTANTE: Después de ejecutar este script, debes materializar los índices:
--   ALTER TABLE contractor_activity_15s MATERIALIZE INDEX idx_session_id;
--   ALTER TABLE events_raw MATERIALIZE INDEX idx_session_id;

-- Skip index para session_id en contractor_activity_15s
-- Mejora: JOINs y filtros por session_id en processActivityToSessionSummary
ALTER TABLE contractor_activity_15s 
ADD INDEX IF NOT EXISTS idx_session_id session_id TYPE set(100) GRANULARITY 4;

-- Skip index para session_id en events_raw
-- Mejora: JOINs por session_id cuando se busca AppUsage/Browser por sesión
ALTER TABLE events_raw 
ADD INDEX IF NOT EXISTS idx_session_id session_id TYPE set(100) GRANULARITY 4;

-- Nota: Los índices se materializan automáticamente en nuevas inserciones,
-- pero para datos existentes, ejecuta:
-- ALTER TABLE contractor_activity_15s MATERIALIZE INDEX idx_session_id;
-- ALTER TABLE events_raw MATERIALIZE INDEX idx_session_id;






