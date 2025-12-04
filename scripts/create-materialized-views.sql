-- Materialized Views para procesar eventos al vuelo
-- Requiere que existan las tablas de destino:
--   - contractor_activity_15s
--   - app_usage_summary
-- Nota: Para backfills históricos, puedes deshabilitar temporalmente las MVs o usar INSERT SELECT directo.

--
-- Lógica alineada con EtlService.processEventsToActivity (ClickHouse SQL)
-- is_idle = 1 cuando (Keyboard.InputsCount + Mouse.ClicksCount) == 0
--
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_events_to_activity
TO contractor_activity_15s AS
SELECT
  contractor_id,
  agent_id,
  session_id,
  agent_session_id,
  timestamp AS beat_timestamp,
  if(
    (
      toUInt32OrZero(JSON_VALUE(payload, '$.Keyboard.InputsCount')) +
      toUInt32OrZero(JSON_VALUE(payload, '$.Mouse.ClicksCount'))
    ) = 0,
    1,
    0
  ) AS is_idle,
  toUInt32OrZero(JSON_VALUE(payload, '$.Keyboard.InputsCount')) AS keyboard_count,
  toUInt32OrZero(JSON_VALUE(payload, '$.Mouse.ClicksCount')) AS mouse_clicks,
  toDate(timestamp) AS workday,
  now() AS created_at
FROM events_raw;

--
-- Lógica alineada con EtlService.processEventsToAppUsage (ClickHouse SQL)
-- active_beats: segundos de AppUsage / 15s por beat, redondeado y agregado por día
--
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_app_usage_summary
TO app_usage_summary AS
SELECT
  contractor_id,
  app_name,
  toDate(timestamp) AS workday,
  -- Suma los segundos (aceptando números o strings), divide por 15s y redondea, sin negativos
  toUInt32(
    greatest(
      0,
      round(
        sum(
          JSONExtractFloat(payload, 'AppUsage', app_name)
          + toFloat64OrZero(JSONExtractString(payload, 'AppUsage', app_name))
        ) / 15.0
      )
    )
  ) AS active_beats,
  now() AS created_at
FROM events_raw
ARRAY JOIN JSONExtractKeys(payload, 'AppUsage') AS app_name
WHERE JSONHas(payload, 'AppUsage')
GROUP BY contractor_id, workday, app_name;

--
-- Uso recomendado (modelo híbrido):
-- - Producción normal (flujo incremental):
--     * Insertar eventos en events_raw
--     * Las MVs poblarán contractor_activity_15s y app_usage_summary en tiempo real
-- - Backfill / corrección histórica:
--     * Opcionalmente DROPEAR las MVs antes de un backfill masivo
--     * Usar los ETL actuales (DELETE + INSERT SELECT) para recalcular rangos completos
--     * Recrear las MVs ejecutando nuevamente este script
--


