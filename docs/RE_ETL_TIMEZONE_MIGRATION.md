# Re-ejecución del ETL tras migración de timezone

Tras aplicar la corrección de timezone (`toDate(timestamp, 'America/New_York')`),
los datos históricos en `contractor_activity_15s`, `contractor_daily_metrics` y
`session_summary` pueden tener `workday` incorrecto (calculado en UTC).

## Pasos para corregir datos históricos

### 1. Re-procesar beats (events_raw → contractor_activity_15s)

Usar el endpoint NATS `adt.processEventsForce` con el rango de fechas histórico.
Esto hace DELETE + INSERT, recalculando `workday` con la nueva timezone.

```
Patrón NATS: {env}.adt.processEventsForce
Payload: { "from": "2025-01-01", "to": "2026-04-15" }
```

### 2. Re-procesar métricas diarias (contractor_activity_15s → contractor_daily_metrics)

```
Patrón NATS: {env}.adt.processDailyMetrics
Payload: { "from": "2025-01-01", "to": "2026-04-15" }
```

### 3. Re-procesar resúmenes de sesión

```
Patrón NATS: {env}.adt.processSessionSummaries
Payload: { "from": "2025-01-01", "to": "2026-04-15" }
```

## Notas

- Los queries de lectura ya usan `toDate(timestamp, 'America/New_York')`, así que
  las métricas calculadas "al vuelo" (realtime) ya son correctas sin re-ETL.
- La columna `workday` pre-calculada en `contractor_activity_15s` se recalcula
  con el INSERT del ETL, que ahora usa la timezone correcta en la query SQL.
- El DDL de `contractor_activity_15s` tiene `workday Date DEFAULT toDate(beat_timestamp, 'America/New_York')`,
  pero esto solo aplica a filas nuevas insertadas directamente (no vía INSERT SELECT del ETL).
- Ajustar las fechas del rango según el volumen de datos históricos reales.
- Se recomienda ejecutar en horario de baja carga.
