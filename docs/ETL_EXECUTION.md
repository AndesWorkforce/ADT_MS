# Ejecución de ETL - ADT_MS

> 📖 **Para información detallada sobre producción, duplicados y mejores prácticas, consulta:** [`ETL_PRODUCTION_GUIDE.md`](./ETL_PRODUCTION_GUIDE.md)

## 📋 Modelo Híbrido: Materialized Views + ETL Manual

El sistema ADT_MS utiliza un **modelo híbrido** que combina:

1. **Materialized Views (MVs)** para procesamiento automático en tiempo real
2. **ETL manual** para backfills, correcciones y consolidación

### 🔄 Flujo de Datos Actual

```
1. Eventos llegan vía NATS → EventsListener → RawService.saveEvent() → events_raw (ClickHouse)

2. Materialized Views procesan automáticamente (tiempo real):
   events_raw → mv_events_to_activity → contractor_activity_15s (beats de 15s)
   events_raw → mv_app_usage_summary → app_usage_summary (uso de apps)

3. ETL manual se ejecuta periódicamente para:
   contractor_activity_15s → contractor_daily_metrics (agregaciones diarias, 1x/día)
   contractor_activity_15s → session_summary (resúmenes por sesión, cada hora)

4. ETL manual también disponible para backfill/correcciones:
   events_raw → contractor_activity_15s (con DELETE + INSERT SELECT)
   events_raw → app_usage_summary (con DELETE + INSERT SELECT)
```

### ✅ Ventajas del Modelo Híbrido

- **Escalabilidad**: Las MVs procesan eventos en tiempo real sin sobrecargar el sistema
- **Idempotencia**: Los ETL manuales usan `ALTER TABLE ... DELETE` antes de insertar para evitar duplicados
- **Flexibilidad**: Puedes reprocesar datos históricos sin afectar el flujo en tiempo real

## 🚀 Opciones para Ejecutar ETL

### Opción 1: Manualmente vía Endpoints HTTP (Recomendado para desarrollo/testing)

El `AdtController` expone endpoints para ejecutar ETL manualmente:

```bash
# Procesar eventos RAW → contractor_activity_15s (solo para backfill)
GET http://localhost:3000/adt/etl/process-events?from=2025-01-01&to=2025-01-31

# Procesar eventos RAW → app_usage_summary (solo para backfill, puede ejecutarse en paralelo)
GET http://localhost:3000/adt/etl/process-app-usage?from=2025-01-01&to=2025-01-31

# Procesar activity → contractor_daily_metrics
# Opciones:
# - Día actual (por defecto): GET http://localhost:3000/adt/etl/process-daily-metrics
# - Un día específico: GET http://localhost:3000/adt/etl/process-daily-metrics?workday=2025-01-15
# - Rango de días: GET http://localhost:3000/adt/etl/process-daily-metrics?from=2025-01-01&to=2025-01-31

# Procesar activity → session_summary
GET http://localhost:3000/adt/etl/process-session-summaries
```

### Opción 2: Automáticamente con Scheduler (Recomendado para producción)

Puedes usar `@nestjs/schedule` para ejecutar ETL periódicamente:

```typescript
// En un servicio
@Cron('0 5 * * *') // Todos los días a las 5 AM
async processDailyMetrics() {
  await this.etlService.processActivityToDailyMetrics();
}
```

### Opción 3: Materialized Views (Automático - Ya Implementado)

✅ **Las Materialized Views ya están configuradas y activas** en el sistema.

Las MVs procesan automáticamente:

- `events_raw → contractor_activity_15s` (vía `mv_events_to_activity`)
- `events_raw → app_usage_summary` (vía `mv_app_usage_summary`)

**No necesitas ejecutar manualmente** `process-events` ni `process-app-usage` para el flujo normal. Solo úsalos para:

- Backfills históricos
- Correcciones de datos
- Reprocesamiento de rangos específicos

#### 🧰 Endpoints FORCE (DELETE + INSERT)

Para un backfill duro que garantice recomputar completamente un rango, utiliza las rutas FORCE del API Gateway (requiere rol Superadmin):

```bash
# Reprocesar actividad (borra y vuelve a insertar contractor_activity_15s en el rango)
GET /adt/etl/process-events-force?from=YYYY-MM-DD&to=YYYY-MM-DD

# Reprocesar uso de apps (borra y vuelve a insertar app_usage_summary en el rango)
GET /adt/etl/process-app-usage-force?from=YYYY-MM-DD&to=YYYY-MM-DD
```

**Para activar/desactivar MVs:**

```sql
-- Deshabilitar temporalmente (antes de backfill masivo)
DROP VIEW IF EXISTS mv_events_to_activity;
DROP VIEW IF EXISTS mv_app_usage_summary;

-- Recrear después del backfill
-- Opción A: ejecutar el SQL
--   scripts/create-materialized-views.sql
-- Opción B (recomendada): script TS que aplica el SQL en la DB configurada
--   pnpm ts-node -r tsconfig-paths/register scripts/create-materialized-views.ts
```

## 🆕 Idempotencia sin DELETE (por día/sesión)

Los procesos ETL ahora son idempotentes sin borrar previamente:

- `processEventsToActivity` y `processEventsToAppUsage`: procesan por día y solo insertan si el día NO existe en la tabla destino. Si ya existe, se omite y se loguea “Skipping ... (already populated)”.
- `processActivityToDailyMetrics`: recorre días (parámetro único, rango o default: día actual) y solo calcula/inserta si el día NO existe en `contractor_daily_metrics`. Si existe, se omite y se devuelven las filas existentes.
- `processActivityToSessionSummary`: inserta solo `session_id` que no existan aún en `session_summary` (si se pasa `sessionId`, aplica lo mismo).

Para recomputar un día o una sesión, borra manualmente las filas objetivo y vuelve a ejecutar el proceso:

```sql
ALTER TABLE contractor_daily_metrics DELETE WHERE workday = toDate('2025-12-02');
ALTER TABLE contractor_activity_15s DELETE WHERE workday = toDate('2025-12-02');
ALTER TABLE session_summary DELETE WHERE session_id = 'session-abc';
```

## 📊 Orden Recomendado de Ejecución

1. **Primero**: `processEventsToActivity()` - Convierte eventos RAW en beats de 15s
2. **En paralelo con el paso 1**: `processEventsToAppUsage()` - Genera uso de aplicaciones desde eventos RAW
3. **Segundo**: `processActivityToDailyMetrics()` - Genera métricas diarias
4. **Tercero**: `processActivityToSessionSummary()` - Genera resúmenes de sesión

**Nota:** El paso 2 (`processEventsToAppUsage`) puede ejecutarse en paralelo con el paso 1 porque ambos leen desde `events_raw` pero generan tablas diferentes.

### Ejecución por rango y “skip-existing”

Ejemplo:

```
GET /adt/etl/process-events?from=2025-11-20&to=2025-12-03
```

- Itera día por día e inserta únicamente los días que no existen aún.
- Para días ya poblados, se omite el procesamiento.

## ⚙️ Configuración Recomendada

### Desarrollo/Testing:

- Las MVs procesan automáticamente los eventos nuevos
- Ejecutar ETL manualmente vía endpoints solo para backfills o testing

### Producción (Modelo Híbrido):

**Flujo Automático (Materialized Views):**

- ✅ `events_raw → contractor_activity_15s` (automático vía MV)
- ✅ `events_raw → app_usage_summary` (automático vía MV)

**ETL Programado (Cron):**

- **Batch diario**: Ejecutar `process-daily-metrics` todas las noches (ej: 2 AM)
- **Batch por sesión**: Ejecutar `process-session-summaries` cada hora

**ETL Manual (Solo para backfill/correcciones):**

- `process-events`: Solo cuando necesites reprocesar histórico
- `process-app-usage`: Solo cuando necesites reprocesar histórico

## 🔍 Verificar si hay datos

```bash
# Verificar eventos RAW
GET http://localhost:3000/adt/activity/{contractorId}?limit=10

# Verificar métricas diarias
GET http://localhost:3000/adt/daily-metrics/{contractorId}?days=7
```

---

## ⚠️ Prevención de Duplicados

**Problema:** Si ejecutas el mismo ETL dos veces con los mismos datos, puedes crear duplicados:

- `contractor_activity_15s` (MergeTree): Crea filas duplicadas
- `contractor_daily_metrics` (SummingMergeTree): Suma valores duplicados (incorrecto)
- `app_usage_summary` (SummingMergeTree): Suma valores duplicados (incorrecto)
- `session_summary` (MergeTree): Crea filas duplicadas

**Solución:**

1. **Usar rangos de fechas** para procesar solo eventos nuevos
2. **Filtrar eventos ya procesados** antes de insertar
3. **Usar Materialized Views** para procesamiento automático

📖 **Ver guía completa:** [`ETL_PRODUCTION_GUIDE.md`](./ETL_PRODUCTION_GUIDE.md)

---

## 📊 Frecuencia Recomendada (Resumen)

| Proceso                     | Tipo              | Frecuencia              | Notas                                  |
| --------------------------- | ----------------- | ----------------------- | -------------------------------------- |
| `mv_events_to_activity`     | Materialized View | **Automático**          | Se ejecuta al insertar en `events_raw` |
| `mv_app_usage_summary`      | Materialized View | **Automático**          | Se ejecuta al insertar en `events_raw` |
| `process-daily-metrics`     | ETL Manual        | **1 vez al día (2 AM)** | Consolidación diaria                   |
| `process-session-summaries` | ETL Manual        | **Cada hora**           | Procesar sesiones cerradas             |
| `process-events`            | ETL Manual        | **Solo backfill**       | No necesario en flujo normal           |
| `process-app-usage`         | ETL Manual        | **Solo backfill**       | No necesario en flujo normal           |

📖 **Ver detalles completos y estrategias:** [`ETL_PRODUCTION_GUIDE.md`](./ETL_PRODUCTION_GUIDE.md)
