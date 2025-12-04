# Guía: Dashboard en Tiempo Real

## 🎯 Problema Resuelto

Para 200-500 contratistas con dashboard que necesita actualización cada 30 segundos, **NO es recomendable ejecutar ETL cada 30 segundos** porque:

- ❌ Re-procesa datos constantemente
- ❌ Sobrecarga ClickHouse con queries pesadas
- ❌ Latencia alta (10-30 segundos)
- ❌ No escala bien

## ✅ Solución Implementada

### Endpoint de Tiempo Real

**`GET /adt/realtime-metrics/:contractorId`**

Este endpoint:

1. ✅ Lee directamente desde `contractor_activity_15s` (datos más recientes)
2. ✅ Calcula productividad score completo on-demand
3. ✅ Usa caché en memoria (TTL 30 segundos) para reducir carga
4. ✅ Latencia: < 1 segundo (con caché) o 2-5 segundos (sin caché)

### Ejemplo de Uso

```bash
# Obtener métricas en tiempo real para hoy
curl http://localhost:3000/adt/realtime-metrics/contractor-123

# Obtener métricas para un día específico
curl http://localhost:3000/adt/realtime-metrics/contractor-123?workday=2025-01-15

# Forzar recálculo (sin caché)
curl http://localhost:3000/adt/realtime-metrics/contractor-123?useCache=false
```

### Respuesta

```json
{
  "contractor_id": "contractor-123",
  "workday": "2025-01-15",
  "total_beats": 1200,
  "active_beats": 900,
  "idle_beats": 300,
  "active_percentage": 75.0,
  "total_keyboard_inputs": 4500,
  "total_mouse_clicks": 1200,
  "avg_keyboard_per_min": 12.5,
  "avg_mouse_per_min": 3.3,
  "total_session_time_seconds": 18000,
  "effective_work_seconds": 13500,
  "productivity_score": 82.5,
  "is_realtime": true,
  "calculated_at": "2025-01-15T10:30:00.000Z"
}
```

## 🔄 Flujo Recomendado

### 1. Dashboard Consulta cada 30 segundos

```typescript
// En tu frontend
setInterval(async () => {
  const metrics = await fetch(`/adt/realtime-metrics/${contractorId}`).then(
    (r) => r.json(),
  );

  updateDashboard(metrics);
}, 30000); // 30 segundos
```

### 2. Materialized Views procesan automáticamente

Las **Materialized Views** procesan automáticamente los eventos nuevos:

- `events_raw → contractor_activity_15s` (vía `mv_events_to_activity`)
- `events_raw → app_usage_summary` (vía `mv_app_usage_summary`)

Esto significa que `contractor_activity_15s` se actualiza en tiempo real sin necesidad de ejecutar ETL manuales.

### 3. ETL se ejecuta periódicamente para consolidación

El ETL `process-daily-metrics` se ejecuta **1 vez al día (2 AM)** y guarda resultados en `contractor_daily_metrics` para:

- Reportes históricos
- Análisis de tendencias
- Backups de datos procesados

**No es necesario para el dashboard en tiempo real**, pero es útil para:

- Consultas históricas más rápidas
- Datos consolidados para reportes

### 3. Caché Automático

El servicio `RealtimeMetricsService` mantiene un caché en memoria:

- **TTL**: 30 segundos
- **Scope**: Por contractor y día
- **Beneficio**: Reduce queries a ClickHouse en 90%+

## 📊 Performance Esperada

### Con 200-500 contratistas:

| Métrica                | Valor                            |
| ---------------------- | -------------------------------- |
| Latencia (con caché)   | < 1 segundo                      |
| Latencia (sin caché)   | 2-5 segundos                     |
| Queries ClickHouse/min | ~10-20 (vs 120 con ETL cada 30s) |
| CPU ClickHouse         | Baja                             |
| Memoria servicio       | < 100 MB (caché)                 |
| Escalabilidad          | Hasta 2000+ contratistas         |

## 🚀 Próximos Pasos (Opcional)

### 1. Agregar Redis para Caché Distribuido

Si tienes múltiples instancias del servicio:

```typescript
// Reemplazar Map por Redis
await this.redis.setex(cacheKey, 30, JSON.stringify(metrics));
```

### 2. Materialized Views (Ya Implementadas)

✅ **Las Materialized Views ya están implementadas y activas**:

- `mv_events_to_activity`: Procesa `events_raw → contractor_activity_15s` automáticamente
- `mv_app_usage_summary`: Procesa `events_raw → app_usage_summary` automáticamente

Estas MVs aseguran que los datos estén disponibles en tiempo real para el endpoint de métricas en tiempo real.

### 3. WebSocket para Actualizaciones Push

En lugar de polling cada 30 segundos, usar WebSocket:

```typescript
// Servidor envía actualizaciones cuando hay cambios
ws.on('connection', (client) => {
  setInterval(() => {
    const metrics = await getRealtimeMetrics(contractorId);
    client.send(JSON.stringify(metrics));
  }, 30000);
});
```

## 🛠️ Debug / Diagnóstico

Para auditar el cálculo del score (sub-scores y final) en tiempo real, habilita logs en los transformers:

```bash
ETL_DEBUG_LOGS=1 pnpm start:dev
```

Verás en logs, por contractor/sesión:

- `S_active`, `S_inputs`, `S_apps`, `S_browser` y `score` final.

## ⚠️ Notas Importantes

1. **El endpoint realtime calcula desde `contractor_activity_15s`**, que se actualiza automáticamente vía Materialized Views cuando llegan eventos nuevos a `events_raw`

2. **El caché se limpia automáticamente** después de 30 segundos

3. **Para datos históricos**, usa `/adt/daily-metrics/:contractorId` (más rápido, desde `contractor_daily_metrics`)

4. **El ETL `process-daily-metrics` se ejecuta 1 vez al día (2 AM)** para consolidar datos históricos en `contractor_daily_metrics`, pero no es necesario para tiempo real

5. **Las Materialized Views procesan automáticamente** los eventos nuevos, así que no necesitas ejecutar ETL manuales para el flujo normal
