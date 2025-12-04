# Análisis de Performance: ETL cada 30 segundos

## 📊 Análisis de Carga

### Escenario:

- **200-500 contratistas** activos
- **Beats cada 15 segundos** por contratista
- **ETL cada 30 segundos** para actualizar productividad
- **Dashboard** necesita ver % de productividad actualizado

### Cálculo de Volumen:

```
Por minuto:
- 200 contratistas × 4 beats/min = 800 beats/min
- 500 contratistas × 4 beats/min = 2,000 beats/min

Por hora:
- 200 contratistas = 48,000 beats/hora
- 500 contratistas = 120,000 beats/hora

ETL cada 30 segundos:
- 120 ejecuciones/hora
- Cada ejecución procesaría ~400-1,000 beats nuevos (en el mejor caso)
- Pero el ETL actual lee TODOS los beats sin filtro incremental
```

## ⚠️ Problemas con ETL cada 30 segundos

### 1. **Re-procesamiento de Datos**

El ETL actual (`processActivityToDailyMetrics`) lee TODOS los beats del día:

```typescript
SELECT * FROM contractor_activity_15s
WHERE toDate(beat_timestamp) = today() - 1
```

- **Problema**: Re-procesa beats ya procesados
- **Impacto**: CPU y memoria desperdiciados

### 2. **Queries Pesadas a ClickHouse**

Cada 30 segundos:

- Leer todos los beats del día (puede ser 10,000+ filas)
- Leer `AppUsage` y `Browser` desde `events_raw` para cada contractor
- Calcular productividad score (multi-factor)
- Insertar/actualizar métricas diarias

### 3. **Contención de Recursos**

- ClickHouse procesando queries cada 30 segundos
- Múltiples escrituras simultáneas en `contractor_daily_metrics`
- Si el ETL tarda > 30 segundos, se acumulan jobs

### 4. **Latencia del Dashboard**

- El dashboard esperaría a que termine el ETL
- Si el ETL tarda 10-20 segundos, el dashboard se siente lento

## ✅ Soluciones Recomendadas

### **Opción 1: Materialized Views (MEJOR para tiempo real)**

**Ventajas:**

- ✅ Se ejecutan automáticamente cuando se insertan datos
- ✅ Sin overhead de polling
- ✅ ClickHouse optimiza las MVs internamente
- ✅ Actualización en tiempo real (< 1 segundo de latencia)

**Implementación:**

```sql
-- MV que calcula productividad en tiempo real
CREATE MATERIALIZED VIEW mv_contractor_daily_metrics_realtime
ENGINE = SummingMergeTree()
PARTITION BY workday
ORDER BY (contractor_id, workday)
AS
SELECT
  contractor_id,
  toDate(beat_timestamp) AS workday,
  count() AS total_beats,
  sum(if(is_idle = 0, 1, 0)) AS active_beats,
  sum(if(is_idle = 1, 1, 0)) AS idle_beats,
  sum(keyboard_count) AS total_keyboard_inputs,
  sum(mouse_clicks) AS total_mouse_clicks,
  -- ... más agregaciones
FROM contractor_activity_15s
GROUP BY contractor_id, workday;
```

**Para productividad score:**

- Calcular en el dashboard o en un endpoint que lea desde la MV
- O crear una segunda MV que calcule el score usando dimensiones

### **Opción 2: Procesamiento Incremental + Caché (RECOMENDADO para balance)**

**Estrategia:**

1. **ETL cada 5-15 minutos** (no cada 30 segundos)
2. **Procesar solo beats nuevos** desde la última ejecución
3. **Caché en memoria/Redis** para el dashboard (TTL 30-60 segundos)
4. **WebSocket/SSE** para actualizar dashboard cuando cambia el caché

**Implementación:**

```typescript
// ETL incremental
async processActivityToDailyMetricsIncremental(
  lastProcessedTimestamp: Date,
): Promise<void> {
  const query = `
    SELECT * FROM contractor_activity_15s
    WHERE beat_timestamp > '${lastProcessedTimestamp}'
    ORDER BY beat_timestamp
  `;
  // ... procesar solo beats nuevos
}
```

**Caché:**

```typescript
// En el controller
@Get('daily-metrics/:contractorId')
async getDailyMetrics(@Param('contractorId') contractorId: string) {
  const cacheKey = `daily-metrics:${contractorId}:${today()}`;
  const cached = await this.cache.get(cacheKey);

  if (cached) {
    return cached;
  }

  // Calcular desde contractor_activity_15s (no desde contractor_daily_metrics)
  const metrics = await this.calculateRealtimeMetrics(contractorId);
  await this.cache.set(cacheKey, metrics, 60); // TTL 60 segundos

  return metrics;
}
```

### **Opción 3: Híbrida (MEJOR para producción)**

**Combinar:**

1. **Materialized Views** para agregaciones básicas (beats, inputs, clicks)
2. **ETL cada 15 minutos** para productividad score completo (con AppUsage/Browser)
3. **Caché** para el dashboard con actualización cada 30-60 segundos
4. **Endpoint "realtime"** que calcula productividad on-demand desde `contractor_activity_15s`

## 🎯 Recomendación Final (IMPLEMENTADA)

### Para 200-500 contratistas con dashboard en tiempo real:

**✅ Solución Híbrida Implementada:**

1. **Endpoint `/adt/realtime-metrics/:contractorId`** (IMPLEMENTADO):
   - Lee desde `contractor_activity_15s` para el día actual
   - Calcula productividad score on-demand usando el transformer completo
   - Usa caché en memoria con TTL de 30 segundos
   - Latencia: < 1 segundo (con caché) o 2-5 segundos (sin caché)

2. **ETL cada 15 minutos** (recomendado):
   - Ejecutar `processActivityToDailyMetrics()` cada 15 minutos
   - Guarda resultados en `contractor_daily_metrics` para consultas históricas
   - No bloquea el dashboard

3. **Dashboard**:
   - Consulta `/adt/realtime-metrics/:contractorId` cada 30 segundos
   - Obtiene datos actualizados con caché automático
   - Si necesita datos históricos, usa `/adt/daily-metrics/:contractorId`

**Ventajas:**

- ✅ Dashboard se siente en tiempo real (< 1 segundo de latencia con caché)
- ✅ ETL no sobrecarga el sistema (cada 15 minutos, no cada 30 segundos)
- ✅ Productividad score completo disponible cuando se necesita
- ✅ Escalable a 1000+ contratistas
- ✅ Caché reduce carga en ClickHouse

## 📝 Uso de la Solución Implementada

### Para el Dashboard (Tiempo Real):

```typescript
// Consultar métricas en tiempo real (con caché de 30s)
GET /adt/realtime-metrics/{contractorId}

// Respuesta incluye:
{
  contractor_id: "...",
  workday: "2025-01-15",
  total_beats: 1200,
  active_beats: 900,
  idle_beats: 300,
  active_percentage: 75.0,
  productivity_score: 82.5,
  is_realtime: true,
  calculated_at: "2025-01-15T10:30:00Z"
}
```

### Para Reportes Históricos:

```typescript
// Consultar métricas históricas (desde tabla ADT)
GET /adt/daily-metrics/{contractorId}?days=30
```

### Configurar ETL Automático (Opcional):

Usar `@nestjs/schedule` para ejecutar ETL cada 15 minutos:

```typescript
@Cron('*/15 * * * *') // Cada 15 minutos
async processDailyMetrics() {
  await this.etlService.processActivityToDailyMetrics();
}
```

## 📈 Estimación de Performance

### Novedades aplicadas

- Skip indexes en `session_id` y `agent_session_id` para acelerar filtros/joins.
- `LowCardinality(String)` en columnas con alta repetición (`contractor_id`, `app_name`, `domain`, etc.).
- Deletes por partición (`workday`) en ETL donde corresponde.
- ETL idempotente por día/sesión (evita recomputes innecesarios).

Scripts útiles:

- `scripts/add-skip-indexes.ts`
- `scripts/apply-lowcardinality.ts`

Impacto esperado (aprox.):

- JOIN por `session_id`: 5–15s → 0.5–2s
- Búsquedas por `session_id`: 2–5s → 0.1–0.5s
- ETL diario (10 contractors × 8h): 30–60s → 10–20s

### Con Materialized Views + Caché:

- **Latencia dashboard**: < 1 segundo
- **Carga ClickHouse**: Mínima (MVs optimizadas)
- **Carga ETL**: Baja (cada 15 minutos)
- **Escalabilidad**: Hasta 2000+ contratistas sin problemas

### Con ETL cada 30 segundos (actual):

- **Latencia dashboard**: 10-30 segundos (esperando ETL)
- **Carga ClickHouse**: Alta (120 queries/hora pesadas)
- **Carga ETL**: Muy alta (re-procesa datos constantemente)
- **Escalabilidad**: Problemas con > 300 contratistas
