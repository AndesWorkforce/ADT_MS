# Optimizaciones para ClickHouse - Escala Masiva

Este documento contiene optimizaciones críticas para escalar el sistema a **500 contratistas × 6 meses** (~120 millones de eventos, ~85 millones de beats).

## ⚠️ Análisis de Escala

### Situación Actual

- **30 contratistas × 2 meses**: Problemas de memoria (heap out of memory) y slow queries (5-15 segundos)
- **Volumen**: ~2.4 millones de eventos, ~1.7 millones de beats

### Escala Objetivo

- **500 contratistas × 6 meses**: ~50x más datos que la situación actual
- **Volumen estimado**: ~120 millones de eventos, ~85 millones de beats
- **Riesgo**: Sin optimizaciones, el sistema colapsará por memoria y tiempo de respuesta

### Conclusión

**Se requieren optimizaciones agresivas de arquitectura y procesamiento**. Las optimizaciones básicas NO son suficientes.

---

## 1. Optimizaciones de Esquema (ClickHouse)

### 1.1 Columnas Materializadas para JSON

**Problema**: Procesar JSON en cada query es extremadamente lento con millones de eventos.

```sql
-- Agregar columnas materializadas para campos JSON frecuentes
ALTER TABLE events_raw ADD COLUMN IF NOT EXISTS
  date_col Date MATERIALIZED toDate(timestamp);

ALTER TABLE events_raw ADD COLUMN IF NOT EXISTS
  app_usage_json String MATERIALIZED JSONExtractString(payload, 'AppUsage');

ALTER TABLE events_raw ADD COLUMN IF NOT EXISTS
  browser_json String MATERIALIZED JSONExtractString(payload, 'browser');

ALTER TABLE events_raw ADD COLUMN IF NOT EXISTS
  has_app_usage UInt8 MATERIALIZED JSONHas(payload, 'AppUsage');

ALTER TABLE events_raw ADD COLUMN IF NOT EXISTS
  has_browser UInt8 MATERIALIZED JSONHas(payload, 'browser');

-- Modificar ORDER BY para optimizar filtros por fecha
ALTER TABLE events_raw MODIFY ORDER BY (contractor_id, date_col, timestamp, event_id);
```

**Impacto**: Reducción del 50-70% en tiempo de queries que usan JSON.

---

### 1.2 Índices Secundarios (Skip Indexes)

```sql
-- Índices para búsquedas por contractor_id y fecha
ALTER TABLE events_raw ADD INDEX IF NOT EXISTS idx_contractor_date
  (contractor_id, date_col) TYPE minmax GRANULARITY 4;

-- Índices bloom filter para búsquedas en JSON
ALTER TABLE events_raw ADD INDEX IF NOT EXISTS idx_appusage
  has_app_usage TYPE bloom_filter GRANULARITY 4;

ALTER TABLE events_raw ADD INDEX IF NOT EXISTS idx_browser
  has_browser TYPE bloom_filter GRANULARITY 4;

-- Índice para contractor_activity_15s
ALTER TABLE contractor_activity_15s ADD INDEX IF NOT EXISTS idx_is_idle
  is_idle TYPE set(2) GRANULARITY 4;

-- Índice para contractor_info_raw
ALTER TABLE contractor_info_raw ADD INDEX IF NOT EXISTS idx_isactive
  isActive TYPE set(2) GRANULARITY 1;
```

**Impacto**: Reducción del 30-50% en tiempo de filtrado.

---

### 1.3 Materialized Views con Agregación Completa

**Problema**: Procesar JSON en queries de tiempo real es inviable con millones de eventos.

```sql
-- MV que extrae y agrega AppUsage directamente desde eventos
CREATE MATERIALIZED VIEW IF NOT EXISTS app_usage_aggregated_mv
ENGINE = SummingMergeTree()
PARTITION BY toDate(timestamp)
ORDER BY (contractor_id, app_name, toDate(timestamp))
AS SELECT
  contractor_id,
  app_name String,
  toDate(timestamp) AS date_col,
  sum(seconds) AS total_seconds
FROM (
  SELECT
    contractor_id,
    timestamp,
    JSONExtractKeysAndValues(app_usage_json, 'Int64') AS app_usage_pairs
  FROM events_raw
  WHERE has_app_usage = 1
) ARRAY JOIN app_usage_pairs AS (app_name, seconds)
GROUP BY contractor_id, app_name, date_col;

-- Similar para BrowserUsage
CREATE MATERIALIZED VIEW IF NOT EXISTS browser_usage_aggregated_mv
ENGINE = SummingMergeTree()
PARTITION BY toDate(timestamp)
ORDER BY (contractor_id, domain, toDate(timestamp))
AS SELECT
  contractor_id,
  domain String,
  toDate(timestamp) AS date_col,
  sum(seconds) AS total_seconds
FROM (
  SELECT
    contractor_id,
    timestamp,
    JSONExtractKeysAndValues(browser_json, 'Int64') AS browser_pairs
  FROM events_raw
  WHERE has_browser = 1
) ARRAY JOIN browser_pairs AS (domain, seconds)
GROUP BY contractor_id, domain, date_col;
```

**Uso en queries**:

```sql
-- En lugar de procesar JSON en cada query
SELECT * FROM app_usage_aggregated_mv
WHERE contractor_id = 'xxx'
  AND date_col >= '2024-01-01'
  AND date_col <= '2024-06-30';
```

**Impacto**: Elimina completamente el procesamiento de JSON en queries, reducción del 70-80% en tiempo.

---

### 1.4 Tablas de Agregación Pre-calculadas

**Problema**: Calcular métricas desde beats raw para 500 contratistas × 6 meses es inviable.

```sql
-- Tabla para métricas pre-agregadas por contractor y rango de fechas
CREATE TABLE IF NOT EXISTS contractor_metrics_aggregated
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(date_from)
ORDER BY (contractor_id, date_from, date_to)
AS SELECT
  contractor_id,
  min(workday) AS date_from,
  max(workday) AS date_to,
  -- Métricas agregadas
  sum(total_beats) AS total_beats,
  sum(active_beats) AS active_beats,
  avg(active_percentage) AS avg_active_percentage,
  sum(total_keyboard_inputs) AS total_keyboard_inputs,
  sum(total_mouse_clicks) AS total_mouse_clicks,
  avg(productivity_score) AS avg_productivity_score,
  updated_at DateTime DEFAULT now()
FROM contractor_daily_metrics
GROUP BY contractor_id;
```

**Uso en queries**:

```sql
-- ❌ NO HACER: Calcular desde beats raw
SELECT * FROM contractor_activity_15s WHERE contractor_id = 'xxx' AND ...

-- ✅ HACER: Usar tabla agregada
SELECT * FROM contractor_metrics_aggregated
WHERE contractor_id = 'xxx'
  AND date_from >= '2024-01-01'
  AND date_to <= '2024-06-30';
```

**Impacto**: Reducción del 95-99% en tiempo de query (de minutos a segundos).

---

### 1.5 Particionado Optimizado

```sql
-- Optimizar particiones para mejor paralelización
-- Considerar sharding por contractor_id para escalas muy grandes
CREATE TABLE IF NOT EXISTS contractor_activity_15s_optimized
ENGINE = MergeTree()
PARTITION BY (toYYYYMM(beat_timestamp), contractor_id)
ORDER BY (contractor_id, beat_timestamp)
AS SELECT * FROM contractor_activity_15s;
```

**Impacto**: Reducción del 40-60% en tiempo de escaneo de particiones.

---

## 2. Optimizaciones de Procesamiento (Backend)

### 2.1 Procesamiento por Streaming

**Problema**: Cargar millones de filas en memoria causa heap out of memory.

```typescript
// ❌ NO HACER: Cargar todo en memoria
const beats = await clickHouseService.query(beatsQuery); // Millones de filas → OOM

// ✅ HACER: Procesar en chunks
async function* streamQuery<T>(query: string, chunkSize: number = 10000) {
  let offset = 0;
  while (true) {
    const chunk = await clickHouseService.query<T>(
      `${query} ORDER BY beat_timestamp LIMIT ${chunkSize} OFFSET ${offset}`,
    );
    if (chunk.length === 0) break;
    yield chunk;
    offset += chunkSize;
  }
}

// Uso:
const appUsageMap: Record<string, number> = {};
for await (const chunk of streamQuery(eventsQuery, 10000)) {
  for (const event of chunk) {
    // Procesar chunk (10k filas a la vez)
    processEvent(event, appUsageMap);
  }
  // Liberar memoria después de cada chunk
}
```

**Impacto**: Reduce uso de memoria de ~4GB a ~100MB por query.

---

### 2.2 Procesamiento por Lotes (Batching)

**Problema**: Procesar 500 contratistas en paralelo colapsa el sistema.

```typescript
// ❌ NO HACER: Promise.all con 500 contratistas
const allMetrics = await Promise.all(
  contractors.map((c) => calculateMetrics(c.id, from, to)),
); // 500 queries simultáneas → colapso

// ✅ HACER: Procesar en batches pequeños
async function processInBatches<T, R>(
  items: T[],
  processor: (item: T) => Promise<R>,
  batchSize: number = 3, // Ajustar según carga
  delayMs: number = 200,
): Promise<R[]> {
  const results: R[] = [];
  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);
    const batchResults = await Promise.all(
      batch.map((item) => processor(item)),
    );
    results.push(...batchResults);

    // Delay entre batches para liberar memoria
    if (i + batchSize < items.length) {
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
  return results;
}
```

**Configuración recomendada según escala**:

- **10-30 contratistas**: batchSize = 10, delay = 50ms
- **31-100 contratistas**: batchSize = 5, delay = 100ms
- **101-500 contratistas**: batchSize = 3, delay = 200ms
- **500+ contratistas**: batchSize = 2, delay = 300ms

**Impacto**: Prevención de colapso de memoria y estabilidad del sistema.

---

### 2.3 Procesamiento Incremental (Delta Processing)

**Problema**: Recalcular todo desde cero es ineficiente.

```typescript
// Solo procesar datos nuevos/modificados
async function getIncrementalMetrics(
  contractorId: string,
  from: Date,
  to: Date,
  lastProcessedAt: Date,
) {
  // Solo obtener datos nuevos desde lastProcessedAt
  const newBeats = await clickHouseService.query(`
    SELECT * FROM contractor_activity_15s
    WHERE contractor_id = '${contractorId}'
      AND beat_timestamp >= '${lastProcessedAt.toISOString()}'
      AND beat_timestamp <= '${to.toISOString()}'
  `);

  // Combinar con datos pre-agregados existentes
  const existingMetrics = await getCachedMetrics(
    contractorId,
    from,
    lastProcessedAt,
  );
  return mergeMetrics(existingMetrics, newBeats);
}
```

**Impacto**: Reducción del 90-95% en tiempo de procesamiento para actualizaciones.

---

### 2.4 Optimización de Queries con PREWHERE y LIMIT

```sql
-- Usar PREWHERE en lugar de WHERE cuando sea posible
-- PREWHERE se ejecuta antes de leer columnas no necesarias
SELECT * FROM events_raw
PREWHERE contractor_id = 'xxx' AND date_col >= '2024-01-01'
WHERE has_app_usage = 1;

-- Siempre usar LIMIT en queries de exploración
SELECT * FROM contractor_activity_15s
WHERE contractor_id = 'xxx'
  AND beat_timestamp >= '2024-01-01'
ORDER BY beat_timestamp DESC
LIMIT 10000;  -- Máximo 10k filas

-- Usar sampling para análisis exploratorios rápidos
SELECT
  contractor_id,
  avg(productivity_score) AS avg_score
FROM contractor_daily_metrics
SAMPLE 0.1  -- 10% de los datos
WHERE workday >= '2024-01-01'
GROUP BY contractor_id;
```

**Impacto**: Reducción del 20-40% en lectura de datos.

---

## 3. Optimizaciones de Arquitectura

### 3.1 Cache Agresivo Multi-nivel

**Problema**: Queries repetidas recalculan los mismos datos.

```typescript
// Cache en memoria (Redis/Memcached) - Nivel 1
const cacheKey = `metrics:${contractorId}:${from}:${to}`;
const cached = await redis.get(cacheKey);
if (cached) return JSON.parse(cached);

// Cache en ClickHouse (tabla de resultados) - Nivel 2
const cachedResult = await clickHouseService.query(`
  SELECT result_json, updated_at
  FROM metrics_cache
  WHERE cache_key = '${cacheKey}'
    AND updated_at > now() - INTERVAL 1 HOUR
`);

if (cachedResult.length > 0) {
  return JSON.parse(cachedResult[0].result_json);
}

// Calcular y guardar en cache
const result = await calculateMetrics(contractorId, from, to);
await redis.setex(cacheKey, 3600, JSON.stringify(result)); // 1 hora
await clickHouseService.command(`
  INSERT INTO metrics_cache (cache_key, result_json, updated_at)
  VALUES ('${cacheKey}', '${JSON.stringify(result)}', now())
`);
```

**Tabla de cache en ClickHouse**:

```sql
CREATE TABLE IF NOT EXISTS metrics_cache
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY cache_key
TTL updated_at + INTERVAL 24 HOUR
AS SELECT
  cache_key String,
  result_json String,
  updated_at DateTime DEFAULT now();
```

**Impacto**: Reducción del 80-90% en queries repetidas.

---

### 3.2 Procesamiento Asíncrono con Cola de Trabajos

**Problema**: Cálculos pesados bloquean el API.

```typescript
// En lugar de calcular en tiempo real
async function getAllRealtimeMetrics(from: Date, to: Date) {
  // Agregar trabajo a cola (Bull/BullMQ)
  const job = await metricsQueue.add('calculate-metrics', {
    from: from.toISOString(),
    to: to.toISOString(),
  });

  // Retornar job ID, el cliente puede consultar el estado
  return { jobId: job.id, status: 'processing' };
}

// Worker procesa en background
metricsQueue.process('calculate-metrics', async (job) => {
  const { from, to } = job.data;
  // Procesar en batches pequeños
  const contractors = await getActiveContractors();
  return await processInBatches(
    contractors,
    (c) => calculateMetrics(c.id, from, to),
    3, // batchSize pequeño
    200, // delay entre batches
  );
});
```

**Impacto**: No bloquea el API, permite procesamiento distribuido.

---

### 3.3 Diccionarios para JOINs

**Problema**: JOINs en ClickHouse son costosos con grandes volúmenes.

```sql
-- Crear diccionario para contractor_info
CREATE DICTIONARY contractor_info_dict
(
  contractor_id String,
  name String,
  client_id String,
  team_id String
)
PRIMARY KEY contractor_id
SOURCE(CLICKHOUSE(
  HOST 'localhost'
  PORT 9000
  USER 'default'
  PASSWORD ''
  DB 'metrics_db'
  TABLE 'contractor_info_raw'
))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 600);

-- Uso en queries
SELECT
  dictGet('contractor_info_dict', 'name', contractor_id) AS name,
  productivity_score
FROM contractor_daily_metrics;
```

**Impacto**: Reducción del 40-60% en tiempo de JOINs.

---

## 4. Configuración de Infraestructura

### 4.1 Configuración del Servidor ClickHouse

```xml
<!-- /etc/clickhouse-server/config.xml -->
<!-- Aumentar memoria para queries complejas -->
<max_server_memory_usage_to_ram_ratio>0.9</max_server_memory_usage_to_ram_ratio>
<max_memory_usage>20000000000</max_memory_usage> <!-- 20GB para escala masiva -->

<!-- Optimizar para queries analíticas -->
<max_threads>16</max_threads>
<max_insert_threads>8</max_insert_threads>

<!-- Cache de queries -->
<query_cache>
    <max_size>2147483648</max_size> <!-- 2GB -->
    <max_entries>2048</max_entries>
    <max_entry_size>20971520</max_entry_size> <!-- 20MB -->
</query_cache>

<!-- Optimizaciones de merge -->
<merge_tree>
    <max_bytes_to_merge_at_max_space_in_pool>322122547200</max_bytes_to_merge_at_max_space_in_pool>
    <max_bytes_to_merge_at_min_space_in_pool>2147483648</max_bytes_to_merge_at_min_space_in_pool>
</merge_tree>
```

---

### 4.2 Configuración del Cliente ClickHouse

```typescript
// clickhouse.service.ts
this.client = createClient({
  host: `http://${envs.clickhouse.host}:${envs.clickhouse.port}`,
  username: envs.clickhouse.username,
  password: envs.clickhouse.password,
  database: envs.clickhouse.database,
  request_timeout: 600000, // 10 minutos para queries complejas
  max_open_connections: 20, // Aumentar pool
  compression: true, // Comprimir respuestas
  application: 'ADT_MS',
  clickhouse_settings: {
    max_memory_usage: 20000000000, // 20GB por query
    max_threads: 16,
    use_client_time_zone: true,
  },
});
```

---

### 4.3 Configuración de Node.js

```bash
# En package.json scripts
"start": "node --max-old-space-size=8192 dist/main.js"  # 8GB heap
"start:prod": "node --max-old-space-size=16384 dist/main.js"  # 16GB heap
```

**Impacto**: Prevención de heap out of memory.

---

## 5. Monitoreo y Optimización Continua

### 5.1 Habilitar Query Log

```xml
<!-- En configuración del servidor -->
<query_log>
    <database>system</database>
    <table>query_log</table>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_log>
```

### 5.2 Consultar Queries Lentas

```sql
-- Ver las 10 queries más lentas
SELECT
  query,
  query_duration_ms,
  read_rows,
  read_bytes,
  formatReadableSize(read_bytes) AS read_size
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query_duration_ms > 1000
ORDER BY query_duration_ms DESC
LIMIT 10;
```

---

## 📊 Plan de Implementación

### Fase 1: CRÍTICO (Implementar INMEDIATAMENTE)

1. ✅ **Agregaciones pre-calculadas** (Sección 1.4)
   - Crear tabla `contractor_metrics_aggregated`
   - Modificar queries para usar agregaciones
   - **Impacto**: 95% reducción en tiempo

2. ✅ **Procesamiento por streaming** (Sección 2.1)
   - Implementar `streamQuery` en backend
   - Procesar en chunks de 10k
   - **Impacto**: 90% reducción en memoria

3. ✅ **Batching en backend** (Sección 2.2)
   - Limitar a 3-5 contratistas en paralelo
   - Delay de 200ms entre batches
   - **Impacto**: Prevención de colapso de memoria

4. ✅ **Columnas materializadas para JSON** (Sección 1.1)
   - Agregar columnas `app_usage_json`, `browser_json`, `has_app_usage`, `has_browser`
   - **Impacto**: 50-70% reducción en tiempo

### Fase 2: ALTA PRIORIDAD (1-2 semanas)

5. ✅ **Materialized Views agregadas** (Sección 1.3)
   - Crear MVs para AppUsage y BrowserUsage
   - Eliminar procesamiento de JSON en queries
   - **Impacto**: 70% reducción en tiempo

6. ✅ **Cache agresivo** (Sección 3.1)
   - Implementar Redis + ClickHouse cache
   - Cache de 1 hora para métricas
   - **Impacto**: 80% reducción en queries repetidas

7. ✅ **Configuración Node.js y ClickHouse** (Sección 4)
   - Aumentar heap a 8-16GB
   - Optimizar configuración ClickHouse
   - **Impacto**: Prevención de OOM

8. ✅ **Índices secundarios** (Sección 1.2)
   - Agregar skip indexes
   - **Impacto**: 30-50% reducción en filtrado

### Fase 3: MEDIA PRIORIDAD (1 mes)

9. ✅ **Procesamiento incremental** (Sección 2.3)
   - Solo procesar datos nuevos
   - Combinar con cache
   - **Impacto**: 90% reducción en actualizaciones

10. ✅ **Cola de trabajos** (Sección 3.2)
    - Mover cálculos pesados a background
    - Procesamiento asíncrono
    - **Impacto**: Mejora en UX, no bloquea API

11. ✅ **Particionado optimizado** (Sección 1.5)
    - Sharding por contractor_id
    - Optimizar particiones
    - **Impacto**: 50% reducción en escaneo

12. ✅ **Diccionarios para JOINs** (Sección 3.3)
    - Crear diccionarios para datos de referencia
    - **Impacto**: 40-60% reducción en JOINs

---

## ⚠️ Advertencias Importantes

### Sin estas optimizaciones, el sistema NO escalará

- **30 contratistas × 2 meses**: Ya hay problemas
- **500 contratistas × 6 meses**: **50x más datos** = Colapso garantizado sin optimizaciones

### Orden de implementación

**NO implementar todo a la vez**. Seguir el plan de fases:

1. Fase 1 (CRÍTICO) primero
2. Medir impacto
3. Continuar con Fase 2
4. Evaluar necesidad de Fase 3

### Backup y pruebas

- **Backup antes de cambios**: Siempre hacer backup de datos antes de aplicar modificaciones estructurales
- **Probar en desarrollo**: Aplicar cambios primero en ambiente de desarrollo
- **Monitorear impacto**: Usar `system.query_log` para medir mejoras
- **Considerar downtime**: Algunas operaciones (ALTER TABLE) pueden requerir tiempo de inactividad

---

## Referencias

- [ClickHouse Documentation - Performance Tips](https://clickhouse.com/docs/en/guides/best-practices/performance-tuning)
- [ClickHouse Documentation - Indexes](https://clickhouse.com/docs/en/guides/improving-query-performance/sparse-primary-indexes)
- [ClickHouse Documentation - Materialized Views](https://clickhouse.com/docs/en/sql-reference/statements/create/view#materialized-view)

---

**Última actualización**: Diciembre 2024
