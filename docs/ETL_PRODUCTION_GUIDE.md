# Guía de ETL en Producción

## 📋 Resumen Ejecutivo

Esta guía explica:

- ✅ **Frecuencia recomendada** de ejecución de ETL en producción
- ✅ **Cómo funcionan los engines** de ClickHouse y la deduplicación
- ✅ **Estrategias para evitar duplicados**
- ✅ **Mejores prácticas** para producción

---

## 🔄 Frecuencia Recomendada de Ejecución (Modelo Híbrido)

### Estrategia Recomendada para Producción (con Materialized Views activas)

Con las Materialized Views definidas en `scripts/create-materialized-views.sql`:

- `mv_events_to_activity` pobla `contractor_activity_15s` desde `events_raw` en tiempo real.
- `mv_app_usage_summary` pobla `app_usage_summary` desde `events_raw` en tiempo real.

En este esquema híbrido:

- **No es necesario ejecutar continuamente** los ETL de eventos (`process-events`, `process-app-usage`) para el flujo normal.
- Esos ETL quedan como **herramientas de backfill / corrección histórica**.

| Proceso                                                                 | Tipo                   | Frecuencia                                        | Cuándo Ejecutar                      | Razón                                                                                                                   |
| ----------------------------------------------------------------------- | ---------------------- | ------------------------------------------------- | ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------- |
| **Materialized Views `mv_events_to_activity` / `mv_app_usage_summary`** | Incremental automático | N/A (se disparan en cada `INSERT` a `events_raw`) | Siempre activas en producción        | Mantener `contractor_activity_15s` y `app_usage_summary` actualizados en tiempo real                                    |
| **`process-daily-metrics`**                                             | ETL batch              | **1 vez al día**                                  | A las **2:00 AM** (día anterior)     | Consolidar métricas diarias (`contractor_daily_metrics`) usando los beats ya calculados                                 |
| **`process-session-summaries`**                                         | ETL batch              | **Cada hora** o cuando se cierra una sesión       | Durante horario laboral              | Generar/actualizar resúmenes de sesión en `session_summary`                                                             |
| **`process-events`**                                                    | ETL batch (backfill)   | Solo cuando se necesite backfill/corrección       | Bajo demanda / fuera de horario pico | Recalcular `contractor_activity_15s` para un rango de fechas específico usando `ALTER TABLE ... DELETE + INSERT SELECT` |
| **`process-app-usage`**                                                 | ETL batch (backfill)   | Solo cuando se necesite backfill/corrección       | Bajo demanda / fuera de horario pico | Recalcular `app_usage_summary` para un rango de fechas específico usando `ALTER TABLE ... DELETE + INSERT SELECT`       |

### Ejemplo de Cron Schedule (con MVs)

```bash
# Métricas diarias: consolidar día anterior (2 AM)
0 2 * * *  curl -X GET "http://localhost:3001/adt/etl/process-daily-metrics" -H "Authorization: Bearer $TOKEN"

# Opcional: Procesar rango de días para backfill histórico (ej: primeros 7 días del mes anterior)
# 0 3 1 * *  curl -X GET "http://localhost:3001/adt/etl/process-daily-metrics?from=$(date -d 'first day of last month' +\%Y-\%m-\%d)&to=$(date -d 'last day of last month' +\%Y-\%m-\%d)" -H "Authorization: Bearer $TOKEN"

# Resúmenes de sesión cada hora
0 * * * *  curl -X GET "http://localhost:3001/adt/etl/process-session-summaries" -H "Authorization: Bearer $TOKEN"

# Opcional: jobs de backfill (DESACTIVADOS por defecto, solo habilitar cuando se necesiten)

## Reprocesar beats (contractor_activity_15s) para un rango concreto (ejemplo: día anterior)
# 30 1 * * *  curl -X GET "http://localhost:3001/adt/etl/process-events?from=$(date -d 'yesterday 00:00:00' --iso-8601=seconds)&to=$(date -d 'yesterday 23:59:59' --iso-8601=seconds)" -H "Authorization: Bearer $TOKEN"

## Reprocesar app_usage_summary para un rango concreto
# 40 1 * * *  curl -X GET "http://localhost:3001/adt/etl/process-app-usage?from=$(date -d 'yesterday 00:00:00' --iso-8601=seconds)&to=$(date -d 'yesterday 23:59:59' --iso-8601=seconds)" -H "Authorization: Bearer $TOKEN"
```

---

## 🛡️ Protección Contra Duplicados

### Cómo Funcionan los Engines de ClickHouse

#### 1. **MergeTree** (Sin deduplicación automática)

**Tablas que usan MergeTree:**

- `contractor_activity_15s`
- `session_summary`

**Comportamiento:**

- ❌ **NO suma valores duplicados automáticamente**
- ✅ **Permite múltiples filas** con la misma clave de orden
- ⚠️ **Si ejecutas el ETL dos veces con los mismos eventos, crearás duplicados**

**Ejemplo:**

```sql
-- Si ejecutas process-events dos veces con los mismos eventos:
-- Primera ejecución: Inserta 1000 beats
-- Segunda ejecución: Inserta otros 1000 beats (duplicados)
-- Resultado: 2000 filas (1000 duplicadas)
```

#### 2. **SummingMergeTree** (Suma automática)

**Tablas que usan SummingMergeTree:**

- `contractor_daily_metrics`
- `app_usage_summary`

**Comportamiento:**

- ✅ **Suma automáticamente** valores numéricos cuando hay filas con la misma clave de orden
- ✅ **Protege contra duplicados** en agregaciones
- ⚠️ **La suma ocurre durante merges de partes**, no inmediatamente

**Ejemplo:**

```sql
-- Si ejecutas process-daily-metrics dos veces para el mismo día:
-- Primera ejecución: Inserta {contractor_id: "123", workday: "2025-01-15", total_beats: 1000}
-- Segunda ejecución: Inserta {contractor_id: "123", workday: "2025-01-15", total_beats: 1000}
-- Resultado después del merge: {contractor_id: "123", workday: "2025-01-15", total_beats: 2000}
-- ⚠️ Esto es INCORRECTO porque suma valores duplicados
```

---

## ⚠️ Problema: Duplicados en ETL

### Escenario Problemático

Si ejecutas el mismo ETL dos veces con los mismos datos:

1. **`contractor_activity_15s` (MergeTree)**: Crea filas duplicadas
2. **`contractor_daily_metrics` (SummingMergeTree)**: Suma valores duplicados (incorrecto)
3. **`app_usage_summary` (SummingMergeTree)**: Suma valores duplicados (incorrecto)
4. **`session_summary` (MergeTree)**: Crea filas duplicadas

### Soluciones

#### Solución 1: **Idempotencia por día/sesión (sin DELETE)** ✅ (Actual)

Los procesos ETL fueron actualizados para:

- Procesar por día/sesión y verificar existencia antes de insertar.
- Omitir días/sesiones ya procesados (logs: “Skipping ... (already populated)”).
- Insertar únicamente lo faltante.

Para recomputar, borrar explícitamente el objetivo:

```sql
ALTER TABLE contractor_daily_metrics DELETE WHERE workday = toDate('YYYY-MM-DD');
ALTER TABLE contractor_activity_15s DELETE WHERE workday = toDate('YYYY-MM-DD');
ALTER TABLE session_summary DELETE WHERE session_id = 'session-xyz';
```

#### Solución 2: **Filtrado por Rango de Fechas**

Ejecutar ETL solo para eventos **nuevos** o **no procesados**:

```typescript
// En EtlService.processEventsToActivity()
// Solo procesar eventos que NO estén ya en contractor_activity_15s

async processEventsToActivity(fromDate?: Date, toDate?: Date) {
  // Si no se especifica rango, usar solo eventos de las últimas 2 horas
  if (!fromDate) {
    fromDate = new Date(Date.now() - 2 * 60 * 60 * 1000); // 2 horas atrás
  }

  // Verificar qué eventos ya fueron procesados
  const query = `
    SELECT DISTINCT contractor_id, timestamp
    FROM contractor_activity_15s
    WHERE beat_timestamp >= '${this.formatDate(fromDate)}'
      AND beat_timestamp <= '${this.formatDate(toDate || new Date())}'
  `;

  const processed = await this.clickHouseService.query(query);
  const processedSet = new Set(
    processed.map(p => `${p.contractor_id}__${p.timestamp}`)
  );

  // Filtrar eventos ya procesados
  const events = await this.clickHouseService.query<EventRawDto>(`
    SELECT * FROM events_raw
    WHERE timestamp >= '${this.formatDate(fromDate)}'
      AND timestamp <= '${this.formatDate(toDate || new Date())}'
  `);

  const newEvents = events.filter(e => {
    const key = `${e.contractor_id}__${e.timestamp}`;
    return !processedSet.has(key);
  });

  // Procesar solo eventos nuevos
  // ...
}
```

#### Solución 2: **Usar Materialized Views** (Más Eficiente)

ClickHouse puede procesar automáticamente cuando se insertan datos nuevos:

```sql
-- Crear Materialized View que procesa automáticamente
CREATE MATERIALIZED VIEW mv_events_to_activity
TO contractor_activity_15s
AS
SELECT
  contractor_id,
  agent_id,
  session_id,
  agent_session_id,
  timestamp AS beat_timestamp,
  -- ... transformaciones
FROM events_raw
WHERE timestamp >= now() - INTERVAL 2 HOUR; -- Solo eventos recientes
```

**Ventajas:**

- ✅ Procesamiento automático en tiempo real
- ✅ No requiere ejecutar ETL manualmente
- ✅ Evita duplicados (solo procesa eventos nuevos)

**Desventajas:**

- ⚠️ Requiere configurar las Materialized Views
- ⚠️ Más complejo de mantener

#### Solución 3: **Marcar Eventos Procesados** (Más Simple)

Agregar una columna `processed` a `events_raw`:

```sql
ALTER TABLE events_raw ADD COLUMN processed UInt8 DEFAULT 0;
```

```typescript
// En EtlService
async processEventsToActivity() {
  // Solo procesar eventos no procesados
  const events = await this.clickHouseService.query(`
    SELECT * FROM events_raw
    WHERE processed = 0
    ORDER BY timestamp
    LIMIT 10000
  `);

  // Procesar eventos...

  // Marcar como procesados
  const eventIds = events.map(e => e.event_id);
  await this.clickHouseService.command(`
    ALTER TABLE events_raw
    UPDATE processed = 1
    WHERE event_id IN (${eventIds.map(id => `'${id}'`).join(',')})
  `);
}
```

---

## 📊 Estrategia Recomendada para Producción

### Opción A: ETL Incremental con Filtrado (Recomendado para empezar)

**Ventajas:**

- ✅ Simple de implementar
- ✅ Control total sobre cuándo se ejecuta
- ✅ Fácil de debuggear

**Implementación:**

1. Modificar `EtlService` para filtrar eventos ya procesados
2. Ejecutar ETL cada 15-30 minutos solo para eventos nuevos
3. Usar rangos de fechas para evitar reprocesar datos antiguos

### Opción B: Materialized Views (Recomendado para escalar)

**Ventajas:**

- ✅ Procesamiento automático en tiempo real
- ✅ No requiere ejecutar ETL manualmente
- ✅ Más eficiente para grandes volúmenes

**Implementación:**

1. Crear Materialized Views para cada transformación
2. Los eventos se procesan automáticamente al insertarse
3. ETL manual solo para reprocesar datos históricos

### Opción C: Híbrida (Mejor de ambos mundos) — Modelo actual

**Ventajas:**

- ✅ Materialized Views para tiempo real (`events_raw → contractor_activity_15s` y `events_raw → app_usage_summary`)
- ✅ ETL manual para consolidación diaria (`contractor_daily_metrics`, `session_summary`)
- ✅ ETL manual como herramienta de backfill / corrección con `ALTER TABLE ... DELETE + INSERT SELECT`

**Implementación (tal como está implementado en el código):**

1. **Activar Materialized Views** ejecutando `scripts/create-materialized-views.sql` en ClickHouse.
2. **Insertar eventos** en `events_raw` desde `EVENTS_MS` / agentes; las MVs poblan automáticamente `contractor_activity_15s` y `app_usage_summary`.
3. **Ejecutar diariamente** `process-daily-metrics` (2 AM) para poblar `contractor_daily_metrics` desde `contractor_activity_15s` + dimensiones.
4. **Ejecutar periódicamente** `process-session-summaries` (ej. cada hora) para poblar `session_summary` desde `contractor_activity_15s` + `events_raw` + dimensiones.
5. **Usar `process-events` y `process-app-usage` solo para backfill/corrección**, aprovechando que ambos:
   - Borran primero el rango objetivo con `ALTER TABLE ... DELETE`.
   - Luego recalculan con `INSERT ... SELECT` desde `events_raw`, evitando duplicados.

---

## 🎯 Mejores Prácticas

### 1. **Nunca Ejecutar ETL Sin Filtros de Fecha**

❌ **Malo:**

```bash
GET /adt/etl/process-events  # Procesa TODOS los eventos (puede crear duplicados)
```

✅ **Bueno:**

```bash
GET /adt/etl/process-events?from=2025-01-15T08:00:00&to=2025-01-15T08:30:00
```

### 2. **Usar Rangos de Tiempo Incrementales**

Ejecutar ETL solo para el último período (ej: últimas 2 horas):

```typescript
// En el scheduler
const twoHoursAgo = new Date(Date.now() - 2 * 60 * 60 * 1000);
const now = new Date();

await etlService.processEventsToActivity(twoHoursAgo, now);
```

### 3. **Verificar Antes de Reprocesar**

Si necesitas reprocesar datos históricos:

```bash
# 1. Verificar qué datos ya existen
SELECT COUNT(*) FROM contractor_activity_15s
WHERE beat_timestamp >= '2025-01-15' AND beat_timestamp < '2025-01-16';

# 2. Si hay datos, eliminar antes de reprocesar
ALTER TABLE contractor_activity_15s DELETE
WHERE beat_timestamp >= '2025-01-15' AND beat_timestamp < '2025-01-16';

# 3. Reprocesar
GET /adt/etl/process-events?from=2025-01-15&to=2025-01-15
```

### 4. **Monitorear Duplicados**

Crear una query para detectar duplicados:

```sql
-- Detectar beats duplicados en contractor_activity_15s
SELECT
  contractor_id,
  beat_timestamp,
  COUNT(*) as duplicates
FROM contractor_activity_15s
GROUP BY contractor_id, beat_timestamp
HAVING duplicates > 1
ORDER BY duplicates DESC
LIMIT 100;

-- Detectar métricas duplicadas en contractor_daily_metrics
SELECT
  contractor_id,
  workday,
  COUNT(*) as duplicates,
  SUM(total_beats) as total_beats_sum
FROM contractor_daily_metrics
GROUP BY contractor_id, workday
HAVING duplicates > 1
ORDER BY duplicates DESC
LIMIT 100;
```

### 5. **Usar `FINAL` en Queries para SummingMergeTree**

Cuando consultes tablas con `SummingMergeTree`, usa `FINAL` para obtener valores sumados:

```sql
-- Sin FINAL: Puede mostrar valores no sumados
SELECT * FROM contractor_daily_metrics
WHERE contractor_id = '123' AND workday = '2025-01-15';

-- Con FINAL: Muestra valores sumados correctamente
SELECT * FROM contractor_daily_metrics FINAL
WHERE contractor_id = '123' AND workday = '2025-01-15';
```

**Nota:** `FINAL` es más lento, úsalo solo cuando necesites valores exactos.

---

## 📈 Frecuencia por Escenario

### Escenario 1: 50-100 Contractors

**Recomendación:**

- `process-events`: Cada 30 minutos
- `process-app-usage`: Cada 30 minutos
- `process-daily-metrics`: 1 vez al día (2 AM)
- `process-session-summaries`: Cada hora

### Escenario 2: 200-500 Contractors

**Recomendación:**

- `process-events`: Cada 15 minutos
- `process-app-usage`: Cada 15 minutos
- `process-daily-metrics`: 1 vez al día (2 AM)
- `process-session-summaries`: Cada hora

### Escenario 3: 500+ Contractors

**Recomendación:**

- Usar **Materialized Views** para procesamiento automático
- ETL manual solo para consolidación diaria
- Considerar particionado adicional y optimizaciones

---

## 🔍 Detección y Corrección de Duplicados

### Script para Detectar Duplicados

```sql
-- 1. Detectar beats duplicados
SELECT
  contractor_id,
  beat_timestamp,
  COUNT(*) as count
FROM contractor_activity_15s
GROUP BY contractor_id, beat_timestamp
HAVING count > 1
ORDER BY count DESC;

-- 2. Detectar métricas duplicadas (antes del merge)
SELECT
  contractor_id,
  workday,
  COUNT(*) as count,
  SUM(total_beats) as total_beats_sum
FROM contractor_daily_metrics
GROUP BY contractor_id, workday
HAVING count > 1
ORDER BY count DESC;
```

### Script para Eliminar Duplicados

```sql
-- Eliminar beats duplicados (mantener solo el primero)
ALTER TABLE contractor_activity_15s DELETE
WHERE (contractor_id, beat_timestamp) IN (
  SELECT contractor_id, beat_timestamp
  FROM (
    SELECT
      contractor_id,
      beat_timestamp,
      ROW_NUMBER() OVER (PARTITION BY contractor_id, beat_timestamp ORDER BY created_at) as rn
    FROM contractor_activity_15s
  )
  WHERE rn > 1
);

-- Para SummingMergeTree, forzar merge para consolidar
OPTIMIZE TABLE contractor_daily_metrics FINAL;
OPTIMIZE TABLE app_usage_summary FINAL;
```

---

## ✅ Checklist para Producción

- [ ] Configurar scheduler (cron) con frecuencias recomendadas
- [ ] Implementar filtrado de eventos ya procesados en ETL
- [ ] Agregar monitoreo de duplicados (alertas si se detectan)
- [ ] Documentar procedimiento para reprocesar datos históricos
- [ ] Configurar backups antes de reprocesar
- [ ] Usar `FINAL` en queries críticas de SummingMergeTree
- [ ] Monitorear performance de ETL (tiempo de ejecución, volumen procesado)
- [ ] Configurar alertas si ETL falla o tarda demasiado

---

## 📚 Referencias

- [ClickHouse MergeTree Engines](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree)
- [ClickHouse SummingMergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/summingmergetree)
- [ClickHouse Materialized Views](https://clickhouse.com/docs/en/sql-reference/statements/create/view#materialized-view)
