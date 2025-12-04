# Análisis de Viabilidad: Data Warehouse en ClickHouse

## 📋 Resumen Ejecutivo

Este documento analiza la viabilidad de implementar la arquitectura de Data Warehouse propuesta en ClickHouse basándose en los schemas actuales de Prisma y la estructura de datos existente.

**Conclusión:** ✅ **Es viable y compatible con el payload definitivo del agente**, y solo requiere **diseñar bien las tablas ADT y las transformaciones ETL (Postgres → ClickHouse)**.

---

## 🔍 Comparación: Estructura Actual vs Propuesta

### 1. Schema de Prisma - Event (EVENTS_MS)

#### ✅ Lo que SÍ tienes y coincide:

| Campo Propuesta    | Campo Actual       | Estado      |
| ------------------ | ------------------ | ----------- |
| `event_id`         | `id` (cuid)        | ✅ Coincide |
| `contractor_id`    | `contractor_id`    | ✅ Coincide |
| `agent_id`         | `agent_id`         | ✅ Coincide |
| `session_id`       | `session_id`       | ✅ Coincide |
| `agent_session_id` | `agent_session_id` | ✅ Coincide |
| `timestamp`        | `timestamp`        | ✅ Coincide |
| `created_at`       | `created_at`       | ✅ Coincide |
| `payload`          | `payload` (JSON)   | ✅ Coincide |

#### ❌ Lo que NO tienes en el payload actual (ANTES) vs lo que tienes AHORA con el payload definitivo

**Nuevo payload definitivo recibido (ejemplo):**

```json
{
  "contractor_id": "test-contractor-456",
  "agent_id": "test-agent-local-123",
  "payload": {
    "Keyboard": {
      "InactiveTime": 0.47,
      "InputsCount": 4
    },
    "Mouse": {
      "InactiveTime": 0.01,
      "ClicksCount": 1
    },
    "IdleTime": 0.01,
    "AppUsage": {
      "Code": 15.3,
      "Chrome": 0,
      "Edge": 0
    },
    "browser": {
      "teamandes.atlassian.net": 15,
      "www.bing.com": 15,
      "extensions": 15,
      "www.youtube.com": 15,
      "github.com": 15
    }
  },
  "timestamp": "2025-11-27T18:58:30.374244+00:00"
}
```

Con este payload definitivo, la situación cambia así:

| Campo Requerido Propuesta | Estructura Actual               | Diferencia / Estado                             |
| ------------------------- | ------------------------------- | ----------------------------------------------- |
| `keyboard_count` (UInt32) | `Keyboard.InputsCount` (number) | ✅ Ya disponible como conteo de inputs por beat |
| `mouse_clicks` (UInt32)   | `Mouse.ClicksCount` (number)    | ✅ Ya disponible como conteo de clicks por beat |

Además, el nuevo campo:

- `browser: { [hostname: string]: seconds }`

permite construir métricas ricas de **uso de web por dominio/pestaña**, especialmente si en el ETL se calculan **diferencias entre beats** para saber cuánto tiempo adicional se sumó a cada dominio en cada intervalo.

### 2. Schema de Prisma - Otras Tablas (USER_MS)

#### ✅ Tablas RAW disponibles:

| Tabla Propuesta       | Tabla Actual Prisma | Estado        | Mapeo                                                                             |
| --------------------- | ------------------- | ------------- | --------------------------------------------------------------------------------- |
| `sessions_raw`        | `Session`           | ✅ Disponible | Directo (session_id, contractor_id, session_start, session_end, total_duration)   |
| `agent_sessions_raw`  | `AgentSession`      | ✅ Disponible | Directo (id, contractor_id, agent_id, session_start, session_end, total_duration) |
| `contractor_info_raw` | `Contractor`        | ✅ Disponible | Directo + enriquecimiento con Client, Team                                        |

#### ✅ Relaciones disponibles:

- `Contractor` → `Client` (client_id) ✅
- `Contractor` → `Team` (team_id) ✅
- `Contractor` → `ContractorApp` → `Application` (apps permitidas) ✅
- `Agent` → `Contractor` (contractor_id) ✅

---

## 🚨 Problemas Identificados y Soluciones

### Problema 1: Payload del Agente - Métricas Incrementales vs Acumulativas (ACTUALIZADO CON NUEVO PAYLOAD)

**Situación ACTUAL con el payload definitivo:**

- `Keyboard.InactiveTime` + `Keyboard.InputsCount` ✅ → tiempo inactivo y **conteo de teclas por beat**.
- `Mouse.InactiveTime` + `Mouse.ClicksCount` ✅ → tiempo inactivo y **conteo de clicks por beat**.
- `IdleTime` ✅ → indicador de inactividad global del beat.
- `AppUsage` ✅ → duración (acumulada o por intervalo) por aplicación.
- `browser` ✅ → segundos por dominio/pestaña en ese intervalo (o acumulados).

**Propuesta original necesitaba:** `keyboard_count` y `mouse_clicks` por heartbeat.

Con el nuevo payload:

- `keyboard_count` y `mouse_clicks` **ya no requieren aproximaciones**: vienen directos del agente (`Keyboard.InputsCount`, `Mouse.ClicksCount`).

No es necesario agregar campos nuevos al agente para esta parte: el ETL y las MVs pueden trabajar directamente con `Keyboard`, `Mouse`, `IdleTime`, `AppUsage` y `browser`.

### Problema 2: Estructura de `AppUsage` vs `ActiveApplications`

**Actual (conceptual):**

```json
{
  "AppUsage": {
    "Chrome": 450,
    "Word": 1200,
    "Excel": 600
  }
}
```

**Propuesta inicial incluía (ya no requerido):**

- `active_app`: String con el nombre de la app activa en ese momento.
- `active_window`: String con el título de la ventana.

**Solución con el nuevo payload y las decisiones actuales:**

- Mantener `AppUsage` para análisis de duración acumulada por aplicación.
- Usar el campo `browser` para análisis de navegación web por dominio/pestaña.
- No es necesario trackear explícitamente qué app/ventana está activa en cada heartbeat; basta con saber **cuánto tiempo acumulado** tuvo cada app/dominio en la ventana de 15s.

### Problema 3: Métricas de Mouse - Alcance Real

**Actual:** `Mouse.InactiveTime` + `Mouse.ClicksCount`

En la propuesta inicial se contemplaba `mouse_distance` (píxeles recorridos), pero **decidimos no usarlo nunca**, por lo que:

- No es necesario implementarlo en el agente.
- No es necesario modelarlo en las tablas ADT ni en las MVs.

👉 **Conclusión de los Problemas 1–3 con el nuevo payload (y sin mouse_distance / active_window / active_app):**

- `keyboard_count` ✅ cubierto nativamente.
- `mouse_clicks` ✅ cubierto nativamente.
- `AppUsage` + `browser` ✅ permiten obtener tiempos totales por aplicación y por dominio, que es lo que realmente necesitas por heartbeat.

---

## ✅ Viabilidad de las Tablas RAW

### `events_raw`

**Estado:** ✅ **100% Viable**

**Implementación actual (según `RawService.saveEvent` y `EventRawDto`):**

- DTO: `EventRawDto` (`src/raw/dto/event-raw.dto.ts`)
  - `event_id: string`
  - `contractor_id: string`
  - `agent_id: string | null`
  - `session_id: string | null`
  - `agent_session_id: string | null`
  - `timestamp: Date`
  - `payload: string` (JSON string)
  - `created_at: Date`
- Insert real en ClickHouse (`src/raw/raw.service.ts`):
  - Se inserta en la tabla `events_raw` con:
    - `event_id`
    - `contractor_id`
    - `agent_id` (o `null`)
    - `session_id` (o `null`)
    - `agent_session_id` (o `null`)
    - `timestamp`
    - `payload` (si no es string, se hace `JSON.stringify`)
    - `created_at` (tomado desde Postgres, no desde `now()` de ClickHouse)

```sql
CREATE TABLE events_raw (
  event_id String,
  contractor_id String,
  agent_id Nullable(String),
  session_id Nullable(String),
  agent_session_id Nullable(String),
  timestamp DateTime,
  payload String,        -- JSON string (como viene del EVENTS_MS)
  created_at DateTime    -- se respeta el valor enviado desde Postgres
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (contractor_id, timestamp);
```

**Fuente:** `EVENTS_MS.prisma.Event` → `ADT_MS.RawService.saveEvent` → ClickHouse

### `sessions_raw`

**Estado:** ✅ **100% Viable**

**Implementación actual (según `RawService.saveSession` y `SessionRawDto`):**

- DTO: `SessionRawDto` (`src/raw/dto/session-raw.dto.ts`)
  - `session_id: string`
  - `contractor_id: string`
  - `session_start: Date`
  - `session_end: Date | null`
  - `total_duration: number | null`
  - `created_at: Date`
  - `updated_at: Date`
- Insert real en ClickHouse (`src/raw/raw.service.ts`):
  - Se inserta en la tabla `sessions_raw` con:
    - `session_id`
    - `contractor_id`
    - `session_start`
    - `session_end` (o `null`)
    - `total_duration` (o `null`)
    - `created_at`
    - `updated_at`

**Fuente:** `USER_MS.prisma.Session` → `ADT_MS.RawService.saveSession` → ClickHouse

### `agent_sessions_raw`

**Estado:** ✅ **100% Viable**

**Implementación actual (según `RawService.saveAgentSession` y `AgentSessionRawDto`):**

- DTO: `AgentSessionRawDto` (`src/raw/dto/agent-session-raw.dto.ts`)
  - `agent_session_id: string`
  - `contractor_id: string`
  - `agent_id: string`
  - `session_id: string | null`
  - `session_start: Date`
  - `session_end: Date | null`
  - `total_duration: number | null`
  - `created_at: Date`
  - `updated_at: Date`
- Insert real en ClickHouse (`src/raw/raw.service.ts`):
  - Se inserta en la tabla `agent_sessions_raw` con:
    - `agent_session_id`
    - `contractor_id`
    - `agent_id`
    - `session_id` (o `null`)
    - `session_start`
    - `session_end` (o `null`)
    - `total_duration` (o `null`)
    - `created_at`
    - `updated_at`

**Fuente:** `USER_MS.prisma.AgentSession` → `ADT_MS.RawService.saveAgentSession` → ClickHouse

### `contractor_info_raw`

**Estado:** ✅ **100% Viable con Enriquecimiento**

**Implementación actual (según `RawService.saveContractor` y `ContractorRawDto`):**

- DTO: `ContractorRawDto` (`src/raw/dto/contractor-raw.dto.ts`)
  - `contractor_id: string`
  - `name: string`
  - `email: string | null`
  - `job_position: string`
  - `work_schedule_start: string | null`
  - `work_schedule_end: string | null`
  - `country: string | null`
  - `client_id: string`
  - `team_id: string | null`
  - `created_at: Date`
  - `updated_at: Date`
- Insert real en ClickHouse (`src/raw/raw.service.ts`):
  - Se inserta en la tabla `contractor_info_raw` con:
    - `contractor_id`
    - `name`
    - `email` (o `null`)
    - `job_position`
    - `work_schedule_start` (o `null`)
    - `work_schedule_end` (o `null`)
    - `country` (o `null`)
    - `client_id`
    - `team_id` (o `null`)
    - `created_at`
    - `updated_at`

**Fuente:** `USER_MS.prisma.Contractor` (+ joins con Client y Team en Postgres) → `ADT_MS.RawService.saveContractor` → ClickHouse

---

## ✅ Viabilidad de las Tablas ADT

### `contractor_activity_15s`

**Estado:** ✅ **100% Viable con el payload definitivo**

**Semántica de la tabla (definición funcional):**

- Cada fila representa **exactamente lo que pasó en un intervalo de 15 segundos** (un _heartbeat_), no una diferencia contra el beat anterior.
- El agente **siempre mide en ventanas fijas de 15s** y envía un evento por ventana.
- Si no hay conexión, los heartbeats se _stashean_ localmente y se envían más tarde, pero:
  - El `timestamp` del evento refleja **el momento real** del heartbeat.
  - El orden lógico en ClickHouse se define por `timestamp`, **independientemente del orden de llegada**.

**Campos requeridos (versión ajustada a lo que realmente vas a usar):**

- ✅ `contractor_id`, `agent_id`, `session_id`, `agent_session_id` → Disponibles
- ✅ `beat_timestamp` → `timestamp` del evento (inicio o centro del intervalo de 15s)
- ✅ `keyboard_count` → **YA DISPONIBLE** como `Keyboard.InputsCount`
- ✅ `mouse_clicks` → **YA DISPONIBLE** como `Mouse.ClicksCount`
- ✅ `is_idle` → Calculable desde `IdleTime > threshold`

**Materialized View Adaptada al nuevo payload:**

```sql
CREATE MATERIALIZED VIEW mv_events_to_activity
TO contractor_activity_15s AS
SELECT
  contractor_id,
  agent_id,
  session_id,
  agent_session_id,
  timestamp AS beat_timestamp,

  -- Calculado desde payload actual
  if(JSONExtractFloat(payload, 'IdleTime', 0.0) > 0, 1, 0) AS is_idle,

  -- Métricas directas desde el nuevo payload
  toUInt32(JSONExtractFloat(payload, 'Keyboard', 'InputsCount', 0.0)) AS keyboard_count,
  toUInt32(JSONExtractFloat(payload, 'Mouse', 'ClicksCount', 0.0)) AS mouse_clicks,

  toDate(timestamp) AS workday,
  now() AS created_at
FROM events_raw;
```

⚠️ **Nota:** Con el nuevo payload, ya **no se usan aproximaciones para `keyboard_count` ni `mouse_clicks`**.  
No es necesario modelar `active_app`, `mouse_distance` ni `active_window` para las métricas que se van a usar.

### `contractor_daily_metrics`

**Estado:** ✅ **100% Viable** (depende de `contractor_activity_15s`)

Una vez que `contractor_activity_15s` esté poblada, las agregaciones son directas.

### `app_usage_summary`

**Estado:** ✅ **100% Viable con el payload actual (AppUsage + browser)**

Puede usar `AppUsage` directamente del payload, y además el campo `browser` para diferenciar claramente **uso de aplicaciones vs navegación web**:

```sql
CREATE MATERIALIZED VIEW mv_app_usage_summary
TO app_usage_summary AS
SELECT
  contractor_id,
  k AS app_name,  -- Key del diccionario AppUsage
  toDate(timestamp) AS workday,

  -- Aproximar beats activos desde duración (si AppUsage es acumulado)
  round(JSONExtractFloat(payload, 'AppUsage', k) / 15.0) AS active_beats,

  now() AS created_at
FROM events_raw
ARRAY JOIN JSONExtractKeys(payload, 'AppUsage') AS k
WHERE JSONHas(payload, 'AppUsage', k);
```

Y opcionalmente, una MV específica para navegación web:

```sql
CREATE MATERIALIZED VIEW mv_web_usage_summary
TO web_usage_summary AS
SELECT
  contractor_id,
  host AS domain,  -- dominio / pestaña del navegador
  toDate(timestamp) AS workday,
  JSONExtractFloat(payload, 'browser', host) AS seconds_in_tab,
  now() AS created_at
FROM events_raw
ARRAY JOIN JSONExtractKeys(payload, 'browser') AS host
WHERE JSONHas(payload, 'browser', host);
```

### `session_summary`

**Estado:** ✅ **100% Viable**

Agregaciones desde `contractor_activity_15s` agrupando por `session_id`.

---

## 🔄 Estrategia de Implementación Recomendada

### Fase 1: Implementación Inmediata (Con Datos Actuales)

1. ✅ Crear tablas RAW en ClickHouse
2. ✅ Implementar ETL `Postgres → ClickHouse` para eventos, sesiones, agent_sessions
3. ✅ Crear `contractor_activity_15s` usando directamente `Keyboard.InputsCount`, `Mouse.ClicksCount`, `IdleTime` (semántica de heartbeat de 15s)
4. ✅ Crear agregaciones diarias básicas (`contractor_daily_metrics`, `session_summary`, etc.)
5. ✅ Implementar `productivity_score` basado en idle vs activo + conteos de teclado/mouse + uso de apps/web

### Fase 2: Mejora del Agente (Opcional)

A la luz de las decisiones actuales, **no hay requisitos duros de cambio en el agente** para soportar las métricas de ADT; cualquier mejora sería solo para enriquecer metadatos, pero no es necesaria para la viabilidad del modelo.

### Fase 3: Optimización

1. Calibrar `productivity_score` con datos reales (por cliente/rol si hace falta)
2. Ajustar pesos de las métricas y umbrales
3. Agregar alertas y detección de anomalías
4. Optimizar queries y dashboards (índices, proyecciones, dimensiones de apps/domains)

---

## 📊 Adaptaciones Necesarias en la Propuesta

### Cambios en Materialized Views

Las MVs propuestas deben adaptarse para trabajar con la estructura real del payload:

- `Keyboard.InactiveTime` / `Keyboard.InputsCount`
- `Mouse.InactiveTime` / `Mouse.ClicksCount`
- `IdleTime`
- `AppUsage` dict
- `browser` dict

### Cambios en Cálculo de Productividad

**Productividad simplificada (Fase 1):**

```sql
-- Basado solo en IdleTime
productivity_score = 100 * (1 - (total_idle_time / total_time))
```

**Productividad completa (Fase 2) con el nuevo payload:**

Con el payload definitivo (`Keyboard`, `Mouse`, `IdleTime`, `AppUsage`, `browser`), es posible definir un **productivity_score 100% soportado por datos reales**, por ejemplo combinando:

- `keyboard_count` (desde `Keyboard.InputsCount`).
- `mouse_clicks` (desde `Mouse.ClicksCount`).
- Proporción de tiempo activo vs idle.
- Tiempo en apps/dominos considerados “productivos” vs “no productivos” (usando dimensiones de apps/domains).

El trabajo de Fase 2 es principalmente de **calibración de pesos y umbrales**, no de cambios en payload.

### Cambios en Queries de Dashboard

Los queries deben adaptarse a la estructura actual:

```sql
SELECT
  k AS app_name,
  sum(JSONExtractFloat(payload, 'AppUsage', k)) AS total_duration
FROM events_raw
ARRAY JOIN JSONExtractKeys(payload, 'AppUsage') AS k
GROUP BY k
```

---

## ✅ Checklist de Viabilidad

| Componente                   | Viabilidad | Requisitos                                                                             |
| ---------------------------- | ---------- | -------------------------------------------------------------------------------------- |
| **Tablas RAW**               | ✅ 100%    | Solo ETL Postgres → ClickHouse                                                         |
| **events_raw**               | ✅ 100%    | Directo                                                                                |
| **sessions_raw**             | ✅ 100%    | Directo                                                                                |
| **agent_sessions_raw**       | ✅ 100%    | Directo                                                                                |
| **contractor_info_raw**      | ✅ 100%    | JOIN con Client/Team                                                                   |
| **Tablas ADT**               | ✅ 100%    | Definición funcional cerrada + MVs implementadas sobre payload definitivo              |
| **contractor_activity_15s**  | ✅ 100%    | Heartbeats de 15s con `keyboard_count`, `mouse_clicks`, `is_idle` + joins con sesiones |
| **contractor_daily_metrics** | ✅ 100%    | Agregaciones sobre contractor_activity_15s                                             |
| **app_usage_summary**        | ✅ 100%    | Agregaciones sobre AppUsage + browser (con posibilidad de dimensiones de apps/domains) |
| **session_summary**          | ✅ 100%    | Agregaciones sobre contractor_activity_15s y sesiones                                  |
| **ETL Pipelines**            | ✅ 100%    | Implementar consumers NATS → ClickHouse                                                |
| **Productivity Score**       | ✅ 100%    | Fórmula basada en teclado/mouse/idle/AppUsage/browser + calibración de negocio         |
| **Arquitectura**             | ✅ 100%    | NATS ya existe, agregar ClickHouse                                                     |

---

## 🎯 Recomendación Final

**✅ La propuesta ES VIABLE al 100% con el payload definitivo**, y recomiendo:

1. **Implementar Fase 1 inmediatamente**: crear tablas RAW y ADT, MVs (`contractor_activity_15s`, `app_usage_summary`, `web_usage_summary`, `session_summary`) y un primer `productivity_score` funcional.
2. **Ejecutar Fase 3 de Optimización** tras unas semanas de datos: calibrar el `productivity_score`, ajustar pesos/umbrales y optimizar dashboards.

**Beneficios de Fase 1:**

- Dashboard funcional con métricas completas (actividad 15s, uso de apps/web, sesiones).
- Productividad calculable desde el día 1 (aunque con pesos iniciales).

**Beneficios de Fase 3:**

- Precisión mejorada en el cálculo de productividad (calibrado con datos reales).
- Queries y dashboards más optimizados sobre ClickHouse.

---

## 📝 Próximos Pasos

1. ✅ Validar este análisis con el equipo
2. ✅ Decidir si implementar Fase 1 con aproximaciones o esperar Fase 2
3. ✅ Diseñar arquitectura ETL (NATS → ClickHouse)
4. ✅ Crear scripts de migración de datos históricos
5. ✅ Definir calendario de implementación
