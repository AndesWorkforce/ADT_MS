# Análisis: Triggers ETL automáticos al terminar una sesión

Este documento analiza cómo funciona el trigger de ETL al cerrar una sesión, cómo se procesan los tres ETL (`process-events`, `process-daily-metrics`, `process-session-summaries`) y **qué hay que modificar** para que, al cerrar una sesión, las métricas se procesen y el **% de productividad** esté disponible en reportes y en la página.

**Alcance del análisis:** ADT_MS (listeners, colas, ETL), rutas HTTP en API Gateway y dependencias entre ETL. **Sin modificar código**; solo diagnóstico y propuestas.

---

## 1. Rutas HTTP de los tres ETL (API Gateway)

Las tres rutas que mencionas están expuestas en el **API Gateway** y delegan en ADT_MS vía NATS:

| Ruta HTTP ({{base_url}}/adt/etl/...)                                    | Patrón NATS (ADT_MS)          | Descripción                                                                                                                        |
| ----------------------------------------------------------------------- | ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `GET /adt/etl/process-events?from=&to=`                                 | `adt.processEvents`           | Lee `events_raw` y escribe en `contractor_activity_15s` (beats de 15s). Sin rango: últimas 2h.                                     |
| `GET /adt/etl/process-daily-metrics` (opcional `workday`, `from`, `to`) | `adt.processDailyMetrics`     | Lee `contractor_activity_15s` y escribe en `contractor_daily_metrics` (métricas diarias consolidadas).                             |
| `GET /adt/etl/process-session-summaries?sessionId=`                     | `adt.processSessionSummaries` | Lee `contractor_activity_15s` + `events_raw` y escribe en `session_summary` (una fila por sesión/agente con `productivity_score`). |

- **Quién puede llamarlas:** actualmente `@Roles(Role.Superadmin)` en el controller del Gateway.
- **Comportamiento en ADT_MS:**
  - `processEvents`: se ejecuta **síncrono** en el listener (no usa cola).
  - `processDailyMetrics` y `processSessionSummaries`: si `USE_ETL_QUEUE=true`, **encolan** un job en BullMQ y responden “job queued”; si no, ejecutan el ETL **síncrono** en el listener.

---

## 2. Cómo funciona el trigger al cerrar una sesión

### 2.1 Origen del evento (fuera de ADT_MS)

El flujo documentado en código asume que **EVENTS_MS** (u otro servicio que gestione el fin de sesión) emite por NATS el evento:

- **Patrón:** `etl.session.trigger`
- **Payload esperado:** `{ sessionId, contractorId, triggeredAt, triggerReason: 'timeout' | 'explicit' }`

En este repositorio **no está implementado el emisor**; solo está el consumidor en ADT_MS. Quien cierra la sesión (timeout de inactividad o fin de turno explícito) debe publicar ese evento para que ADT_MS reaccione.

### 2.2 Cadena en ADT_MS (desde el evento hasta session_summary)

1. **EtlTriggerListener** (`src/listeners/etl-trigger.listener.ts`)
   - Escucha `etl.session.trigger`.
   - Si `EtlQueueService` no está disponible (por ejemplo `USE_ETL_QUEUE=false`), solo loguea una advertencia y **no encola nada**.
   - Si está disponible, llama a `EtlQueueService.addSessionSummaryJob(sessionId, contractorId)`.

2. **EtlQueueService** (`src/queues/services/etl-queue.service.ts`)
   - Añade un job a la cola **`adt-etl-session-summaries`** con:
     - `jobId` determinista: `session-summary-${sessionId}` (evita duplicados).
     - Datos: `{ jobType: SESSION_SUMMARIES, sessionId, contractorId, requestedAt }`.
   - No encola jobs de `process-events` ni de `process-daily-metrics`.

3. **SessionSummaryProcessor** (`src/queues/processors/session-summary.processor.ts`)
   - Consume jobs de `adt-etl-session-summaries` (concurrencia 1).
   - Llama a `EtlService.processActivityToSessionSummary(sessionId)`.
   - Reintentos: 3 con backoff exponencial (configuración por defecto de BullMQ).

4. **EtlService.processActivityToSessionSummary** (`src/etl/services/etl.service.ts`)
   - **Lee:** `contractor_activity_15s` (beats) y `events_raw` (app/browser para la fórmula de productividad).
   - **Escribe:** `session_summary` (session_id, contractor_id, agent_id, session_start, session_end, total_seconds, active_seconds, idle_seconds, **productivity_score**, created_at).
   - Excluye pares (session_id, agent_id) que ya existen en `session_summary` (idempotencia).
   - El **productivity_score** se calcula con la misma fórmula multi-factor (actividad, teclado/ratón, app, browser) que en el ETL.

**Resumen del trigger actual:** al recibir `etl.session.trigger`, ADT_MS **solo** encola (o ejecuta) el ETL de **session summaries**. No dispara ni `process-events` ni `process-daily-metrics`.

---

## 3. Dependencias entre los tres ETL y fuentes de datos

Flujo de datos relevante para productividad y reportes:

```
event.created (NATS)
    → EventsListener → EventQueueService / RawService
        → events_raw (ClickHouse)   [siempre en tiempo real vía cola o directo]

GET /adt/etl/process-events (manual o cron)
    → events_raw → contractor_activity_15s

contractor_activity_15s + events_raw
    → GET /adt/etl/process-session-summaries (o job encolado por etl.session.trigger)
        → session_summary (productivity_score por sesión)

contractor_activity_15s
    → GET /adt/etl/process-daily-metrics (manual o cron)
        → contractor_daily_metrics
```

- **RealtimeMetricsService** (`src/etl/services/realtime-metrics.service.ts`): calcula métricas en tiempo real para el dashboard leyendo **contractor_activity_15s** (y uso de apps/browser desde `events_raw`). No escribe en `session_summary` ni en `contractor_daily_metrics`.
- **Reportes y página:** los reportes de detalle y listados de sesiones usan principalmente **session_summary** (y endpoints que leen de ahí: resúmenes por día, productividad por hora, etc.). El **% de productividad** que se ve por sesión viene de `session_summary.productivity_score`.

Conclusión importante: **session_summary depende de que `contractor_activity_15s` ya tenga los beats de esa sesión.** Esos beats solo se generan a partir de `events_raw` mediante **process-events** (o `processEventsToActivity`). No hay otro camino en el código actual que llene `contractor_activity_15s` desde los eventos.

---

## 4. Problema actual: por qué puede no verse el % de productividad al cerrar sesión

1. **Solo se dispara session summary**
   - El trigger `etl.session.trigger` solo encola (o ejecuta) **process-session-summaries** para esa `sessionId`.
   - No se ejecuta **process-events** ni **process-daily-metrics** en ese momento.

2. **Session summary necesita beats en contractor_activity_15s**
   - `processActivityToSessionSummary(sessionId)` hace un `INSERT ... SELECT` desde `contractor_activity_15s` (y JOINs con `events_raw`).
   - Si **process-events** no se ha ejecutado para el rango de tiempo de esa sesión, `contractor_activity_15s` no tendrá filas para esa sesión y el ETL no insertará nada en `session_summary` (o insertará filas vacías/incompletas). Resultado: no se ve el % de productividad para esa sesión en reportes.

3. **Orden lógico necesario**
   - Para que el % de productividad aparezca al cerrar la sesión, hace falta que **antes** de (o como parte de) el procesamiento de esa sesión:
     - Los eventos de esa sesión estén en `events_raw` (ya ocurre en tiempo real).
     - Esos eventos se hayan convertido en beats en `contractor_activity_15s` (**process-events** para el rango de esa sesión o del día).
   - Después, **process-session-summaries** puede calcular y guardar `productivity_score` en `session_summary`.

4. **process-daily-metrics**
   - Actualiza `contractor_daily_metrics` (totales del día). Es útil para vistas diarias o reportes agregados por día, pero **no es obligatorio** para que aparezca el % de productividad **por sesión** en la lista de sesiones y en el detalle del reporte. Ese dato sale de `session_summary`.

---

## 5. Qué se tiene que modificar (resumen)

Para que, **al cerrar una sesión**, las métricas se procesen y el **% de productividad** esté disponible en reportes y en la página, hace falta lo siguiente (sin implementar aquí, solo definido):

### 5.1 Emisor del evento `etl.session.trigger`

- **Dónde:** en el servicio que detecta el cierre de sesión (p. ej. EVENTS_MS: timer de inactividad o fin de turno).
- **Qué hacer:** al cerrar una sesión, publicar en NATS el evento `etl.session.trigger` con `{ sessionId, contractorId, triggeredAt, triggerReason }`.
- **Estado:** el listener en ADT_MS ya está preparado; falta garantizar que este evento se emita en todos los flujos de cierre de sesión.

### 5.2 Asegurar que existan beats antes de session summary

Hoy el trigger solo lanza el ETL de session summaries. Para que ese ETL tenga datos, una de estas opciones (o una combinación):

- **Opción A – Cron frecuente de process-events**
  - Ejecutar `GET /adt/etl/process-events` cada X minutos (p. ej. 5–15) para un rango reciente (última 1–2 horas), de forma que cuando llegue `etl.session.trigger`, `contractor_activity_15s` ya tenga los beats de la sesión recién cerrada.
  - No requiere cambios en el trigger; solo asegura que el orden temporal sea correcto (el cron “va por delante” del cierre de sesión).

- **Opción B – Encadenar en el trigger (recomendado para “al cerrar sesión”)**
  - Al recibir `etl.session.trigger`, antes de encolar (o ejecutar) el job de session summary:
    1. Ejecutar **process-events** para un rango que cubra la sesión recién cerrada (p. ej. últimas 2 horas del día actual, o desde `session_start` hasta “ahora” si el payload incluye esas fechas).
    2. Luego encolar/ejecutar **process-session-summaries** para esa `sessionId`.
  - Posibles implementaciones:
    - En **EtlTriggerListener**: antes de `addSessionSummaryJob`, llamar a `EtlService.processEventsToActivity(from, to)` para un rango reciente (o recibir `sessionStart`/`sessionEnd` en el payload y usarlo). Después llamar a `addSessionSummaryJob`.
    - O encolar primero un job “process-events” para un rango fijo (ej. última hora) y, al completarse ese job, encolar el job de session summary (requiere una cola o un job compuesto que encadene los dos).
  - Así se garantiza que, cuando corre session summary, los beats ya están en `contractor_activity_15s`.

- **Opción C – process-events por sesión (si existiera)**
  - Hoy no hay un “process-events solo para esta sessionId”. Si se añadiera un método que, dado un `sessionId`, procese solo los eventos de esa sesión desde `events_raw` hacia `contractor_activity_15s`, el trigger podría llamar primero a eso y luego a session summary. Sería un cambio mayor en el ETL (filtrar por session_id en events_raw y escribir solo esos beats).

### 5.3 process-daily-metrics al cerrar sesión (opcional)

- Si se quiere que el **resumen diario** (contractor_daily_metrics) se actualice en cuanto cierra la sesión, se puede, **después** de session summary, encolar un job de **process-daily-metrics** para el día actual (y opcionalmente solo para ese `contractorId` si el ETL lo soporta).
- No es necesario para ver el **% de productividad por sesión** en reportes; solo para tener el día actualizado al instante.

### 5.4 Configuración ADT_MS

- **USE_ETL_QUEUE=true** en ADT_MS para que el trigger encole el job de session summary (y, si se implementa, los de process-events/daily-metrics) en BullMQ en lugar de ejecutarlos síncronos en el listener.
- Que **QueuesModule** esté cargado y **EtlQueueService** inyectado en **EtlTriggerListener** (ya está condicionado a `useEtlQueue`).

---

## 6. Resumen en una tabla

| Objetivo                                                    | Estado actual                                                   | Qué modificar                                                                                                                                        |
| ----------------------------------------------------------- | --------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| Que al cerrar sesión se ejecute **session summary**         | ✅ Trigger solo encola session summary (si USE_ETL_QUEUE=true). | Asegurar que EVENTS_MS (u otro) emita `etl.session.trigger` al cerrar sesión.                                                                        |
| Que **process-events** se ejecute antes que session summary | ❌ El trigger no llama a process-events.                        | Opción A: cron frecuente de process-events. Opción B: en el trigger, ejecutar/encolar process-events para un rango reciente y luego session summary. |
| Que **process-daily-metrics** se ejecute al cerrar sesión   | ❌ No está en el trigger.                                       | Opcional: encolar también un job de daily-metrics para “hoy” (y contractor si aplica) después de session summary.                                    |
| Ver **% de productividad** en reportes por sesión           | Depende de `session_summary.productivity_score`.                | Asegurar la cadena: eventos en `events_raw` → process-events → `contractor_activity_15s` → process-session-summaries → `session_summary`.            |

---

## 7. Archivos clave (referencia)

- **Trigger:** `ADT_MS/src/listeners/etl-trigger.listener.ts` — escucha `etl.session.trigger`, encola session summary.
- **Colas ETL:** `ADT_MS/src/queues/queues.module.ts` — registra colas y processors; `ADT_MS/src/queues/services/etl-queue.service.ts` — `addSessionSummaryJob`, `addDailyMetricsJob`.
- **Processors:** `session-summary.processor.ts` (session summary), `daily-metrics.processor.ts` (daily metrics). No hay processor de “process-events” en cola; el ETL de eventos se invoca síncrono desde el listener.
- **ETL:** `ADT_MS/src/etl/services/etl.service.ts` — `processEventsToActivity`, `processActivityToDailyMetrics`, `processActivityToSessionSummary`.
- **Rutas HTTP:** `API_GATEWAY/src/adt/adt.controller.ts` — `GET etl/process-events`, `etl/process-daily-metrics`, `etl/process-session-summaries`.
- **Handlers NATS en ADT_MS:** `ADT_MS/src/listeners/adt.listener.ts` — `adt.processEvents`, `adt.processDailyMetrics`, `adt.processSessionSummaries`.
- **Métricas en tiempo real (dashboard):** `ADT_MS/src/etl/services/realtime-metrics.service.ts` — lee `contractor_activity_15s`; no es el flujo de reportes por sesión.

Con estos cambios (emisor del evento + asegurar process-events antes de session summary, y opcionalmente daily-metrics), al cerrar una sesión las métricas se procesan y el % de productividad estará disponible en reportes y en la página.
