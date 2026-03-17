## Plan de simplificación de rutas ADT

Basado en el análisis de `ADT_MS_etl_raw_listeners_analysis.md`, `API_GATEWAY/src/adt/adt.controller.ts` y `SOFTWARE_DEVELOPMEN_CLIENT/packages/api/adt/adt.service.ts`.

La idea de este plan es:

- **Reducir la cantidad de fuentes de información** para productividad, sesiones y uso de apps.
- **Unificar rutas** cuando devuelven datos muy parecidos.
- **Marcar qué endpoints son core para el frontend**, cuáles son solo admin/ETL, y cuáles podrían deprecarse.
- Proveer suficiente detalle para poder convertir este archivo en un checklist de TODOs.

---

### 1. Rutas core usadas por el frontend (mantener, pero simplificar internamente)

Estas rutas son llamadas directamente desde `adt.service.ts` y son parte del producto actual. No deben eliminarse, pero sí se pueden simplificar **por dentro** en ADT_MS siguiendo el análisis previo (unificar SQL, helpers, etc.).

#### 1.1. `/adt/realtime-metrics` → `adt.getAllRealtimeMetrics`

- **Gateway**: `AdtController.getAllRealtimeMetrics`.
- **ADT_MS**: `AdtListener.getAllRealtimeMetrics` (servicio no visto en detalle, pero existe).
- **Tablas base**:
  - Principalmente `contractor_daily_metrics` y/o `contractor_activity_15s` (según implementación interna).
- **Uso en frontend**:
  - `adtService.getAllRealtimeMetrics(filters)` → grids/tablas de contratistas con productividad actual o en rango.

**Cambios propuestos (internos, no de contrato):**

- Documentar en `ADT_MS_etl_raw_listeners_analysis.md` que esta ruta es la **fuente canónica** para:
  - Vista de “todos los contractors” con productividad.
- Revisar la implementación interna:
  - Preferir **una sola tabla canónica**, idealmente `contractor_daily_metrics` para históricos + filtros por rango.
  - Evitar recalcular lo mismo desde `contractor_activity_15s` salvo que realmente necesites granularidad extra.
- Unificar construcción de filtros (`workday`, `from/to`, `name/cliente/equipo`) en helpers reutilizables.

#### 1.2. `/adt/realtime-metrics/:contractorId` → `adt.getRealtimeMetrics`

- **Gateway**: `AdtController.getRealtimeMetrics`.
- **ADT_MS**: `AdtListener.getRealtimeMetrics` → `RealtimeMetricsService.getRealtimeMetrics`.
- **Tablas base**: `contractor_activity_15s` + derivados (`UsageDataService`, etc.).
- **Uso en frontend**:
  - `adtService.getRealtimeMetrics(contractorId, ...)` para vistas de detalle de un contractor.

**Cambios propuestos:**

- Confirmar en `RealtimeMetricsService` que:
  - Para “día puntual” usa solo el día (`toDate(beat_timestamp) = workday`).
  - Para rango `from/to`, suma métricas coherentes con lo que muestra `/adt/realtime-metrics` (no devolver valores contradictorios).
- Reutilizar helpers de rango de fechas y beats (ver recomendaciones de extraer `ActivityRepository`).

#### 1.3. `/adt/productivity/:contractorId` → `adt.getProductivitySummary`

- **Gateway**: `AdtController.getProductivitySummary`.
- **ADT_MS**: `AdtListener.getProductivitySummary` → `RealtimeMetricsService.getConsolidatedProductivity` + `getProductivityByAgent`.
- **Uso en frontend**:
  - `adtService.getProductivitySummary` para resumen consolidado y por agente.

**Cambios propuestos:**

- Confirmar que este endpoint es la **única** fuente de verdad para:
  - Resumen mixto consolidado + por agente.
- Internamente, reutilizar `RealtimeMetricsService`:
  - Asegurar que no recalcula métricas desde otra ruta (evitar duplicar transformaciones).

#### 1.4. `/adt/active-talent` → `adt.getActiveTalentPercentage`

- **Gateway**: `AdtController.getActiveTalentPercentage`.
- **ADT_MS**: `AdtListener.getActiveTalentPercentage`.
- **Tablas base**: típicamente `contractor_daily_metrics` o `contractor_activity_15s`.
- **Uso en frontend**:
  - `adtService.getActiveTalentPercentage(period)` para tarjetas/kpis de “talento activo”.

**Cambios propuestos:**

- Asegurar que esta ruta:
  - Usa la misma definición de “contractor activo” que `/adt/realtime-metrics`.
  - Se basa en **una sola tabla canónica** (idealmente `contractor_daily_metrics` o conteo de beats en `contractor_activity_15s`).
- Documentar en el análisis que:
  - `/adt/active-talent` + `/adt/realtime-metrics` deben mantenerse alineadas conceptualmente.

#### 1.5. `/adt/ranking/top5` → `adt.getTopRanking`

- **Gateway**: `AdtController.getTopRanking`.
- **ADT_MS**: `AdtListener.getTopRanking` → `RankingService` + lógica adicional (periodos, orden).
- **Uso en frontend**:
  - `adtService.getTopRanking(period, order, useCache)` para leaderboard/top 5.

**Cambios propuestos:**

- Mantener `RankingService` como el punto único para ranking.
- Asegurar que ranking se basa siempre en `contractor_daily_metrics` (no recalcular desde beats).
- Posiblemente:
  - Exponer solo `/adt/ranking/top5` para UI, y mantener `/adt/ranking` como endpoint más técnico/admin (ver sección 2).

#### 1.6. Rutas de sesiones y vistas derivadas

Rutas usadas por `adtService`:

- `/adt/sessions/:contractorId` → `adt.getSessionSummaries`.
- `/adt/sessions/:contractorId/by-day` → `adt.getSessionSummariesByDay`.
- `/adt/hourly-activity/:contractorId` → `adt.getHourlyActivity`.
- `/adt/hourly-session-duration/:contractorId` → `adt.getHourlySessionDuration`.
- `/adt/hourly-productivity/:contractorId` → `adt.getHourlyProductivity`.
- `/adt/grouped-avg-duration` → `adt.getGroupedAvgSessionDuration`.

Todas delegan en `SessionSummariesService` (y en uno o dos métodos relacionados).

**Cambios propuestos (conceptuales):**

- Declarar `session_summary` como la tabla canónica para:
  - Sesiones individuales.
  - Métricas por hora.
  - Duraciones agrupadas (client/team/contractor).
- En `SessionSummariesService`:
  - Factorear un **núcleo común** que devuelva una colección de filas `session_summary` o agregados básicos.
  - Encima, construir:
    - Vista “flat” (`getSessionSummaries`).
    - Vista “by-day” (`getSessionSummariesByDay`).
    - Vistas horarias (`getHourlyActivity`, `getHourlySessionDuration`, `getHourlyProductivity`).
    - Vista agrupada (`getGroupedAvgSessionDuration`).
- A nivel rutas HTTP:
  - Mantener las existentes porque el frontend las usa.
  - Pero internamente, que todas pasen por una misma función/módulo que haga el cálculo de base (evitar fórmulas duplicadas).

---

### 2. Rutas ADT expuestas pero **no usadas por el frontend** (candidatas a admin-only o deprecación)

Estas rutas existen en `AdtController`, pero **no aparecen en `adt.service.ts`**:

1. `GET /adt/daily-metrics/:contractorId` → `adt.getDailyMetrics` → `DailyMetricsService.getDailyMetrics`.
2. `GET /adt/ranking` → `adt.getRanking` → `RankingService.getRanking`.
3. `GET /adt/activity/:contractorId` → `adt.getActivity` → `ActivityService.getActivity`.
4. `GET /adt/app-usage/:contractorId` → `adt.getAppUsage` → `AppUsageService.getAppUsage`.
5. Endpoints ETL manuales:
   - `/adt/etl/process-events` → `adt.processEvents`.
   - `/adt/etl/process-events-force` → `adt.processEventsForce`.
   - `/adt/etl/process-daily-metrics` → `adt.processDailyMetrics`.
   - `/adt/etl/process-session-summaries` → `adt.processSessionSummaries`.
   - `/adt/etl/process-app-usage` → `adt.processAppUsage`.
   - `/adt/etl/process-app-usage-force` → `adt.processAppUsageForce`.

Para cada uno, definimos intención y acción propuesta.

#### 2.1. `/adt/daily-metrics/:contractorId`

- **Estado actual**:
  - No es usado por el frontend.
  - Devuelve métricas diarias por contractor desde `contractor_daily_metrics`.
  - Funcionalidad similar se obtiene vía `/adt/realtime-metrics` (con rango) o `/adt/sessions` + `/adt/hourly-*`.

- **Propuesta**:
  - Marcarlo como **endpoint técnico/admin**:
    - Documentar en README que es para debugging o consumo BI directo.
  - Si en práctica nadie externo lo usa:
    - Considerar deprecarlo a favor de `/adt/realtime-metrics/:contractorId` con parámetros `from/to`.

#### 2.2. `/adt/ranking`

- **Estado actual**:
  - Frontend solo usa `/adt/ranking/top5`.
  - `/adt/ranking` permite ranking más general (sin limitar a top5).

- **Propuesta**:
  - Mantener `/adt/ranking` como endpoint de **uso avanzado o admin** (BI).
  - Clarificar en documentación:
    - UI usa siempre `/adt/ranking/top5`.
    - `/adt/ranking` puede considerarse parte de una API más “raw” para integraciones externas.

#### 2.3. `/adt/activity/:contractorId`

- **Estado actual**:
  - Frontend no lo llama vía `adtService`.
  - Devuelve beats crudos de `contractor_activity_15s`.

- **Propuesta**:
  - Etiquetarlo claramente como endpoint **técnico de debugging**:
    - Útil para inspeccionar datos de ClickHouse cuando se debuga el pipeline.
  - Opcional:
    - Añadir un flag de entorno para deshabilitarlo en producción si no se quiere exponer beats crudos a clientes.

#### 2.4. `/adt/app-usage/:contractorId`

- **Estado actual**:
  - No es usado por `adtService`.
  - Expone `app_usage_summary` (materialized view).
  - Existe lógica alternativa para AppUsage via `UsageDataService` (desde `events_raw`).

- **Propuesta**:
  - Decidir si:
    - **Fuente canónica de app-usage** será `app_usage_summary`:
      - En ese caso, planificar que cualquier vista nueva de uso de apps (frontend) pase por `/adt/app-usage`.
      - Dejar `UsageDataService` para ETL/Realtime pero no exponerlo directamente.
    - O si, por el contrario, se prefiere calcular siempre desde `events_raw`:
      - Entonces `/adt/app-usage` puede marcarse como candidato a deprecación futura.
  - En cualquier caso:
    - Documentar en el análisis la elección de fuente canónica para AppUsage.

#### 2.5. Endpoints ETL manuales (`/adt/etl/...`)

- **Estado actual**:
  - No usados por el frontend.
  - Pensados para scripts/admin (ejecutar ETL manual, backfill, pruebas).

- **Propuesta**:
  - Mantenerlos como **endpoints admin-only**:
    - Ya están restringidos con `@Roles(Role.Superadmin)`.
  - Para simplificación conceptual:
    - Documentarlos en un bloque separado “ETL Admin”.
    - No mezclarlos con la API pública de datos ADT.

---

### 3. Tabla resumen por ruta (clasificación rápida)

| Ruta HTTP                                | Pattern NATS                       | Servicio ADT_MS                  | Tabla(s) base                         | ¿Usada por frontend? | Rol propuesto                   |
| ---------------------------------------- | ---------------------------------- | -------------------------------- | ------------------------------------- | -------------------- | ------------------------------- |
| `GET /adt/realtime-metrics`              | `adt.getAllRealtimeMetrics`        | `RealtimeMetricsService` / otros | `contractor_daily_metrics` / activity | Sí                   | Core UI                         |
| `GET /adt/realtime-metrics/:id`          | `adt.getRealtimeMetrics`           | `RealtimeMetricsService`         | `contractor_activity_15s`             | Sí                   | Core UI                         |
| `GET /adt/productivity/:id`              | `adt.getProductivitySummary`       | `RealtimeMetricsService`         | `contractor_activity_15s` + usage     | Sí                   | Core UI                         |
| `GET /adt/active-talent`                 | `adt.getActiveTalentPercentage`    | Servicio propio                  | `contractor_daily_metrics` / activity | Sí                   | Core UI                         |
| `GET /adt/ranking/top5`                  | `adt.getTopRanking`                | `RankingService`                 | `contractor_daily_metrics`            | Sí                   | Core UI                         |
| `GET /adt/sessions/:id`                  | `adt.getSessionSummaries`          | `SessionSummariesService`        | `session_summary`                     | Sí                   | Core UI                         |
| `GET /adt/sessions/:id/by-day`           | `adt.getSessionSummariesByDay`     | `SessionSummariesService`        | `session_summary`                     | Sí                   | Core UI                         |
| `GET /adt/hourly-activity/:id`           | `adt.getHourlyActivity`            | `SessionSummariesService`        | `session_summary`                     | Sí                   | Core UI                         |
| `GET /adt/hourly-session-duration/:id`   | `adt.getHourlySessionDuration`     | `SessionSummariesService`        | `session_summary`                     | Sí                   | Core UI                         |
| `GET /adt/hourly-productivity/:id`       | `adt.getHourlyProductivity`        | `SessionSummariesService`        | `session_summary`                     | Sí                   | Core UI                         |
| `GET /adt/grouped-avg-duration`          | `adt.getGroupedAvgSessionDuration` | `SessionSummariesService`        | `session_summary`                     | Sí                   | Core UI                         |
| `GET /adt/daily-metrics/:id`             | `adt.getDailyMetrics`              | `DailyMetricsService`            | `contractor_daily_metrics`            | No                   | Admin / candidato a deprecación |
| `GET /adt/ranking`                       | `adt.getRanking`                   | `RankingService`                 | `contractor_daily_metrics`            | No                   | Avanzado/Admin                  |
| `GET /adt/activity/:id`                  | `adt.getActivity`                  | `ActivityService`                | `contractor_activity_15s`             | No                   | Debug técnico                   |
| `GET /adt/app-usage/:id`                 | `adt.getAppUsage`                  | `AppUsageService`                | `app_usage_summary`                   | No                   | Decidir: canónica o deprecar    |
| `GET /adt/etl/process-events`            | `adt.processEvents`                | `EtlService`                     | RAW → activity                        | No (solo admin)      | ETL Admin                       |
| `GET /adt/etl/process-events-force`      | `adt.processEventsForce`           | `EtlService`                     | RAW → activity                        | No (solo admin)      | ETL Admin                       |
| `GET /adt/etl/process-daily-metrics`     | `adt.processDailyMetrics`          | `EtlService`                     | activity → daily                      | No (solo admin)      | ETL Admin                       |
| `GET /adt/etl/process-session-summaries` | `adt.processSessionSummaries`      | `EtlService`                     | daily → session_summary               | No (solo admin)      | ETL Admin                       |
| `GET /adt/etl/process-app-usage`         | `adt.processAppUsage`              | Servicio ETL apps                | RAW → app_usage_summary               | No (solo admin)      | ETL Admin                       |
| `GET /adt/etl/process-app-usage-force`   | `adt.processAppUsageForce`         | Servicio ETL apps                | RAW → app_usage_summary               | No (solo admin)      | ETL Admin                       |

---

### 4. Cómo usar este archivo como TODO

Para convertir esto en un backlog de simplificación:

1. **Marcar fuentes canónicas**:
   - En `ADT_MS_etl_raw_listeners_analysis.md`, completar la sección de “tablas canónicas” usando la tabla de arriba.
2. **Documentar roles de rutas**:
   - En `API_GATEWAY/src/adt/adt.controller.ts`, añadir comentarios JSDoc simples:
     - `// CORE UI`, `// ADMIN ONLY`, `// ADVANCED/BI`, `// DEBUG`.
3. **Refactor internos sin romper contratos**:
   - `SessionSummariesService`: extraer núcleo común y usarlo desde todos los métodos.
   - `UsageDataService`: extraer agregador genérico.
   - `EtlService`: helpers de rango y de INSERT SELECT.
4. **Decisiones de deprecación** (cuando tengas claro el uso externo):
   - Si nadie usa `/adt/daily-metrics`, `/adt/activity`, `/adt/app-usage` ni `/adt/ranking` desde BI u otros sistemas:
     - Marcarlos como deprecated en comentarios y documentación.
     - A largo plazo, considerar eliminarlos o agruparlos detrás de rutas más expresivas ya existentes.

Con este plan, podés ir ruta por ruta, sabiendo:

- Qué hace.
- Desde dónde se consume.
- Qué tabla impacta.
- Y qué tipo de acción corresponde (mantener/simplificar/deprecar).

---

### 5. Notas adicionales tras refactors recientes

Al aplicar los ETL en ClickHouse y poner en marcha las colas BullMQ en producción, aparecieron algunos ajustes que impactan (ligeramente) este plan de rutas y administración:

#### 5.1. ETL admin `/adt/etl/...` y colas BullMQ

- Los endpoints de ETL manual (`/adt/etl/process-events`, `/process-daily-metrics`, `/process-session-summaries`, `/process-app-usage*`) ya no dependen de `jobId` deterministas en BullMQ:
  - Cada request HTTP crea un job con un `jobId` que incluye `Date.now()`.
  - Esto evita que jobs fallidos en el pasado bloqueen nuevas ejecuciones con el mismo día o `sessionId`.
- A efectos de este plan:
  - Sigue siendo válido tratarlos como **endpoints admin-only**.
  - Es aún más importante que la **idempotencia se garantice en el lado de ClickHouse / ETL** (DELETE + INSERT, checks de existencia), porque las colas ya no hacen de “filtro” por `jobId`.
- Si en el futuro se quiere simplificar la superficie de ETL admin:
  - Una opción es exponer un único endpoint orquestador tipo `/adt/etl/run-full` (contratista + rango), que internamente encadene `process-events`, `process-daily-metrics` y `process-session-summaries` usando las colas.

#### 5.2. Unificación de la semántica de productividad y fuentes canónicas

- Los ajustes recientes en `EtlService` para el cálculo de productividad (sobre todo en joins de `browser` con `domains_dimension`) refuerzan la necesidad de:
  - Tratar `contractor_daily_metrics` y `session_summary` como **tablas canónicas** para reports y ranking.
  - Usar `RealtimeMetricsService` solo para vistas “en vivo” basadas en beats (`contractor_activity_15s`), pero alineadas con la misma fórmula.
- Para este plan de rutas significa:
  - Las rutas core (`/adt/realtime-metrics*`, `/adt/productivity/:id`, `/adt/ranking/top5`, `/adt/active-talent`, rutas de sesiones) deberían documentar explícitamente cuál de estas tablas canónicas usan.
  - Cuando se diseñen nuevas rutas, priorizar reusar esas tablas canónicas antes que nuevos agregados ad-hoc desde `events_raw`.

Estas notas no cambian los contratos HTTP actuales, pero dan contexto adicional para futuras simplificaciones: menos caminos para calcular lo mismo y una capa ETL más fácil de razonar y de operar en producción.
