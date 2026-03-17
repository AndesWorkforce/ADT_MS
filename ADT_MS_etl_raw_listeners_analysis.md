## Análisis ADT_MS: `raw`, `etl` y `listeners`

### 1. Visión general por carpeta

- **`src/raw`**
  - `RawService` centraliza los inserts en ClickHouse:
    - `saveEvent` → `events_raw` (+ logs opcionales de materialized views para `contractor_activity_15s`).
    - `saveSession` → `sessions_raw`.
    - `saveAgentSession` → `agent_sessions_raw`.
    - `saveContractor` → `contractor_info_raw`.
    - `saveTeam` → `teams_dimension`.
    - `saveClient` → `clients_dimension`.
  - Todas las funciones tienen el mismo patrón: `try/catch`, `clickHouseService.insert`, logs `debug`/`error` y uso de `logError`.

- **`src/listeners`**
  - **`events.listener.ts` (`EventsListener`)**
    - Escucha `event.created`.
    - Transforma el payload genérico de EVENTS_MS → `EventRawDto`.
    - Si `envs.queues.useEventQueue` es `true`, encola el evento en `EventQueueService`.
    - Si no, llama directo a `rawService.saveEvent`.
    - Además, gestiona tracking de inactividad con Redis (`trackActivityForInactivityAlerts`) y publica/resuelve alertas vía NATS.
  - **`contractors.listener.ts` (`ContractorsListener`)**
    - Escucha `contractor.created` y `contractor.updated`.
    - Ambas rutas construyen un `ContractorRawDto` casi idéntico y llaman a `rawService.saveContractor`.
  - **`sessions.listener.ts` (`SessionsListener`)**
    - Escucha `session.created` y `session.updated`.
    - Mismo patrón: transformar a `SessionRawDto` y delegar en `rawService.saveSession`.
  - **`agent-sessions.listener.ts` (`AgentSessionsListener`)**
    - Escucha `agentSession.created` y `agentSession.updated`.
    - Mismo patrón para `AgentSessionRawDto` → `rawService.saveAgentSession`.
  - **`dimensions.listener.ts`, `adt.listener.ts`** (no se leyeron aún en detalle, pero por convención siguen el mismo patrón listener → servicio).
  - **`etl-trigger.listener.ts` (`EtlTriggerListener`)**
    - Escucha `etl.session.trigger` desde USER_MS/EVENTS_MS.
    - Loguea el evento y, si `EtlQueueService` está disponible (`USE_ETL_QUEUE=true`), encola un job de ETL completo para cierre de sesión.
    - Maneja errores sólo con log; no relanza (fire-and-forget).

- **`src/etl` (servicios seleccionados)**
  - **`etl.service.ts` (`EtlService`)**
    - Servicio “grande” que orquesta:
      - `processEventsToActivity` (RAW → `contractor_activity_15s` por día, idempotente).
      - `processEventsToActivityForce` (misma lógica pero con `DELETE + INSERT SELECT`).
      - Otros métodos (no leídos completos) para `processActivityToDailyMetrics`, `processDailyMetricsToSessionSummary`, etc.
    - Aplica transformadores:
      - `EventsToActivityTransformer`.
      - `ActivityToDailyMetricsTransformer`.
      - `ActivityToSessionSummaryTransformer`.
    - Mucha lógica repetida: cálculo de rangos de fechas, normalización a día UTC, construcción de queries ClickHouse y logs.
  - **`daily-metrics.service.ts` (`DailyMetricsService`)**
    - Exposición de métricas diarias ya calculadas (`contractor_daily_metrics`):
      - `getDailyMetrics(contractorId, days)` con caché Redis (`RedisKeys.dailyMetricsByContractor`).
      - Query SELECT fijo con `WHERE contractor_id` y `workday >= today() - days`.
      - `formatResults` convierte `workday` a `YYYY-MM-DD` y normaliza `app_usage`/`browser_usage` (Map → array tipado).
  - **`activity.service.ts` (`ActivityService`)**
    - Exposición de beats detallados (`contractor_activity_15s`):
      - `getActivity(contractorId, from?, to?, limit?)` con caché Redis.
      - Construye dinámicamente el `WHERE` para `beat_timestamp` según formatos de `from`/`to`.
      - `formatDateForClickHouse` reutilizada para DateTime (`YYYY-MM-DD HH:MM:SS` UTC).
  - **`realtime-metrics.service.ts` (`RealtimeMetricsService`)**
    - Métricas de productividad “en vivo” a partir de `contractor_activity_15s`:
      - `getRealtimeMetrics` con caché Redis (`RedisKeys.realTimeMetricsByContractor`), retorna un objeto con `workday`, métricas y `is_realtime`.
      - `calculateMetrics` lee beats del día, consolida multi-agente (`consolidateBeatsByTimestamp`), llama a `UsageDataService` y luego a `ActivityToDailyMetricsTransformer.aggregate`.
      - `getConsolidatedProductivity` y `getProductivityByAgent` reutilizan esa lógica.
  - **`session-summaries.service.ts` (`SessionSummariesService`)**
    - Consulta de `session_summary`:
      - `getSessionSummaries` soporta vista por agente vs consolidada; ambas construyen queries muy parecidas, con diferencias en columnas de agregación.
      - `getSessionSummariesByDay` repite gran parte de la lógica para agrupar por día.
      - Usa Redis para cachear con claves que incluyen contractor, rango y agente.
  - **`usage-data.service.ts` (`UsageDataService`)**
    - Consolidación de AppUsage/Browser a partir de `events_raw`:
      - `getAppUsageForDay`, `getBrowserUsageForDay` (por contractor + día).
      - `getAppUsageForDateRange` (rango de fechas con heurística de LIMIT).
      - `getAppTypesFromDimension` para enriquecer con tipos desde `apps_dimension`.
    - Incluye lógica repetitiva de:
      - Parseo de JSON de payload.
      - Suma defensiva de segundos (`safe = seconds < 0 ? 0 : seconds`).
      - Map temporal `Record<string, number>` y conversión a array.
  - **`dimensions.service.ts` (`DimensionsService`)**
    - Carga de dimensiones (`apps_dimension`, `domains_dimension`) con múltiples “fallbacks” para esquemas legacy.
    - Mantiene `Map`s en memoria y provee:
      - `getAppWeight`, `getDomainWeight`.
      - `getAllApps`, `getAllDomains`.
      - `reload`, `isLoadedFromClickHouse`.

---

### 2. Capacidades claras de simplificación (sin tocar código todavía)

#### 2.1. Listeners de `contractor`, `session`, `agentSession`

Patrón actual:

- Cada listener (`ContractorsListener`, `SessionsListener`, `AgentSessionsListener`) tiene:
  - Dos handlers (`*.created`, `*.updated`) casi idénticos.
  - Construyen un DTO “raw” a mano campo a campo.
  - Llaman al método correspondiente de `RawService`.

**Posible simplificación:**

- Extraer funciones puras de mapeo en `raw/dto/...` o en helpers:
  - `toContractorRawDto(contractor: any): ContractorRawDto`.
  - `toSessionRawDto(session: any): SessionRawDto`.
  - `toAgentSessionRawDto(agentSession: any): AgentSessionRawDto`.
- En los listeners:
  - Reutilizar el mismo mapeo para `created` y `updated`.
  - Reducir duplicación a:
    - `const dto = toContractorRawDto(contractor); await rawService.saveContractor(dto);`.
- Beneficio:
  - Menos puntos para errores cuando se agrega un campo nuevo en USER_MS.
  - Tests unitarios sencillos sobre los helpers sin depender de Nest.

#### 2.2. Listeners con misma estructura de try/catch

Casi todos los listeners tienen este patrón:

- Log inicial (DEBUG).
- Construcción DTO.
- `await rawService.saveXxx(dto);`.
- Log final (DEBUG).
- `catch` → `logError(this.logger, 'Error processing ...', error);`.

**Posible simplificación:**

- Introducir un helper interno por listener o reutilizable:
  - Algo como `handleWithLogging(name, fn)` que envuelva try/catch.
  - O bien, un pequeño decorador a nivel método (si se quiere ir más lejos).

Sin embargo, aquí la ganancia es menor; es más importante unificar mapeos DTO.

#### 2.3. `RawService`: patrón repetido de inserts

Todas las funciones de `RawService` tienen la misma forma:

- `clickHouseService.insert(tabla, { ... })`.
- Logs y `logError`.

**Posible simplificación:**

- Extraer un método privado genérico como:
  - `private async safeInsert(table: string, payload: any, logContext: string): Promise<void>`.
- Cada método público solo arma el objeto y delega en `safeInsert`.
- Mejora la consistencia de logs y reduce boilerplate.

#### 2.4. `EtlService`: muchos métodos con lógica de rango de fechas muy similar

Ejemplos:

- `processEventsToActivity`:
  - Normaliza rango por defecto (últimas 2 horas).
  - Itera por días `fromDate` → `toDate`.
  - Para cada día:
    - Chequea si ya hay datos en destino (`contractor_activity_15s`) y, si hay, salta.
    - Hace `INSERT INTO ... SELECT ... FROM events_raw WHERE toDate(timestamp)=day`.
- `processEventsToActivityForce`:
  - Normaliza rango por defecto (últimas 2 horas).
  - Calcula `fromStr`, `toStr`, `fromDay`, `toDay`.
  - Hace `ALTER TABLE ... DELETE` dentro del rango.
  - Vuelve a hacer el `INSERT INTO ... SELECT ...` similar.

**Oportunidades:**

- Extraer una utilidad de manejo de rangos:
  - `normalizeDateRange(from?, to?, defaultHoursBack: number)`.
  - `iterateDays(from, to, callbackForDay)`.
- Unificar la construcción de filtros por `contractorId`.
- Unificar el SQL de `INSERT SELECT` en una sola función que reciba:
  - Día o rango.
  - Filtro opcional de contractor.
  - Si se usa en modo “force” (borra antes) o “idempotente” (saltea si ya existe).

Esto haría el archivo mucho más legible y menos propenso a bugs al modificar condiciones de filtrado.

#### 2.5. `ActivityService` y `RealtimeMetricsService`

- Ambos tocan `contractor_activity_15s`:
  - `ActivityService.getActivity` → para listar beats crudos.
  - `RealtimeMetricsService.calculateMetrics` → para listar beats del día y calcular métricas.

**Posible simplificación:**

- Extraer una pequeña capa de acceso a datos compartida (por ejemplo, `ActivityRepository`):
  - `getBeatsForContractor(contractorId, from?, to?)`.
  - `getBeatsForWorkday(contractorId, workdayDate)`.
- `ActivityService` y `RealtimeMetricsService` podrían usar esos métodos en vez de escribir queries similares cada uno.
- `RealtimeMetricsService` ya añade consolidación y combinación con `UsageDataService`, así que no se toca esa parte.

#### 2.6. `UsageDataService`: lógica prácticamente duplicada en 3 métodos

- Métodos:
  - `getAppUsageForDay`.
  - `getBrowserUsageForDay`.
  - `getAppUsageForDateRange`.
- Todos:
  - Construyen una query sobre `events_raw`.
  - Parsean JSON (`app_usage_json` o `browser_json`).
  - Suman segundos defensivamente (`safe = seconds < 0 ? 0 : seconds`).
  - Devuelven arrays con `{ appName/domain, seconds, (type?) }`.

**Posible simplificación:**

- Patrones:
  - “Parsear JSON de un campo y acumular segundos en un `Record<string, number>`”.
  - “Aplicar `safe` para valores negativos”.
- Se puede:
  - Extraer una función interna genérica:
    - `private async aggregateUsageFromEvents(query: string, jsonField: 'AppUsage' | 'browser'): Promise<Record<string, number>>`.
  - Reutilizarla en los tres métodos variando solo:
    - La query base.
    - El post-procesado (añadir `type` vía `getAppTypesFromDimension` en el caso de apps).

#### 2.7. `SessionSummariesService`: queries muy parecidas entre métodos

- `getSessionSummaries` vs `getSessionSummariesByDay`:
  - Ambos calculan:
    - `dateFilter` (`from/to` vs `days`).
    - `agentFilter`.
  - Ambos tienen rama `if (effectiveAgentId) { ... } else { ... }` con queries SQL muy parecidas:
    - Selección de `total_seconds`, `active_seconds`, `idle_seconds` y `productivity_score`.
  - `getSessionSummariesByDay` luego agrupa resultados por `session_day`.

**Posible simplificación:**

- Extraer constructores de query:
  - `buildSessionSummaryQuery(contractorId, dateFilter, agentId?)`.
  - `buildSessionSummaryConsolidatedQuery(contractorId, dateFilter)`.
- `getSessionSummaries` y `getSessionSummariesByDay` podrían:
  - Reutilizar esas funciones y luego:
    - En `getSessionSummaries`: devolver el array tal cual.
    - En `getSessionSummariesByDay`: hacer solo el agrupado en memoria.

#### 2.8. `DimensionsService`: carga de dimensiones con muchos fallbacks

- La lógica de carga (`loadFromClickHouse`) es robusta pero compleja:
  - Intenta múltiples estructuras de `apps_dimension` (con `id`, con `name`, con `app_name`).
  - Para cada fallo hace `this.logger.warn` y prueba otra variante.

**Posible simplificación conceptual:**

- Extraer la parte de “descubrir estructura de apps_dimension” a una función dedicada:
  - `private async loadAppsDimension(): Promise<AppDimensionDto[]>`.
- Esto aislaría los `try/catch` anidados y haría `loadFromClickHouse` más lineal:
  - `const apps = await this.loadAppsDimension();`.
  - `const domains = await this.loadDomainsDimension();`.

No es crítico, pero mejora legibilidad para futuros cambios de esquema.

---

### 3. Cálculos de sesiones y productividad potencialmente redundantes o sobre-específicos

Aquí miramos no solo duplicación de código, sino también **superposición funcional**: distintos servicios que calculan métricas parecidas (sesiones, productividad, uso de apps) usando tablas diferentes.

#### 3.1. Múltiples fuentes para “productividad” y “uso de apps”

Fuentes principales:

- `contractor_daily_metrics` (tabla pre-calculada, usada por:
  - `DailyMetricsService` (`getDailyMetrics`).
  - `RankingService` (`getRanking`).
  - ETL histórico (`EtlService.processActivityToDailyMetrics`).
- `session_summary` (pre-calculada por ETL, usada por:
  - `SessionSummariesService` (métodos de sesiones, hourly activity/productivity).
  - Endpoints de `AdtListener` tipo `adt.getHourlyActivity`, `adt.getHourlySessionDuration`, `adt.getHourlyProductivity`, `adt.getGroupedAvgSessionDuration`, etc.
- `contractor_activity_15s` (beats crudos):
  - `ActivityService` (detalle crudo) — usado para visualizaciones finas.
  - `RealtimeMetricsService` (productividad “online”/último día).
- `events_raw` (crudo… crudo):
  - `UsageDataService` (`getAppUsageForDay`, `getAppUsageForDateRange`, `getBrowserUsageForDay`).
  - ETL (`EtlService.processEventsToActivity`) que genera `contractor_activity_15s`.
- `app_usage_summary` (tabla pre-calculada por materialized view):
  - `AppUsageService` (`getAppUsage`) con parámetros `from/to/days`.

**Observaciones:**

- Para **uso de aplicaciones** hay dos caminos:
  - Vía ETL/materialized view → `app_usage_summary` (usado por `AppUsageService`).
  - Vía lectura directa desde `events_raw` (`UsageDataService`), tanto por día como por rango.
- Para **productividad** también hay dos caminos:
  - Vía métricas diarias consolidadas (`contractor_daily_metrics` + `session_summary`).
  - Vía cálculo “ad-hoc” desde beats (`RealtimeMetricsService.calculateMetrics`).

**Posible simplificación conceptual (sin tocar aún el código):**

- Definir claramente en documentación interna:
  - **Cuándo** se espera usar métricas _pre-calculadas_ (ETL → más baratas, ideales para reports, dashboards históricos).
  - **Cuándo** se espera usar métricas _on-the-fly_ (Realtime → para vistas de “ahora mismo”).
- Revisar en el cliente (API_GATEWAY + frontend) si:
  - Todos los endpoints de `AppUsageService` y `UsageDataService` están efectivamente en uso.
  - Hay endpoints redundantes que calculan prácticamente lo mismo por caminos distintos (por ejemplo, app usage por rango vs `app_usage_summary`).
  - De encontrarse, podrías:
    - Mantener **uno** como “source of truth” y deprecar gradualmente el otro.

#### 3.2. Múltiples vistas de sesiones con lógica de tiempo muy parecida

`SessionSummariesService` + `AdtListener` exponen muchas formas de ver casi la misma información de sesiones:

- `getSessionSummaries` / `adt.getSessionSummaries`.
- `getSessionSummariesByDay` / `adt.getSessionSummariesByDay`.
- `getHourlyActivity` / `adt.getHourlyActivity`.
- `getHourlySessionDuration` / `adt.getHourlySessionDuration`.
- `getHourlyProductivity` / `adt.getHourlyProductivity`.
- `getGroupedAvgSessionDuration` / `adt.getGroupedAvgSessionDuration`.
- Y además, existe `RankingService.getRanking` y `DailyMetricsService.getDailyMetrics`.

Todas estas funciones:

- Terminan construyendo filtros muy similares:
  - `dateFilter` (`from/to` vs `days`).
  - `agentFilter` (opcional).
  - `startHour`/`endHour` (para vistas por hora).
- Recalcan siempre las mismas ideas:
  - Duración total de sesión (`dateDiff('second', session_start, session_end)`).
  - Tiempo activo vs idle (`active_seconds` vs `idle_seconds`).
  - Productividad ponderada (`productivity_score` / combinación con seconds).

**¿Hay “cálculos que no se usan”?**

- A nivel código del microservicio, todo está **expuesto** por `AdtListener`, así que desde ADT_MS no hay forma de saber solo mirando aquí si un endpoint no lo usa nadie:
  - Habría que revisar los controladores de `API_GATEWAY` (`adt.controller.ts` o similar) y el frontend (`SOFTWARE_DEVELOPMEN_CLIENT`) para ver:
    - Qué `getMessagePattern('adt.*')` se mandan realmente.
    - Cuáles endpoints no se llaman nunca (o son solo pruebas internas).
- Lo que sí se nota es que:
  - Muchas funciones en `SessionSummariesService` comparten el mismo “núcleo” de cálculo (queries casi idénticas) pero cambian:
    - Agrupación (por sesión, por día, por cliente/equipo).
    - Presentación (listado plano vs agrupado en arrays por día).

**Simplificación posible:**

- A nivel diseño:
  - Documentar internamente qué endpoints son “core” (usados en UI/BI) y cuáles son experimentales.
  - Eventualmente marcar endpoints experimentales para deprecarlos si no hay consumidores.
- A nivel implementación (ya mencionado en 2.7):
  - Factorear una única función/núcleo que calcule métricas de sesión sobre un conjunto de filas (`session_summary`) y a partir de ahí:
    - Derivar vistas por día, por hora, por agrupación (client/team/contractor).

#### 3.3. ETL vs Materialized Views: doble trabajo potencial

En varios puntos se ve coexistencia de:

- ETL explícito (`EtlService`) que:
  - Genera `contractor_activity_15s`.
  - Genera `contractor_daily_metrics`.
  - Genera `session_summary`.
- Materialized Views de ClickHouse:
  - `mv_app_usage_summary` llena `app_usage_summary`.
  - Otros MVs (implícitos) pueden poblar tablas derivadas.

**Riesgo conceptual:**

- Si no hay una visión clara de **qué métricas vienen de ETL secuencial** y **qué métricas vienen de MVs “online”**, se puede terminar:
  - Recalculando curvas parecidas por dos caminos.
  - Usando tablas no totalmente alineadas (ej: `app_usage_summary` vs agregados desde `events_raw`).

**Recomendación a futuro:**

- En el README o en un `ARCHITECTURE.md` de ADT_MS, dejar explícito:
  - “Flujo principal” (por ejemplo: `events_raw` → `contractor_activity_15s` → `contractor_daily_metrics` → `session_summary`).
  - Qué tablas son consideradas **canonicales** para:
    - Productividad diaria.
    - Resúmenes de sesión.
    - Ranking.
    - Uso de apps.
  - Cuando un nuevo feature necesite métricas, obligarse a elegir una de esas fuentes canónicas, en lugar de inventar un nuevo pipeline paralelo.

---

### 4. Cobertura de endpoints: ADT_MS vs API_GATEWAY (`adt.controller.ts`)

Ahora, con `API_GATEWAY/src/adt/adt.controller.ts` leído, podemos confirmar qué rutas de ADT_MS están realmente expuestas hacia afuera.

#### 4.1. Lecturas ADT → ADT_MS (`AdtListener`)

El `AdtController` usa **exclusivamente** `getMessagePattern('adt.*')`, todos atendidos por `AdtListener` en ADT_MS:

- **Métricas diarias y tiempo real:**
  - `GET /adt/daily-metrics/:contractorId`
    - → `adt.getDailyMetrics` → `DailyMetricsService.getDailyMetrics`.
  - `GET /adt/realtime-metrics`
    - → `adt.getAllRealtimeMetrics` (método en `AdtListener`, no revisado en detalle pero claramente existe).
  - `GET /adt/realtime-metrics/:contractorId`
    - → `adt.getRealtimeMetrics` → `RealtimeMetricsService.getRealtimeMetrics`.
  - `GET /adt/productivity/:contractorId`
    - → `adt.getProductivitySummary` → `RealtimeMetricsService` (consolidado + por agente).

- **Sesiones y vistas derivadas desde `session_summary`:**
  - `GET /adt/sessions/:contractorId`
    - → `adt.getSessionSummaries` → `SessionSummariesService.getSessionSummaries`.
  - `GET /adt/sessions/:contractorId/by-day`
    - → `adt.getSessionSummariesByDay` → `SessionSummariesService.getSessionSummariesByDay`.
  - `GET /adt/hourly-activity/:contractorId`
    - → `adt.getHourlyActivity` → `SessionSummariesService.getHourlyActivity`.
  - `GET /adt/hourly-session-duration/:contractorId`
    - → `adt.getHourlySessionDuration` → `SessionSummariesService.getHourlySessionDuration`.
  - `GET /adt/hourly-productivity/:contractorId`
    - → `adt.getHourlyProductivity` → `SessionSummariesService.getHourlyProductivity`.
  - `GET /adt/grouped-avg-duration`
    - → `adt.getGroupedAvgSessionDuration` → `SessionSummariesService.getGroupedAvgSessionDuration`.

- **Actividad cruda y uso de apps:**
  - `GET /adt/activity/:contractorId`
    - → `adt.getActivity` → `ActivityService.getActivity`.
  - `GET /adt/app-usage/:contractorId`
    - → `adt.getAppUsage` → `AppUsageService.getAppUsage` (sobre `app_usage_summary`).

- **Ranking y talento activo:**
  - `GET /adt/ranking`
    - → `adt.getRanking` → `RankingService.getRanking`.
  - `GET /adt/ranking/top5`
    - → `adt.getTopRanking` → service correspondiente (no visto, pero claramente implementado).
  - `GET /adt/active-talent`
    - → `adt.getActiveTalentPercentage` → servicio en ADT_MS (no leído, pero existe).

En resumen: **todas las funcionalidades “grandes” que vimos en servicios de ADT_MS están efectivamente expuestas** vía HTTP a través de `API_GATEWAY/adt.controller.ts`. No hay servicios importantes “huérfanos” (sin controlador) en lo que revisamos.

#### 4.2. ETL manual / administración

`AdtController` también expone endpoints ADMIN (solo Superadmin) para disparar ETL manualmente:

- `GET /adt/etl/process-events`
  - → `adt.processEvents` → `EtlService.processEventsToActivity`.
- `GET /adt/etl/process-events-force`
  - → `adt.processEventsForce` → `EtlService.processEventsToActivityForce`.
- `GET /adt/etl/process-daily-metrics`
  - → `adt.processDailyMetrics` → `EtlService.processActivityToDailyMetrics`.
- `GET /adt/etl/process-session-summaries`
  - → `adt.processSessionSummaries` → `EtlService.processActivityToSessionSummary` (o equivalente).
- `GET /adt/etl/process-app-usage`
  - → `adt.processAppUsage` → servicio ETL asociado (app usage).
- `GET /adt/etl/process-app-usage-force`
  - → `adt.processAppUsageForce` → versión FORCE del anterior.

Estos endpoints confirman que las rutas ETL del `EtlService` **sí están expuestas** (aunque típicamente solo usadas por admins o scripts de mantenimiento).

#### 4.3. Implicación para “código no usado”

A nivel “exposición”:

- Todo lo que revisamos en ADT_MS (servicios de lectura: daily metrics, sesiones, actividad, uso de apps, ranking, talento activo; y servicios ETL) tiene un endpoint correspondiente en `API_GATEWAY/adt.controller.ts`.
- Por lo tanto, **no hay funciones claramente muertas** en ADT_MS desde el punto de vista del gateway: todas están cableadas.

A nivel “uso real”:

- Saber si un endpoint se usa o no requiere mirar:
  - Dónde se consumen los endpoints `/adt/...` en el frontend (`SOFTWARE_DEVELOPMEN_CLIENT`) o en integraciones externas.
  - O analizar logs/metrics de producción.
- Desde el código que vimos, lo único que podemos afirmar es:
  - No hay rutas de ADT_MS (al menos de las importantes) que se hayan quedado sin mapping en `adt.controller.ts`.
  - Si decides simplificar/deprecar, habría que:
    - Marcar primero la ruta HTTP como “deprecated” a nivel API_GATEWAY.
    - Verificar consumidores antes de borrar lógica en ADT_MS.

---

### 5. Posibles rutas / servicios que podrían unificarse con filtros

- `DailyMetricsService.getDailyMetrics(contractorId, days)`:
  - Podría aceptar opcionalmente:
    - `from` / `to` en vez de solo `days`.
    - Filtro por `teamId`/`clientId` (si la tabla lo soporta) para vistas agregadas.

- `ActivityService.getActivity(contractorId, from?, to?, limit?)`:
  - Ya soporta `from`, `to` y `limit`.
  - Podría:
    - Aceptar `agentId` para filtrar beats por agente, similar a cómo `UsageDataService` lo hace para AppUsage/Browser.

- `UsageDataService`:
  - Hoy distingue día vs rango con métodos distintos.
  - Se podría unificar en una API más expresiva:
    - `getAppUsage({ contractorId, from, to, workday, agentId, limit })`.
    - Internamente decide si usar consulta “por día” o “por rango”.

- `SessionSummariesService`:
  - `getSessionSummaries` y `getSessionSummariesByDay` podrían compartir una misma ruta HTTP con un parámetro `groupBy=day|session`, reutilizando la misma lógica de servicio (ya bastante parecida).

---

### 6. Recomendaciones de siguiente paso (sin tocar código aún)

Orden sugerido de refactor si quieres “simplificar” con impacto real y riesgo bajo:

1. **Unificar mapeos en listeners** (`contractor`, `session`, `agentSession`):
   - Crear helpers DTO puros.
   - Reducir duplicación y lugares donde hay que mantener campos en paralelo con USER_MS.

2. **Refactor interno de `UsageDataService`**:
   - Extraer la función genérica para parsear/aggregate JSON.
   - Dejar la interfaz pública igual para no romper nada.

3. **Reducir complejidad en `SessionSummariesService`**:
   - Unificar construcción de `dateFilter`/`agentFilter`.
   - Extraer builders de query SQL.
   - Reutilizar la misma query base entre `getSessionSummaries` y `getSessionSummariesByDay`.

4. **Extraer utilidades de rango de fechas en `EtlService`**:
   - Poner en helpers privados de clase o en un pequeño módulo `etl-range.utils.ts`.
   - No cambiar comportamiento, solo mover repetición.

5. **(Opcional) Crear un pequeño repositorio para `contractor_activity_15s`**:
   - Encapsular queries compartidas de `ActivityService` y `RealtimeMetricsService`.

Con estos pasos se reduce bastante la duplicación y la complejidad cognitiva del microservicio ADT_MS, manteniendo su arquitectura actual (listeners → raw → etl → servicios de lectura) y sin modificar contratos externos.

---

### 7. Ajustes recientes y oportunidades extra de simplificación

Esta sección documenta cambios ya realizados y algunas optimizaciones adicionales detectadas al repasar `src/` completo.

#### 7.1. ETL de métricas diarias y sesiones: joins de browser simplificados

- En `EtlService` se simplificó el cálculo de pesos de dominios para evitar el error de ClickHouse:
  - Antes: combinaba `CROSS JOIN` con `ARRAY JOIN` sobre `domains_dimension`, lo que provocaba errores del tipo _"Multiple JOIN does not support mix with ARRAY JOINs"_.
  - Ahora: se usa `LEFT JOIN domains_dimension d ON d.domain = dc` y un peso `ifNull(d.weight, 0.5)` tanto en:
    - La query que alimenta `contractor_daily_metrics`.
    - La query que alimenta `session_summary`.
- Efecto:
  - Se mantiene la lógica de pesos por dominio.
  - Se reduce complejidad SQL y se mejora la compatibilidad con versiones futuras de ClickHouse.

**Acción recomendada:** documentar explícitamente en comentarios de código o en un `CLICKHOUSE_NOTES.md` que la fuente de pesos para dominios es siempre `domains_dimension` vía `LEFT JOIN`, evitando usar nuevamente `CROSS JOIN` + `ARRAY JOIN` para el mismo propósito.

#### 7.2. ETL y BullMQ: jobIds únicos para evitar jobs “atascados”

- En `EtlQueueService` se ajustaron los `jobId` de BullMQ:
  - **Antes**:
    - `daily-metrics`: `jobId = daily-metrics-YYYY-MM-DD[...]`.
    - `session-summaries`: `jobId = session-summary-${sessionId}`.
    - Resultado: si un job fallaba 3 veces y quedaba en estado `failed`, nuevos intentos con el mismo `jobId` no volvían a ejecutarse (BullMQ los trataba como duplicados).
  - **Ahora**:
    - `daily-metrics`: `jobId = daily-metrics-YYYY-MM-DD[...] - ${Date.now()}`.
    - `session-summaries`: `jobId = session-summary-${sessionId}-${Date.now()}`.
    - Resultado: cada ejecución administrativa del ETL genera un job nuevo y claramente identificable en las colas.

**Acciones recomendadas:**

- Mantener la idempotencia a nivel de datos (p.ej. `DELETE + INSERT` o checks en ClickHouse) y no depender de la idempotencia basada en `jobId`.
- Si en el futuro se quieren reintroducir `jobId` deterministas, hacerlo solo junto con:
  - Un mecanismo de _reset/cleanup_ de jobs `failed` en BullMQ.
  - Métricas/monitorización claras de colas para evitar “falsos positivos” de éxito.

#### 7.3. Unificación de fórmulas de productividad entre tablas

- Actualmente, la fórmula de `productivity_score` se aplica en:
  - ETL hacia `contractor_daily_metrics`.
  - ETL hacia `session_summary`.
  - Cálculos “on-the-fly” en `RealtimeMetricsService`.
- Aunque las fórmulas están alineadas conceptualmente, no existe todavía:
  - Un módulo único que las defina.
  - Ni tests unitarios compartidos que verifiquen que todas las implementaciones calculan lo mismo para el mismo set de beats.

**Oportunidad de simplificación adicional (a futuro):**

- Extraer un pequeño módulo puro, por ejemplo `src/metrics/productivity-formula.ts`, con funciones tipo:
  - `computeProductivityFromBeats(...)`.
  - `computeProductivityFromDailyAggregates(...)`.
- Hacer que:
  - Los ETL SQL estén lo más cerca posible de esa definición (comentarios + equivalencia matemática).
  - `RealtimeMetricsService` y cualquier otro cálculo ad-hoc reutilicen esa definición documentada.

Esto no requiere cambios inmediatos en el código, pero dejarlo anotado aquí ayuda a que futuros refactors mantengan coherencia entre todas las métricas de productividad.
