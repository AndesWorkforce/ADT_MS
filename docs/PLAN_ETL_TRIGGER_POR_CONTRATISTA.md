# Plan de acción: ETL al cerrar sesión, solo para el contratista

Este documento es un **plan de acción** para implementar lo siguiente, a partir del análisis en `ANALISIS_TRIGGERS_ETL_AL_CERRAR_SESION.md` y de tu petición:

1. Al cerrar una sesión, que se ejecuten los **tres ETL en orden** (process-events → process-daily-metrics → process-session-summaries) **solo para el contratista** que cerró la sesión.
2. Crear una **función orquestadora** que ejecute los 3 ETL en secuencia; desde el trigger se llama solo a esa función.
3. En el flujo automático (trigger), procesar **todo el día de hoy** (TODAY), sin límite de “últimas 2 horas”.
4. **Mantener** en los ETL existentes los parámetros `from`/`to` (y equivalentes) para uso **manual** vía HTTP.

**No se modifica código en este documento;** solo se describe el estado actual, la brecha y los pasos a seguir.

---

## 1. Confirmación: hoy no se tiene en cuenta el contratista

En el análisis anterior no se detallaba el alcance por contratista. Resumen del estado actual:

| ETL                                                               | ¿Acepta contractorId / contractorIds?                                                                 | Comportamiento actual                                                                                                                                 |
| ----------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| **process-events** (`processEventsToActivity`)                    | **No.** Solo `fromDate`, `toDate`.                                                                    | Procesa **todos** los contratistas del rango. Sin rango: **últimas 2 horas** (default).                                                               |
| **process-daily-metrics** (`processActivityToDailyMetrics`)       | **No** en `EtlService`. La cola sí recibe `contractorIds` en el job pero **no se pasan** al servicio. | Procesa **todos** los contratistas del día. Si el día ya tiene datos de **cualquier** contratista, salta todo el día (idempotencia por día completo). |
| **process-session-summaries** (`processActivityToSessionSummary`) | Implícito por `sessionId`.                                                                            | Ya está acotado a la sesión (y por tanto a un contratista).                                                                                           |

Conclusión: si hoy, al cerrar sesión, se ejecutaran los tres ETL “en bruto”, **process-events** y **process-daily-metrics** se ejecutarían para **todos los contratistas** (o para todo el día global), no solo para el que cerró la sesión. Por tanto, **sí**: actualmente no se tiene en cuenta el id del contratista para acotar process-events ni process-daily-metrics.

---

## 2. Objetivo deseado (resumen)

- **Trigger:** Al cerrar una sesión, se dispara el flujo para **ese contratista** (y esa sesión).
- **Orden:** 1) process-events, 2) process-daily-metrics, 3) process-session-summaries.
- **Alcance temporal en el trigger:** **Todo el día de hoy (TODAY)** para ese contratista, no “últimas 2 horas”.
- **Función orquestadora:** Una sola función que reciba (como mínimo) `contractorId` y `sessionId` y ejecute los 3 ETL en orden; el trigger solo llama a esa función.
- **Uso manual:** Las rutas HTTP y los métodos ETL existentes siguen permitiendo `from`/`to` (y workday/from/to) para ejecuciones manuales sin cambiar la firma pública de esos métodos.

---

## 3. Estado actual relevante (breve)

- **Default “2 horas”:** En `EtlService.processEventsToActivity` y `processEventsToActivityForce`, cuando no se pasan `fromDate`/`toDate`, se usan “últimas 2 horas”. Hay que dejar eso solo para llamadas sin rango en contexto manual, y en el flujo del trigger usar siempre “hoy” (00:00 TODAY → fin de día o “now”).
- **process-events:** No filtra por `contractor_id`. Las queries leen `events_raw` por rango de fechas y escriben en `contractor_activity_15s` para todos los contratistas del rango.
- **process-daily-metrics:** No filtra por contratista en `EtlService`; la cola guarda `contractorIds` en el job pero el processor no los pasa al servicio. La idempotencia es “si existe el día para cualquier contratista, no recalcular nada”.
- **process-session-summaries:** Ya acotado por `sessionId` (una sesión = un contratista).

---

## 4. Plan de acción (pasos concretos)

### Fase 0: Obtención del ID del contratista — Opción A (emitir `etl.session.trigger` desde USER_MS)

Hoy **nadie emite** el evento `etl.session.trigger`. ADT_MS solo lo escucha; por tanto el trigger nunca se dispara. El `contractor_id` **sí llega** a ADT_MS cuando se cierra una sesión, dentro de los eventos `session.updated` y `agentSession.updated` que USER_MS ya envía (vía interceptores). Para que el flujo ETL use ese contratista sin acoplar ADT_MS a la forma de los eventos de sesión, se adopta la **Opción A**: que sea USER_MS quien emita `etl.session.trigger` con `sessionId` y `contractorId` cuando se cierra una sesión.

- **Objetivo:** Que, al cerrar una sesión, ADT_MS reciba un único evento `etl.session.trigger` con `sessionId` y `contractorId`, de forma que el `EtlTriggerListener` pueda ejecutar la orquestadora con ambos IDs.
- **Dónde:** En **USER_MS**, en el mismo punto donde ya se envían `session.updated` y `agentSession.updated` a ADT_MS (interceptores `AdtSessionInterceptor` y `AdtAgentSessionInterceptor`). Ahí se tiene acceso a la sesión/agent session con `contractor_id` y `id` (y en agent session, `session_id` para la sesión padre).
- **Qué hacer:**
  1. Cuando se detecte que la operación es **update** (o `endSession`) **y** que la sesión se está cerrando (p. ej. `session.session_end` o `agentSession.session_end` pasa a no nulo), además de emitir `session.updated` / `agentSession.updated`, emitir **también** el evento `etl.session.trigger`.
  2. Payload de `etl.session.trigger`:
     - **sessionId:** Para `Session`: `session.id`. Para `AgentSession`: `agentSession.session_id` (sesión padre) si existe, o `agentSession.id` según lo que ADT_MS espere en `processActivityToSessionSummary` (normalmente el `session_id` de la sesión padre).
     - **contractorId:** `session.contractor_id` o `agentSession.contractor_id`.
     - **triggeredAt:** `new Date().toISOString()`.
     - **triggerReason:** `'explicit'` si el cierre es por acción del usuario; si en el futuro se distingue cierre por timeout, usar `'timeout'` en ese caso.
  3. Patrón NATS: el mismo que usa ADT_MS para escuchar, p. ej. `getMessagePattern('etl.session.trigger')` (o el que esté configurado en el proyecto USER_MS para eventos hacia ADT).
- **Archivos en USER_MS:** `USER_MS/src/session/interceptors/adt-session.interceptor.ts`, `USER_MS/src/session/interceptors/adt-agent-session.interceptor.ts`. En el método que envía a ADT_MS (p. ej. después de emitir `session.updated` / `agentSession.updated`), añadir la emisión de `etl.session.trigger` cuando `session_end` esté definido.
- **Configuración:** USER_MS debe tener acceso al mismo patrón de mensaje que ADT_MS (ej. prefijo de entorno o config compartida) para que el nombre del evento coincida.

Con esto, el ID del contratista (y el de la sesión) llegan a ADT_MS explícitamente en el evento que ya consume `EtlTriggerListener`, sin depender de reaccionar a `session.updated` / `agentSession.updated` dentro de ADT_MS.

---

### Fase 1: Alcance por contratista en los ETL

**1.1 process-events con opción de contratista**

- **Objetivo:** Poder ejecutar “eventos → beats” solo para **un** `contractorId` (o sin filtrar, para mantener el comportamiento actual en manual).
- **Cambios sugeridos:**
  - Añadir parámetro opcional `contractorId?: string` a `processEventsToActivity` y `processEventsToActivityForce`.
  - Cuando `contractorId` viene informado:
    - En la lectura: filtrar `events_raw` por `contractor_id = :contractorId` en el rango.
    - En la escritura: para idempotencia/consistencia, o bien (a) borrar solo las filas de ese `contractor_id` en los días del rango y luego insertar solo ese contratista, o (b) definir una política clara (ej. “solo insertar si no existe ese contractor_id en ese workday” según reglas de negocio). Documentar la elección.
  - Cuando no se pasa `contractorId`, comportamiento actual: procesar todos los contratistas del rango (uso manual).
- **Archivos:** `ADT_MS/src/etl/services/etl.service.ts` (y si aplica, tipos/interfaces usados por el listener).

**1.2 process-daily-metrics con contratistas**

- **Objetivo:** Poder ejecutar métricas diarias solo para uno o varios contratistas.
- **Cambios sugeridos:**
  - Añadir parámetro opcional `contractorIds?: string[]` a `processActivityToDailyMetrics` en `EtlService`.
  - Cuando `contractorIds` viene informado:
    - Restringir el `INSERT` (y los JOINs) a esos `contractor_id` (cláusulas `WHERE contractor_id IN (...)` en las subconsultas / origen).
    - Ajustar idempotencia: para ese día, o bien (a) borrar solo las filas de esos contratistas y reinsertar, o (b) no hacer “skip del día completo” y sí recalcular solo esos contratistas. Definir y documentar.
  - En el **DailyMetricsProcessor**: pasar `job.data.contractorIds` al llamar a `processActivityToDailyMetrics` (hoy no se pasa).
  - Cuando no se pasa `contractorIds`, comportamiento actual: todos los contratistas del día.
- **Archivos:** `ADT_MS/src/etl/services/etl.service.ts`, `ADT_MS/src/queues/processors/daily-metrics.processor.ts`.

---

### Fase 2: Default “día de hoy” en el flujo automático (y mantener from/to para manual)

**2.1 Eliminar / no usar el default “2 horas” en el flujo del trigger**

- **Objetivo:** Cuando se invoque desde el trigger, usar siempre “todo el día de hoy” (TODAY), no últimas 2 horas.
- **Cambios sugeridos:**
  - En la **función orquestadora** (ver Fase 3): al llamar a process-events, calcular explícitamente `fromDate = inicio del día de hoy (UTC o zona configurada)` y `toDate = fin del día de hoy` (o “now” si se prefiere no incluir eventos futuros). Pasar siempre `fromDate` y `toDate`; no llamar a process-events sin rango desde el trigger.
  - En `EtlService.processEventsToActivity` y `processEventsToActivityForce`: **mantener** el default actual (2 horas) cuando no se pasa `from`/`to`, para no romper llamadas manuales existentes (HTTP sin query params). Opcional: documentar que “sin rango = 2h por defecto, solo para uso manual”.
- **Resumen:** No eliminar el default de 2 horas del método, pero **nunca** invocar ese método sin rango desde el trigger; el orquestador siempre enviará “hoy”.

---

### Fase 3: Función orquestadora y uso desde el trigger

**3.1 Crear la función que ejecuta los 3 ETL en orden**

- **Objetivo:** Una sola función que reciba al menos `contractorId` y `sessionId` y ejecute en orden: (1) process-events, (2) process-daily-metrics, (3) process-session-summaries, todos acotados a ese contratista y al día de hoy.
- **Ubicación sugerida:** Puede vivir en `EtlService` (ej. `runFullEtlForContractorOnSessionClose(contractorId: string, sessionId: string)`) o en un servicio dedicado (ej. `EtlOrchestratorService`) que use `EtlService` y opcionalmente `EtlQueueService`. Si se quiere que el trigger siga encolando un **único job** que hace los 3 pasos, el processor de esa cola sería el que llama a esta función.
- **Comportamiento esperado:**
  1. Calcular `todayStart` y `todayEnd` (o “now”) para el día actual.
  2. Llamar a `processEventsToActivity(todayStart, todayEnd, contractorId)` (o el nombre que se defina con el nuevo parámetro).
  3. Llamar a `processActivityToDailyMetrics(workday, undefined, undefined, [contractorId])` (o la firma que se adopte con `contractorIds`), con `workday = hoy`.
  4. Llamar a `processActivityToSessionSummary(sessionId)`.
- **Manejo de errores:** Decidir si un fallo en (1) o (2) debe abortar el resto (recomendable para consistencia) o solo loguear y continuar. Documentar.
- **Idempotencia:** Cada ETL ya tiene (o tendrá) su propia idempotencia; la orquestadora no necesita duplicarla, solo garantizar el orden.

**3.2 Trigger: llamar solo a la orquestadora**

- **Objetivo:** Al recibir `etl.session.trigger` con `sessionId` y `contractorId`, ejecutar únicamente la función orquestadora (no llamar por separado a las 3 funciones).
- **Opciones de diseño:**
  - **A) Síncrono en el listener:** Si `EtlQueueService` no está disponible o se elige no usar cola para este flujo, el `EtlTriggerListener` podría llamar directamente a la orquestadora (await). Así los 3 ETL corren en el mismo proceso; el tiempo de respuesta del evento puede ser alto.
  - **B) Un solo job en cola:** Crear un nuevo tipo de job (ej. “full-etl-on-session-close”) y una cola (o la misma de session-summaries con otro job type). El listener encola un solo job con `{ contractorId, sessionId }`. El processor de ese job ejecuta la orquestadora. Ventaja: no bloquea el listener y se beneficia de reintentos y observabilidad de BullMQ.
- **Cambios en listener:** En `EtlTriggerListener.handleSessionEtlTrigger`, en lugar de llamar solo a `addSessionSummaryJob(sessionId, contractorId)`, llamar a la orquestadora (o encolar el job que la ejecuta) pasando `contractorId` y `sessionId`. Si se usa cola, el jobId puede ser determinista, ej. `session-etl-${sessionId}`.

---

### Fase 4: Mantener from/to para uso manual

- **Objetivo:** Las rutas HTTP y los handlers NATS existentes siguen permitiendo ejecutar los ETL de forma manual con rangos arbitrarios y sin contratista (o con contratista si se añaden query params).
- **Acciones:**
  - No eliminar los parámetros `from`, `to` (y `workday`, `sessionId`) de los métodos ETL actuales.
  - No eliminar las rutas `GET /adt/etl/process-events`, `process-daily-metrics`, `process-session-summaries` ni sus handlers; pueden seguir llamando a los mismos métodos con `from`/`to` cuando vengan en la petición.
  - Opcional: en el Gateway, añadir query param `contractorId` a process-events y process-daily-metrics para ejecuciones manuales por contratista sin tocar la orquestadora.

---

## 5. Resumen de archivos a tocar (referencia)

| Área                                  | Archivos                                                                                                                                                                                                         |
| ------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Obtención contractorId (Opción A)** | `USER_MS/src/session/interceptors/adt-session.interceptor.ts`, `USER_MS/src/session/interceptors/adt-agent-session.interceptor.ts` (emitir `etl.session.trigger` al cerrar sesión con sessionId y contractorId). |
| ETL por contratista                   | `ADT_MS/src/etl/services/etl.service.ts` (processEventsToActivity\*, processActivityToDailyMetrics con contractorId/contractorIds).                                                                              |
| Processor daily-metrics               | `ADT_MS/src/queues/processors/daily-metrics.processor.ts` (pasar contractorIds al EtlService).                                                                                                                   |
| Orquestadora                          | Nuevo método en `EtlService` o nuevo `EtlOrchestratorService`; si se usa cola, nuevo job type y processor o reutilizar cola existente.                                                                           |
| Trigger                               | `ADT_MS/src/listeners/etl-trigger.listener.ts` (llamar orquestadora o encolar job único).                                                                                                                        |
| Cola (si aplica)                      | `ADT_MS/src/queues/services/etl-queue.service.ts`, `queues.module.ts`, tipos en `queues/types` (nuevo job type / nueva cola si se crea).                                                                         |
| API Gateway (opcional)                | `API_GATEWAY/src/adt/adt.controller.ts` (query param contractorId en process-events y process-daily-metrics si se desea uso manual por contratista).                                                             |

---

## 6. Orden recomendado de implementación

1. **Fase 0 (Opción A):** En USER_MS, emitir `etl.session.trigger` con `sessionId` y `contractorId` cuando se cierra una sesión (en los interceptores de sesión y agent session), para que ADT_MS reciba el ID del contratista.
2. **Fase 1.1 y 1.2:** Añadir soporte opcional por `contractorId` / `contractorIds` en los ETL y en el processor de daily-metrics, sin cambiar el comportamiento cuando no se pasan (todos los contratistas).
3. **Fase 2.1:** Definir en la orquestadora el rango “hoy” y usarlo siempre al llamar a process-events desde el trigger (sin tocar el default de 2h en la firma pública para manual).
4. **Fase 3.1:** Implementar la función orquestadora que ejecuta los 3 ETL en orden con contractorId y sessionId.
5. **Fase 3.2:** Cambiar el trigger para que invoque solo la orquestadora (o encole el job que la ejecuta).
6. **Fase 4:** Revisar que las rutas manuales sigan funcionando con from/to y documentar.

---

## 7. Checklist final (objetivos de tu petición)

- [ ] **Opción A:** USER_MS emite `etl.session.trigger` con `sessionId` y `contractorId` al cerrar una sesión, de modo que ADT_MS reciba el ID del contratista.
- [ ] Al cerrar una sesión se dispara el flujo **solo para el contratista** que cerró la sesión.
- [ ] Se ejecutan los **tres ETL en orden:** process-events → process-daily-metrics → process-session-summaries.
- [ ] Existe una **función única** que engloba los 3 ETL; desde el trigger solo se llama a esa función (o a un job que la ejecuta).
- [ ] En el flujo automático se procesa **toda la información del día (TODAY)** para ese contratista; se elimina o no se usa el default de “2 horas” en ese flujo.
- [ ] Los métodos ETL conservan **from/to** (y workday/from/to) para **uso manual** vía HTTP.

Este plan puede usarse como guía para implementar los cambios sin modificar código en este documento.
