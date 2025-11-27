# Fase 3: NATS Listeners - Resumen de Implementación

## ✅ Estado Actual: COMPLETAMENTE FUNCIONAL

### Arquitectura Implementada

El sistema utiliza una arquitectura de **Interceptores → NATS → Listeners**:

- **Interceptores** (en EVENTS_MS y USER_MS): Envían eventos a NATS después de crear/actualizar datos
- **Listeners** (en ADT_MS): Reciben eventos de NATS y los guardan en ClickHouse

**⚠️ Importante:** Los listeners son **NECESARIOS** porque los interceptores solo pueden interceptar handlers locales. Para recibir eventos de otros microservicios vía NATS, se requieren listeners con `@EventPattern()`.

### 1. Estructura de DTOs RAW

Se crearon DTOs para mapear datos desde los eventos NATS a ClickHouse:

- `EventRawDto` - Para eventos de actividad
- `SessionRawDto` - Para sesiones de contractors
- `AgentSessionRawDto` - Para sesiones de agentes
- `ContractorRawDto` - Para información de contractors

**Ubicación:** `ADT_MS/src/raw/dto/`

### 2. Servicio RAW

Se creó `RawService` que encapsula la lógica para guardar datos en ClickHouse:

- `saveEvent()` - Guarda eventos en `events_raw`
- `saveSession()` - Guarda sesiones en `sessions_raw`
- `saveAgentSession()` - Guarda agent sessions en `agent_sessions_raw`
- `saveContractor()` - Guarda/actualiza contractors en `contractor_info_raw`

**Ubicación:** `ADT_MS/src/raw/raw.service.ts`

**Optimizaciones implementadas:**
- ✅ Cache de verificaciones de tabla (evita queries repetidas)
- ✅ Logs reducidos (solo en debug para reducir ruido)
- ✅ Formateo automático de fechas para ClickHouse

### 3. Listeners NATS (ADT_MS)

Se implementaron 4 listeners para escuchar eventos de otros microservicios:

#### a) EventsListener
- **Evento:** `event.created` (de EVENTS_MS)
- **Interceptor origen:** `AdtEventInterceptor` en `EVENTS_MS/src/events/interceptors/adt-event.interceptor.ts`
- **Acción:** Guarda eventos en `events_raw`
- **Estado:** ✅ **FUNCIONANDO** - Recibe eventos con ID real del evento creado

#### b) SessionsListener
- **Eventos:** `session.created`, `session.updated` (de USER_MS)
- **Interceptor origen:** `AdtSessionInterceptor` en `USER_MS/src/session/interceptors/adt-session.interceptor.ts`
- **Acción:** Guarda sesiones en `sessions_raw`
- **Estado:** ✅ **FUNCIONANDO** - Implementado y activo

#### c) AgentSessionsListener
- **Eventos:** `agentSession.created`, `agentSession.updated` (de USER_MS)
- **Interceptor origen:** `AdtAgentSessionInterceptor` en `USER_MS/src/session/interceptors/adt-agent-session.interceptor.ts`
- **Acción:** Guarda agent sessions en `agent_sessions_raw`
- **Estado:** ✅ **FUNCIONANDO** - Implementado y activo

#### d) ContractorsListener
- **Eventos:** `contractor.created`, `contractor.updated` (de USER_MS)
- **Interceptor origen:** `AdtContractorInterceptor` en `USER_MS/src/contractor/interceptors/adt-contractor.interceptor.ts`
- **Acción:** Guarda/actualiza contractors en `contractor_info_raw`
- **Estado:** ✅ **FUNCIONANDO** - Implementado y activo

**Ubicación:** `ADT_MS/src/listeners/`

### 4. Interceptores (EVENTS_MS y USER_MS)

Los interceptores están implementados y funcionando correctamente:

#### EVENTS_MS
- **AdtEventInterceptor**: Intercepta la creación de eventos y envía `event.created` a NATS
  - Ubicación: `EVENTS_MS/src/events/interceptors/adt-event.interceptor.ts`
  - Aplicado en: `EventsController.create()` con `@UseInterceptors(AdtEventInterceptor)`

#### USER_MS
- **AdtSessionInterceptor**: Intercepta creación/actualización de sesiones y envía eventos a NATS
  - Ubicación: `USER_MS/src/session/interceptors/adt-session.interceptor.ts`
  - Aplicado en: `SessionController.create()`, `update()`, `endSession()`

- **AdtAgentSessionInterceptor**: Intercepta creación/actualización de agent sessions y envía eventos a NATS
  - Ubicación: `USER_MS/src/session/interceptors/adt-agent-session.interceptor.ts`
  - Aplicado en: `AgentSessionController.create()`, `update()`, `endSession()`

- **AdtContractorInterceptor**: Intercepta creación/actualización de contractors y envía eventos a NATS
  - Ubicación: `USER_MS/src/contractor/interceptors/adt-contractor.interceptor.ts`
  - Aplicado en: `ContractorController.create()`, `update()`

### 4. Tablas RAW en ClickHouse

Se agregaron las tablas RAW al script de inicialización de ClickHouse:

- `events_raw` - MergeTree, particionado por fecha
- `sessions_raw` - MergeTree, particionado por fecha
- `agent_sessions_raw` - MergeTree, particionado por fecha
- `contractor_info_raw` - ReplacingMergeTree (para manejar actualizaciones)

**Ubicación:** `CLICKHOUSE/init/01-init-database.sql`

### 5. Integración en AppModule

Se registraron todos los listeners en `AppModule` para que NestJS los active automáticamente.

**Ubicación:** `ADT_MS/src/app.module.ts`

---

## 📊 Flujo de Datos Completo

```
┌─────────────────────────────────────────────────────────────┐
│                    EVENTS_MS                                │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ EventsController.create()                            │  │
│  │   ↓                                                  │  │
│  │ AdtEventInterceptor (intercepta respuesta)          │  │
│  │   ↓                                                  │  │
│  │ natsClient.emit('event.created', eventData)         │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
                    ┌───────────────┐
                    │     NATS      │
                    │  (Message     │
                    │   Broker)     │
                    └───────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
┌───────────────┐  ┌───────────────┐  ┌───────────────┐
│   ADT_MS      │  │   ADT_MS      │  │   ADT_MS      │
│               │  │               │  │               │
│ EventsListener│  │ Sessions      │  │ AgentSessions │
│ @EventPattern │  │ Listener      │  │ Listener      │
│ ('event.      │  │ @EventPattern │  │ @EventPattern │
│  created')    │  │ ('session.*') │  │ ('agent.*')   │
│       ↓       │  │       ↓       │  │       ↓       │
│ RawService    │  │ RawService    │  │ RawService    │
│ .saveEvent()  │  │ .saveSession()│  │ .saveAgent    │
│       ↓       │  │       ↓       │  │ Session()     │
└───────┼───────┘  └───────┼───────┘  └───────┼───────┘
        │                   │                   │
        └───────────────────┼───────────────────┘
                            │
                            ▼
                    ┌───────────────┐
                    │  ClickHouse   │
                    │  RAW Tables   │
                    │  - events_raw │
                    │  - sessions_  │
                    │    raw        │
                    │  - agent_     │
                    │    sessions_  │
                    │    raw        │
                    │  - contractor_│
                    │    info_raw   │
                    └───────────────┘

┌─────────────────────────────────────────────────────────────┐
│                    USER_MS                                  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ SessionController.create() / update()                │  │
│  │   ↓                                                  │  │
│  │ AdtSessionInterceptor (intercepta respuesta)        │  │
│  │   ↓                                                  │  │
│  │ natsClient.emit('session.created/updated', data)    │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ AgentSessionController.create() / update()           │  │
│  │   ↓                                                  │  │
│  │ AdtAgentSessionInterceptor (intercepta respuesta)   │  │
│  │   ↓                                                  │  │
│  │ natsClient.emit('agentSession.created/updated', ...)│  │
│  └──────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ ContractorController.create() / update()             │  │
│  │   ↓                                                  │  │
│  │ AdtContractorInterceptor (intercepta respuesta)     │  │
│  │   ↓                                                  │  │
│  │ natsClient.emit('contractor.created/updated', ...)  │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## 🎯 Estado Actual: TODO FUNCIONANDO

### ✅ Sistema Completo Implementado

**Todos los componentes están implementados y funcionando:**

1. **EventsListener** ✅
   - Recibe `event.created` de EVENTS_MS
   - El interceptor `AdtEventInterceptor` envía eventos automáticamente
   - Guarda datos en `events_raw` con el ID real del evento

2. **SessionsListener** ✅
   - Recibe `session.created` y `session.updated` de USER_MS
   - El interceptor `AdtSessionInterceptor` envía eventos automáticamente
   - Guarda datos en `sessions_raw`

3. **AgentSessionsListener** ✅
   - Recibe `agentSession.created` y `agentSession.updated` de USER_MS
   - El interceptor `AdtAgentSessionInterceptor` envía eventos automáticamente
   - Guarda datos en `agent_sessions_raw`

4. **ContractorsListener** ✅
   - Recibe `contractor.created` y `contractor.updated` de USER_MS
   - El interceptor `AdtContractorInterceptor` envía eventos automáticamente
   - Guarda datos en `contractor_info_raw`

### 🔄 Flujo de Comunicación

**Arquitectura Interceptores → NATS → Listeners:**

1. **EVENTS_MS/USER_MS**: Los interceptores interceptan la respuesta de los handlers locales
2. **Interceptores**: Envían eventos a NATS usando `natsClient.emit()`
3. **NATS**: Distribuye los eventos a todos los suscriptores
4. **ADT_MS**: Los listeners reciben los eventos usando `@EventPattern()`
5. **RawService**: Procesa y guarda los datos en ClickHouse

**⚠️ Nota importante:** Los listeners son **NECESARIOS** porque:
- Los interceptores solo pueden interceptar handlers **locales** (del mismo microservicio)
- Para recibir eventos de **otros microservicios** vía NATS, se requieren listeners con `@EventPattern()`
- Esta es la arquitectura correcta para microservicios en NestJS

---

## 📁 Estructura de Archivos Creados

```
ADT_MS/
├── src/
│   ├── listeners/
│   │   ├── events.listener.ts          ✅ Creado
│   │   ├── sessions.listener.ts        ✅ Creado
│   │   ├── agent-sessions.listener.ts  ✅ Creado
│   │   └── contractors.listener.ts     ✅ Creado
│   ├── raw/
│   │   ├── dto/
│   │   │   ├── event-raw.dto.ts        ✅ Creado
│   │   │   ├── session-raw.dto.ts      ✅ Creado
│   │   │   ├── agent-session-raw.dto.ts ✅ Creado
│   │   │   └── contractor-raw.dto.ts   ✅ Creado
│   │   ├── raw.service.ts              ✅ Creado
│   │   └── raw.module.ts               ✅ Creado
│   └── app.module.ts                   ✅ Modificado
├── TESTING_INSTRUCTIONS.md             ✅ Creado
└── PHASE3_SUMMARY.md                   ✅ Este archivo

CLICKHOUSE/
└── init/
    └── 01-init-database.sql            ✅ Modificado (agregadas tablas RAW)
```

---

## 🚀 Próximos Pasos

1. **✅ Sistema Completo Funcionando**
   - Todos los interceptores implementados
   - Todos los listeners funcionando
   - Datos fluyendo correctamente a ClickHouse

2. **Fase 4: DTOs y Transformaciones ETL** (Futuro)
   - Crear DTOs para tablas ADT
   - Crear servicios de transformación (RAW → ADT)
   - Implementar lógica de agregación
   - Crear vistas materializadas en ClickHouse

3. **Optimizaciones Futuras** (Opcional)
   - Implementar sistema de batching para agrupar inserciones
   - Agregar métricas y monitoreo
   - Implementar retry logic para eventos fallidos

---

## 📝 Notas Importantes

1. **Event ID Real**: El interceptor `AdtEventInterceptor` envía el evento con el **ID real** del evento creado en PostgreSQL, no un ID temporal. Esto permite trazabilidad completa.

2. **ReplacingMergeTree**: La tabla `contractor_info_raw` usa `ReplacingMergeTree` para manejar actualizaciones. ClickHouse deduplicará automáticamente basándose en `updated_at`.

3. **TTL**: Todas las tablas RAW tienen TTL configurado (365 días para eventos/sesiones, 730 días para contractors). Los datos se eliminarán automáticamente después del período.

4. **Particionado**: Las tablas están particionadas por fecha para optimizar queries y mantenimiento.

5. **Optimizaciones de Performance**:
   - ✅ Cache de verificaciones de tabla (evita queries repetidas a ClickHouse)
   - ✅ Logs reducidos (solo en nivel debug para producción)
   - ✅ Formateo automático de fechas para ClickHouse (YYYY-MM-DD HH:MM:SS)
   - ✅ Manejo de errores sin romper el flujo principal

6. **Fire-and-Forget**: Los interceptores usan `natsClient.emit()` que es fire-and-forget, por lo que no bloquean el flujo principal si ADT_MS falla.

7. **Escalabilidad**: El sistema está optimizado para manejar 200-500 computadores enviando eventos cada 15 segundos (~33 eventos/segundo en pico).

---

## 🔍 Verificación

Para verificar que todo está funcionando:

1. ✅ Compilación exitosa: `pnpm run build`
2. ✅ Sin errores de linting
3. ✅ Tablas RAW creadas en ClickHouse
4. ✅ Listeners registrados en AppModule
5. ✅ Interceptores implementados en EVENTS_MS y USER_MS
6. ✅ Cache de tablas funcionando
7. ✅ Logs optimizados (en inglés, solo debug en producción)
8. ✅ Comentarios en español
9. ✅ Sistema probado y funcionando con datos reales

## 📋 Resumen de Arquitectura

**Flujo completo:**
```
Handler (EVENTS_MS/USER_MS) 
  → Interceptor (intercepta respuesta local)
  → natsClient.emit() (envía a NATS)
  → NATS (distribuye evento)
  → Listener (ADT_MS recibe con @EventPattern)
  → RawService (procesa y transforma)
  → ClickHouse (almacena en tablas RAW)
```

**Componentes clave:**
- **Interceptores**: Envían eventos (EVENTS_MS, USER_MS)
- **Listeners**: Reciben eventos (ADT_MS) - **NECESARIOS**
- **RawService**: Procesa y guarda datos
- **ClickHouseService**: Maneja conexión y optimizaciones

