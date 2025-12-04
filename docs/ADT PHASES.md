# Plan de Implementación - ADT_MS

Este documento describe las fases de implementación del microservicio ADT (Analytical Data Tables) que procesa y almacena métricas de productividad en ClickHouse.

---

## 📋 Fase 1: Configuración Base

**Estado:** ✅ **Completada**

**Objetivo:** Configurar la estructura base del microservicio.

### Tareas Realizadas

- ✅ Crear carpeta `config/` con variables de entorno
  - `envs.ts` - Variables de NATS y ClickHouse
  - `logging.ts` - Utilidades de logging
  - `index.ts` - Exportaciones centralizadas

- ✅ Instalar dependencias:
  - `@clickhouse/client` - Cliente ClickHouse
  - `@nestjs/microservices` - Soporte NATS
  - `@nestjs/config` - Configuración
  - `nats`, `joi`, `dotenv`

- ✅ Configurar `main.ts` como microservicio NATS puro
- ✅ Crear filtro de excepciones RPC (`RpcExceptionFilter`)

---

## 📋 Fase 2: Conexión a ClickHouse

**Estado:** ✅ **Completada**

**Objetivo:** Establecer conexión con ClickHouse.

### Tareas Realizadas

- ✅ Crear `ClickHouseService`:
  - Conexión automática al iniciar
  - Cierre de conexión al destruir
  - Métodos: `query()`, `command()`, `insert()`, `tableExists()`

- ✅ Crear `ClickHouseModule` como módulo global
- ✅ Integrar en `AppModule`
- ✅ Verificar conexión con `ping()`

---

## 📋 Fase 3: NATS Listeners

**Estado:** ✅ **Completada**

**Objetivo:** Escuchar eventos de otros microservicios vía NATS.

### Tareas Realizadas

- ✅ Crear listeners para eventos de:
  - **EVENTS_MS** → `events.listener.ts` - Escucha `event.created`
  - **USER_MS** →
    - `sessions.listener.ts` - Escucha `session.created`, `session.updated`
    - `agent-sessions.listener.ts` - Escucha `agentSession.created`, `agentSession.updated`
    - `contractors.listener.ts` - Escucha `contractor.created`, `contractor.updated`
  - **API_GATEWAY** → `adt.listener.ts` - Responde peticiones HTTP vía NATS

### Estructura Implementada

```
src/
├── listeners/
│   ├── events.listener.ts         # Escucha eventos de EVENTS_MS
│   ├── sessions.listener.ts       # Escucha sesiones de USER_MS
│   ├── agent-sessions.listener.ts # Escucha agent sessions de USER_MS
│   ├── contractors.listener.ts    # Escucha contractors de USER_MS
│   └── adt.listener.ts            # Responde peticiones desde API_GATEWAY
```

- ✅ Implementar `@EventPattern()` y `@MessagePattern()` para cada tipo de evento
- ✅ Manejo de errores y logging en todos los listeners

---

## 📋 Fase 4: DTOs y Servicios de Transformación ETL

**Estado:** ✅ **Completada**

**Objetivo:** Crear DTOs y servicios para transformar RAW → ADT.

### Tareas Realizadas

- ✅ Crear DTOs para tablas ADT:

  ```
  src/etl/dto/
  ├── contractor-activity-15s.dto.ts
  ├── contractor-daily-metrics.dto.ts
  ├── app-usage-summary.dto.ts
  ├── session-summary.dto.ts
  ├── app-dimension.dto.ts
  └── domain-dimension.dto.ts
  ```

- ✅ Crear servicios de transformación:

  ```
  src/etl/transformers/
  ├── events-to-activity.transformer.ts      # events_raw → contractor_activity_15s
  ├── activity-to-daily-metrics.transformer.ts # contractor_activity_15s → contractor_daily_metrics
  ├── events-to-app-usage.transformer.ts     # events_raw → app_usage_summary
  └── activity-to-session-summary.transformer.ts # contractor_activity_15s → session_summary
  ```

- ✅ Crear servicios adicionales:
  - `DimensionsService` - Gestiona pesos de productividad para apps y dominios
  - `RealtimeMetricsService` - Calcula métricas en tiempo real con caché
  - `EtlService` - Orquesta las transformaciones ETL

- ✅ Lógica de transformación implementada:
  - Extraer datos del payload JSON
  - Calcular métricas agregadas
  - Aplicar fórmulas de productividad multi-factor
  - Generar DTOs listos para insertar en ClickHouse

---

## 📋 Fase 5: Creación de Tablas RAW y ADT en ClickHouse

**Estado:** ✅ **Completada**

**Objetivo:** Definir y crear las tablas en ClickHouse.

### Tareas Realizadas

- ✅ Crear métodos de inicialización en `ClickHouseService`:
  - `ensureRawTables()` - Crea tablas RAW automáticamente
  - `ensureDimensionsTables()` - Crea tablas de dimensiones
  - `ensureAdtTables()` - Crea tablas ADT automáticamente

- ✅ Tablas RAW creadas:
  - `events_raw`
  - `sessions_raw`
  - `agent_sessions_raw`
  - `contractor_info_raw`

- ✅ Tablas ADT creadas:
  - `contractor_activity_15s`
  - `contractor_daily_metrics`
  - `app_usage_summary`
  - `session_summary`

- ✅ Tablas de dimensiones creadas:
  - `apps_dimension` - Pesos de productividad por aplicación
  - `domains_dimension` - Pesos de productividad por dominio web

- ✅ Scripts SQL en carpeta `scripts/`:
  - `create-dimensions-tables.sql`
  - `populate-dimensions.sql`

- ✅ Servicio de inicialización (`DimensionsInitService`) que puebla dimensiones al iniciar

---

## 📋 Fase 6: Integración Completa

**Estado:** ✅ **Completada**

**Objetivo:** Conectar listeners → transformadores → ClickHouse.

### Tareas Realizadas

- ✅ Conectar listeners con transformadores:
  - Cuando llega evento → transformar → guardar RAW
  - Procesamiento de ADT en batch o tiempo real

- ✅ Implementar lógica de procesamiento:
  - Guardar eventos RAW inmediatamente (vía listeners)
  - `EtlService` para procesar ADT:
    - `processEventsToActivity()` - Convierte eventos RAW a beats de 15s
    - `processActivityToDailyMetrics()` - Genera métricas diarias
    - `processActivityToSessionSummary()` - Genera resúmenes de sesión

- ✅ Servicio de métricas en tiempo real:
  - `RealtimeMetricsService` con caché de 30 segundos
  - Cálculo on-demand desde `contractor_activity_15s`
  - Optimizado para dashboards con actualización frecuente

- ✅ Integración con API_GATEWAY:
  - Endpoints HTTP en API_GATEWAY
  - Comunicación vía NATS con ADT_MS
  - `AdtListener` responde todas las peticiones

- ✅ Manejo de errores y logging:
  - Logs de eventos procesados
  - Manejo de errores en todos los servicios
  - Filtros de excepciones RPC

---

## 📋 Fase 7: Optimización y Testing

**Estado:** 🔄 **En Progreso**

**Objetivo:** Optimizar y probar el sistema completo.

### Tareas Completadas

- [x] Materialized Views implementadas para procesamiento automático
  - `mv_events_to_activity`: Procesa `events_raw → contractor_activity_15s`
  - `mv_app_usage_summary`: Procesa `events_raw → app_usage_summary`
- [x] Modelo híbrido implementado (MVs para tiempo real + ETL para backfill)
- [x] Caché en RealtimeMetricsService (30 segundos TTL)

### Tareas Pendientes

#### Optimización

- [ ] Índices en ClickHouse (evaluar necesidad según queries)
- [x] Batch processing optimizado para ADT (Materialized Views implementadas)
- [x] Evaluar necesidad de caché adicional (actualmente solo RealtimeMetricsService - suficiente)
- [x] Materialized Views para agregaciones automáticas ✅ **Completado**

#### Testing

- [ ] Tests unitarios de transformadores
- [ ] Tests de integración con ClickHouse
- [ ] Tests de listeners NATS
- [ ] Tests end-to-end del flujo completo
- [ ] Tests de performance con carga simulada

#### Documentación

- [x] README del microservicio
- [x] Documentación de DTOs (en código)
- [x] Guía de deployment (scripts SQL)
- [x] Documentación de productividad score (`PRODUCTIVITY_SCORE.md`)
- [x] Análisis de performance (`PERFORMANCE_ANALYSIS.md`)
- [x] Guía de dashboard en tiempo real (`REALTIME_DASHBOARD_GUIDE.md`)
- [ ] Documentación de API (Swagger/OpenAPI)
- [ ] Guía de troubleshooting

---

## 📊 Resumen de Progreso

| Fase                           | Estado         | Progreso |
| ------------------------------ | -------------- | -------- |
| Fase 1: Configuración Base     | ✅ Completada  | 100%     |
| Fase 2: Conexión ClickHouse    | ✅ Completada  | 100%     |
| Fase 3: NATS Listeners         | ✅ Completada  | 100%     |
| Fase 4: DTOs y Transformers    | ✅ Completada  | 100%     |
| Fase 5: Tablas ADT ClickHouse  | ✅ Completada  | 100%     |
| Fase 6: Integración Completa   | ✅ Completada  | 100%     |
| Fase 7: Optimización y Testing | 🔄 En Progreso | 30%      |

**Progreso General:** 90% (6/7 fases completadas, Fase 7 parcialmente completada)

---

## 🚀 Próximos Pasos

1. **Testing**: Implementar suite de tests completa
2. **Optimización**: Evaluar y optimizar queries de ClickHouse
3. **Monitoreo**: Agregar métricas y alertas
4. **Documentación API**: Generar documentación Swagger/OpenAPI
