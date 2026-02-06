# Sistema de Alertas de Inactividad

## 📋 Descripción General

El Sistema de Alertas de Inactividad detecta automáticamente cuando un contractor no genera eventos de actividad durante un período prolongado después de iniciar su jornada laboral. El sistema está diseñado para ser ligero, no sobrecargar la infraestructura, y permitir que otros servicios consuman las alertas vía eventos NATS.

### Características Clave

- ✅ **Detección automática**: Escaneo periódico que verifica inactividad
- ✅ **Respeto de horarios laborales**: Solo alerta durante `work_schedule_start` y `work_schedule_end`
- ✅ **Ligero y eficiente**: Solo 6 ops BullMQ/hora (~99% menos carga que alternativas event-driven)
- ✅ **Resolución automática**: Alertas se resuelven cuando el usuario reanuda actividad
- ✅ **Eventos NATS**: Notificaciones desacopladas para consumo por otros servicios
- ✅ **Persistencia en PostgreSQL**: Historial completo de alertas para análisis
- ✅ **Feature flag**: Sistema se puede activar/desactivar sin deployments

---

## 🏗️ Arquitectura

### Flujo Completo

```
PY_AGENT (eventos cada 15s)
    ↓
EVENTS_MS (recibe eventos HTTP)
    ↓
ADT_MS EventsListener (via NATS event.created)
    ↓
Redis Tracking (SET last_activity, session_start)
    ↓
BullMQ Job Recurrente (cada 10 minutos)
    ↓
InactivityScanProcessor
    ├─ Query active AgentSessions (RPC a EVENTS_MS)
    ├─ Verificar última actividad (Redis)
    ├─ Verificar work_schedule (RPC a EVENTS_MS)
    └─ Si inactividad >= threshold + dentro horario laboral:
        ├─ Crear alerta (RPC a EVENTS_MS)
        ├─ Publicar evento NATS (inactivity.alert.triggered)
        └─ Marcar alerta activa (Redis)
```

### Resolución de Alertas

```
PY_AGENT reanuda actividad
    ↓
EVENTS_MS (recibe evento HTTP)
    ↓
ADT_MS EventsListener
    ↓
Redis: Actualizar last_activity
    ↓
Verificar si hay alert_active en Redis
    ↓
Si hay alerta activa:
    ├─ Resolver alerta (RPC a EVENTS_MS)
    ├─ Publicar evento NATS (inactivity.alert.resolved)
    └─ Limpiar flag alert_active (Redis)
```

---

## ⚙️ Configuración

### Variables de Entorno (ADT_MS)

Agregar en `.env`:

```bash
# Inactivity Alerts System
USE_INACTIVITY_ALERTS=false
INACTIVITY_THRESHOLD_MINUTES=60
INACTIVITY_SCAN_INTERVAL_MINUTES=10
```

**Descripción de Variables**:

| Variable                           | Descripción                            | Default | Recomendado         |
| ---------------------------------- | -------------------------------------- | ------- | ------------------- |
| `USE_INACTIVITY_ALERTS`            | Activar/desactivar sistema completo    | `false` | `true` (producción) |
| `INACTIVITY_THRESHOLD_MINUTES`     | Minutos sin actividad antes de alertar | `60`    | `60` (1 hora)       |
| `INACTIVITY_SCAN_INTERVAL_MINUTES` | Frecuencia del escaneo                 | `10`    | `10` (balance)      |

### Para Testing Rápido

```bash
USE_INACTIVITY_ALERTS=true
INACTIVITY_THRESHOLD_MINUTES=5
INACTIVITY_SCAN_INTERVAL_MINUTES=2
```

---

## 📊 Eventos NATS

El sistema publica dos tipos de eventos NATS que otros servicios pueden consumir:

### 1. `inactivity.alert.triggered`

Publicado cuando se detecta inactividad que cumple criterios.

**Pattern**: `{env}.inactivity.alert.triggered` (ej: `dev.inactivity.alert.triggered`)

**Payload**:

```typescript
{
  alert_id: string; // ID único de la alerta
  contractor_id: string; // ID del contractor inactivo
  agent_session_id: string; // ID de la sesión del agente
  session_id: string | null; // ID de la sesión padre (si existe)
  inactivity_start: string; // ISO timestamp de última actividad
  inactivity_duration_minutes: number; // Minutos transcurridos sin actividad
  detected_at: string; // ISO timestamp de detección
}
```

**Ejemplo**:

```json
{
  "alert_id": "clxyz123abc",
  "contractor_id": "contractor-uuid-123",
  "agent_session_id": "session-uuid-456",
  "session_id": "parent-session-789",
  "inactivity_start": "2026-02-05T14:30:00.000Z",
  "inactivity_duration_minutes": 60,
  "detected_at": "2026-02-05T15:30:15.234Z"
}
```

### 2. `inactivity.alert.resolved`

Publicado cuando el usuario reanuda actividad y la alerta se resuelve.

**Pattern**: `{env}.inactivity.alert.resolved` (ej: `dev.inactivity.alert.resolved`)

**Payload**:

```typescript
{
  alert_id: string;                 // ID de la alerta resuelta
  agent_session_id: string;         // ID de la sesión del agente
  contractor_id: string;            // ID del contractor
  resolved_at: string;              // ISO timestamp de resolución
  total_duration_minutes?: number;  // Duración total de inactividad (opcional)
}
```

**Ejemplo**:

```json
{
  "alert_id": "clxyz123abc",
  "agent_session_id": "session-uuid-456",
  "contractor_id": "contractor-uuid-123",
  "resolved_at": "2026-02-05T16:15:30.123Z",
  "total_duration_minutes": 105
}
```

### Suscribirse a Eventos (Ejemplo)

**CLI NATS**:

```bash
# Todos los eventos de alerta
nats sub "dev.inactivity.alert.*"

# Solo alertas triggered
nats sub "dev.inactivity.alert.triggered"

# Solo alertas resolved
nats sub "dev.inactivity.alert.resolved"
```

**NestJS Service**:

```typescript
@EventPattern(getMessagePattern('inactivity.alert.triggered'))
async handleInactivityAlert(@Payload() alert: any) {
  // Enviar email, notificación Slack, etc.
  console.log(`Alert triggered for contractor: ${alert.contractor_id}`);
}
```

---

## 🗄️ Almacenamiento

### PostgreSQL (EVENTS_MS)

Tabla `inactivity_alerts`:

```sql
CREATE TABLE inactivity_alerts (
  id                      TEXT PRIMARY KEY,
  contractor_id           TEXT NOT NULL,
  agent_session_id        TEXT NOT NULL,
  session_id              TEXT,
  inactivity_start        TIMESTAMP NOT NULL,
  inactivity_detected_at  TIMESTAMP DEFAULT NOW(),
  alert_resolved_at       TIMESTAMP,
  total_duration_minutes  INTEGER,
  notified                BOOLEAN DEFAULT FALSE,
  created_at              TIMESTAMP DEFAULT NOW(),
  updated_at              TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_inactivity_alerts_agent_session
  ON inactivity_alerts(agent_session_id, alert_resolved_at);

CREATE INDEX idx_inactivity_alerts_contractor
  ON inactivity_alerts(contractor_id, inactivity_detected_at);
```

**Queries útiles**:

```sql
-- Alertas activas (sin resolver)
SELECT * FROM inactivity_alerts
WHERE alert_resolved_at IS NULL;

-- Alertas de un contractor específico
SELECT * FROM inactivity_alerts
WHERE contractor_id = 'contractor-uuid-123'
ORDER BY inactivity_detected_at DESC;

-- Estadísticas diarias
SELECT
  DATE(inactivity_detected_at) as date,
  COUNT(*) as total_alerts,
  AVG(total_duration_minutes) as avg_duration,
  COUNT(CASE WHEN alert_resolved_at IS NOT NULL THEN 1 END) as resolved
FROM inactivity_alerts
GROUP BY DATE(inactivity_detected_at)
ORDER BY date DESC;
```

### Redis (ADT_MS)

Keys temporales para tracking:

| Key Pattern                            | TTL      | Descripción                                |
| -------------------------------------- | -------- | ------------------------------------------ |
| `adt:last_activity:{agent_session_id}` | 2 horas  | Última actividad detectada (ISO timestamp) |
| `adt:session_start:{agent_session_id}` | 24 horas | Inicio de sesión (ISO timestamp)           |
| `adt:alert_active:{agent_session_id}`  | 24 horas | ID de alerta activa (si existe)            |

**Verificar keys**:

```bash
# Listar keys de tracking
redis-cli KEYS "adt:last_activity:*"
redis-cli KEYS "adt:session_start:*"
redis-cli KEYS "adt:alert_active:*"

# Ver valor específico
redis-cli GET "adt:last_activity:session-uuid-456"
```

---

## 🔍 Monitoreo y Troubleshooting

### Logs Clave (ADT_MS)

**Startup**:

```
✅ Inactivity scan job registered (interval: 10 minutes)
🔄 Periodic inactivity scan started (every 10 minutes)
```

**Durante Escaneo**:

```
🔍 Starting inactivity scan at 2026-02-05T15:30:00.000Z
📊 Found 85 active agent sessions
⚠️ Found 3 inactivity candidates
✅ 2 alerts pass work schedule validation
🚨 Inactivity alert created: clxyz123abc for contractor contractor-uuid-123 (inactive 62 min)
✅ Inactivity scan completed: 2 alerts created in 1247ms
```

**Resolución de Alertas**:

```
✅ Resolving inactivity alert clxyz123abc for agent_session session-uuid-456
✅ Inactivity alert resolved: clxyz123abc
```

### BullMQ Dashboard

**Queue Stats**:

```typescript
// Obtener estadísticas programáticamente
const stats = await inactivityScanQueueService.getQueueStats();
console.log(stats);
// Output:
// {
//   waiting: 0,
//   active: 0,
//   completed: 144,  // 24 horas * 6 scans/hora
//   failed: 0,
//   delayed: 0
// }
```

**Verificar Jobs**:

```bash
# Con Redis CLI
redis-cli --scan --pattern "bull:adt-inactivity-scan:*"
```

### Métricas a Monitorear

| Métrica                  | Qué Buscar                              | Acción si Anormal                                  |
| ------------------------ | --------------------------------------- | -------------------------------------------------- |
| **Scan Duration**        | <30s con 100 contractors                | Si >60s, revisar queries o aumentar concurrency    |
| **Alertas Creadas/Hora** | Depende del negocio                     | Muchas alertas = problema de conectividad agentes? |
| **Falsos Positivos**     | Alertas que se resuelven <5 min         | Revisar threshold o lógica de horarios             |
| **Redis Keys Activas**   | ~100-200 keys (proporcional a sessions) | Si miles, verificar TTL funcionando                |
| **Failed Jobs**          | 0-1%                                    | Si >5%, revisar logs y conectividad NATS/DB        |

### Debugging Común

**Problema**: Alertas no se disparan

1. Verificar feature flag:

   ```bash
   # En .env de ADT_MS
   USE_INACTIVITY_ALERTS=true
   ```

2. Verificar job está registrado:

   ```bash
   # Logs de inicio deben mostrar:
   ✅ Inactivity scan job registered
   ```

3. Verificar Redis tracking funciona:

   ```bash
   redis-cli GET "adt:last_activity:{agent_session_id}"
   # Debe retornar ISO timestamp
   ```

4. Verificar work_schedule configurado:
   ```sql
   SELECT work_schedule_start, work_schedule_end
   FROM contractors
   WHERE id = 'contractor-uuid-123';
   ```

**Problema**: Alertas no se resuelven

1. Verificar eventos llegan a ADT_MS:

   ```bash
   # Logs deben mostrar:
   ✅ Event processed (direct) - Event ID: ...
   ```

2. Verificar flag `alert_active` existe en Redis:

   ```bash
   redis-cli GET "adt:alert_active:{agent_session_id}"
   ```

3. Verificar RPC `resolveInactivityAlert` funciona:
   ```bash
   # Logs EVENTS_MS:
   📨 RPC: resolveInactivityAlert for agent_session ...
   ```

**Problema**: Muchas alertas falsas

- **Causa**: Threshold muy bajo o trabajo intermitente
- **Solución**: Aumentar `INACTIVITY_THRESHOLD_MINUTES` a 90-120 min

**Problema**: Scan tarda mucho

- **Causa**: Muchas sesiones activas o queries lentas
- **Solución**:
  - Optimizar índices en `agent_sessions` y `contractors`
  - Aumentar `INACTIVITY_SCAN_INTERVAL_MINUTES` a 15-20 min
  - Considerar pagination en processor

---

## 🚀 Deployment

### Pre-requisitos

1. **EVENTS_MS**: Migración Prisma aplicada

   ```bash
   cd EVENTS_MS
   npx prisma migrate deploy
   ```

2. **ADT_MS**: Variables configuradas en `.env`

   ```bash
   USE_INACTIVITY_ALERTS=true
   INACTIVITY_THRESHOLD_MINUTES=60
   INACTIVITY_SCAN_INTERVAL_MINUTES=10
   ```

3. **Redis**: Disponible y conectado (misma instancia usada por BullMQ)

4. **NATS**: Disponible para RPC y eventos

### Rollout Gradual

**Fase 1: Testing (dev)**

```bash
# .env
USE_INACTIVITY_ALERTS=true
INACTIVITY_THRESHOLD_MINUTES=5  # Testing rápido
INACTIVITY_SCAN_INTERVAL_MINUTES=2
```

**Fase 2: Staging**

```bash
# .env
USE_INACTIVITY_ALERTS=true
INACTIVITY_THRESHOLD_MINUTES=30  # Más conservador
INACTIVITY_SCAN_INTERVAL_MINUTES=10
```

**Fase 3: Production**

```bash
# .env
USE_INACTIVITY_ALERTS=true
INACTIVITY_THRESHOLD_MINUTES=60  # Configuración final
INACTIVITY_SCAN_INTERVAL_MINUTES=10
```

### Rollback

Si necesitas desactivar el sistema:

```bash
# .env
USE_INACTIVITY_ALERTS=false
```

Reinicia ADT_MS. El sistema NO generará más alertas, pero:

- Alertas existentes en DB permanecen
- Redis keys expiran automáticamente por TTL
- BullMQ job repetible se cancela en próximo restart

---

## 📈 Impacto en Infraestructura

### Comparación con Alternativa Event-Driven

| Recurso                | Este Sistema      | Event-Driven | Ahorro    |
| ---------------------- | ----------------- | ------------ | --------- |
| **BullMQ ops/min**     | 0.1               | 800          | **99.9%** |
| **Redis ops/min**      | 400 SET           | 800 SET/GET  | **50%**   |
| **NATS msgs/min**      | <1 (solo alertas) | 400          | **99.7%** |
| **PostgreSQL queries** | <10/hora          | N/A          | Mínimo    |

### Carga Esperada (100 contractors activos)

- **CPU**: <1% adicional en ADT_MS
- **Memoria**: +10 MB (BullMQ queue metadata)
- **Redis**: ~200 keys activas (~40 KB)
- **Network**: ~5 KB/10min (escaneos) + evento NATS por alerta

### Escalabilidad

**Hasta 500 contractors**:

- Sin cambios necesarios
- Scan duration: <2 min
- Memory: ~50 MB Redis

**500-1000 contractors**:

- Considerar aumentar `INACTIVITY_SCAN_INTERVAL_MINUTES` a 15
- Agregar pagination en processor

**1000+ contractors**:

- Multiple workers para scan (aumentar concurrency)
- Sharding de Redis keys
- Considerar arquitectura distribuida

---

## 📚 Referencias

- **BullMQ Docs**: [ADT_MS/docs/BULLMQ_DOCUMENTATION.md](../docs/BULLMQ_DOCUMENTATION.md)
- **Event Flow**: [DOCS_FLUJO_EVENTOS/](../../DOCS_FLUJO_EVENTOS/)
- **Redis Keys**: [ADT_MS/src/redis/redis-keys.ts](../src/redis/redis-keys.ts)
- **Processor**: [ADT_MS/src/queues/processors/inactivity-scan.processor.ts](../src/queues/processors/inactivity-scan.processor.ts)

---

## 🤝 Contribuir

Para agregar nuevas funcionalidades al sistema de alertas:

1. Extender `InactivityAlertsService` con nuevos métodos
2. Agregar RPC handlers en `InactivityAlertsController`
3. Actualizar processor si lógica de detección cambia
4. Documentar nuevos eventos NATS en esta guía
5. Agregar tests unitarios y de integración

---

## 📝 Changelog

### [1.0.0] - 2026-02-05

#### Agregado

- Sistema completo de alertas de inactividad
- Escaneo periódico con BullMQ
- Tracking ligero en Redis
- Eventos NATS para notificaciones
- Tabla PostgreSQL para historial
- Feature flag para control granular
- Documentación completa

#### Decisiones Técnicas

- Escaneo periódico (vs event-driven) para reducir 99% carga
- Redis temporal (vs DB queries) para performance
- NATS events (vs webhooks) para desacoplamiento
- Threshold 60 min default basado en requerimientos negocio
