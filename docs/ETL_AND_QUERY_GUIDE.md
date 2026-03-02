# Guía Rápida: Ejecutar ETL y Consultar ADT

## 🚀 Paso 1: Procesamiento de Datos

### ⚡ Procesamiento Automático (Materialized Views)

**Las Materialized Views procesan automáticamente** los eventos nuevos:

- `events_raw → contractor_activity_15s` (vía `mv_events_to_activity`)
- `events_raw → app_usage_summary` (vía `mv_app_usage_summary`)

**No necesitas ejecutar manualmente** estos procesos para el flujo normal. Los eventos se procesan automáticamente cuando llegan a `events_raw`.

### 🔧 ETL Manual (Solo para Backfill/Correcciones)

Si necesitas reprocesar datos históricos o corregir datos, puedes usar los ETL manuales:

#### 1️⃣ Procesar Eventos → Actividad (RAW → `contractor_activity_15s`)

**⚠️ Solo usar para backfill/correcciones, no para flujo normal**

**Endpoint:**

```bash
GET http://localhost:3001/adt/etl/process-events?from=2025-11-22&to=2025-11-27
```

**Qué hace (actual):**

- Itera por día entre `from` y `to`
- Verifica si `contractor_activity_15s` ya tiene datos para ese día
  - Si ya existen: omite procesamiento (idempotente sin DELETE)
  - Si no existen: lee desde `events_raw`, transforma e inserta

**Ejemplo con cURL:**

```bash
curl -X GET "http://localhost:3001/adt/etl/process-events?from=2025-11-22&to=2025-11-27" \
  -H "Authorization: Bearer TU_TOKEN_AQUI"
```

**Nota:** Este ETL es **idempotente** - puedes ejecutarlo múltiples veces para el mismo rango sin crear duplicados.

---

#### 2️⃣ Procesar Actividad → Métricas Diarias (`contractor_activity_15s` → `contractor_daily_metrics`)

**Endpoint:**

```bash
GET http://localhost:3001/adt/etl/process-daily-metrics
```

**Opciones de uso:**

**a) Día actual (por defecto):**

```bash
GET http://localhost:3001/adt/etl/process-daily-metrics
```

**b) Un día específico:**

```bash
GET http://localhost:3001/adt/etl/process-daily-metrics?workday=2025-11-27
```

**c) Rango de fechas (múltiples días):**

```bash
GET http://localhost:3001/adt/etl/process-daily-metrics?from=2025-11-01&to=2025-11-30
```

**Qué hace (actual):**

- Itera por días (parámetro único, rango o default: hoy)
- Si `contractor_daily_metrics` ya tiene filas para el día: omite (y devuelve lo existente)
- Si no existen: lee desde `contractor_activity_15s`
- Agrupa por día y contractor
- Calcula métricas diarias y productivity score
- **Inserta** en `contractor_daily_metrics`

**Ejemplo con cURL (procesar últimos 30 días):**

```bash
curl -X GET "http://localhost:3001/adt/etl/process-daily-metrics?from=2025-11-01&to=2025-11-30" \
  -H "Authorization: Bearer TU_TOKEN_AQUI"
```

**Nota:** Este ETL es **idempotente** - puedes ejecutarlo múltiples veces para el mismo rango sin crear duplicados.

---

#### 3️⃣ Procesar Actividad → Resúmenes de Sesión (`contractor_activity_15s` → `session_summary`)

**Endpoint:**

```bash
GET http://localhost:3001/adt/etl/process-session-summaries
```

**Para una sesión específica:**

```bash
GET http://localhost:3001/adt/etl/process-session-summaries?sessionId=session-123
```

**Qué hace (actual):**

- Lee desde `contractor_activity_15s`
- Solo inserta sesiones que no existan aún en `session_summary`
- Agrupa por `session_id`
- Calcula métricas por sesión y productivity score
- Inserta en `session_summary`

**Ejemplo con cURL:**

```bash
curl -X GET "http://localhost:3001/adt/etl/process-session-summaries" \
  -H "Authorization: Bearer TU_TOKEN_AQUI"
```

---

#### 4️⃣ Procesar Eventos → Uso de Aplicaciones (`events_raw` → `app_usage_summary`)

**⚠️ Solo usar para backfill/correcciones, no para flujo normal**

**Endpoint:**

```bash
GET http://localhost:3001/adt/etl/process-app-usage?from=2025-11-22&to=2025-11-27
```

**Qué hace (actual):**

- Itera por día entre `from` y `to`
- Si `app_usage_summary` ya tiene filas para ese día: omite
- Si no: Lee desde `events_raw`
- Extrae datos de `AppUsage` del payload
- Agrupa por contractor, app y día
- Calcula beats activos por app
- **Inserta** en `app_usage_summary`

**Ejemplo con cURL:**

```bash
curl -X GET "http://localhost:3001/adt/etl/process-app-usage?from=2025-11-22&to=2025-11-27" \
  -H "Authorization: Bearer TU_TOKEN_AQUI"
```

**Nota:** Este ETL es **idempotente** - puedes ejecutarlo múltiples veces para el mismo rango sin crear duplicados. Normalmente no es necesario ejecutarlo porque la MV `mv_app_usage_summary` procesa automáticamente los eventos nuevos.

---

## 📊 Paso 2: Consultar Endpoints ADT

Una vez ejecutado el ETL, puedes consultar las tablas ADT usando estos endpoints:

### 1. Métricas Diarias

Obtiene métricas diarias de productividad desde `contractor_daily_metrics`.

**Endpoint:**

```bash
GET http://localhost:3001/adt/daily-metrics/:contractorId?days=30
```

**Ejemplo:**

```bash
# Obtener métricas de los últimos 30 días (default)
GET http://localhost:3001/adt/daily-metrics/contractor-123

# Obtener métricas de los últimos 7 días
GET http://localhost:3001/adt/daily-metrics/contractor-123?days=7
```

**Respuesta incluye:**

- `total_beats`, `active_beats`, `idle_beats`
- `active_percentage`
- `total_keyboard_inputs`, `total_mouse_clicks`
- `productivity_score` (multi-factor)
- `workday` (fecha)

---

### 2. Métricas en Tiempo Real

Calcula métricas del día actual desde `contractor_activity_15s` con caché de 30 segundos.

**Endpoint:**

```bash
GET http://localhost:3001/adt/realtime-metrics/:contractorId?workday=2025-11-27
```

**Ejemplo:**

```bash
# Métricas del día actual
GET http://localhost:3001/adt/realtime-metrics/contractor-123

# Métricas de un día específico
GET http://localhost:3001/adt/realtime-metrics/contractor-123?workday=2025-11-27
```

**Respuesta incluye:**

- Mismas métricas que daily-metrics pero calculadas en tiempo real
- Ideal para dashboards que se actualizan frecuentemente

---

### 3. Resúmenes de Sesión

Obtiene resúmenes de sesiones desde `session_summary`.

**Endpoint:**

```bash
GET http://localhost:3001/adt/sessions/:contractorId?days=30
```

**Ejemplo:**

```bash
# Últimas 30 sesiones
GET http://localhost:3001/adt/sessions/contractor-123

# Últimas 7 sesiones
GET http://localhost:3001/adt/sessions/contractor-123?days=7
```

**Respuesta incluye:**

- `session_id`, `session_start`, `session_end`
- `total_seconds`, `active_seconds`, `idle_seconds`
- `productivity_score`

---

### 4. Actividad Detallada

Obtiene beats individuales de 15 segundos desde `contractor_activity_15s`.

**Endpoint:**

```bash
GET http://localhost:3001/adt/activity/:contractorId?limit=100&from=2025-11-27&to=2025-11-27
```

**Ejemplo:**

```bash
# Últimos 100 beats
GET http://localhost:3001/adt/activity/contractor-123?limit=100

# Beats de un día específico
GET http://localhost:3001/adt/activity/contractor-123?from=2025-11-27&to=2025-11-27&limit=1000
```

---

### 5. Uso de Aplicaciones

Obtiene uso de aplicaciones desde `app_usage_summary`.

**Endpoint:**

```bash
GET http://localhost:3001/adt/app-usage/:contractorId?days=30
```

**Ejemplo:**

```bash
# Uso de apps de los últimos 30 días
GET http://localhost:3001/adt/app-usage/contractor-123

# Uso de apps de los últimos 7 días
GET http://localhost:3001/adt/app-usage/contractor-123?days=7
```

**Respuesta incluye:**

- `app_name`, `workday`, `active_beats`
- Un objeto por app por día

---

### 6. Ranking de Productividad

Obtiene ranking de productividad de múltiples contractors.

**Endpoint:**

```bash
GET http://localhost:3001/adt/ranking?days=7&limit=10
```

**Ejemplo:**

```bash
# Top 10 de los últimos 7 días
GET http://localhost:3001/adt/ranking?days=7&limit=10

# Top 20 de los últimos 30 días
GET http://localhost:3001/adt/ranking?days=30&limit=20
```

---

## 🔐 Autenticación

Todos los endpoints requieren:

**Header:**

```
Authorization: Bearer TU_TOKEN_AQUI
```

**Roles requeridos:**

- **ETL**: Solo `Superadmin`
- **Consultas**: `Superadmin`, `TeamAdmin`, o `Visualizer`

---

## 📝 Ejemplo Completo con Postman

1. **Ejecutar ETL (en orden):**

   ```
   GET http://localhost:3001/adt/etl/process-events
   GET http://localhost:3001/adt/etl/process-app-usage  (puede ejecutarse en paralelo con process-events)
   GET http://localhost:3001/adt/etl/process-daily-metrics
   GET http://localhost:3001/adt/etl/process-session-summaries
   ```

2. **Consultar métricas:**
   ```
   GET http://localhost:3001/adt/daily-metrics/contractor-123?days=5
   GET http://localhost:3001/adt/realtime-metrics/contractor-123
   GET http://localhost:3001/adt/sessions/contractor-123?days=5
   ```

---

## ⚠️ Notas Importantes

1. **Procesamiento Automático**: Las Materialized Views procesan automáticamente los eventos nuevos. No necesitas ejecutar `process-events` ni `process-app-usage` en el flujo normal.

2. **ETL Manual para Backfill**: Los ETL manuales (`process-events`, `process-app-usage`) son **idempotentes** - usan `ALTER TABLE ... DELETE` antes de insertar, así que puedes ejecutarlos múltiples veces sin crear duplicados.

3. **Orden del ETL Manual**: Si ejecutas ETL manual, el orden recomendado es:
   - Primero: `process-events` (o `process-app-usage` en paralelo)
   - Segundo: `process-daily-metrics`
   - Tercero: `process-session-summaries`

4. **Dependencias**:
   - `process-daily-metrics` requiere que `contractor_activity_15s` tenga datos
   - `process-session-summaries` requiere que `contractor_activity_15s` tenga datos

5. **Performance**: Los ETL procesan todos los datos disponibles si no especificas filtros de fecha. Siempre usa rangos de fechas para backfills.

6. **Fechas**: Usa formato `YYYY-MM-DD` o `YYYY-MM-DDTHH:MM:SS` para fechas

---

## 🐛 Troubleshooting

**Error: "No data found"**

- Verifica que hayas ejecutado el ETL en orden
- Verifica que las tablas RAW tengan datos

**Error: "Unauthorized"**

- Verifica que tengas el token Bearer correcto
- Verifica que tu rol tenga permisos (Superadmin para ETL)

**Error: "Table does not exist"**

- Las tablas se crean automáticamente al iniciar ADT_MS
- Verifica que el servicio esté corriendo
