# Scripts de ADT_MS

Este directorio contiene scripts para:

- Población de datos de prueba
- Optimización de índices
- Migraciones y mantenimiento

## populate-dimensions.ts

Script para poblar las tablas de dimensiones (`apps_dimension` y `domains_dimension`) con valores por defecto.

### Uso

```bash
# Desde el directorio ADT_MS
pnpm run populate:dimensions
```

### Descripción

Este script:

- Verifica si las tablas están vacías
- Inserta valores por defecto para apps y dominios
- Incluye categorías: `productive`, `neutral`, `non_productive`
- Define pesos para el cálculo de productividad

**Nota:** Las tablas también se poblan automáticamente al iniciar el servicio `ADT_MS`, pero este script es útil para poblar manualmente sin reiniciar.

---

## populate-test-data.ts

Script para poblar ClickHouse con datos de prueba para validar las rutas ADT.

### Características

- **10 contratistas** con diferentes niveles de productividad
- **5 días de datos** (anteriores a hoy)
- **~8 horas de trabajo por día** por contratista
- **Eventos distribuidos en sesiones** a lo largo del día
- **3 niveles de productividad**:
  - 3 muy productivos (80-90%)
  - 3 medianamente productivos (60-75%)
  - 4 poco productivos (30-50%)

### Uso

```bash
# Desde el directorio ADT_MS
pnpm run populate:test
```

### Requisitos

1. ClickHouse debe estar corriendo y accesible
2. Las variables de entorno deben estar configuradas en `.env`:
   ```
   CLICKHOUSE_HOST=localhost
   CLICKHOUSE_PORT=8123
   CLICKHOUSE_USERNAME=default
   CLICKHOUSE_PASSWORD=
   CLICKHOUSE_DATABASE=metrics_db
   ```
3. Las tablas RAW deben existir (se crean automáticamente al iniciar el servicio)

### Datos Generados

El script genera datos en las siguientes tablas:

- **`contractor_info_raw`**: Información de los 10 contratistas
- **`sessions_raw`**: Sesiones de trabajo (2-4 sesiones por día)
- **`agent_sessions_raw`**: Sesiones de agentes
- **`events_raw`**: Eventos de actividad (heartbeats cada 15 segundos)

### Estructura de los Datos

#### Apps utilizadas (todos los contratistas):

- **Productivas**: Code, Notion
- **Neutrales**: Chrome, Edge, Slack, Teams

#### Dominios utilizados (todos los contratistas):

- **Productivos**: github.com, stackoverflow.com, teamandes.atlassian.net, docs.google.com
- **No productivos**: www.youtube.com, www.reddit.com, www.facebook.com, www.instagram.com

#### Distribución según productividad:

**Muy productivos (80-90%)**:

- 70% tiempo en apps productivas, 30% neutrales
- 80% tiempo en dominios productivos, 20% otros
- 20-50 inputs de teclado por beat
- 5-15 clicks de mouse por beat
- 0-0.5 segundos de idle time

**Medianamente productivos (60-75%)**:

- 50% tiempo en apps productivas, 50% neutrales
- 50% tiempo en dominios productivos, 50% otros
- 10-30 inputs de teclado por beat
- 3-11 clicks de mouse por beat
- 1-3 segundos de idle time

**Poco productivos (30-50%)**:

- 40% tiempo en apps productivas, 60% neutrales
- 30% tiempo en dominios productivos, 70% no productivos
- 2-10 inputs de teclado por beat
- 1-4 clicks de mouse por beat
- 5-13 segundos de idle time

### Próximos Pasos

Después de ejecutar el script:

1. **Ejecutar el ETL** para generar las tablas ADT:

   ```bash
   # Llamar al endpoint de ETL (desde API_GATEWAY)
   POST /adt/etl/process-events-to-activity
   POST /adt/etl/process-activity-to-daily-metrics
   POST /adt/etl/process-activity-to-session-summary
   ```

2. **Consultar los endpoints ADT** para verificar los resultados:

   ```bash
   GET /adt/daily-metrics/:contractorId?days=5
   GET /adt/realtime-metrics/:contractorId
   GET /adt/sessions/:contractorId?days=5
   ```

3. **Verificar la productividad** de los contratistas (debería estar entre los rangos esperados)

### Notas

- El script genera aproximadamente **1,920 beats por día** (8 horas × 60 minutos × 60 segundos / 15 segundos)
- Los eventos se distribuyen en **2-4 sesiones por día** (simulando pausas, almuerzo, etc.)
- Cada beat representa **15 segundos de actividad**
- Los payloads siguen el formato definitivo del sistema

---

## add-skip-indexes.sql

Script para agregar **Skip Indexes** que optimizan búsquedas por `session_id` en las tablas principales.

### ¿Qué hace?

Agrega índices secundarios (skip indexes) en ClickHouse para mejorar el rendimiento de:

- JOINs por `session_id` en `processActivityToSessionSummary`
- Filtros por `session_id` en queries del ETL
- Búsquedas de sesiones específicas

### Uso

```bash
# Conectarse a ClickHouse
clickhouse-client --host localhost --port 9000

# Ejecutar el script
SOURCE add-skip-indexes.sql;

# IMPORTANTE: Materializar los índices para datos existentes
ALTER TABLE contractor_activity_15s MATERIALIZE INDEX idx_session_id;
ALTER TABLE events_raw MATERIALIZE INDEX idx_session_id;
```

### Impacto Esperado

- **Antes**: JOINs por `session_id` tardan 5-15 segundos
- **Después**: JOINs por `session_id` tardan 0.5-2 segundos
- **Overhead**: ~5-10% más lento en escritura (aceptable)

### Verificación

```sql
-- Ver índices creados
SELECT
  table,
  name,
  type,
  expr
FROM system.data_skipping_indices
WHERE database = 'your_database';
```

**Nota**: Ver documentación completa en `docs/INDEX_OPTIMIZATION.md`

---

## create-session-lookup.sql

Script para crear una tabla de lookup que mapea `session_id` → `contractor_id` para JOINs más rápidos.

### ¿Cuándo usar?

- Si los JOINs por `session_id` son muy frecuentes (>100 veces/día)
- Si necesitas búsquedas rápidas de sesiones sin escanear tablas grandes
- Como alternativa o complemento a los skip indexes

### Uso

```bash
# Conectarse a ClickHouse
clickhouse-client --host localhost --port 9000

# Ejecutar el script
SOURCE create-session-lookup.sql;
```

### Mantenimiento

La tabla se puede mantener manualmente o con una Materialized View (ver comentarios en el script).

**Nota**: Este script es opcional. Los skip indexes suelen ser suficientes para la mayoría de casos.

---

## create-materialized-views.sql

Script para crear Materialized Views que procesan eventos automáticamente.

Ver documentación en `docs/ETL_PRODUCTION_GUIDE.md` para más detalles.
