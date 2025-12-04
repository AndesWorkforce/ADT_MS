# Guía de Migración de contractor_daily_metrics

## 📋 Resumen

Este script corrige los valores incorrectos en `contractor_daily_metrics` causados por el uso de `SummingMergeTree` que sumaba `active_percentage` y `productivity_score` cuando había duplicados.

## ⚠️ Antes de Ejecutar

1. **Hacer backup de la base de datos** (recomendado):

   ```bash
   # Si usas docker-compose, puedes hacer backup del volumen
   docker-compose exec clickhouse clickhouse-client --query "BACKUP DATABASE adt_db TO Disk('backups', 'backup_$(date +%Y%m%d_%H%M%S)')"
   ```

2. **Verificar que las variables de entorno estén configuradas**:
   - `CLICKHOUSE_HOST`
   - `CLICKHOUSE_PORT`
   - `CLICKHOUSE_USERNAME`
   - `CLICKHOUSE_PASSWORD`
   - `CLICKHOUSE_DATABASE`

3. **Asegurarse de que el servicio ADT_MS no esté ejecutando ETL** durante la migración (opcional pero recomendado).

## 🚀 Ejecución

### Opción 1: Usando npm/pnpm (Recomendado)

```bash
cd ADT_MS
pnpm migrate:daily-metrics
```

### Opción 2: Ejecutar directamente con ts-node

```bash
cd ADT_MS
pnpm ts-node -r tsconfig-paths/register scripts/migrate-contractor-daily-metrics.ts
```

## 📝 Qué Hace el Script

El script ejecuta los siguientes pasos:

1. **Detecta problemas**: Busca registros con:
   - Duplicados (múltiples filas para el mismo contractor_id + workday)
   - `active_percentage` > 100
   - `productivity_score` > 100

2. **Crea tabla temporal**: Crea `contractor_daily_metrics_new` con el engine `ReplacingMergeTree(created_at)`

3. **Recalcula métricas**: Para los días afectados, recalcula las métricas desde `contractor_activity_15s`

4. **Copia datos correctos**: Copia los datos que no tienen problemas de la tabla original

5. **Verifica resultados**: Verifica que no haya valores incorrectos en la nueva tabla

6. **Hace backup y renombra**:
   - Crea `contractor_daily_metrics_backup` (copia de seguridad)
   - Renombra `contractor_daily_metrics` → `contractor_daily_metrics_old`
   - Renombra `contractor_daily_metrics_new` → `contractor_daily_metrics`

## ✅ Después de la Migración

### 1. Reprocesar productivity_score

El script recalcula `active_percentage` correctamente, pero `productivity_score` se establece en 0 porque requiere datos de AppUsage/Browser. Debes reprocesar los días afectados:

```bash
# Para cada día afectado, ejecutar:
GET /adt/etl/process-daily-metrics?workday=2025-12-01
```

El script te mostrará los días afectados al final.

### 2. Verificar que todo funciona

```bash
# Probar el endpoint de ranking
GET /adt/ranking?workday=2025-12-01&limit=20

# Verificar que active_percentage y productivity_score estén ≤ 100
```

### 3. Limpiar tablas de backup (Opcional)

Una vez que verifiques que todo funciona correctamente, puedes eliminar las tablas de backup:

```sql
DROP TABLE contractor_daily_metrics_old;
DROP TABLE contractor_daily_metrics_backup;
```

## 🔍 Verificación Manual

Si quieres verificar manualmente antes o después de la migración:

```sql
-- Verificar valores incorrectos
SELECT
  contractor_id,
  workday,
  active_percentage,
  productivity_score,
  CASE
    WHEN active_percentage > 100 THEN 'ERROR: active_percentage > 100'
    WHEN productivity_score > 100 THEN 'ERROR: productivity_score > 100'
    ELSE 'OK'
  END as status
FROM contractor_daily_metrics FINAL
WHERE active_percentage > 100 OR productivity_score > 100
ORDER BY workday DESC
LIMIT 50;

-- Debe devolver 0 filas si todo está correcto
```

## ⚠️ Solución de Problemas

### Error: "Table already exists"

Si el script falla y necesitas reiniciarlo, primero elimina la tabla temporal:

```sql
DROP TABLE IF EXISTS contractor_daily_metrics_new;
```

### Error: "Connection refused"

Verifica que ClickHouse esté corriendo y que las variables de entorno estén correctas.

### Error durante el renombrado

Si algo sale mal durante el renombrado, puedes restaurar:

```sql
-- Restaurar desde backup
RENAME TABLE contractor_daily_metrics TO contractor_daily_metrics_failed;
RENAME TABLE contractor_daily_metrics_backup TO contractor_daily_metrics;
```

### Valores aún incorrectos después de la migración

1. Verifica que estés usando `FINAL` en las queries (ya está implementado en el código)
2. Verifica que el ETL haya reprocesado los días afectados
3. Ejecuta `OPTIMIZE TABLE contractor_daily_metrics FINAL;` para forzar el merge

## 📚 Referencias

- [Documentación del problema y solución](./../docs/FIX_RANKING_METRICS.md)
- [Script SQL alternativo](./fix-contractor-daily-metrics.sql)
