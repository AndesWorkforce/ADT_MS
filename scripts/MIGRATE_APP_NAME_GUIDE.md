# Guía de Migración: app_name → name

Esta guía te ayudará a ejecutar la migración que renombra la columna `app_name` a `name` y asigna tipos a las aplicaciones existentes en ClickHouse.

## 📋 Requisitos Previos

1. **Acceso a ClickHouse**: Necesitas tener acceso al servidor de ClickHouse
2. **Credenciales**: Usuario y contraseña de ClickHouse
3. **Cliente de ClickHouse**: `clickhouse-client` instalado o acceso vía web UI

## 🔍 Paso 1: Verificar si necesitas la migración

Antes de ejecutar la migración, verifica si tu tabla `apps_dimension` tiene la columna `app_name`:

### Opción A: Usando clickhouse-client (Línea de comandos)

```bash
clickhouse-client --host 72.61.129.234 --port 9000 --user <TU_USUARIO> --password <TU_PASSWORD> --database andes_db --query "SELECT name FROM system.columns WHERE database = 'andes_db' AND table = 'apps_dimension' AND name = 'app_name'"
```

Si devuelve una fila con `app_name`, necesitas ejecutar la migración.

### Opción B: Usando la interfaz web de ClickHouse

1. Accede a `http://72.61.129.234:8123/play`
2. Ejecuta esta query:

```sql
SELECT name
FROM system.columns
WHERE database = 'andes_db'
  AND table = 'apps_dimension'
  AND name = 'app_name'
```

Si devuelve resultados, necesitas la migración.

## 🚀 Paso 2: Hacer Backup (Recomendado)

Antes de ejecutar cualquier migración, es recomendable hacer un backup:

```bash
# Exportar datos actuales
clickhouse-client --host 72.61.129.234 --port 9000 --user <TU_USUARIO> --password <TU_PASSWORD> --database andes_db --query "SELECT * FROM apps_dimension FORMAT CSV" > backup_apps_dimension_$(date +%Y%m%d_%H%M%S).csv
```

## 📝 Paso 3: Ejecutar la Migración

### Método 1: Usando clickhouse-client (Recomendado)

```bash
# Navegar al directorio del proyecto
cd ADT_MS

# Ejecutar el script de migración
clickhouse-client \
  --host 72.61.129.234 \
  --port 9000 \
  --user <TU_USUARIO> \
  --password <TU_PASSWORD> \
  --database andes_db \
  --multiquery \
  < scripts/migrate-app-name-to-name.sql
```

**Reemplaza:**

- `<TU_USUARIO>` con tu usuario de ClickHouse
- `<TU_PASSWORD>` con tu contraseña de ClickHouse

### Método 2: Copiar y pegar en la interfaz web

1. Abre el archivo `ADT_MS/scripts/migrate-app-name-to-name.sql`
2. Copia todo el contenido
3. Accede a `http://72.61.129.234:8123/play`
4. Selecciona la base de datos `andes_db`
5. Pega el contenido del script
6. Haz clic en "Execute" o presiona `Ctrl+Enter`

### Método 3: Ejecutar query por query

Si prefieres ejecutar paso a paso, puedes ejecutar cada sección del script manualmente:

```sql
-- 1. Crear tabla temporal
CREATE TABLE IF NOT EXISTS apps_dimension_temp (
  id String,
  name String,
  category Nullable(String),
  type Nullable(String),
  weight Nullable(Float64) DEFAULT 0.5,
  created_at DateTime DEFAULT now(),
  updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;

-- 2. Migrar datos (ejecutar el SELECT completo del script)

-- 3. Eliminar tabla antigua
DROP TABLE IF EXISTS apps_dimension;

-- 4. Renombrar tabla temporal
RENAME TABLE apps_dimension_temp TO apps_dimension;
```

## ✅ Paso 4: Verificar la Migración

Después de ejecutar la migración, verifica que todo esté correcto:

```sql
-- Verificar que la columna name existe
SELECT name
FROM system.columns
WHERE database = 'andes_db'
  AND table = 'apps_dimension'
  AND name = 'name';

-- Verificar que app_name ya no existe
SELECT name
FROM system.columns
WHERE database = 'andes_db'
  AND table = 'apps_dimension'
  AND name = 'app_name';

-- Ver algunos registros con sus tipos asignados
SELECT id, name, category, type, weight
FROM apps_dimension
LIMIT 10;

-- Contar cuántas apps tienen tipo asignado
SELECT
  count() as total,
  countIf(type IS NOT NULL AND type != '') as with_type,
  countIf(type IS NULL OR type = '') as without_type
FROM apps_dimension;
```

## 🔧 Solución de Problemas

### Error: "Table already exists"

Si obtienes un error de que la tabla temporal ya existe:

```sql
DROP TABLE IF EXISTS apps_dimension_temp;
```

Luego vuelve a ejecutar la migración.

### Error: "Column app_name does not exist"

Si no existe `app_name`, significa que ya tienes la estructura nueva. No necesitas ejecutar esta migración.

### Error de permisos

Asegúrate de tener permisos para:

- `CREATE TABLE`
- `DROP TABLE`
- `RENAME TABLE`
- `INSERT INTO`
- `SELECT FROM`

### Verificar datos antes de eliminar

Si quieres ser más cauteloso, puedes verificar los datos antes de eliminar la tabla original:

```sql
-- Ver cuántos registros se migrarán
SELECT count() FROM apps_dimension;

-- Ver una muestra de los datos que se migrarán
SELECT
  if(id = '' OR id IS NULL, 'SIN_ID', id) as id,
  if(name != '' AND name IS NOT NULL, name,
    if(app_name != '' AND app_name IS NOT NULL, app_name, 'Unknown')
  ) as name,
  category,
  type
FROM apps_dimension
LIMIT 20;
```

## 📊 Resultado Esperado

Después de la migración exitosa:

1. ✅ La columna `app_name` ya no existe
2. ✅ La columna `name` contiene todos los nombres de las apps
3. ✅ Las apps tienen tipos asignados según su nombre (Code, Web, Design, Chat, etc.)
4. ✅ Todos los datos se mantienen intactos
5. ✅ Los IDs se generan automáticamente si no existían

## 🆘 Soporte

Si encuentras algún problema durante la migración:

1. Verifica los logs de ClickHouse
2. Revisa que tengas los permisos necesarios
3. Asegúrate de que la base de datos `andes_db` existe
4. Verifica que la tabla `apps_dimension` existe antes de ejecutar
