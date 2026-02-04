-- Script de migración para actualizar apps_dimension a la nueva estructura
-- Ejecutar este script si ya tienes datos en apps_dimension con la estructura antigua

-- Paso 1: Crear tabla temporal con la nueva estructura
CREATE TABLE IF NOT EXISTS apps_dimension_new (
  id String,
  name String,
  category Nullable(String),
  type Nullable(String),
  weight Nullable(Float64) DEFAULT 0.5,
  created_at DateTime DEFAULT now(),
  updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;

-- Paso 2: Migrar datos existentes (si los hay)
-- Generar IDs temporales para apps existentes y mapear app_name -> name
INSERT INTO apps_dimension_new (id, name, category, type, weight, created_at, updated_at)
SELECT 
  generateUUIDv4() as id,
  app_name as name,
  category,
  NULL as type,  -- No hay tipo en datos antiguos
  weight,
  created_at,
  now() as updated_at
FROM apps_dimension
WHERE 1=1;

-- Paso 3: Eliminar tabla antigua
DROP TABLE IF EXISTS apps_dimension;

-- Paso 4: Renombrar tabla nueva
RENAME TABLE apps_dimension_new TO apps_dimension;

