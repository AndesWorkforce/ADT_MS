-- Script de migración para renombrar app_name a name y asignar tipos a apps existentes
-- Este script migra la estructura antigua (app_name) a la nueva (name) y asigna tipos según el nombre de la app
-- IMPORTANTE: Ejecutar este script solo si tienes datos existentes con app_name

-- Paso 1: Crear tabla temporal con la nueva estructura
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

-- Paso 2: Migrar datos existentes
-- Si existe app_name, copiarlo a name. Si existe name, usarlo directamente.
-- Asignar tipos basados en el nombre de la app
INSERT INTO apps_dimension_temp (id, name, category, type, weight, created_at, updated_at)
SELECT 
  -- Generar ID si no existe, o usar el existente
  if(id = '' OR id IS NULL, generateUUIDv4(), id) as id,
  -- Usar name si existe y no está vacío, sino usar app_name
  if(name != '' AND name IS NOT NULL, name, 
    if(app_name != '' AND app_name IS NOT NULL, app_name, 'Unknown')
  ) as name,
  category,
  -- Asignar tipo basado en el nombre de la app (usar COALESCE para manejar ambos casos)
  CASE
    -- Code/IDEs
    WHEN lower(COALESCE(name, app_name, '')) LIKE '%code%' OR lower(COALESCE(name, app_name, '')) LIKE '%visual studio%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%intellij%' OR lower(COALESCE(name, app_name, '')) LIKE '%eclipse%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%sublime%' OR lower(COALESCE(name, app_name, '')) LIKE '%atom%'
         OR lower(COALESCE(name, app_name, '')) LIKE '%vim%' OR lower(COALESCE(name, app_name, '')) LIKE '%emacs%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%jetbrains%' OR lower(COALESCE(name, app_name, '')) = 'code' THEN 'Code'
    -- Web/Browsers
    WHEN lower(COALESCE(name, app_name, '')) LIKE '%chrome%' OR lower(COALESCE(name, app_name, '')) LIKE '%edge%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%firefox%' OR lower(COALESCE(name, app_name, '')) LIKE '%safari%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%opera%' OR lower(COALESCE(name, app_name, '')) LIKE '%brave%'
         OR lower(COALESCE(name, app_name, '')) = 'web' THEN 'Web'
    -- Design
    WHEN lower(COALESCE(name, app_name, '')) LIKE '%figma%' OR lower(COALESCE(name, app_name, '')) LIKE '%photoshop%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%illustrator%' OR lower(COALESCE(name, app_name, '')) LIKE '%sketch%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%adobe%' OR lower(COALESCE(name, app_name, '')) LIKE '%canva%'
         OR lower(COALESCE(name, app_name, '')) LIKE '%invision%' OR lower(COALESCE(name, app_name, '')) LIKE '%xd%' THEN 'Design'
    -- Chat/Messaging
    WHEN lower(COALESCE(name, app_name, '')) LIKE '%slack%' OR lower(COALESCE(name, app_name, '')) LIKE '%teams%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%discord%' OR lower(COALESCE(name, app_name, '')) LIKE '%whatsapp%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%telegram%' OR lower(COALESCE(name, app_name, '')) LIKE '%zoom%'
         OR lower(COALESCE(name, app_name, '')) LIKE '%skype%' OR lower(COALESCE(name, app_name, '')) LIKE '%messenger%' 
         OR lower(COALESCE(name, app_name, '')) = 'chat' THEN 'Chat'
    -- Office
    WHEN lower(COALESCE(name, app_name, '')) LIKE '%word%' OR lower(COALESCE(name, app_name, '')) LIKE '%excel%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%powerpoint%' OR lower(COALESCE(name, app_name, '')) LIKE '%outlook%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%onenote%' OR lower(COALESCE(name, app_name, '')) LIKE '%office%'
         OR lower(COALESCE(name, app_name, '')) LIKE '%libreoffice%' OR lower(COALESCE(name, app_name, '')) LIKE '%openoffice%' THEN 'Office'
    -- Productivity
    WHEN lower(COALESCE(name, app_name, '')) LIKE '%notion%' OR lower(COALESCE(name, app_name, '')) LIKE '%trello%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%asana%' OR lower(COALESCE(name, app_name, '')) LIKE '%jira%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%confluence%' OR lower(COALESCE(name, app_name, '')) LIKE '%todoist%'
         OR lower(COALESCE(name, app_name, '')) LIKE '%evernote%' THEN 'Productivity'
    -- Development Tools
    WHEN lower(COALESCE(name, app_name, '')) LIKE '%git%' OR lower(COALESCE(name, app_name, '')) LIKE '%docker%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%kubernetes%' OR lower(COALESCE(name, app_name, '')) LIKE '%terminal%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%cmd%' OR lower(COALESCE(name, app_name, '')) LIKE '%powershell%'
         OR lower(COALESCE(name, app_name, '')) LIKE '%postman%' OR lower(COALESCE(name, app_name, '')) LIKE '%insomnia%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%wsl%' THEN 'Development'
    -- Database
    WHEN lower(COALESCE(name, app_name, '')) LIKE '%pgadmin%' OR lower(COALESCE(name, app_name, '')) LIKE '%dbeaver%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%mysql%' OR lower(COALESCE(name, app_name, '')) LIKE '%mongodb%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%redis%' OR lower(COALESCE(name, app_name, '')) LIKE '%sql%'
         OR lower(COALESCE(name, app_name, '')) LIKE '%datagrip%' OR lower(COALESCE(name, app_name, '')) LIKE '%tableplus%' THEN 'Database'
    -- Cloud
    WHEN lower(COALESCE(name, app_name, '')) LIKE '%aws%' OR lower(COALESCE(name, app_name, '')) LIKE '%azure%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%gcp%' OR lower(COALESCE(name, app_name, '')) LIKE '%google cloud%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%heroku%' OR lower(COALESCE(name, app_name, '')) LIKE '%vercel%'
         OR lower(COALESCE(name, app_name, '')) LIKE '%netlify%' THEN 'Cloud'
    -- Entertainment
    WHEN lower(COALESCE(name, app_name, '')) LIKE '%youtube%' OR lower(COALESCE(name, app_name, '')) LIKE '%spotify%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%netflix%' OR lower(COALESCE(name, app_name, '')) LIKE '%twitch%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%steam%' OR lower(COALESCE(name, app_name, '')) LIKE '%game%'
         OR lower(COALESCE(name, app_name, '')) LIKE '%music%' OR lower(COALESCE(name, app_name, '')) LIKE '%media%' 
         OR lower(COALESCE(name, app_name, '')) = 'entertainment' THEN 'Entertainment'
    -- System
    WHEN lower(COALESCE(name, app_name, '')) LIKE '%explorer%' OR lower(COALESCE(name, app_name, '')) LIKE '%finder%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%settings%' OR lower(COALESCE(name, app_name, '')) LIKE '%control panel%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%task manager%' OR lower(COALESCE(name, app_name, '')) LIKE '%system%'
         OR lower(COALESCE(name, app_name, '')) LIKE '%windows%' OR lower(COALESCE(name, app_name, '')) LIKE '%macos%' 
         OR lower(COALESCE(name, app_name, '')) LIKE '%linux%' THEN 'System'
    ELSE type
  END as type,
  weight,
  created_at,
  now() as updated_at
FROM apps_dimension
WHERE 1=1;

-- Paso 3: Eliminar tabla antigua
DROP TABLE IF EXISTS apps_dimension;

-- Paso 4: Renombrar tabla temporal a apps_dimension
RENAME TABLE apps_dimension_temp TO apps_dimension;

