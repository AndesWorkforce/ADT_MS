-- Script para crear las tablas de dimensiones en ClickHouse
-- Estas tablas definen los pesos de productividad para apps y dominios

-- Tabla apps_dimension: Define el peso/productividad de cada aplicación (sincronizada con Prisma apps)
CREATE TABLE IF NOT EXISTS apps_dimension (
  id String,
  name String,
  category Nullable(String),  -- 'productive', 'neutral', 'non_productive'
  type Nullable(String),      -- Tipo de aplicación (ej: 'IDE', 'Browser', 'Communication', etc.)
  weight Nullable(Float64) DEFAULT 0.5,   -- 0.0 - 2.0 (1.0 = neutro, >1.0 = productivo, <1.0 = no productivo)
  created_at DateTime DEFAULT now(),
  updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;

-- Tabla domains_dimension: Define el peso/productividad de cada dominio web
CREATE TABLE IF NOT EXISTS domains_dimension (
  domain String,
  category String,  -- 'productive', 'neutral', 'non_productive'
  weight Float64,   -- 0.0 - 2.0
  created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY domain;

