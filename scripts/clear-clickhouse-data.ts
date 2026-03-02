import 'dotenv/config';
import { createClient } from '@clickhouse/client';

/**
 * Script para limpiar todas las tablas de la base de datos de ClickHouse configurada.
 *
 * Por defecto elimina datos de tablas RAW y ADT.
 * Opcionalmente puede limpiar también tablas de dimensiones (apps_dimension, domains_dimension).
 *
 * Uso:
 *   pnpm ts-node -r tsconfig-paths/register scripts/clear-clickhouse-data.ts
 *
 * Opciones por variables de entorno:
 *   INCLUDE_DIMENSIONS=true  -> también limpia tablas de dimensiones
 */

const CLICKHOUSE_HOST = process.env.CLICKHOUSE_HOST || 'localhost';
const CLICKHOUSE_PORT = parseInt(process.env.CLICKHOUSE_PORT || '8123', 10);
const CLICKHOUSE_USERNAME = process.env.CLICKHOUSE_USERNAME || 'default';
const CLICKHOUSE_PASSWORD = process.env.CLICKHOUSE_PASSWORD || '';
const CLICKHOUSE_DATABASE = process.env.CLICKHOUSE_DATABASE || 'default';
const INCLUDE_DIMENSIONS =
  (process.env.INCLUDE_DIMENSIONS || 'false').toLowerCase() === 'true';

type TableItem = { name: string; category: 'RAW' | 'ADT' | 'DIMENSION' };

const TABLES: TableItem[] = [
  // RAW
  { name: 'events_raw', category: 'RAW' },
  { name: 'sessions_raw', category: 'RAW' },
  { name: 'agent_sessions_raw', category: 'RAW' },
  { name: 'contractor_info_raw', category: 'RAW' },
  // ADT
  { name: 'contractor_activity_15s', category: 'ADT' },
  { name: 'contractor_daily_metrics', category: 'ADT' },
  { name: 'session_summary', category: 'ADT' },
  { name: 'app_usage_summary', category: 'ADT' },
  // DIMENSIONS (opcional)
  { name: 'apps_dimension', category: 'DIMENSION' },
  { name: 'domains_dimension', category: 'DIMENSION' },
];

async function main() {
  console.log('🚨 Limpieza de datos en ClickHouse');
  console.log('----------------------------------');
  console.log(`Host: ${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}`);
  console.log(`DB:   ${CLICKHOUSE_DATABASE}`);
  console.log(`User: ${CLICKHOUSE_USERNAME}`);
  console.log(`Incluye dimensiones: ${INCLUDE_DIMENSIONS ? 'Sí' : 'No'}`);
  console.log('');

  const client = createClient({
    host: `http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}`,
    username: CLICKHOUSE_USERNAME,
    password: CLICKHOUSE_PASSWORD,
    database: CLICKHOUSE_DATABASE,
  });

  try {
    await client.ping();
    console.log('✅ Conectado a ClickHouse\n');
  } catch (error) {
    console.error('❌ No se pudo conectar a ClickHouse:', error);
    process.exit(1);
  }

  // Filtrar tablas según opción de dimensiones
  const tablesToClear = TABLES.filter(
    (t) => t.category !== 'DIMENSION' || INCLUDE_DIMENSIONS,
  );

  // Consultar qué tablas existen realmente en la DB
  const tableParams = tablesToClear.map((t, i) => ({
    key: `t${i}`,
    value: t.name,
  }));
  const inPlaceholders = tableParams.map((p) => `{${p.key}:String}`).join(',');
  const existing = await client.query({
    query: `
      SELECT name 
      FROM system.tables 
      WHERE database = {db:String}
        AND name IN (${inPlaceholders})
    `,
    query_params: {
      db: CLICKHOUSE_DATABASE,
      ...Object.fromEntries(tableParams.map((p) => [p.key, p.value])),
    },
    format: 'JSONEachRow',
  });
  const existingRows = (await existing.json()) as { name: string }[];
  const existingNames = new Set(existingRows.map((r) => r.name));

  // Orden sugerido: ADT luego RAW, dimensiones al final si se incluyen
  // (no hay FK, pero es un orden razonable)
  const order: TableItem[] = [
    ...tablesToClear.filter((t) => t.category === 'ADT'),
    ...tablesToClear.filter((t) => t.category === 'RAW'),
    ...tablesToClear.filter((t) => t.category === 'DIMENSION'),
  ];

  for (const table of order) {
    if (!existingNames.has(table.name)) {
      console.log(`ℹ️  Tabla no existe: ${table.name} (omitida)`);
      continue;
    }
    const fqName = `${CLICKHOUSE_DATABASE}.${table.name}`;
    process.stdout.write(
      `🧹 Limpiando ${table.category} -> ${table.name} ... `,
    );
    try {
      // Intentar TRUNCATE primero (más rápido)
      await client.command({ query: `TRUNCATE TABLE ${fqName}` });
      console.log('OK (TRUNCATE)');
    } catch {
      // Fallback: ALTER DELETE WHERE 1
      try {
        await client.command({ query: `ALTER TABLE ${fqName} DELETE WHERE 1` });
        // Forzar merge para engines MergeTree
        await client.command({ query: `OPTIMIZE TABLE ${fqName} FINAL` });
        console.log('OK (ALTER DELETE)');
      } catch (err) {
        console.log('ERROR');
        console.error(
          `   ❌ No se pudo limpiar ${table.name}:`,
          (err as Error).message,
        );
      }
    }
  }

  console.log('\n✅ Limpieza finalizada.');
  if (!INCLUDE_DIMENSIONS) {
    console.log(
      'ℹ️ Las tablas de dimensiones NO fueron limpiadas. Usa INCLUDE_DIMENSIONS=true para incluirlas.',
    );
  } else {
    console.log(
      '⚠️ Recuerda repoblar tablas de dimensiones después (scripts/populate-dimensions.ts).',
    );
  }

  await client.close();
}

main().catch((err) => {
  console.error('❌ Error inesperado:', err);
  process.exit(1);
});
