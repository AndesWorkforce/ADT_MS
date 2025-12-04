import 'dotenv/config';
import { createClient } from '@clickhouse/client';

/**
 * Agrega y materializa Skip Indexes para acelerar búsquedas por session_id.
 *
 * Uso:
 *   pnpm ts-node -r tsconfig-paths/register scripts/add-skip-indexes.ts
 *
 * Variables de entorno (opcionales):
 *   CLICKHOUSE_HOST=localhost
 *   CLICKHOUSE_PORT=8123
 *   CLICKHOUSE_USERNAME=default
 *   CLICKHOUSE_PASSWORD=
 *   CLICKHOUSE_DATABASE=default
 */

const CLICKHOUSE_HOST = process.env.CLICKHOUSE_HOST || 'localhost';
const CLICKHOUSE_PORT = parseInt(process.env.CLICKHOUSE_PORT || '8123', 10);
const CLICKHOUSE_USERNAME = process.env.CLICKHOUSE_USERNAME || 'default';
const CLICKHOUSE_PASSWORD = process.env.CLICKHOUSE_PASSWORD || '';
const CLICKHOUSE_DATABASE = process.env.CLICKHOUSE_DATABASE || 'default';

async function ensureTableExists(
  client: ReturnType<typeof createClient>,
  table: string,
) {
  const res = await client.query({
    query: `
      SELECT count() AS cnt
      FROM system.tables 
      WHERE database = {db:String} AND name = {tbl:String}
    `,
    format: 'JSONEachRow',
    query_params: {
      db: CLICKHOUSE_DATABASE,
      tbl: table,
    },
  });
  const [{ cnt }] = (await res.json()) as { cnt: number }[];
  if (!cnt) {
    throw new Error(`La tabla ${CLICKHOUSE_DATABASE}.${table} no existe`);
  }
}

async function addAndMaterializeIndex(
  client: ReturnType<typeof createClient>,
  table: string,
  indexName: string,
  columnExpr: string,
  typeAndGranularity = 'TYPE set(100) GRANULARITY 4',
) {
  const fqName = `${CLICKHOUSE_DATABASE}.${table}`;
  process.stdout.write(`➕ Añadiendo índice ${indexName} en ${fqName} ... `);
  await client.command({
    query: `ALTER TABLE ${fqName} ADD INDEX IF NOT EXISTS ${indexName} ${columnExpr} ${typeAndGranularity}`,
  });
  console.log('OK');

  process.stdout.write(
    `🧱 Materializando índice ${indexName} en ${fqName} ... `,
  );
  await client.command({
    query: `ALTER TABLE ${fqName} MATERIALIZE INDEX ${indexName}`,
  });
  console.log('OK');
}

async function main() {
  console.log('⚙️  Aplicando Skip Indexes para session_id');
  console.log('----------------------------------------');
  console.log(`Host: ${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}`);
  console.log(`DB:   ${CLICKHOUSE_DATABASE}`);
  console.log(`User: ${CLICKHOUSE_USERNAME}\n`);

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

  try {
    // Verificar tablas
    await ensureTableExists(client, 'contractor_activity_15s');
    await ensureTableExists(client, 'events_raw');
    await ensureTableExists(client, 'session_summary');

    // Índices sobre session_id
    await addAndMaterializeIndex(
      client,
      'contractor_activity_15s',
      'idx_session_id',
      'session_id',
    );
    await addAndMaterializeIndex(
      client,
      'events_raw',
      'idx_session_id',
      'session_id',
    );
    await addAndMaterializeIndex(
      client,
      'session_summary',
      'idx_session_id',
      'session_id',
    );

    // Índice adicional para agent_session_id si se usa en filtros/joins
    await addAndMaterializeIndex(
      client,
      'events_raw',
      'idx_agent_session_id',
      'agent_session_id',
    );

    console.log('\n✅ Índices aplicados y materializados correctamente.');
    console.log(
      'ℹ️ Para nuevos datos no hace falta re-materializar. Solo fue necesario para datos existentes.',
    );
  } catch (error) {
    console.error('❌ Error aplicando índices:', (error as Error).message);
    process.exit(1);
  } finally {
    await client.close();
  }
}

main().catch((err) => {
  console.error('❌ Error inesperado:', err);
  process.exit(1);
});
