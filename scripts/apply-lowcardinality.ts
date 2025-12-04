import 'dotenv/config';
import { createClient } from '@clickhouse/client';

/**
 * Convierte columnas String a LowCardinality(String) donde aporta rendimiento
 * en joins/agrupaciones.
 *
 * Uso:
 *   pnpm ts-node -r tsconfig-paths/register scripts/apply-lowcardinality.ts
 */

const CLICKHOUSE_HOST = process.env.CLICKHOUSE_HOST || 'localhost';
const CLICKHOUSE_PORT = parseInt(process.env.CLICKHOUSE_PORT || '8123', 10);
const CLICKHOUSE_USERNAME = process.env.CLICKHOUSE_USERNAME || 'default';
const CLICKHOUSE_PASSWORD = process.env.CLICKHOUSE_PASSWORD || '';
const CLICKHOUSE_DATABASE = process.env.CLICKHOUSE_DATABASE || 'default';

type AlterPlan = { table: string; column: string; targetType: string };

const PLAN: AlterPlan[] = [
  // RAW
  {
    table: 'events_raw',
    column: 'contractor_id',
    targetType: 'LowCardinality(String)',
  },
  {
    table: 'events_raw',
    column: 'session_id',
    targetType: 'LowCardinality(Nullable(String))',
  },
  {
    table: 'events_raw',
    column: 'agent_session_id',
    targetType: 'LowCardinality(Nullable(String))',
  },
  {
    table: 'events_raw',
    column: 'agent_id',
    targetType: 'LowCardinality(Nullable(String))',
  },

  // ADT
  {
    table: 'contractor_activity_15s',
    column: 'contractor_id',
    targetType: 'LowCardinality(String)',
  },
  {
    table: 'contractor_activity_15s',
    column: 'session_id',
    targetType: 'LowCardinality(Nullable(String))',
  },
  {
    table: 'contractor_activity_15s',
    column: 'agent_session_id',
    targetType: 'LowCardinality(Nullable(String))',
  },
  {
    table: 'contractor_activity_15s',
    column: 'agent_id',
    targetType: 'LowCardinality(Nullable(String))',
  },

  {
    table: 'contractor_daily_metrics',
    column: 'contractor_id',
    targetType: 'LowCardinality(String)',
  },

  {
    table: 'session_summary',
    column: 'contractor_id',
    targetType: 'LowCardinality(String)',
  },

  {
    table: 'app_usage_summary',
    column: 'contractor_id',
    targetType: 'LowCardinality(String)',
  },
  {
    table: 'app_usage_summary',
    column: 'app_name',
    targetType: 'LowCardinality(String)',
  },

  // Dimensions
  {
    table: 'apps_dimension',
    column: 'app_name',
    targetType: 'LowCardinality(String)',
  },
  {
    table: 'apps_dimension',
    column: 'category',
    targetType: 'LowCardinality(String)',
  },

  {
    table: 'domains_dimension',
    column: 'domain',
    targetType: 'LowCardinality(String)',
  },
  {
    table: 'domains_dimension',
    column: 'category',
    targetType: 'LowCardinality(String)',
  },
];

async function main() {
  console.log('⚙️  Aplicando LowCardinality en columnas clave');
  console.log('--------------------------------------------');
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

  let success = 0;
  let skipped = 0;
  let failed = 0;

  for (const step of PLAN) {
    const fqName = `${CLICKHOUSE_DATABASE}.${step.table}`;
    process.stdout.write(
      `🔧 ${fqName}.${step.column} → ${step.targetType} ... `,
    );
    try {
      // Ver tipo actual
      const cur = await client.query({
        query: `
          SELECT type 
          FROM system.columns 
          WHERE database = {db:String} AND table = {tbl:String} AND name = {col:String}
        `,
        format: 'JSONEachRow',
        query_params: {
          db: CLICKHOUSE_DATABASE,
          tbl: step.table,
          col: step.column,
        },
      });
      const rows = (await cur.json()) as { type: string }[];
      if (rows.length === 0) {
        console.log('OMITIDO (columna no existe)');
        skipped++;
        continue;
      }
      const currentType = rows[0].type;
      if (
        currentType.replace(/\s+/g, '') === step.targetType.replace(/\s+/g, '')
      ) {
        console.log('OK (ya tiene el tipo objetivo)');
        skipped++;
        continue;
      }

      await client.command({
        query: `ALTER TABLE ${fqName} MODIFY COLUMN ${step.column} ${step.targetType}`,
      });
      console.log('OK');
      success++;
    } catch (err) {
      console.log('ERROR');
      console.error(`   ❌ ${fqName}.${step.column}:`, (err as Error).message);
      failed++;
    }
  }

  console.log(
    `\n✅ Finalizado. Exitosos: ${success}, Omitidos: ${skipped}, Fallidos: ${failed}`,
  );
  await client.close();
}

main().catch((err) => {
  console.error('❌ Error inesperado:', err);
  process.exit(1);
});
