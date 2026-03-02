import 'dotenv/config';
import { createClient } from '@clickhouse/client';
import { readFileSync } from 'fs';
import { resolve } from 'path';

/**
 * Crea (o recrea si no existen) las Materialized Views en la base de datos de ClickHouse
 * configurada por variables de entorno.
 *
 * Usa el SQL de scripts/create-materialized-views.sql, alineado con la lógica de los ETL.
 *
 * Uso:
 *   pnpm ts-node -r tsconfig-paths/register scripts/create-materialized-views.ts
 *
 * Variables de entorno:
 *   CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USERNAME, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE
 */

const CLICKHOUSE_HOST = process.env.CLICKHOUSE_HOST || 'localhost';
const CLICKHOUSE_PORT = parseInt(process.env.CLICKHOUSE_PORT || '8123', 10);
const CLICKHOUSE_USERNAME = process.env.CLICKHOUSE_USERNAME || 'default';
const CLICKHOUSE_PASSWORD = process.env.CLICKHOUSE_PASSWORD || '';
const CLICKHOUSE_DATABASE = process.env.CLICKHOUSE_DATABASE || 'default';

async function main() {
  console.log('⚙️  Creación/actualización de Materialized Views');
  console.log('-----------------------------------------------');
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
  } catch (e) {
    console.error('❌ No se pudo conectar a ClickHouse:', e);
    process.exit(1);
  }

  // Asegurar DB
  try {
    await client.command({
      query: `CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DATABASE}`,
    });
    console.log(`✅ Base de datos ${CLICKHOUSE_DATABASE} OK`);
  } catch (e) {
    console.error(
      '❌ Error asegurando la base de datos:',
      (e as Error).message,
    );
    process.exit(1);
  }

  // Cargar SQL de archivo
  const sqlPath = resolve(__dirname, 'create-materialized-views.sql');
  let sql: string;
  try {
    sql = readFileSync(sqlPath, 'utf8');
  } catch (e) {
    console.error(`❌ No se pudo leer ${sqlPath}:`, (e as Error).message);
    process.exit(1);
    return;
  }

  // Eliminar líneas de comentario y luego dividir por ';'
  const noComments = sql
    .split('\n')
    .map((line) => (line.trim().startsWith('--') ? '' : line))
    .join('\n');

  // Dividir por ';' y ejecutar cada sentencia no vacía
  const statements = noComments
    .split(';')
    .map((s) => s.trim())
    .filter((s) => s.length > 0);

  for (const stmt of statements) {
    try {
      await client.command({ query: stmt });
      console.log(`✅ Ejecutado:\n${stmt.split('\n')[0]}...`);
    } catch (e) {
      console.error('❌ Error ejecutando sentencia:', (e as Error).message);
      console.error('Sentencia:', stmt);
      await client.close();
      process.exit(1);
    }
  }

  // Verificar MVs creadas
  try {
    const res = await client.query({
      query: `
        SELECT name, engine
        FROM system.tables
        WHERE database = {db:String} AND engine = 'MaterializedView'
        ORDER BY name
      `,
      format: 'JSONEachRow',
      query_params: { db: CLICKHOUSE_DATABASE },
    });
    const rows = await res.json<{ name: string; engine: string }[]>();
    if (rows.length === 0) {
      console.warn(
        '⚠️ No se encontraron Materialized Views en la base. Revisa el script SQL y la base seleccionada.',
      );
    } else {
      console.log('\n📋 Materialized Views registradas:');
      for (const r of rows) {
        console.log(` - ${r.name} (${r.engine})`);
      }
    }
  } catch (e) {
    console.error('❌ Error consultando MVs:', (e as Error).message);
  }

  await client.close();
  console.log('\n✅ Proceso finalizado.');
}

main().catch((e) => {
  console.error('❌ Error inesperado:', e);
  process.exit(1);
});
