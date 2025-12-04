import 'dotenv/config';
import {
  createClient as chCreateClient,
  ClickHouseClient,
} from '@clickhouse/client';

/**
 * Script de migración para corregir contractor_daily_metrics
 *
 * Problema: SummingMergeTree estaba sumando active_percentage y productivity_score incorrectamente
 * Solución: Cambiar a ReplacingMergeTree y recalcular métricas desde contractor_activity_15s
 *
 * Uso:
 *   pnpm ts-node scripts/migrate-contractor-daily-metrics.ts
 */

interface ClickHouseConfig {
  host: string;
  port: number;
  username: string;
  password: string;
  database: string;
}

function getClickHouseConfig(): ClickHouseConfig {
  const config: ClickHouseConfig = {
    host: process.env.CLICKHOUSE_HOST || 'localhost',
    port: parseInt(process.env.CLICKHOUSE_PORT || '8123', 10),
    username: process.env.CLICKHOUSE_USERNAME || 'default',
    password: process.env.CLICKHOUSE_PASSWORD || '',
    database: process.env.CLICKHOUSE_DATABASE || 'adt_db',
  };

  console.log('📊 Configuración de ClickHouse:');
  console.log(`   Host: ${config.host}:${config.port}`);
  console.log(`   Database: ${config.database}`);
  console.log(`   Username: ${config.username}\n`);

  return config;
}

async function createCHClient(
  config: ClickHouseConfig,
): Promise<ClickHouseClient> {
  const client = chCreateClient({
    host: `http://${config.host}:${config.port}`,
    username: config.username,
    password: config.password,
    database: config.database,
  });

  // Probar conexión
  await client.ping();
  console.log('✅ Conexión a ClickHouse establecida\n');

  return client;
}

async function executeQuery(
  client: ClickHouseClient,
  query: string,
  description: string,
): Promise<any[]> {
  console.log(`🔄 ${description}...`);
  try {
    const result = await client.query({ query, format: 'JSONEachRow' });
    const data = await result.json();
    console.log(`   ✅ Completado\n`);
    return data as any[];
  } catch (error) {
    console.error(`   ❌ Error: ${error.message}\n`);
    throw error;
  }
}

async function executeCommand(
  client: ClickHouseClient,
  command: string,
  description: string,
): Promise<void> {
  console.log(`🔄 ${description}...`);
  try {
    await client.command({ query: command });
    console.log(`   ✅ Completado\n`);
  } catch (error) {
    console.error(`   ❌ Error: ${error.message}\n`);
    throw error;
  }
}

async function main() {
  console.log('🚀 Iniciando migración de contractor_daily_metrics\n');
  console.log('='.repeat(60));
  console.log('');

  const config = getClickHouseConfig();
  const client = await createCHClient(config);

  try {
    // PASO 1: Detectar duplicados y valores incorrectos
    console.log('📋 PASO 1: Detectando duplicados y valores incorrectos');
    console.log('-'.repeat(60));
    const problems = await executeQuery(
      client,
      `
        SELECT 
          contractor_id,
          workday,
          COUNT(*) as duplicates,
          SUM(total_beats) as total_beats_sum,
          SUM(active_beats) as active_beats_sum,
          MAX(active_percentage) as max_active_percentage,
          MAX(productivity_score) as max_productivity_score,
          CASE WHEN MAX(active_percentage) > 100 THEN 1 ELSE 0 END as has_incorrect_percentage,
          CASE WHEN MAX(productivity_score) > 100 THEN 1 ELSE 0 END as has_incorrect_score
        FROM contractor_daily_metrics
        GROUP BY contractor_id, workday
        HAVING duplicates > 1 OR MAX(active_percentage) > 100 OR MAX(productivity_score) > 100
        ORDER BY duplicates DESC, max_active_percentage DESC
        LIMIT 50
      `,
      'Buscando registros con problemas',
    );

    if (problems.length === 0) {
      console.log(
        '✅ No se encontraron problemas. Los datos están correctos.\n',
      );
      console.log(
        '💡 Si aún ves valores incorrectos, verifica que estés usando FINAL en las queries.\n',
      );
      return;
    }

    console.log(
      `⚠️  Se encontraron ${problems.length} registros con problemas:\n`,
    );
    problems.slice(0, 10).forEach((p: any) => {
      console.log(
        `   - ${p.contractor_id} | ${p.workday} | Duplicados: ${p.duplicates} | ` +
          `active_percentage: ${p.max_active_percentage} | productivity_score: ${p.max_productivity_score}`,
      );
    });
    if (problems.length > 10) {
      console.log(`   ... y ${problems.length - 10} más\n`);
    }

    // Obtener días únicos afectados
    const affectedDays = await executeQuery(
      client,
      `
        SELECT DISTINCT workday 
        FROM contractor_daily_metrics
        WHERE contractor_id IN (
          SELECT contractor_id 
          FROM contractor_daily_metrics
          GROUP BY contractor_id, workday
          HAVING COUNT(*) > 1 OR MAX(active_percentage) > 100 OR MAX(productivity_score) > 100
        )
        ORDER BY workday DESC
      `,
      'Obteniendo días afectados',
    );

    console.log(`📅 Días afectados: ${affectedDays.length}\n`);
    affectedDays.slice(0, 10).forEach((d: any) => {
      console.log(`   - ${d.workday}`);
    });
    if (affectedDays.length > 10) {
      console.log(`   ... y ${affectedDays.length - 10} más\n`);
    }

    // PASO 2: Crear tabla temporal con el engine correcto
    console.log('📋 PASO 2: Creando tabla temporal con engine correcto');
    console.log('-'.repeat(60));
    await executeCommand(
      client,
      `
        CREATE TABLE IF NOT EXISTS contractor_daily_metrics_new (
          contractor_id String,
          workday Date,
          total_beats UInt32,
          active_beats UInt32,
          idle_beats UInt32,
          active_percentage Float64,
          total_keyboard_inputs UInt64,
          total_mouse_clicks UInt64,
          avg_keyboard_per_min Float64,
          avg_mouse_per_min Float64,
          total_session_time_seconds UInt64,
          effective_work_seconds UInt64,
          productivity_score Float64,
          created_at DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(created_at)
        PARTITION BY workday
        ORDER BY (contractor_id, workday)
        TTL workday + INTERVAL 730 DAY
      `,
      'Creando tabla contractor_daily_metrics_new',
    );

    // PASO 3: Recalcular métricas correctamente desde contractor_activity_15s
    console.log(
      '📋 PASO 3: Recalculando métricas desde contractor_activity_15s',
    );
    console.log('-'.repeat(60));

    if (affectedDays.length > 0) {
      const affectedDaysList = affectedDays
        .map((d: any) => `'${d.workday}'`)
        .join(',');

      await executeCommand(
        client,
        `
          INSERT INTO contractor_daily_metrics_new
          SELECT 
            contractor_id,
            workday,
            COUNT() AS total_beats,
            SUM(1 - is_idle) AS active_beats,
            SUM(is_idle) AS idle_beats,
            round(100.0 * SUM(1 - is_idle) / COUNT(), 2) AS active_percentage,
            SUM(keyboard_count) AS total_keyboard_inputs,
            SUM(mouse_clicks) AS total_mouse_clicks,
            round(SUM(keyboard_count) / (COUNT() / 4.0), 2) AS avg_keyboard_per_min,
            round(SUM(mouse_clicks) / (COUNT() / 4.0), 2) AS avg_mouse_per_min,
            (COUNT() * 15) AS total_session_time_seconds,
            (SUM(1 - is_idle) * 15) AS effective_work_seconds,
            0.0 AS productivity_score,
            now() AS created_at
          FROM contractor_activity_15s
          WHERE workday IN (${affectedDaysList})
          GROUP BY contractor_id, workday
        `,
        `Recalculando métricas para ${affectedDays.length} días afectados`,
      );
    } else {
      console.log('   ⏭️  No hay días afectados para recalcular\n');
    }

    // PASO 4: Copiar datos correctos de la tabla original (días sin problemas)
    console.log('📋 PASO 4: Copiando datos correctos de la tabla original');
    console.log('-'.repeat(60));
    await executeCommand(
      client,
      `
        INSERT INTO contractor_daily_metrics_new
        SELECT *
        FROM contractor_daily_metrics FINAL
        WHERE (contractor_id, workday) NOT IN (
          SELECT contractor_id, workday
          FROM contractor_daily_metrics
          GROUP BY contractor_id, workday
          HAVING COUNT(*) > 1 OR MAX(active_percentage) > 100 OR MAX(productivity_score) > 100
        )
      `,
      'Copiando datos correctos',
    );

    // Verificar resultados antes de renombrar
    console.log('📋 PASO 5: Verificando resultados');
    console.log('-'.repeat(60));
    const verification = await executeQuery(
      client,
      `
        SELECT 
          COUNT(*) as total_records,
          COUNT(CASE WHEN active_percentage > 100 THEN 1 END) as incorrect_percentage,
          COUNT(CASE WHEN productivity_score > 100 THEN 1 END) as incorrect_score,
          MAX(active_percentage) as max_active_percentage,
          MAX(productivity_score) as max_productivity_score
        FROM contractor_daily_metrics_new FINAL
      `,
      'Verificando datos en la nueva tabla',
    );

    const stats = verification[0];
    console.log(`   Total de registros: ${stats.total_records}`);
    console.log(
      `   active_percentage incorrectos (>100): ${stats.incorrect_percentage}`,
    );
    console.log(
      `   productivity_score incorrectos (>100): ${stats.incorrect_score}`,
    );
    console.log(`   Max active_percentage: ${stats.max_active_percentage}`);
    console.log(`   Max productivity_score: ${stats.max_productivity_score}\n`);

    if (stats.incorrect_percentage > 0 || stats.incorrect_score > 100) {
      console.log(
        '⚠️  Aún hay valores incorrectos. Revisa los datos antes de continuar.\n',
      );
      return;
    }

    // PASO 6: Hacer backup y renombrar tablas
    console.log('📋 PASO 6: Haciendo backup y renombrando tablas');
    console.log('-'.repeat(60));
    console.log(
      '⚠️  ATENCIÓN: Esto renombrará las tablas. Asegúrate de tener un backup.\n',
    );

    // Hacer backup de la tabla original
    await executeCommand(
      client,
      `CREATE TABLE IF NOT EXISTS contractor_daily_metrics_backup AS contractor_daily_metrics`,
      'Creando backup (contractor_daily_metrics_backup)',
    );

    // Renombrar tablas
    await executeCommand(
      client,
      `RENAME TABLE contractor_daily_metrics TO contractor_daily_metrics_old`,
      'Renombrando tabla original a contractor_daily_metrics_old',
    );

    await executeCommand(
      client,
      `RENAME TABLE contractor_daily_metrics_new TO contractor_daily_metrics`,
      'Renombrando nueva tabla a contractor_daily_metrics',
    );

    console.log('✅ Migración completada exitosamente!\n');
    console.log('📋 Próximos pasos:');
    if (affectedDays.length > 0) {
      console.log(
        '   1. Reprocesar productivity_score para los días afectados:',
      );
      affectedDays.slice(0, 5).forEach((d: any) => {
        console.log(
          `      GET /adt/etl/process-daily-metrics?workday=${d.workday}`,
        );
      });
      if (affectedDays.length > 5) {
        console.log(`      ... y ${affectedDays.length - 5} días más`);
      }
      console.log('');
    }
    console.log(
      '   2. Verificar que el endpoint /adt/ranking devuelve valores correctos',
    );
    console.log('');
    console.log(
      '   3. Si todo está bien, puedes eliminar las tablas de backup:',
    );
    console.log('      DROP TABLE contractor_daily_metrics_old;');
    console.log('      DROP TABLE contractor_daily_metrics_backup;');
    console.log('');
  } catch (error) {
    console.error('\n❌ Error durante la migración:', error);
    console.error('\n💡 Si algo salió mal, puedes restaurar desde el backup:');
    console.error(
      '   RENAME TABLE contractor_daily_metrics TO contractor_daily_metrics_failed;',
    );
    console.error(
      '   RENAME TABLE contractor_daily_metrics_backup TO contractor_daily_metrics;',
    );
    process.exit(1);
  } finally {
    await client.close();
  }
}

// Ejecutar
main().catch((error) => {
  console.error('Error fatal:', error);
  process.exit(1);
});
