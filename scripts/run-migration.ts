/**
 * Script para ejecutar la migración de app_name a name usando las credenciales del proyecto
 *
 * Uso:
 *   cd ADT_MS
 *   pnpm ts-node scripts/run-migration.ts
 */

import { createClient } from '@clickhouse/client';
import 'dotenv/config';

const CLICKHOUSE_HOST = process.env.CLICKHOUSE_HOST || 'localhost';
const CLICKHOUSE_PORT = parseInt(process.env.CLICKHOUSE_PORT || '8123', 10);
const CLICKHOUSE_USERNAME = process.env.CLICKHOUSE_USERNAME || 'default';
const CLICKHOUSE_PASSWORD = process.env.CLICKHOUSE_PASSWORD || '';
const CLICKHOUSE_DATABASE = process.env.CLICKHOUSE_DATABASE || 'andes_db';

async function runMigration() {
  console.log('🔄 Iniciando migración de app_name a name...\n');
  console.log(`Host: ${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}`);
  console.log(`Database: ${CLICKHOUSE_DATABASE}`);
  console.log(`User: ${CLICKHOUSE_USERNAME}\n`);

  const client = createClient({
    host: `http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}`,
    username: CLICKHOUSE_USERNAME,
    password: CLICKHOUSE_PASSWORD,
    database: CLICKHOUSE_DATABASE,
  });

  try {
    // Probar conexión
    await client.ping();
    console.log('✅ Conexión exitosa a ClickHouse\n');

    // El script de migración se ejecuta paso a paso más abajo

    // Verificar si existe app_name antes de migrar
    const checkAppName = await client.query({
      query: `
        SELECT name 
        FROM system.columns 
        WHERE database = '${CLICKHOUSE_DATABASE}' 
          AND table = 'apps_dimension' 
          AND name = 'app_name'
      `,
      format: 'JSONEachRow',
    });

    const appNameExists =
      ((await checkAppName.json()) as Array<{ name: string }>).length > 0;

    if (!appNameExists) {
      console.log(
        'ℹ️  La columna app_name no existe. La tabla ya tiene la estructura nueva.',
      );
      console.log('   Verificando si hay apps sin tipo asignado...\n');

      // Verificar si hay apps sin tipo
      const appsWithoutType = await client.query({
        query: `
          SELECT count() as count
          FROM ${CLICKHOUSE_DATABASE}.apps_dimension
          WHERE type IS NULL OR type = ''
        `,
        format: 'JSONEachRow',
      });

      const count =
        ((await appsWithoutType.json()) as Array<{ count: number }>)[0]
          ?.count || 0;

      if (count > 0) {
        console.log(`   Se encontraron ${count} apps sin tipo.`);
        console.log(
          '   ¿Deseas asignar tipos automáticamente? (Esto actualizará los tipos basados en el nombre)\n',
        );
        console.log(
          '   Para asignar tipos, ejecuta solo la parte de asignación del script SQL manualmente.',
        );
      } else {
        console.log('   ✅ Todas las apps ya tienen tipo asignado.');
      }

      await client.close();
      return;
    }

    console.log(
      '⚠️  Se encontró la columna app_name. Iniciando migración...\n',
    );

    // Ejecutar el script de migración paso a paso
    // ClickHouse no permite múltiples statements, así que ejecutamos cada query individualmente

    console.log('📝 Paso 1: Creando tabla temporal...');
    await client.command({
      query: `
        CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DATABASE}.apps_dimension_temp (
          id String,
          name String,
          category Nullable(String),
          type Nullable(String),
          weight Nullable(Float64) DEFAULT 0.5,
          created_at DateTime DEFAULT now(),
          updated_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY id
      `,
    });
    console.log('   ✅ Tabla temporal creada\n');

    console.log('📝 Paso 2: Migrando datos y asignando tipos...');
    await client.command({
      query: `
        INSERT INTO ${CLICKHOUSE_DATABASE}.apps_dimension_temp (id, name, category, type, weight, created_at, updated_at)
        SELECT 
          if(id = '' OR id IS NULL, toString(generateUUIDv4()), id) as id,
          if(name != '' AND name IS NOT NULL, name, 
            if(app_name != '' AND app_name IS NOT NULL, app_name, 'Unknown')
          ) as name,
          category,
          CASE
            WHEN lower(COALESCE(name, app_name, '')) LIKE '%code%' OR lower(COALESCE(name, app_name, '')) LIKE '%visual studio%' 
                 OR lower(COALESCE(name, app_name, '')) LIKE '%intellij%' OR lower(COALESCE(name, app_name, '')) LIKE '%eclipse%' 
                 OR lower(COALESCE(name, app_name, '')) LIKE '%sublime%' OR lower(COALESCE(name, app_name, '')) LIKE '%atom%'
                 OR lower(COALESCE(name, app_name, '')) LIKE '%vim%' OR lower(COALESCE(name, app_name, '')) LIKE '%emacs%' 
                 OR lower(COALESCE(name, app_name, '')) LIKE '%jetbrains%' OR lower(COALESCE(name, app_name, '')) = 'code' THEN 'Code'
            WHEN lower(COALESCE(name, app_name, '')) LIKE '%chrome%' OR lower(COALESCE(name, app_name, '')) LIKE '%edge%' 
                 OR lower(COALESCE(name, app_name, '')) LIKE '%firefox%' OR lower(COALESCE(name, app_name, '')) LIKE '%safari%' 
                 OR lower(COALESCE(name, app_name, '')) LIKE '%opera%' OR lower(COALESCE(name, app_name, '')) LIKE '%brave%'
                 OR lower(COALESCE(name, app_name, '')) = 'web' THEN 'Web'
            WHEN lower(COALESCE(name, app_name, '')) LIKE '%figma%' OR lower(COALESCE(name, app_name, '')) LIKE '%photoshop%' 
                 OR lower(COALESCE(name, app_name, '')) LIKE '%illustrator%' OR lower(COALESCE(name, app_name, '')) LIKE '%sketch%' 
                 OR lower(COALESCE(name, app_name, '')) LIKE '%adobe%' OR lower(COALESCE(name, app_name, '')) LIKE '%canva%'
                 OR lower(COALESCE(name, app_name, '')) LIKE '%invision%' OR lower(COALESCE(name, app_name, '')) LIKE '%xd%' THEN 'Design'
            WHEN lower(COALESCE(name, app_name, '')) LIKE '%slack%' OR lower(COALESCE(name, app_name, '')) LIKE '%teams%' 
                 OR lower(COALESCE(name, app_name, '')) LIKE '%discord%' OR lower(COALESCE(name, app_name, '')) LIKE '%whatsapp%' 
                 OR lower(COALESCE(name, app_name, '')) LIKE '%telegram%' OR lower(COALESCE(name, app_name, '')) LIKE '%zoom%'
                 OR lower(COALESCE(name, app_name, '')) LIKE '%skype%' OR lower(COALESCE(name, app_name, '')) LIKE '%messenger%' 
                 OR lower(COALESCE(name, app_name, '')) = 'chat' THEN 'Chat'
            WHEN lower(COALESCE(name, app_name, '')) LIKE '%word%' OR lower(COALESCE(name, app_name, '')) LIKE '%excel%' 
                 OR lower(COALESCE(name, app_name, '')) LIKE '%powerpoint%' OR lower(COALESCE(name, app_name, '')) LIKE '%outlook%' 
                 OR lower(COALESCE(name, app_name, '')) LIKE '%onenote%' OR lower(COALESCE(name, app_name, '')) LIKE '%office%'
                 OR lower(COALESCE(name, app_name, '')) LIKE '%libreoffice%' OR lower(COALESCE(name, app_name, '')) LIKE '%openoffice%' THEN 'Office'
            WHEN lower(COALESCE(name, app_name, '')) LIKE '%notion%' OR lower(COALESCE(name, app_name, '')) LIKE '%trello%' 
                 OR lower(COALESCE(name, app_name, '')) LIKE '%asana%' OR lower(COALESCE(name, app_name, '')) LIKE '%jira%' 
                 OR lower(COALESCE(name, app_name, '')) LIKE '%confluence%' OR lower(COALESCE(name, app_name, '')) LIKE '%todoist%'
                 OR lower(COALESCE(name, app_name, '')) LIKE '%evernote%' THEN 'Productivity'
            WHEN lower(COALESCE(name, app_name, '')) LIKE '%git%' OR lower(COALESCE(name, app_name, '')) LIKE '%docker%' 
                 OR lower(COALESCE(name, app_name, '')) LIKE '%kubernetes%' OR lower(COALESCE(name, app_name, '')) LIKE '%terminal%' 
                 OR lower(COALESCE(name, app_name, '')) LIKE '%cmd%' OR lower(COALESCE(name, app_name, '')) LIKE '%powershell%'
                 OR lower(COALESCE(name, app_name, '')) LIKE '%postman%' OR lower(COALESCE(name, app_name, '')) LIKE '%insomnia%' 
                 OR lower(COALESCE(name, app_name, '')) LIKE '%wsl%' THEN 'Development'
            WHEN lower(COALESCE(name, app_name, '')) LIKE '%pgadmin%' OR lower(COALESCE(name, app_name, '')) LIKE '%dbeaver%' 
                 OR lower(COALESCE(name, app_name, '')) LIKE '%mysql%' OR lower(COALESCE(name, app_name, '')) LIKE '%mongodb%' 
                 OR lower(COALESCE(name, app_name, '')) LIKE '%redis%' OR lower(COALESCE(name, app_name, '')) LIKE '%sql%'
                 OR lower(COALESCE(name, app_name, '')) LIKE '%datagrip%' OR lower(COALESCE(name, app_name, '')) LIKE '%tableplus%' THEN 'Database'
            WHEN lower(COALESCE(name, app_name, '')) LIKE '%aws%' OR lower(COALESCE(name, app_name, '')) LIKE '%azure%' 
                 OR lower(COALESCE(name, app_name, '')) LIKE '%gcp%' OR lower(COALESCE(name, app_name, '')) LIKE '%google cloud%' 
                 OR lower(COALESCE(name, app_name, '')) LIKE '%heroku%' OR lower(COALESCE(name, app_name, '')) LIKE '%vercel%'
                 OR lower(COALESCE(name, app_name, '')) LIKE '%netlify%' THEN 'Cloud'
            WHEN lower(COALESCE(name, app_name, '')) LIKE '%youtube%' OR lower(COALESCE(name, app_name, '')) LIKE '%spotify%' 
                 OR lower(COALESCE(name, app_name, '')) LIKE '%netflix%' OR lower(COALESCE(name, app_name, '')) LIKE '%twitch%' 
                 OR lower(COALESCE(name, app_name, '')) LIKE '%steam%' OR lower(COALESCE(name, app_name, '')) LIKE '%game%'
                 OR lower(COALESCE(name, app_name, '')) LIKE '%music%' OR lower(COALESCE(name, app_name, '')) LIKE '%media%' 
                 OR lower(COALESCE(name, app_name, '')) = 'entertainment' THEN 'Entertainment'
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
        FROM ${CLICKHOUSE_DATABASE}.apps_dimension
      `,
    });
    console.log('   ✅ Datos migrados\n');

    console.log('📝 Paso 3: Eliminando tabla antigua...');
    await client.command({
      query: `DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.apps_dimension`,
    });
    console.log('   ✅ Tabla antigua eliminada\n');

    console.log('📝 Paso 4: Renombrando tabla temporal...');
    await client.command({
      query: `RENAME TABLE ${CLICKHOUSE_DATABASE}.apps_dimension_temp TO ${CLICKHOUSE_DATABASE}.apps_dimension`,
    });
    console.log('   ✅ Tabla renombrada\n');

    // Verificar la migración
    console.log('🔍 Verificando migración...\n');

    const verifyName = await client.query({
      query: `
        SELECT name 
        FROM system.columns 
        WHERE database = '${CLICKHOUSE_DATABASE}' 
          AND table = 'apps_dimension' 
          AND name = 'name'
      `,
      format: 'JSONEachRow',
    });

    const nameExists =
      ((await verifyName.json()) as Array<{ name: string }>).length > 0;

    const verifyAppName = await client.query({
      query: `
        SELECT name 
        FROM system.columns 
        WHERE database = '${CLICKHOUSE_DATABASE}' 
          AND table = 'apps_dimension' 
          AND name = 'app_name'
      `,
      format: 'JSONEachRow',
    });

    const appNameStillExists =
      ((await verifyAppName.json()) as Array<{ name: string }>).length > 0;

    const appsWithType = await client.query({
      query: `
        SELECT 
          count() as total,
          countIf(type IS NOT NULL AND type != '') as with_type
        FROM ${CLICKHOUSE_DATABASE}.apps_dimension
      `,
      format: 'JSONEachRow',
    });

    const stats = (
      (await appsWithType.json()) as Array<{ total: number; with_type: number }>
    )[0];

    console.log('📊 Resultados de la migración:');
    console.log(`   ✅ Columna 'name' existe: ${nameExists ? 'Sí' : 'No'}`);
    console.log(
      `   ${appNameStillExists ? '⚠️' : '✅'} Columna 'app_name' eliminada: ${!appNameStillExists ? 'Sí' : 'No'}`,
    );
    console.log(`   📈 Total de apps: ${stats?.total || 0}`);
    console.log(`   🏷️  Apps con tipo asignado: ${stats?.with_type || 0}\n`);

    if (nameExists && !appNameStillExists) {
      console.log('✅ ¡Migración completada exitosamente!\n');
    } else {
      console.log(
        '⚠️  La migración puede no haberse completado correctamente. Verifica manualmente.\n',
      );
    }

    await client.close();
  } catch (error: any) {
    console.error('❌ Error durante la migración:', error.message);
    console.error('\nDetalles del error:', error);
    await client.close();
    process.exit(1);
  }
}

// Ejecutar la migración
runMigration().catch((error) => {
  console.error('❌ Error fatal:', error);
  process.exit(1);
});
