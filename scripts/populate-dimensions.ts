/**
 * Script para poblar las tablas de dimensiones (apps_dimension y domains_dimension)
 *
 * Este script puede ejecutarse manualmente para asegurar que las tablas tengan valores.
 * También se ejecuta automáticamente al iniciar el servicio, pero este script es útil
 * para poblar las tablas sin reiniciar el servicio.
 */

import 'dotenv/config';
import { createClient } from '@clickhouse/client';

// Configuración desde variables de entorno
const CLICKHOUSE_HOST = process.env.CLICKHOUSE_HOST || 'localhost';
const CLICKHOUSE_PORT = parseInt(process.env.CLICKHOUSE_PORT || '8123');
const CLICKHOUSE_USERNAME = process.env.CLICKHOUSE_USERNAME || 'default';
const CLICKHOUSE_PASSWORD = process.env.CLICKHOUSE_PASSWORD || '';
const CLICKHOUSE_DATABASE = process.env.CLICKHOUSE_DATABASE || 'metrics_db';

// Apps dimension data
const APPS = [
  // Productivas
  { app_name: 'Code', category: 'productive', weight: 1.2 },
  { app_name: 'Visual Studio Code', category: 'productive', weight: 1.2 },
  { app_name: 'IntelliJ', category: 'productive', weight: 1.2 },
  { app_name: 'Word', category: 'productive', weight: 1.0 },
  { app_name: 'Excel', category: 'productive', weight: 1.0 },
  { app_name: 'PowerPoint', category: 'productive', weight: 1.0 },
  { app_name: 'Notion', category: 'productive', weight: 1.0 },
  // Neutras
  { app_name: 'Slack', category: 'neutral', weight: 0.8 },
  { app_name: 'Teams', category: 'neutral', weight: 0.8 },
  { app_name: 'Chrome', category: 'neutral', weight: 0.6 },
  { app_name: 'Edge', category: 'neutral', weight: 0.6 },
  { app_name: 'Firefox', category: 'neutral', weight: 0.6 },
  // No productivas
  { app_name: 'YouTube', category: 'non_productive', weight: 0.2 },
  { app_name: 'Spotify', category: 'non_productive', weight: 0.3 },
  { app_name: 'Discord', category: 'non_productive', weight: 0.4 },
  { app_name: 'Games', category: 'non_productive', weight: 0.1 },
];

// Domains dimension data
const DOMAINS = [
  // Productivos
  { domain: 'github.com', category: 'productive', weight: 1.3 },
  { domain: 'stackoverflow.com', category: 'productive', weight: 1.2 },
  { domain: 'atlassian.net', category: 'productive', weight: 1.1 },
  { domain: 'teamandes.atlassian.net', category: 'productive', weight: 1.1 },
  { domain: 'jira.', category: 'productive', weight: 1.1 }, // Prefijo para match
  { domain: 'confluence.', category: 'productive', weight: 1.1 }, // Prefijo para match
  { domain: 'docs.google.com', category: 'productive', weight: 1.0 },
  { domain: 'notion.so', category: 'productive', weight: 1.0 },
  // Neutros
  { domain: 'google.com', category: 'neutral', weight: 0.7 },
  { domain: 'bing.com', category: 'neutral', weight: 0.7 },
  { domain: 'extensions', category: 'neutral', weight: 0.5 },
  // No productivos
  { domain: 'youtube.com', category: 'non_productive', weight: 0.2 },
  { domain: 'www.youtube.com', category: 'non_productive', weight: 0.2 },
  { domain: 'facebook.com', category: 'non_productive', weight: 0.1 },
  { domain: 'www.facebook.com', category: 'non_productive', weight: 0.1 },
  { domain: 'twitter.com', category: 'non_productive', weight: 0.2 },
  { domain: 'instagram.com', category: 'non_productive', weight: 0.1 },
  { domain: 'www.instagram.com', category: 'non_productive', weight: 0.1 },
  { domain: 'reddit.com', category: 'non_productive', weight: 0.3 },
  { domain: 'www.reddit.com', category: 'non_productive', weight: 0.3 },
];

async function populateDimensions() {
  console.log('🚀 Iniciando población de tablas de dimensiones...\n');

  // Conectar a ClickHouse
  const client = createClient({
    host: `http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}`,
    username: CLICKHOUSE_USERNAME,
    password: CLICKHOUSE_PASSWORD,
    database: CLICKHOUSE_DATABASE,
  });

  try {
    await client.ping();
    console.log(
      `✅ Conectado a ClickHouse: ${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}`,
    );
    console.log(`📊 Base de datos: ${CLICKHOUSE_DATABASE}\n`);
  } catch (error) {
    console.error('❌ Error conectando a ClickHouse:', error);
    process.exit(1);
  }

  try {
    // Verificar si apps_dimension tiene datos
    const appsCountResult = await client.query({
      query: `SELECT count() as count FROM ${CLICKHOUSE_DATABASE}.apps_dimension`,
      format: 'JSONEachRow',
    });
    const appsCount = await appsCountResult.json<{ count: number }[]>();
    const appsCountValue = appsCount[0]?.count || 0;

    if (appsCountValue > 0) {
      console.log(`⚠️  apps_dimension ya tiene ${appsCountValue} registros.`);
      console.log('   ¿Deseas limpiar y repoblar? (S/N)');
      // Por ahora, solo insertamos si está vacía. Para limpiar, ejecuta manualmente:
      // TRUNCATE TABLE apps_dimension;
      console.log(
        '   Para limpiar manualmente, ejecuta: TRUNCATE TABLE apps_dimension;\n',
      );
    } else {
      // Insertar apps
      console.log(`📦 Insertando ${APPS.length} apps en apps_dimension...`);
      await client.insert({
        table: 'apps_dimension',
        values: APPS,
        format: 'JSONEachRow',
      });
      console.log(`✅ Insertados ${APPS.length} registros en apps_dimension\n`);
    }

    // Verificar si domains_dimension tiene datos
    const domainsCountResult = await client.query({
      query: `SELECT count() as count FROM ${CLICKHOUSE_DATABASE}.domains_dimension`,
      format: 'JSONEachRow',
    });
    const domainsCount = await domainsCountResult.json<{ count: number }[]>();
    const domainsCountValue = domainsCount[0]?.count || 0;

    if (domainsCountValue > 0) {
      console.log(
        `⚠️  domains_dimension ya tiene ${domainsCountValue} registros.`,
      );
      console.log('   ¿Deseas limpiar y repoblar? (S/N)');
      console.log(
        '   Para limpiar manualmente, ejecuta: TRUNCATE TABLE domains_dimension;\n',
      );
    } else {
      // Insertar dominios
      console.log(
        `📦 Insertando ${DOMAINS.length} dominios en domains_dimension...`,
      );
      await client.insert({
        table: 'domains_dimension',
        values: DOMAINS,
        format: 'JSONEachRow',
      });
      console.log(
        `✅ Insertados ${DOMAINS.length} registros en domains_dimension\n`,
      );
    }

    // Verificar resultados finales
    const finalAppsCount = await client.query({
      query: `SELECT count() as count FROM ${CLICKHOUSE_DATABASE}.apps_dimension`,
      format: 'JSONEachRow',
    });
    const finalApps = await finalAppsCount.json<{ count: number }[]>();

    const finalDomainsCount = await client.query({
      query: `SELECT count() as count FROM ${CLICKHOUSE_DATABASE}.domains_dimension`,
      format: 'JSONEachRow',
    });
    const finalDomains = await finalDomainsCount.json<{ count: number }[]>();

    console.log('📊 Resumen final:');
    console.log(`   - apps_dimension: ${finalApps[0]?.count || 0} registros`);
    console.log(
      `   - domains_dimension: ${finalDomains[0]?.count || 0} registros`,
    );
    console.log('\n🎉 ¡Tablas de dimensiones pobladas exitosamente!\n');
  } catch (error) {
    console.error('\n❌ Error poblando tablas de dimensiones:', error);
    if (error instanceof Error) {
      console.error('   Mensaje:', error.message);
      console.error('   Stack:', error.stack);
    }
    process.exit(1);
  } finally {
    await client.close();
  }
}

// Ejecutar script
populateDimensions().catch((error) => {
  console.error('❌ Error fatal:', error);
  process.exit(1);
});
