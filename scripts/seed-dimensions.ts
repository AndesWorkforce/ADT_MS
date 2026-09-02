/**
 * Puebla apps_dimension y domains_dimension con el catálogo real de la operación.
 *
 * POR QUÉ EXISTE (además de populate-dimensions.ts):
 * El script anterior insertaba el campo `app_name`, pero la columna de la tabla
 * se llama `name`. ClickHouse con JSONEachRow ignora los campos desconocidos en
 * silencio y rellena los faltantes con el default, así que las 16 filas
 * quedaron con `name = ''`. El JOIN del ETL es `ON d.name = app`, o sea que
 * nunca matcheaba y TODA app caía al peso neutro por defecto: la mitad de
 * S_quality estaba inerte. Este script usa los nombres de columna correctos y
 * hace TRUNCATE + INSERT para dejar el catálogo en un estado conocido.
 *
 * ESCALA DE PESOS (definida por producto):
 *   productive      1.5   multiplica la productividad
 *   neutral         1.0   no altera
 *   non_productive  0.5   la reduce a la mitad
 *
 * Los nombres de app son la salida de normalize() del agente
 * (PY_AGENT/core/tracking/app_normalizer.py), NO nombres de proceso crudos.
 * Si acá dice 'Microsoft Outlook' pero el agente emite 'Olk', no matchea.
 *
 * Los dominios se comparan contra la salida de normalize_domain(): host limpio,
 * en minúsculas, sin www. y sin path.
 *
 * Uso:
 *   pnpm run seed:dimensions
 *   pnpm run seed:dimensions -- --dry-run
 */

import 'dotenv/config';
import { createClient } from '@clickhouse/client';
import * as crypto from 'crypto';

const CLICKHOUSE_HOST = process.env.CLICKHOUSE_HOST || 'localhost';
const CLICKHOUSE_PORT = parseInt(process.env.CLICKHOUSE_PORT || '8123', 10);
const CLICKHOUSE_USERNAME = process.env.CLICKHOUSE_USERNAME || 'default';
const CLICKHOUSE_PASSWORD = process.env.CLICKHOUSE_PASSWORD || '';
const CLICKHOUSE_DATABASE = process.env.CLICKHOUSE_DATABASE || 'adt_db';

type Category = 'productive' | 'neutral' | 'non_productive';

const WEIGHT: Record<Category, number> = {
  productive: 1.5,
  neutral: 1.0,
  non_productive: 0.5,
};

// ---------------------------------------------------------------------------
// APPS
// ---------------------------------------------------------------------------
// Extraídas de payloads reales de producción (equipo legal / VA benefits) más
// las que ya estaban en el catálogo anterior.
//
// Nombres tal como los emite normalize(): 'Olk' -> 'Microsoft Outlook',
// 'foxitpdfreader' -> 'Foxit PDF Reader', etc. Ver _FULL_MAPPING en el agente.
const APPS: Array<{ name: string; category: Category }> = [
  // --- Productivas: herramientas de trabajo directo ---
  { name: 'Microsoft Outlook', category: 'productive' }, // 'Olk' en los payloads
  { name: 'Word', category: 'productive' },
  { name: 'Excel', category: 'productive' },
  { name: 'PowerPoint', category: 'productive' },
  { name: 'OneNote', category: 'productive' },
  { name: 'Notepad', category: 'productive' },
  { name: 'Notepad++', category: 'productive' },
  { name: 'WordPad', category: 'productive' },
  { name: 'Adobe', category: 'productive' },
  { name: 'Foxit PDF Reader', category: 'productive' },
  { name: 'FoxitReader', category: 'productive' },
  { name: 'SumatraPDF', category: 'productive' },
  { name: 'Microsoft 365 Copilot', category: 'productive' },
  { name: 'Remote Desktop', category: 'productive' },
  // Telefonía: para un equipo de intake, atender llamadas ES el trabajo.
  { name: 'Dialpad', category: 'productive' },
  { name: 'Cebod', category: 'productive' },
  { name: 'RingCentral', category: 'productive' },
  { name: 'LawRuler', category: 'productive' },
  // Desarrollo
  { name: 'Cursor', category: 'productive' },
  { name: 'VSCode', category: 'productive' },
  // El agente pre-Fase 1 emitia 'Code' a secas; es la app con mas segundos en
  // el historico (1.57M). Sin esta entrada, todo ese tiempo cae al fallback.
  { name: 'Code', category: 'productive' },
  { name: 'Visual Studio Code', category: 'productive' },
  { name: 'Notion', category: 'productive' },
  { name: 'Claude', category: 'productive' },
  { name: 'Pgadmin4', category: 'productive' },
  { name: 'PgAdmin', category: 'productive' },
  { name: 'DBeaver', category: 'productive' },
  { name: 'Postman', category: 'productive' },
  { name: 'IntelliJ', category: 'productive' },
  { name: 'VisualStudio', category: 'productive' },
  { name: 'IntelliJ IDEA', category: 'productive' },
  { name: 'PyCharm', category: 'productive' },
  { name: 'WebStorm', category: 'productive' },
  { name: 'Sublime Text', category: 'productive' },

  // --- Neutras: uso mixto legítimo ---
  // Teams y Slack son mensajería de trabajo, pero también donde se va el tiempo.
  // Neutro = ni premia ni castiga.
  { name: 'Teams', category: 'neutral' },
  { name: 'Slack', category: 'neutral' },
  { name: 'WhatsApp', category: 'neutral' },
  { name: 'Chrome', category: 'neutral' },
  { name: 'Edge', category: 'neutral' },
  { name: 'Firefox', category: 'neutral' },
  { name: 'Brave', category: 'neutral' },
  { name: 'Snipping Tool', category: 'neutral' },
  { name: 'Task Manager', category: 'neutral' },
  { name: 'Live Captions', category: 'neutral' },
  { name: 'VirtualMachine', category: 'neutral' },
  { name: 'VPN_Fortinet', category: 'neutral' },

  // --- No productivas ---
  { name: 'Discord', category: 'non_productive' },
  { name: 'Spotify', category: 'non_productive' },
  { name: 'Steam', category: 'non_productive' },
  { name: 'Steamwebhelper', category: 'non_productive' },
  { name: 'Netflix', category: 'non_productive' },
  { name: 'VLC', category: 'non_productive' },
];

// ---------------------------------------------------------------------------
// DOMINIOS
// ---------------------------------------------------------------------------
// Los que aparecen como entrada suelta en AppUsage de los payloads de prod,
// más los que se ven dentro de títulos de ventana (Filevine, SSA, Power Apps)
// y que el agente v3 ahora sí extrae como dominio limpio.
const DOMAINS: Array<{ domain: string; category: Category }> = [
  // --- Productivos: sistemas del negocio ---
  { domain: 'lawruler.com', category: 'productive' },
  { domain: 'tabakattorneys.lawruler.com', category: 'productive' },
  // Blob de documentos de Law Ruler: abrir un PDF de un caso es trabajo.
  {
    domain: 'lawrulerprodstandardblob.blob.core.windows.net',
    category: 'productive',
  },
  { domain: 'lawmatics.com', category: 'productive' },
  { domain: 'app.lawmatics.com', category: 'productive' },
  { domain: 'filevine.com', category: 'productive' },
  { domain: 'ssa.gov', category: 'productive' },
  { domain: 'secure.ssa.gov', category: 'productive' },
  { domain: 'va.gov', category: 'productive' },
  { domain: 'va-submit.herokuapp.com', category: 'productive' },
  { domain: 'va-ssd-intakedashboard.up.railway.app', category: 'productive' },
  { domain: 'sharepoint.com', category: 'productive' },
  { domain: 'tabakattorneys-my.sharepoint.com', category: 'productive' },
  { domain: 'egnyte.com', category: 'productive' },
  { domain: 'rocketbenefits.egnyte.com', category: 'productive' },
  { domain: 'docs.google.com', category: 'productive' },
  { domain: 'sheets.google.com', category: 'productive' },
  { domain: 'make.powerapps.com', category: 'productive' },
  { domain: 'powerapps.com', category: 'productive' },
  { domain: 'andesworkforce.com', category: 'productive' },
  { domain: 'pulse-aw.com', category: 'productive' },
  { domain: 'dropbox.com', category: 'productive' },
  { domain: 'aws.amazon.com', category: 'productive' },
  { domain: 'console.aws.amazon.com', category: 'productive' },
  { domain: 'us-east-2.signin.aws.amazon.com', category: 'productive' },
  { domain: 'github.com', category: 'productive' },
  { domain: 'stackoverflow.com', category: 'productive' },
  { domain: 'atlassian.net', category: 'productive' },
  { domain: 'teamandes.atlassian.net', category: 'productive' },
  { domain: 'notion.so', category: 'productive' },
  { domain: 'outlook.office.com', category: 'productive' },
  { domain: 'outlook.office365.com', category: 'productive' },

  // --- Neutros ---
  { domain: 'google.com', category: 'neutral' },
  { domain: 'bing.com', category: 'neutral' },
  { domain: 'mail.google.com', category: 'neutral' },
  { domain: 'app.slack.com', category: 'neutral' },
  { domain: 'slack.com', category: 'neutral' },
  { domain: 'teams.microsoft.com', category: 'neutral' },
  { domain: 'events.teams.microsoft.com', category: 'neutral' },
  { domain: 'teams.public.onecdn.static.microsoft', category: 'neutral' },
  { domain: 'dialpad.com', category: 'neutral' },
  { domain: 'web.whatsapp.com', category: 'neutral' },
  { domain: 'chatgpt.com', category: 'neutral' },
  { domain: 'claude.ai', category: 'neutral' },

  // --- No productivos ---
  { domain: 'youtube.com', category: 'non_productive' },
  { domain: 'reddit.com', category: 'non_productive' },
  { domain: 'netflix.com', category: 'non_productive' },
  { domain: 'twitch.tv', category: 'non_productive' },
  { domain: 'crunchyroll.com', category: 'non_productive' },
  { domain: 'facebook.com', category: 'non_productive' },
  { domain: 'instagram.com', category: 'non_productive' },
  { domain: 'tiktok.com', category: 'non_productive' },
  { domain: 'x.com', category: 'non_productive' },
  { domain: 'twitter.com', category: 'non_productive' },
];

function nowStr(): string {
  return new Date().toISOString().slice(0, 19).replace('T', ' ');
}

async function main() {
  const dryRun = process.argv.includes('--dry-run');

  const byCategory = (rows: Array<{ category: Category }>) => {
    const acc: Record<string, number> = {};
    for (const r of rows) acc[r.category] = (acc[r.category] || 0) + 1;
    return acc;
  };

  console.log('Catálogo a cargar:\n');
  console.log('  apps    :', APPS.length, JSON.stringify(byCategory(APPS)));
  console.log(
    '  dominios:',
    DOMAINS.length,
    JSON.stringify(byCategory(DOMAINS)),
  );
  console.log('\n  pesos   :', JSON.stringify(WEIGHT), '\n');

  if (dryRun) {
    console.log('--dry-run: no se escribió nada.');
    return;
  }

  const client = createClient({
    host: `http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}`,
    username: CLICKHOUSE_USERNAME,
    password: CLICKHOUSE_PASSWORD,
    database: CLICKHOUSE_DATABASE,
    request_timeout: 120000,
  });

  const ts = nowStr();

  // TRUNCATE + INSERT: el catálogo es una lista curada, no un acumulado.
  // Insertar sin limpiar dejaría las filas viejas con name='' conviviendo con
  // las nuevas y el JOIN podría tomar cualquiera.
  console.log('Limpiando apps_dimension...');
  await client.command({ query: 'TRUNCATE TABLE apps_dimension' });
  await client.insert({
    table: 'apps_dimension',
    values: APPS.map((a) => ({
      id: crypto.randomUUID(),
      // OJO: la columna es `name`, no `app_name`. Ese fue el bug del script
      // anterior: JSONEachRow descarta el campo desconocido sin avisar.
      name: a.name,
      category: a.category,
      type: null,
      weight: WEIGHT[a.category],
      created_at: ts,
      updated_at: ts,
    })),
    format: 'JSONEachRow',
  });
  console.log(`  ${APPS.length} apps insertadas`);

  console.log('Limpiando domains_dimension...');
  await client.command({ query: 'TRUNCATE TABLE domains_dimension' });
  await client.insert({
    table: 'domains_dimension',
    values: DOMAINS.map((d) => ({
      id: crypto.randomUUID(),
      domain: d.domain,
      category: d.category,
      weight: WEIGHT[d.category],
      created_at: ts,
      updated_at: ts,
    })),
    format: 'JSONEachRow',
  });
  console.log(`  ${DOMAINS.length} dominios insertados`);

  // Verificación: lo que más importa es que `name` NO quede vacío.
  const check = await client.query({
    query: `
      SELECT 'apps' AS tabla, count() AS total, countIf(name = '') AS sin_nombre
      FROM apps_dimension
      UNION ALL
      SELECT 'domains', count(), countIf(domain = '')
      FROM domains_dimension
    `,
    format: 'JSONEachRow',
  });
  console.log('\nVerificación:');
  for (const row of await check.json<
    { tabla: string; total: number; sin_nombre: number }[]
  >()) {
    const flag = Number(row.sin_nombre) === 0 ? 'OK' : 'ERROR';
    console.log(
      `  ${flag}  ${row.tabla}: ${row.total} filas, ${row.sin_nombre} sin nombre`,
    );
  }

  await client.close();
}

main().catch((e) => {
  console.error('FALLO:', e);
  process.exit(1);
});
