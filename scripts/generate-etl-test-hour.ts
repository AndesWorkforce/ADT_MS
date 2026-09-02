/**
 * Genera UNA HORA de datos sinteticos para ejercitar el ETL de punta a punta.
 *
 * Por que existe: con ~90 beats reales no se puede evaluar si el ETL calcula
 * bien. Este script produce una hora con composicion CONOCIDA, de modo que el
 * resultado del ETL se pueda comparar contra un valor esperado en vez de
 * mirarlo y asumir que esta bien.
 *
 * Que genera:
 *   - contractor_info_raw   1 contratista sintetico
 *   - sessions_raw          1 sesion de 1 hora
 *   - agent_sessions_raw    1 sesion de agente
 *   - events_raw            240 beats de 15s (= 3600s exactos)
 *
 * Los beats se reparten en fases con proposito, cada una probando algo:
 *
 *   fase          beats  min  que ejercita
 *   ------------- -----  ---  --------------------------------------------
 *   focus            60   15  actividad plena, dominio productivo (1.5)
 *   research         40   10  mezcla de dominios de peso alto y bajo
 *   call             40   10  presencia por mic/camara SIN teclado ni mouse
 *   idle             40   10  is_idle + salida de la ventana de gracia
 *   locked           20    5  power_state != 'active' -> debe EXCLUIRSE
 *   unknown          20    5  app/dominio fuera del catalogo (fallback
 *                              neutro) + segundos negativos (deben filtrarse)
 *   wrapup           20    5  vuelta a actividad
 *
 * La mitad de los beats sale en formato v3 (plano) y la otra en v2 (anidado),
 * alternando, para que el ETL tenga que resolver los dos en la misma corrida.
 *
 * Todos los IDs llevan el prefijo SYNTHETIC_PREFIX para poder borrarlos sin
 * tocar datos reales:
 *   pnpm run generate:etl-hour -- --cleanup
 *
 * Uso:
 *   pnpm run generate:etl-hour                  # hoy, 09:00-10:00 hora operativa
 *   pnpm run generate:etl-hour -- --day=2026-08-20
 *   pnpm run generate:etl-hour -- --start-hour=14
 *   pnpm run generate:etl-hour -- --run-etl     # genera + corre ETL (activity, daily, session)
 *   pnpm run generate:etl-hour -- --analyze     # compara CH vs esperado (requiere ETL)
 *   pnpm run generate:etl-hour -- --cleanup     # borra lo sintetico y sale
 */

import 'dotenv/config';
import { createClient } from '@clickhouse/client';

const CLICKHOUSE_HOST = process.env.CLICKHOUSE_HOST || 'localhost';
const CLICKHOUSE_PORT = parseInt(process.env.CLICKHOUSE_PORT || '8123', 10);
const CLICKHOUSE_USERNAME = process.env.CLICKHOUSE_USERNAME || 'default';
const CLICKHOUSE_PASSWORD = process.env.CLICKHOUSE_PASSWORD || '';
const CLICKHOUSE_DATABASE = process.env.CLICKHOUSE_DATABASE || 'adt_db';

/** Prefijo que marca todo lo sintetico. Es la llave del --cleanup. */
const SYNTHETIC_PREFIX = 'synth-etl';

const CONTRACTOR_ID = `${SYNTHETIC_PREFIX}-contractor`;
const AGENT_ID = `${SYNTHETIC_PREFIX}-agent`;
const SESSION_ID = `${SYNTHETIC_PREFIX}-session`;
const AGENT_SESSION_ID = `${SYNTHETIC_PREFIX}-agentsession`;

const BEAT_SECONDS = 15;

// ---------------------------------------------------------------------------
// Definicion de las fases
// ---------------------------------------------------------------------------

interface Phase {
  name: string;
  beats: number;
  /** true = hubo teclado/mouse en el beat. */
  hasInput: boolean;
  /** Segundos sin actividad reportados en el beat. */
  idleTime: number;
  powerState: 'active' | 'locked';
  mic: boolean;
  cam: boolean;
  callApp: boolean;
  /** Apps con sus segundos. La suma deberia dar ~BEAT_SECONDS. */
  apps: Record<string, number>;
  /** Dominios con sus segundos. */
  domains: Record<string, number>;
}

/**
 * Dominios alineados a scripts/seed-dimensions.ts (1.5 / 1.0 / 0.5):
 *   github.com / stackoverflow.com / teamandes.atlassian.net  productive 1.5
 *   google.com                                                neutral    1.0
 *   reddit.com                                                non_prod   0.5
 * Apps: Cursor 1.5, Brave 1.0, Teams 1.0.
 */
const PHASES: Phase[] = [
  {
    name: 'focus',
    beats: 60,
    hasInput: true,
    idleTime: 0.2,
    powerState: 'active',
    mic: false,
    cam: false,
    callApp: false,
    apps: { Cursor: 11, Brave: 4 },
    domains: { 'github.com': 4 },
  },
  {
    name: 'research',
    beats: 40,
    hasInput: true,
    idleTime: 1.5,
    powerState: 'active',
    mic: false,
    cam: false,
    callApp: false,
    apps: { Brave: 15 },
    domains: { 'stackoverflow.com': 9, 'google.com': 4, 'reddit.com': 2 },
  },
  {
    name: 'call',
    beats: 40,
    hasInput: false, // sin teclado ni mouse: la presencia es lo unico que lo salva
    idleTime: 15,
    powerState: 'active',
    mic: true,
    cam: true,
    callApp: true,
    apps: { Teams: 15 },
    domains: {},
  },
  {
    name: 'idle',
    beats: 40,
    hasInput: false,
    idleTime: 15,
    powerState: 'active',
    mic: false,
    cam: false,
    callApp: false,
    apps: { Cursor: 15 },
    domains: {},
  },
  {
    name: 'locked',
    beats: 20,
    hasInput: false,
    idleTime: 15,
    powerState: 'locked', // el ETL filtra por power_state='active'
    mic: false,
    cam: false,
    callApp: false,
    apps: {},
    domains: {},
  },
  {
    // Cubre dos caminos que ninguna otra fase tocaba:
    //   a) apps/dominios FUERA del catalogo -> deben caer al fallback neutro
    //      (W_UNKNOWN = 1.0), no castigarse como improductivos.
    //   b) segundos NEGATIVOS -> el agente pre-Fase 1 los emitia para
    //      navegadores (223.503 beats en el historico). usageMapSql los filtra
    //      en el origen, asi que no deben sumar ni al numerador ni al
    //      denominador de S_quality.
    name: 'unknown',
    beats: 20,
    hasInput: true,
    idleTime: 0.3,
    powerState: 'active',
    mic: false,
    cam: false,
    callApp: false,
    apps: { 'Fabrikam CRM': 11, Chrome: -6 },
    domains: { 'vendor-desconocido.example': 4 },
  },
  {
    name: 'wrapup',
    beats: 20,
    hasInput: true,
    idleTime: 0.5,
    powerState: 'active',
    mic: false,
    cam: false,
    callApp: false,
    apps: { Brave: 8, Cursor: 7 },
    domains: { 'teamandes.atlassian.net': 8 },
  },
];

// ---------------------------------------------------------------------------
// Construccion del payload
// ---------------------------------------------------------------------------

/** Payload v3: plano, nombres espejo de las columnas de contractor_activity_15s. */
function buildPayloadV3(phase: Phase, keys: number, clicks: number): string {
  return JSON.stringify({
    v: 3,
    keyboard_count: keys,
    keyboard_idle: phase.idleTime,
    mouse_clicks: clicks,
    mouse_idle: phase.idleTime,
    idle_time: phase.idleTime,
    beat_duration: BEAT_SECONDS,
    power_state: phase.powerState,
    browser_source: Object.keys(phase.domains).length ? 'uia' : 'none',
    mic_active: phase.mic ? 1 : 0,
    cam_active: phase.cam ? 1 : 0,
    call_app_active: phase.callApp ? 1 : 0,
    apps: phase.apps,
    domains: phase.domains,
  });
}

/** Payload v2: anidado, el formato anterior al rebranding. */
function buildPayloadV2(phase: Phase, keys: number, clicks: number): string {
  return JSON.stringify({
    Keyboard: { InputsCount: keys, InactiveTime: phase.idleTime },
    Mouse: { ClicksCount: clicks, InactiveTime: phase.idleTime },
    IdleTime: phase.idleTime,
    BeatDuration: BEAT_SECONDS,
    PowerState: phase.powerState,
    BrowserSource: Object.keys(phase.domains).length ? 'uia' : 'none',
    PresenceSignals: {
      microphone_active: phase.mic,
      camera_active: phase.cam,
      call_app_active: phase.callApp,
    },
    AppUsage: phase.apps,
    DomainUsage: phase.domains,
  });
}

// ---------------------------------------------------------------------------
// Utilidades
// ---------------------------------------------------------------------------

function fmt(dt: Date): string {
  // ClickHouse DateTime: 'YYYY-MM-DD HH:MM:SS'
  const pad = (n: number) => String(n).padStart(2, '0');
  return (
    `${dt.getUTCFullYear()}-${pad(dt.getUTCMonth() + 1)}-${pad(dt.getUTCDate())} ` +
    `${pad(dt.getUTCHours())}:${pad(dt.getUTCMinutes())}:${pad(dt.getUTCSeconds())}`
  );
}

function arg(name: string): string | undefined {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.split('=')[1] : undefined;
}

function hasFlag(name: string): boolean {
  return process.argv.includes(`--${name}`);
}

/** Pesos del catálogo seed-dimensions (productive / neutral / non_productive). */
const APP_WEIGHT: Record<string, number> = {
  Cursor: 1.5,
  Brave: 1.0,
  Teams: 1.0,
};
const DOMAIN_WEIGHT: Record<string, number> = {
  'github.com': 1.5,
  'stackoverflow.com': 1.5,
  'teamandes.atlassian.net': 1.5,
  'google.com': 1.0,
  'reddit.com': 0.5,
};
const W_MAX = 1.5;
/**
 * El SQL usa `ROWS BETWEEN 7 PRECEDING AND CURRENT ROW` (8 filas).
 * El beat actual idle no aporta micro-actividad, así que solo los 7 idle
 * siguientes al último micro-activo siguen viendo esa señal en la ventana.
 * (Un lookback de 8 PRECEDING marcaría 8 idle ≈ 2 min exactos.)
 */
const GRACE_IDLE_BEATS = 7;

function phaseQuality(phase: Phase): { weighted: number; total: number } {
  let weighted = 0;
  let total = 0;
  // Se ignoran los segundos <= 0 para espejar mapFilter de usageMapSql.
  // El `?? 1.0` es el fallback de catalogo: coincide con W_UNKNOWN del ETL.
  for (const [name, sec] of Object.entries(phase.apps)) {
    if (sec <= 0) continue;
    weighted += sec * (APP_WEIGHT[name] ?? 1.0);
    total += sec;
  }
  for (const [name, sec] of Object.entries(phase.domains)) {
    if (sec <= 0) continue;
    weighted += sec * (DOMAIN_WEIGHT[name] ?? 1.0);
    total += sec;
  }
  return { weighted, total };
}

function expectedTotals() {
  const totalBeats = PHASES.reduce((a, p) => a + p.beats, 0);
  const lockedBeats = PHASES.filter((p) => p.powerState !== 'active').reduce(
    (a, p) => a + p.beats,
    0,
  );
  const microActive = PHASES.filter(
    (p) =>
      p.powerState === 'active' && (p.hasInput || p.mic || p.cam || p.callApp),
  ).reduce((a, p) => a + p.beats, 0);
  const scoredBeats = totalBeats - lockedBeats;
  const idleBeats = scoredBeats - microActive;
  const graceActive = Math.min(GRACE_IDLE_BEATS, idleBeats);
  const activeBeats = microActive + graceActive;

  let weighted = 0;
  let totalQ = 0;
  for (const p of PHASES) {
    const q = phaseQuality(p);
    weighted += q.weighted * p.beats;
    totalQ += q.total * p.beats;
  }

  const sActive = scoredBeats > 0 ? (100 * activeBeats) / scoredBeats : 0;
  const sQuality = totalQ > 0 ? (100 * weighted) / (W_MAX * totalQ) : sActive;
  const score = (sActive * sQuality) / 100;

  return {
    totalBeats,
    lockedBeats,
    scoredBeats,
    microActive,
    idleBeats,
    graceActive,
    activeBeats,
    weighted,
    totalQ,
    sActive,
    sQuality,
    score,
  };
}

function printExpected(): void {
  const e = expectedTotals();
  console.log(
    '\nQue esperar del ETL (S_active * S_quality / 100 + gracia 7 idle):',
  );
  console.log(
    `  total_beats (power=active) : ${e.scoredBeats}  (${e.totalBeats} - ${e.lockedBeats} locked)`,
  );
  console.log(`  micro-activos              : ${e.microActive}`);
  console.log(`  idle micro                 : ${e.idleBeats}`);
  console.log(
    `  active_beats (con gracia)  : ${e.activeBeats}  (+${e.graceActive} idle arrastrados)`,
  );
  console.log(`  S_active                   : ${e.sActive.toFixed(2)}`);
  console.log(
    `  calidad weighted/total     : ${e.weighted.toFixed(1)} / ${e.totalQ.toFixed(1)}`,
  );
  console.log(`  S_quality                  : ${e.sQuality.toFixed(2)}`);
  console.log(`  productivity_score         : ${e.score.toFixed(2)}`);
}

async function analyze(client: ReturnType<typeof createClient>): Promise<void> {
  const e = expectedTotals();
  const q = async <T>(sql: string): Promise<T[]> => {
    const res = await client.query({ query: sql, format: 'JSONEachRow' });
    return (await res.json()) as T[];
  };

  const events = await q<{ cnt: string }>(
    `SELECT count() AS cnt FROM events_raw WHERE contractor_id = '${CONTRACTOR_ID}'`,
  );
  const activity = await q<{
    total: string;
    idle: string;
    locked: string;
  }>(`
    SELECT
      count() AS total,
      countIf(is_idle = 1) AS idle,
      countIf(power_state != 'active') AS locked
    FROM contractor_activity_15s
    WHERE contractor_id = '${CONTRACTOR_ID}'
  `);
  const daily = await q<{
    total_beats: string;
    active_beats: string;
    idle_beats: string;
    productivity_score: string;
  }>(`
    SELECT total_beats, active_beats, idle_beats, productivity_score
    FROM contractor_daily_metrics
    WHERE contractor_id = '${CONTRACTOR_ID}'
    ORDER BY created_at DESC
    LIMIT 1
  `);
  const session = await q<{
    total_seconds: string;
    active_seconds: string;
    idle_seconds: string;
    productivity_score: string;
  }>(`
    SELECT total_seconds, active_seconds, idle_seconds, productivity_score
    FROM session_summary
    WHERE contractor_id = '${CONTRACTOR_ID}'
    ORDER BY created_at DESC
    LIMIT 1
  `);

  console.log('\n=== ANALISIS vs esperado ===');
  console.log(
    `  events_raw              : ${events[0]?.cnt ?? 0} (esperado ${e.totalBeats})`,
  );
  if (!activity[0] || Number(activity[0].total) === 0) {
    console.log(
      '  contractor_activity_15s : VACIO — corre ETL primero (--run-etl)',
    );
    return;
  }
  console.log(
    `  activity total/locked   : ${activity[0].total} / ${activity[0].locked} (esperado ${e.totalBeats} / ${e.lockedBeats})`,
  );

  if (!daily[0]) {
    console.log(
      '  daily_metrics           : VACIO — falta processActivityToDailyMetrics',
    );
    return;
  }
  const gotScore = Number(daily[0].productivity_score);
  const delta = gotScore - e.score;
  console.log(
    `  daily total/active      : ${daily[0].total_beats} / ${daily[0].active_beats} ` +
      `(esperado ${e.scoredBeats} / ${e.activeBeats})`,
  );
  console.log(
    `  daily score             : ${gotScore.toFixed(2)}  esperado ${e.score.toFixed(2)}  delta ${delta.toFixed(2)}`,
  );
  if (session[0]) {
    console.log(
      `  session score           : ${Number(session[0].productivity_score).toFixed(2)}  ` +
        `active_s=${session[0].active_seconds} total_s=${session[0].total_seconds}`,
    );
  } else {
    console.log(
      '  session_summary         : VACIO — falta processActivityToSessionSummary',
    );
  }

  const okBeats = Number(daily[0].total_beats) === e.scoredBeats;
  const okActive = Number(daily[0].active_beats) === e.activeBeats;
  const okScore = Math.abs(delta) < 1.5;
  console.log(
    `\n  veredicto: beats=${okBeats ? 'OK' : 'DIFF'} active=${okActive ? 'OK' : 'DIFF'} score=${okScore ? 'OK (±1.5)' : 'DIFF'}`,
  );
}

async function runEtl(day: string): Promise<void> {
  const { NestFactory } = await import('@nestjs/core');
  const { AppModule } = await import('../src/app.module');
  const { EtlService } = await import('../src/etl/services/etl.service');
  const { parseCalendarDayStart, parseCalendarDayEnd } = await import('config');

  console.log('\nCorriendo ETL (Nest application context)...');
  const app = await NestFactory.createApplicationContext(AppModule, {
    logger: ['error', 'warn', 'log'],
  });
  const etl = app.get(EtlService);
  const from = parseCalendarDayStart(day);
  const to = parseCalendarDayEnd(day);

  await etl.processEventsToActivityForce(from, to, CONTRACTOR_ID);
  await etl.processActivityToDailyMetrics(
    undefined,
    from,
    to,
    [CONTRACTOR_ID],
    true,
  );
  try {
    await etl.processActivityToSessionSummary(undefined, CONTRACTOR_ID, from);
  } catch (err) {
    console.error(
      'session_summary ETL falló (daily ya se calculó):',
      err instanceof Error ? err.message : err,
    );
  }
  await app.close();
  console.log('ETL listo.');
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const client = createClient({
    // 'host' y no 'url': el proyecto usa @clickhouse/client 0.2.x, donde 'url'
    // todavia no existe (ver scripts/populate-test-data.ts).
    host: `http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}`,
    username: CLICKHOUSE_USERNAME,
    password: CLICKHOUSE_PASSWORD,
    database: CLICKHOUSE_DATABASE,
    request_timeout: 300000,
  });

  const cleanupOnly = hasFlag('cleanup');

  // --- Limpieza: siempre corre antes de insertar, para que el script sea
  //     idempotente y no acumule beats duplicados entre corridas.
  const tables: Array<[string, string]> = [
    ['events_raw', 'contractor_id'],
    ['contractor_activity_15s', 'contractor_id'],
    ['contractor_daily_metrics', 'contractor_id'],
    ['session_summary', 'contractor_id'],
    ['sessions_raw', 'contractor_id'],
    ['agent_sessions_raw', 'contractor_id'],
    ['contractor_info_raw', 'contractor_id'],
  ];
  console.log(`Limpiando datos sinteticos (prefijo '${SYNTHETIC_PREFIX}')...`);
  for (const [table, col] of tables) {
    await client.command({
      query: `ALTER TABLE ${table} DELETE WHERE ${col} LIKE '${SYNTHETIC_PREFIX}%' SETTINGS mutations_sync = 1`,
    });
  }
  console.log('  listo\n');

  if (cleanupOnly) {
    console.log('--cleanup: no se genera nada nuevo.');
    await client.close();
    return;
  }

  // --- Ventana temporal.
  // Los timestamps se guardan en UTC y el ETL deriva workday con
  // toDate(timestamp, 'America/New_York'). Para que la hora caiga limpia
  // dentro del dia operativo, se ancla a mediodia UTC + offset.
  const day = arg('day') || new Date().toISOString().slice(0, 10);
  const startHour = parseInt(arg('start-hour') || '13', 10); // 13 UTC ~ 09:00 NY
  const start = new Date(`${day}T${String(startHour).padStart(2, '0')}:00:00Z`);

  const totalBeats = PHASES.reduce((acc, p) => acc + p.beats, 0);
  console.log(`Generando ${totalBeats} beats de ${BEAT_SECONDS}s`);
  console.log(`  dia operativo : ${day}`);
  console.log(`  desde (UTC)   : ${fmt(start)}`);
  console.log(`  duracion      : ${(totalBeats * BEAT_SECONDS) / 60} min\n`);

  // --- Dimensiones del contratista.
  await client.insert({
    table: 'contractor_info_raw',
    values: [
      {
        contractor_id: CONTRACTOR_ID,
        name: 'Synthetic ETL Test',
        email: 'synthetic@etl.test',
        job_position: 'QA Fixture',
        work_schedule_start: '09:00',
        work_schedule_end: '18:00',
        country: 'AR',
        client_id: `${SYNTHETIC_PREFIX}-client`,
        team_id: `${SYNTHETIC_PREFIX}-team`,
        isActive: 1,
        created_at: fmt(start),
        updated_at: fmt(start),
      },
    ],
    format: 'JSONEachRow',
  });

  const end = new Date(start.getTime() + totalBeats * BEAT_SECONDS * 1000);
  await client.insert({
    table: 'sessions_raw',
    values: [
      {
        session_id: SESSION_ID,
        contractor_id: CONTRACTOR_ID,
        session_start: fmt(start),
        session_end: fmt(end),
        total_duration: totalBeats * BEAT_SECONDS,
        created_at: fmt(start),
        updated_at: fmt(end),
      },
    ],
    format: 'JSONEachRow',
  });

  await client.insert({
    table: 'agent_sessions_raw',
    values: [
      {
        agent_session_id: AGENT_SESSION_ID,
        contractor_id: CONTRACTOR_ID,
        agent_id: AGENT_ID,
        session_id: SESSION_ID,
        session_start: fmt(start),
        session_end: fmt(end),
        total_duration: totalBeats * BEAT_SECONDS,
        created_at: fmt(start),
        updated_at: fmt(end),
      },
    ],
    format: 'JSONEachRow',
  });

  // --- Beats.
  const rows: Record<string, unknown>[] = [];
  let beatIndex = 0;
  let v3Count = 0;
  let v2Count = 0;

  for (const phase of PHASES) {
    for (let i = 0; i < phase.beats; i++) {
      const ts = new Date(start.getTime() + beatIndex * BEAT_SECONDS * 1000);

      // Valores deterministas (sin random) para que el resultado del ETL sea
      // reproducible y comparable entre corridas.
      const keys = phase.hasInput ? 20 + (beatIndex % 15) : 0;
      const clicks = phase.hasInput ? 5 + (beatIndex % 7) : 0;

      // Alterna v3/v2 para forzar al ETL a resolver ambos formatos.
      const useV3 = beatIndex % 2 === 0;
      const payload = useV3
        ? buildPayloadV3(phase, keys, clicks)
        : buildPayloadV2(phase, keys, clicks);
      if (useV3) v3Count++;
      else v2Count++;

      rows.push({
        event_id: `${SYNTHETIC_PREFIX}-evt-${String(beatIndex).padStart(5, '0')}`,
        contractor_id: CONTRACTOR_ID,
        agent_id: AGENT_ID,
        session_id: SESSION_ID,
        agent_session_id: AGENT_SESSION_ID,
        timestamp: fmt(ts),
        payload,
        created_at: fmt(ts),
      });

      beatIndex++;
    }
  }

  await client.insert({
    table: 'events_raw',
    values: rows,
    format: 'JSONEachRow',
  });

  // --- Resumen de lo generado y de lo que deberia salir.
  console.log('Insertado:');
  console.log(
    `  events_raw          ${rows.length} beats  (v3: ${v3Count}, v2: ${v2Count})`,
  );
  console.log('  sessions_raw        1');
  console.log('  agent_sessions_raw  1');
  console.log('  contractor_info_raw 1\n');

  console.log('Composicion por fase:');
  console.log('  fase        beats   min  power    input  presencia');
  for (const p of PHASES) {
    const presence =
      [p.mic && 'mic', p.cam && 'cam', p.callApp && 'call']
        .filter(Boolean)
        .join('+') || '-';
    console.log(
      `  ${p.name.padEnd(10)} ${String(p.beats).padStart(5)} ` +
        `${String((p.beats * BEAT_SECONDS) / 60).padStart(5)}  ` +
        `${p.powerState.padEnd(8)} ${(p.hasInput ? 'si' : 'no').padEnd(6)} ${presence}`,
    );
  }

  printExpected();
  console.log('\nContractor sintetico:', CONTRACTOR_ID);

  if (hasFlag('run-etl')) {
    await runEtl(day);
  }
  if (hasFlag('analyze') || hasFlag('run-etl')) {
    await analyze(client);
  }

  console.log('\nPara borrar:  pnpm run generate:etl-hour -- --cleanup');
  await client.close();
}

main().catch((e) => {
  console.error('FALLO:', e);
  process.exit(1);
});
