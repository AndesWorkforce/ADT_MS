/**
 * Lote de prueba ETL: varios contratistas × varios días, con perfiles
 * productivos vs no productivos.
 *
 * Qué cubre (lo que la hora única no llega a probar):
 *   1. Multi-día: 3 workdays, 1 sesión/día, ETL por rango.
 *   2. Multi-contratista: 3 productivos + 3 no productivos.
 *   3. Ranking: min(score grupo A) > max(score grupo B).
 *   4. Presencia: un productivo gana actividad por llamada, no por teclado.
 *   5. Idle largo: baja S_active (gracia = 7 beats, no todo el bloque).
 *   6. Locked largo: sale de total_beats (power_state != active).
 *   7. Día con más idle en el grupo productivo (día 2) — score baja, ranking no.
 *   8. Payload v2/v3 intercalado, igual que generate-etl-test-hour.ts.
 *
 * Prefijo synth-bat* (no pisa synth-etl de la hora).
 *
 *   pnpm generate:etl-batch
 *   pnpm generate:etl-batch -- --run-etl
 *   pnpm generate:etl-batch -- --analyze
 *   pnpm generate:etl-batch -- --cleanup
 */

import 'dotenv/config';
import { createClient } from '@clickhouse/client';

type ClickHouseClient = ReturnType<typeof createClient>;

const CLICKHOUSE_HOST = process.env.CLICKHOUSE_HOST || 'localhost';
const CLICKHOUSE_PORT = parseInt(process.env.CLICKHOUSE_PORT || '8123', 10);
const CLICKHOUSE_USERNAME = process.env.CLICKHOUSE_USERNAME || 'default';
const CLICKHOUSE_PASSWORD = process.env.CLICKHOUSE_PASSWORD || '';
const CLICKHOUSE_DATABASE = process.env.CLICKHOUSE_DATABASE || 'adt_db';

const PREFIX = 'synth-bat';
const DAYS = ['2026-08-25', '2026-08-26', '2026-08-27'] as const;
const START_HOUR_UTC = 13;
const BEAT_SECONDS = 15;
const GRACE_IDLE_BEATS = 7;
const W_MAX = 1.5;
const SCORE_TOL = 0.05;

const APP_WEIGHT: Record<string, number> = {
  Cursor: 1.5,
  Excel: 1.5,
  Brave: 1.0,
  Chrome: 1.0,
  Teams: 1.0,
  Discord: 0.5,
  Spotify: 0.5,
};
const DOMAIN_WEIGHT: Record<string, number> = {
  'github.com': 1.5,
  'docs.google.com': 1.5,
  'youtube.com': 0.5,
  'reddit.com': 0.5,
  'netflix.com': 0.5,
};

interface Phase {
  name: string;
  beats: number;
  hasInput: boolean;
  idleTime: number;
  powerState: 'active' | 'locked';
  mic: boolean;
  cam: boolean;
  callApp: boolean;
  apps: Record<string, number>;
  domains: Record<string, number>;
}

type Group = 'productive' | 'unproductive';
type Profile = 'prod' | 'prod_call' | 'unprod' | 'unprod_idle' | 'unprod_lock';

interface ContractorSpec {
  slug: string;
  group: Group;
  profile: Profile;
  displayName: string;
}

const CONTRACTORS: ContractorSpec[] = [
  {
    slug: 'ana',
    group: 'productive',
    profile: 'prod',
    displayName: 'Ana Focus',
  },
  {
    slug: 'ben',
    group: 'productive',
    profile: 'prod',
    displayName: 'Ben Focus',
  },
  {
    slug: 'cam',
    group: 'productive',
    profile: 'prod_call',
    displayName: 'Cam Calls',
  },
  {
    slug: 'dan',
    group: 'unproductive',
    profile: 'unprod',
    displayName: 'Dan Distracted',
  },
  {
    slug: 'eva',
    group: 'unproductive',
    profile: 'unprod_idle',
    displayName: 'Eva Idle',
  },
  {
    slug: 'fox',
    group: 'unproductive',
    profile: 'unprod_lock',
    displayName: 'Fox Locked',
  },
];

const FOCUS: Omit<Phase, 'name' | 'beats'> = {
  hasInput: true,
  idleTime: 0.2,
  powerState: 'active',
  mic: false,
  cam: false,
  callApp: false,
  apps: { Cursor: 11, Brave: 4 },
  domains: { 'github.com': 4 },
};
const CALL: Omit<Phase, 'name' | 'beats'> = {
  hasInput: false,
  idleTime: 15,
  powerState: 'active',
  mic: true,
  cam: true,
  callApp: true,
  apps: { Teams: 15 },
  domains: {},
};
const IDLE: Omit<Phase, 'name' | 'beats'> = {
  hasInput: false,
  idleTime: 15,
  powerState: 'active',
  mic: false,
  cam: false,
  callApp: false,
  apps: { Cursor: 15 },
  domains: {},
};
const LOCKED: Omit<Phase, 'name' | 'beats'> = {
  hasInput: false,
  idleTime: 15,
  powerState: 'locked',
  mic: false,
  cam: false,
  callApp: false,
  apps: {},
  domains: {},
};
const WRAP_PROD: Omit<Phase, 'name' | 'beats'> = {
  hasInput: true,
  idleTime: 0.5,
  powerState: 'active',
  mic: false,
  cam: false,
  callApp: false,
  apps: { Excel: 10, Brave: 5 },
  domains: { 'docs.google.com': 5 },
};
const SOCIAL: Omit<Phase, 'name' | 'beats'> = {
  hasInput: true,
  idleTime: 0.4,
  powerState: 'active',
  mic: false,
  cam: false,
  callApp: false,
  apps: { Chrome: 8, Discord: 7 },
  domains: { 'youtube.com': 8, 'reddit.com': 7 },
};
const WRAP_DIST: Omit<Phase, 'name' | 'beats'> = {
  hasInput: true,
  idleTime: 0.5,
  powerState: 'active',
  mic: false,
  cam: false,
  callApp: false,
  apps: { Spotify: 10, Chrome: 5 },
  domains: { 'netflix.com': 10 },
};
const IDLE_DIST: Omit<Phase, 'name' | 'beats'> = {
  ...IDLE,
  apps: { Chrome: 15 },
};

function p(
  name: string,
  beats: number,
  base: Omit<Phase, 'name' | 'beats'>,
): Phase {
  return { name, beats, ...base };
}

function phasesFor(profile: Profile, dayIndex: number): Phase[] {
  const extraIdle =
    dayIndex === 1 && (profile === 'prod' || profile === 'prod_call') ? 16 : 0;

  if (profile === 'prod') {
    return [
      p('focus', 300 - extraIdle, FOCUS),
      p('call', 40, CALL),
      p('idle', 40 + extraIdle, IDLE),
      p('locked', 20, LOCKED),
      p('wrapup', 80, WRAP_PROD),
    ];
  }
  if (profile === 'prod_call') {
    return [
      p('focus', 240 - extraIdle, FOCUS),
      p('call', 120, CALL),
      p('idle', 40 + extraIdle, IDLE),
      p('locked', 20, LOCKED),
      p('wrapup', 60, WRAP_PROD),
    ];
  }
  if (profile === 'unprod') {
    return [
      p('social', 300, SOCIAL),
      p('idle', 80, IDLE_DIST),
      p('locked', 20, LOCKED),
      p('wrapup', 80, WRAP_DIST),
    ];
  }
  if (profile === 'unprod_idle') {
    return [
      p('social', 200, SOCIAL),
      p('idle', 180, IDLE_DIST),
      p('locked', 20, LOCKED),
      p('wrapup', 80, WRAP_DIST),
    ];
  }
  return [
    p('social', 240, SOCIAL),
    p('idle', 40, IDLE_DIST),
    p('locked', 120, LOCKED),
    p('wrapup', 80, WRAP_DIST),
  ];
}

interface Expected {
  contractorId: string;
  slug: string;
  group: Group;
  day: string;
  scoredBeats: number;
  lockedBeats: number;
  microActive: number;
  activeBeats: number;
  weighted: number;
  totalQ: number;
  sActive: number;
  sQuality: number;
  score: number;
}

function phaseQuality(phase: Phase): { weighted: number; total: number } {
  let weighted = 0;
  let total = 0;
  for (const [name, sec] of Object.entries(phase.apps)) {
    weighted += sec * (APP_WEIGHT[name] ?? 1.0);
    total += sec;
  }
  for (const [name, sec] of Object.entries(phase.domains)) {
    weighted += sec * (DOMAIN_WEIGHT[name] ?? 1.0);
    total += sec;
  }
  return { weighted, total };
}

function expectedFor(
  spec: ContractorSpec,
  day: string,
  dayIndex: number,
): Expected {
  const phases = phasesFor(spec.profile, dayIndex);
  const totalBeats = phases.reduce((a, x) => a + x.beats, 0);
  const lockedBeats = phases
    .filter((x) => x.powerState !== 'active')
    .reduce((a, x) => a + x.beats, 0);
  const microActive = phases
    .filter(
      (x) =>
        x.powerState === 'active' &&
        (x.hasInput || x.mic || x.cam || x.callApp),
    )
    .reduce((a, x) => a + x.beats, 0);
  const scoredBeats = totalBeats - lockedBeats;
  const idleBeats = scoredBeats - microActive;
  const activeBeats = microActive + Math.min(GRACE_IDLE_BEATS, idleBeats);
  let weighted = 0;
  let totalQ = 0;
  for (const phase of phases) {
    const q = phaseQuality(phase);
    weighted += q.weighted * phase.beats;
    totalQ += q.total * phase.beats;
  }
  const sActive = scoredBeats > 0 ? (100 * activeBeats) / scoredBeats : 0;
  const sQuality = totalQ > 0 ? (100 * weighted) / (W_MAX * totalQ) : sActive;
  const score = (sActive * sQuality) / 100;
  return {
    contractorId: `${PREFIX}-${spec.slug}`,
    slug: spec.slug,
    group: spec.group,
    day,
    scoredBeats,
    lockedBeats,
    microActive,
    activeBeats,
    weighted,
    totalQ,
    sActive,
    sQuality,
    score,
  };
}

function allExpected(): Expected[] {
  const out: Expected[] = [];
  DAYS.forEach((day, i) => {
    for (const spec of CONTRACTORS) out.push(expectedFor(spec, day, i));
  });
  return out;
}

function contractorId(slug: string): string {
  return `${PREFIX}-${slug}`;
}
function agentId(slug: string): string {
  return `${PREFIX}-agt-${slug}`;
}
function sessionId(slug: string, day: string): string {
  return `${PREFIX}-ses-${slug}-${day}`;
}
function agentSessionId(slug: string, day: string): string {
  return `${PREFIX}-as-${slug}-${day}`;
}

function fmt(dt: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return (
    `${dt.getUTCFullYear()}-${pad(dt.getUTCMonth() + 1)}-${pad(dt.getUTCDate())} ` +
    `${pad(dt.getUTCHours())}:${pad(dt.getUTCMinutes())}:${pad(dt.getUTCSeconds())}`
  );
}

function hasFlag(name: string): boolean {
  return process.argv.includes(`--${name}`);
}

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

async function cleanup(client: ClickHouseClient): Promise<void> {
  const tables: Array<[string, string]> = [
    ['events_raw', 'contractor_id'],
    ['contractor_activity_15s', 'contractor_id'],
    ['contractor_daily_metrics', 'contractor_id'],
    ['session_summary', 'contractor_id'],
    ['sessions_raw', 'contractor_id'],
    ['agent_sessions_raw', 'contractor_id'],
    ['contractor_info_raw', 'contractor_id'],
  ];
  console.log(`Limpiando datos sinteticos (prefijo '${PREFIX}')...`);
  for (const [table, col] of tables) {
    await client.command({
      query: `ALTER TABLE ${table} DELETE WHERE ${col} LIKE '${PREFIX}%' SETTINGS mutations_sync = 1`,
    });
  }
  console.log('  listo\n');
}

async function insertChunks(
  client: ClickHouseClient,
  table: string,
  rows: Record<string, unknown>[],
): Promise<void> {
  const chunk = 2000;
  for (let i = 0; i < rows.length; i += chunk) {
    await client.insert({
      table,
      values: rows.slice(i, i + chunk),
      format: 'JSONEachRow',
    });
  }
}

async function generate(client: ClickHouseClient): Promise<void> {
  const infoRows: Record<string, unknown>[] = [];
  const sessionRows: Record<string, unknown>[] = [];
  const agentSessionRows: Record<string, unknown>[] = [];
  const eventRows: Record<string, unknown>[] = [];
  let v3 = 0;
  let v2 = 0;

  for (const spec of CONTRACTORS) {
    const cid = contractorId(spec.slug);
    const aid = agentId(spec.slug);
    const firstStart = new Date(
      `${DAYS[0]}T${String(START_HOUR_UTC).padStart(2, '0')}:00:00Z`,
    );
    infoRows.push({
      contractor_id: cid,
      name: spec.displayName,
      email: `${spec.slug}@etl.batch`,
      job_position: spec.group,
      work_schedule_start: '09:00',
      work_schedule_end: '18:00',
      country: 'AR',
      client_id: `${PREFIX}-client`,
      team_id: `${PREFIX}-team-${spec.group}`,
      isActive: 1,
      created_at: fmt(firstStart),
      updated_at: fmt(firstStart),
    });

    DAYS.forEach((day, dayIndex) => {
      const phases = phasesFor(spec.profile, dayIndex);
      const nBeats = phases.reduce((a, x) => a + x.beats, 0);
      const start = new Date(
        `${day}T${String(START_HOUR_UTC).padStart(2, '0')}:00:00Z`,
      );
      const end = new Date(start.getTime() + nBeats * BEAT_SECONDS * 1000);
      const sid = sessionId(spec.slug, day);
      const asid = agentSessionId(spec.slug, day);

      sessionRows.push({
        session_id: sid,
        contractor_id: cid,
        session_start: fmt(start),
        session_end: fmt(end),
        total_duration: nBeats * BEAT_SECONDS,
        created_at: fmt(start),
        updated_at: fmt(end),
      });
      agentSessionRows.push({
        agent_session_id: asid,
        contractor_id: cid,
        agent_id: aid,
        session_id: sid,
        session_start: fmt(start),
        session_end: fmt(end),
        total_duration: nBeats * BEAT_SECONDS,
        created_at: fmt(start),
        updated_at: fmt(end),
      });

      let beatIndex = 0;
      for (const phase of phases) {
        for (let i = 0; i < phase.beats; i++) {
          const ts = new Date(
            start.getTime() + beatIndex * BEAT_SECONDS * 1000,
          );
          const keys = phase.hasInput ? 20 + (beatIndex % 15) : 0;
          const clicks = phase.hasInput ? 5 + (beatIndex % 7) : 0;
          const useV3 = beatIndex % 2 === 0;
          const payload = useV3
            ? buildPayloadV3(phase, keys, clicks)
            : buildPayloadV2(phase, keys, clicks);
          if (useV3) v3++;
          else v2++;
          eventRows.push({
            event_id: `${PREFIX}-evt-${spec.slug}-${day}-${String(beatIndex).padStart(4, '0')}`,
            contractor_id: cid,
            agent_id: aid,
            session_id: sid,
            agent_session_id: asid,
            timestamp: fmt(ts),
            payload,
            created_at: fmt(ts),
          });
          beatIndex++;
        }
      }
    });
  }

  await client.insert({
    table: 'contractor_info_raw',
    values: infoRows,
    format: 'JSONEachRow',
  });
  await client.insert({
    table: 'sessions_raw',
    values: sessionRows,
    format: 'JSONEachRow',
  });
  await client.insert({
    table: 'agent_sessions_raw',
    values: agentSessionRows,
    format: 'JSONEachRow',
  });
  await insertChunks(client, 'events_raw', eventRows);

  console.log('Insertado:');
  console.log(`  contractor_info_raw ${infoRows.length}`);
  console.log(`  sessions_raw        ${sessionRows.length}`);
  console.log(`  agent_sessions_raw  ${agentSessionRows.length}`);
  console.log(
    `  events_raw          ${eventRows.length}  (v3: ${v3}, v2: ${v2})`,
  );
  console.log(`  dias                ${DAYS.join(', ')}`);
  console.log(`  inicio UTC          ${START_HOUR_UTC}:00 (≈ 09:00 NY)\n`);
}

async function runEtl(): Promise<void> {
  const { NestFactory } = await import('@nestjs/core');
  const { AppModule } = await import('../src/app.module');
  const { EtlService } = await import('../src/etl/services/etl.service');
  const { parseCalendarDayStart, parseCalendarDayEnd } = await import('config');

  console.log('\nCorriendo ETL (Nest application context)...');
  const app = await NestFactory.createApplicationContext(AppModule, {
    logger: ['error', 'warn', 'log'],
  });
  const etl = app.get(EtlService);
  const from = parseCalendarDayStart(DAYS[0]);
  const to = parseCalendarDayEnd(DAYS[DAYS.length - 1]);
  const ids = CONTRACTORS.map((c) => contractorId(c.slug));

  for (const id of ids) {
    await etl.processEventsToActivityForce(from, to, id);
  }
  await etl.processActivityToDailyMetrics(undefined, from, to, ids, true);

  for (const spec of CONTRACTORS) {
    for (const day of DAYS) {
      await etl.processActivityToSessionSummary(
        undefined,
        contractorId(spec.slug),
        parseCalendarDayStart(day),
      );
    }
  }

  await app.close();
  console.log('ETL listo.\n');
}

function avg(xs: number[]): number {
  return xs.reduce((a, b) => a + b, 0) / xs.length;
}

async function analyze(client: ClickHouseClient): Promise<void> {
  const expected = allExpected();
  const q = async <T>(query: string): Promise<T[]> => {
    const r = await client.query({ query, format: 'JSONEachRow' });
    return r.json<T[]>();
  };

  const daily = await q<{
    contractor_id: string;
    workday: string;
    total_beats: string;
    active_beats: string;
    idle_beats: string;
    productivity_score: string;
  }>(`
    SELECT contractor_id, formatDateTime(workday, '%F') AS workday, total_beats, active_beats,
           idle_beats, productivity_score
    FROM contractor_daily_metrics
    WHERE contractor_id LIKE '${PREFIX}%'
    ORDER BY contractor_id, workday
  `);

  const sessions = await q<{
    contractor_id: string;
    session_id: string;
    productivity_score: string;
    total_seconds: string;
    active_seconds: string;
  }>(`
    SELECT contractor_id, session_id, productivity_score, total_seconds, active_seconds
    FROM session_summary
    WHERE contractor_id LIKE '${PREFIX}%'
    ORDER BY contractor_id, session_id
  `);

  console.log('=== COMPARACION daily vs esperado ===\n');
  console.log('  slug  dia         beats act  score   esp     dlt     grupo');

  let okRows = 0;
  let badRows = 0;
  const gotByKey = new Map<string, (typeof daily)[0]>();
  for (const row of daily) {
    gotByKey.set(`${row.contractor_id}|${row.workday}`, row);
  }

  for (const e of expected) {
    const row = gotByKey.get(`${e.contractorId}|${e.day}`);
    if (!row) {
      console.log(`  ${e.slug.padEnd(4)} ${e.day}  FALTA fila daily`);
      badRows++;
      continue;
    }
    const score = Number(row.productivity_score);
    const beats = Number(row.total_beats);
    const active = Number(row.active_beats);
    const dlt = score - e.score;
    const ok =
      beats === e.scoredBeats &&
      active === e.activeBeats &&
      Math.abs(dlt) < SCORE_TOL;
    if (ok) okRows++;
    else badRows++;
    console.log(
      `  ${e.slug.padEnd(4)} ${e.day}  ${String(beats).padStart(4)} ${String(active).padStart(4)}  ` +
        `${score.toFixed(2).padStart(6)} ${e.score.toFixed(2).padStart(6)} ${dlt.toFixed(2).padStart(7)}  ` +
        `${e.group} ${ok ? 'OK' : 'DIFF'}`,
    );
  }

  const prodScores = expected
    .filter((e) => e.group === 'productive')
    .map((e) => {
      const row = gotByKey.get(`${e.contractorId}|${e.day}`);
      return Number(row?.productivity_score ?? NaN);
    });
  const distScores = expected
    .filter((e) => e.group === 'unproductive')
    .map((e) => {
      const row = gotByKey.get(`${e.contractorId}|${e.day}`);
      return Number(row?.productivity_score ?? NaN);
    });

  const minProd = Math.min(...prodScores);
  const maxDist = Math.max(...distScores);
  const rankingOk = minProd > maxDist;

  console.log('\n=== RANKING DE GRUPOS ===');
  console.log(
    `  productivo    avg ${avg(prodScores).toFixed(2)}  min ${minProd.toFixed(2)}`,
  );
  console.log(
    `  no productivo avg ${avg(distScores).toFixed(2)}  max ${maxDist.toFixed(2)}`,
  );
  console.log(
    `  min(prod) > max(dist): ${rankingOk ? 'OK' : 'FAIL'}  (${minProd.toFixed(2)} vs ${maxDist.toFixed(2)})`,
  );

  const day2Ana = gotByKey.get(`${PREFIX}-ana|2026-08-26`);
  const day1Ana = gotByKey.get(`${PREFIX}-ana|2026-08-25`);
  const idleDip =
    day1Ana && day2Ana
      ? Number(day2Ana.productivity_score) < Number(day1Ana.productivity_score)
      : false;
  console.log(
    `  dia 2 Ana (mas idle) baja vs dia 1: ${idleDip ? 'OK' : 'FAIL'}`,
  );

  const fox = expected.find((e) => e.slug === 'fox' && e.day === DAYS[0]);
  const foxRow = gotByKey.get(`${PREFIX}-fox|${DAYS[0]}`);
  const foxLockedOk =
    fox && foxRow && Number(foxRow.total_beats) === fox.scoredBeats;
  console.log(
    `  Fox locked excluido (beats=${foxRow?.total_beats}, esp ${fox?.scoredBeats}): ${foxLockedOk ? 'OK' : 'FAIL'}`,
  );

  let sessionOk = 0;
  let sessionBad = 0;
  for (const e of expected) {
    const sid = sessionId(e.slug, e.day);
    const s = sessions.find((x) => x.session_id === sid);
    if (!s) {
      sessionBad++;
      continue;
    }
    if (Math.abs(Number(s.productivity_score) - e.score) < SCORE_TOL)
      sessionOk++;
    else sessionBad++;
  }
  console.log(
    `\n=== SESSION vs esperado ===  ${sessionOk} OK / ${sessionBad} DIFF  (filas session=${sessions.length})`,
  );

  console.log('\n=== VEREDICTO ===');
  console.log(`  daily filas ${daily.length}/${expected.length}`);
  console.log(`  daily match ${okRows} OK / ${badRows} DIFF`);
  console.log(`  session match ${sessionOk} OK / ${sessionBad} DIFF`);
  console.log(`  ranking grupos ${rankingOk ? 'OK' : 'FAIL'}`);
  const pass =
    badRows === 0 &&
    sessionBad === 0 &&
    rankingOk &&
    idleDip &&
    !!foxLockedOk &&
    daily.length === expected.length;
  console.log(`  resultado final: ${pass ? 'PASS' : 'FAIL'}`);
  if (!pass) process.exitCode = 1;
}

async function main() {
  const client = createClient({
    host: `http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}`,
    username: CLICKHOUSE_USERNAME,
    password: CLICKHOUSE_PASSWORD,
    database: CLICKHOUSE_DATABASE,
    request_timeout: 300000,
  });

  await cleanup(client);
  if (hasFlag('cleanup')) {
    console.log('--cleanup: no se genera nada nuevo.');
    await client.close();
    return;
  }

  await generate(client);
  const sample = allExpected().filter((e) => e.day === DAYS[0]);
  console.log('Esperado dia 1 (S_active * S_quality / 100, gracia 7):');
  for (const e of sample) {
    console.log(
      `  ${e.slug.padEnd(4)} ${e.group.padEnd(13)} beats=${e.scoredBeats} active=${e.activeBeats} ` +
        `S_a=${e.sActive.toFixed(1)} S_q=${e.sQuality.toFixed(1)} score=${e.score.toFixed(2)}`,
    );
  }

  if (hasFlag('run-etl')) {
    await runEtl();
  }
  if (hasFlag('analyze') || hasFlag('run-etl')) {
    await analyze(client);
  }

  console.log('\nPara borrar:  pnpm generate:etl-batch -- --cleanup');
  await client.close();
}

main().catch((e) => {
  console.error('FALLO:', e);
  process.exit(1);
});
