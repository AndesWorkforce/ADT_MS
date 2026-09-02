/**
 * Sync Postgres sessions / agent_sessions / event FKs → ClickHouse RAW tables.
 * Only contractors listed in sync-sessions-ch.config.json.
 *
 * Does NOT require ADT_MS microservice to be running.
 *
 * Requires in ADT_MS/.env:
 *   DATABASE_URL           → USER_MS Postgres (sessions, agent_sessions)
 *   EVENTS_DATABASE_URL    → EVENTS_MS Postgres (events)
 *   CLICKHOUSE_*           → target ClickHouse (use prod for real recovery)
 *
 * Usage:
 *   pnpm sync:sessions-ch
 *   pnpm sync:sessions-ch -- --apply
 *   pnpm sync:sessions-ch -- --apply --resume
 *   pnpm sync:sessions-ch -- --apply --contractor-id=xxx
 */
import 'dotenv/config';

import { createClient, ClickHouseClient } from '@clickhouse/client';
import * as fs from 'fs';
import * as path from 'path';
import { DateTime } from 'luxon';
import { Pool, PoolClient } from 'pg';

const OPERATIONAL_TZ = 'America/Bogota';
const INSERT_CHUNK = 500;
const MUTATION_POLL_MS = 2_000;
const MUTATION_TIMEOUT_MS = 30 * 60 * 1000;

type Job = { contractorId: string; from: string; to: string };

type Progress = {
  completed: string[];
  failed: Array<{ contractorId: string; error: string; at: string }>;
  startedAt?: string;
  updatedAt?: string;
};

const apply = process.argv.includes('--apply');
const resume = process.argv.includes('--resume');
const contractorFilter = argValue('--contractor-id');

const configPath = path.join(__dirname, 'sync-sessions-ch.config.json');
const progressPath = path.join(__dirname, 'sync-sessions-ch.progress.json');

function argValue(name: string): string | undefined {
  const prefix = `${name}=`;
  const hit = process.argv.find((a) => a.startsWith(prefix));
  return hit ? hit.slice(prefix.length) : undefined;
}

function jobKey(job: Job): string {
  return `${job.contractorId}|${job.from}|${job.to}`;
}

function esc(value: string): string {
  return value.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
}

function formatChDateTime(date: Date | null): string | null {
  if (!date || Number.isNaN(date.getTime())) return null;
  return date.toISOString().replace('T', ' ').slice(0, 19);
}

function dayBoundsUtc(
  fromDay: string,
  toDay: string,
): {
  fromUtc: Date;
  toExclusiveUtc: Date;
  fromCh: string;
  toExclusiveCh: string;
} {
  const fromUtc = DateTime.fromISO(fromDay, { zone: OPERATIONAL_TZ })
    .startOf('day')
    .toUTC()
    .toJSDate();
  const toExclusiveUtc = DateTime.fromISO(toDay, { zone: OPERATIONAL_TZ })
    .startOf('day')
    .plus({ days: 1 })
    .toUTC()
    .toJSDate();
  return {
    fromUtc,
    toExclusiveUtc,
    fromCh: formatChDateTime(fromUtc)!,
    toExclusiveCh: formatChDateTime(toExclusiveUtc)!,
  };
}

function loadJobs(): Job[] {
  const raw = JSON.parse(fs.readFileSync(configPath, 'utf8')) as {
    jobs: Job[];
  };
  let jobs = raw.jobs;
  if (contractorFilter) {
    jobs = jobs.filter((j) => j.contractorId === contractorFilter);
  }
  if (jobs.length === 0) {
    throw new Error(
      'No jobs to run (config empty or contractor filter mismatch)',
    );
  }
  return jobs;
}

function loadProgress(): Progress {
  if (!resume || !fs.existsSync(progressPath)) {
    return { completed: [], failed: [] };
  }
  return JSON.parse(fs.readFileSync(progressPath, 'utf8')) as Progress;
}

function saveProgress(progress: Progress): void {
  progress.updatedAt = new Date().toISOString();
  fs.writeFileSync(progressPath, JSON.stringify(progress, null, 2));
}

async function waitMutations(
  ch: ClickHouseClient,
  database: string,
  tables: string[],
): Promise<void> {
  const started = Date.now();
  const tableList = tables.map((t) => `'${esc(t)}'`).join(', ');

  for (;;) {
    const res = await ch.query({
      query: `
        SELECT count() AS cnt
        FROM system.mutations
        WHERE database = '${esc(database)}'
          AND table IN (${tableList})
          AND is_done = 0
          AND latest_fail_reason = ''
      `,
      format: 'JSONEachRow',
    });
    const rows = (await res.json()) as Array<{ cnt: string | number }>;
    const pending = Number(rows[0]?.cnt ?? 0);
    if (pending === 0) {
      return;
    }
    if (Date.now() - started > MUTATION_TIMEOUT_MS) {
      throw new Error(
        `Timeout waiting for mutations on ${tables.join(', ')} (pending=${pending})`,
      );
    }
    console.log(`   … waiting mutations pending=${pending}`);
    await new Promise((r) => setTimeout(r, MUTATION_POLL_MS));
  }
}

async function countCh(ch: ClickHouseClient, sql: string): Promise<number> {
  const res = await ch.query({ query: sql, format: 'JSONEachRow' });
  const rows = (await res.json()) as Array<{ cnt: string | number }>;
  return Number(rows[0]?.cnt ?? 0);
}

async function syncSessions(
  ch: ClickHouseClient,
  users: PoolClient,
  job: Job,
  bounds: ReturnType<typeof dayBoundsUtc>,
  database: string,
): Promise<{ pg: number; chBefore: number }> {
  const { contractorId } = job;
  const pgRes = await users.query<{
    id: string;
    contractor_id: string;
    session_start: Date;
    session_end: Date | null;
    total_duration: number | null;
    created_at: Date;
    updated_at: Date;
  }>(
    `SELECT id, contractor_id, session_start, session_end, total_duration, created_at, updated_at
     FROM sessions
     WHERE contractor_id = $1
       AND session_start >= $2
       AND session_start < $3
     ORDER BY session_start ASC`,
    [contractorId, bounds.fromUtc, bounds.toExclusiveUtc],
  );

  const chBefore = await countCh(
    ch,
    `SELECT count() AS cnt FROM sessions_raw
     WHERE contractor_id = '${esc(contractorId)}'
       AND session_start >= toDateTime('${bounds.fromCh}')
       AND session_start < toDateTime('${bounds.toExclusiveCh}')`,
  );

  console.log(`   sessions_raw: pg=${pgRes.rowCount} chBefore=${chBefore}`);

  if (!apply) {
    return { pg: pgRes.rowCount ?? 0, chBefore };
  }

  await ch.command({
    query: `
      ALTER TABLE sessions_raw DELETE
      WHERE contractor_id = '${esc(contractorId)}'
        AND session_start >= toDateTime('${bounds.fromCh}')
        AND session_start < toDateTime('${bounds.toExclusiveCh}')
    `,
  });
  await waitMutations(ch, database, ['sessions_raw']);

  for (let i = 0; i < pgRes.rows.length; i += INSERT_CHUNK) {
    const chunk = pgRes.rows.slice(i, i + INSERT_CHUNK).map((r) => ({
      session_id: r.id,
      contractor_id: r.contractor_id,
      session_start: formatChDateTime(r.session_start),
      session_end: formatChDateTime(r.session_end),
      total_duration: r.total_duration,
      created_at: formatChDateTime(r.created_at),
      updated_at: formatChDateTime(r.updated_at),
    }));
    await ch.insert({
      table: 'sessions_raw',
      values: chunk,
      format: 'JSONEachRow',
    });
  }

  return { pg: pgRes.rowCount ?? 0, chBefore };
}

async function syncAgentSessions(
  ch: ClickHouseClient,
  users: PoolClient,
  job: Job,
  bounds: ReturnType<typeof dayBoundsUtc>,
  database: string,
): Promise<{ pg: number; chBefore: number }> {
  const { contractorId } = job;
  const pgRes = await users.query<{
    id: string;
    contractor_id: string;
    agent_id: string;
    session_id: string | null;
    session_start: Date;
    session_end: Date | null;
    total_duration: number | null;
    created_at: Date;
    updated_at: Date;
  }>(
    `SELECT id, contractor_id, agent_id, session_id, session_start, session_end,
            total_duration, created_at, updated_at
     FROM agent_sessions
     WHERE contractor_id = $1
       AND session_start >= $2
       AND session_start < $3
     ORDER BY session_start ASC`,
    [contractorId, bounds.fromUtc, bounds.toExclusiveUtc],
  );

  const chBefore = await countCh(
    ch,
    `SELECT count() AS cnt FROM agent_sessions_raw
     WHERE contractor_id = '${esc(contractorId)}'
       AND session_start >= toDateTime('${bounds.fromCh}')
       AND session_start < toDateTime('${bounds.toExclusiveCh}')`,
  );

  console.log(
    `   agent_sessions_raw: pg=${pgRes.rowCount} chBefore=${chBefore}`,
  );

  if (!apply) {
    return { pg: pgRes.rowCount ?? 0, chBefore };
  }

  await ch.command({
    query: `
      ALTER TABLE agent_sessions_raw DELETE
      WHERE contractor_id = '${esc(contractorId)}'
        AND session_start >= toDateTime('${bounds.fromCh}')
        AND session_start < toDateTime('${bounds.toExclusiveCh}')
    `,
  });
  await waitMutations(ch, database, ['agent_sessions_raw']);

  for (let i = 0; i < pgRes.rows.length; i += INSERT_CHUNK) {
    const chunk = pgRes.rows.slice(i, i + INSERT_CHUNK).map((r) => ({
      agent_session_id: r.id,
      contractor_id: r.contractor_id,
      agent_id: r.agent_id,
      session_id: r.session_id,
      session_start: formatChDateTime(r.session_start),
      session_end: formatChDateTime(r.session_end),
      total_duration: r.total_duration,
      created_at: formatChDateTime(r.created_at),
      updated_at: formatChDateTime(r.updated_at),
    }));
    await ch.insert({
      table: 'agent_sessions_raw',
      values: chunk,
      format: 'JSONEachRow',
    });
  }

  return { pg: pgRes.rowCount ?? 0, chBefore };
}

async function syncEventFks(
  ch: ClickHouseClient,
  events: PoolClient,
  job: Job,
  bounds: ReturnType<typeof dayBoundsUtc>,
  database: string,
): Promise<{ pg: number; mismatchedBefore: number }> {
  const { contractorId } = job;
  const mapTable = `_tmp_event_session_map_${contractorId.replace(/[^a-zA-Z0-9]/g, '').slice(0, 24)}`;
  const joinTable = `${mapTable}_join`;
  const joinTableFq = `${database}.${joinTable}`;

  const pgRes = await events.query<{
    id: string;
    session_id: string | null;
    agent_session_id: string | null;
  }>(
    `SELECT id, session_id, agent_session_id
     FROM events
     WHERE contractor_id = $1
       AND timestamp >= $2
       AND timestamp < $3`,
    [contractorId, bounds.fromUtc, bounds.toExclusiveUtc],
  );

  const chEvents = await countCh(
    ch,
    `SELECT count() AS cnt FROM events_raw
     WHERE contractor_id = '${esc(contractorId)}'
       AND timestamp >= toDateTime('${bounds.fromCh}')
       AND timestamp < toDateTime('${bounds.toExclusiveCh}')`,
  );

  console.log(
    `   events_raw FK map: pgRows=${pgRes.rowCount} chEventsInRange=${chEvents}`,
  );

  if (!apply) {
    return { pg: pgRes.rowCount ?? 0, mismatchedBefore: chEvents };
  }

  if ((pgRes.rowCount ?? 0) === 0) {
    console.log('   events_raw: no postgres rows, skip UPDATE');
    return { pg: 0, mismatchedBefore: chEvents };
  }

  // This CH version does not support ALTER UPDATE ... FROM; use Join + joinGet.
  // joinGet requires fully-qualified db.table (otherwise resolves in `default`).
  await ch.command({ query: `DROP TABLE IF EXISTS ${joinTableFq}` });
  await ch.command({
    query: `
      CREATE TABLE ${joinTableFq} (
        event_id String,
        session_id String,
        agent_session_id String
      ) ENGINE = Join(ANY, LEFT, event_id)
    `,
  });

  for (let i = 0; i < pgRes.rows.length; i += INSERT_CHUNK) {
    const chunk = pgRes.rows.slice(i, i + INSERT_CHUNK).map((r) => ({
      event_id: r.id,
      session_id: r.session_id ?? '',
      agent_session_id: r.agent_session_id ?? '',
    }));
    await ch.insert({
      table: joinTableFq,
      values: chunk,
      format: 'JSONEachRow',
    });
  }

  await ch.command({
    query: `
      ALTER TABLE ${database}.events_raw
      UPDATE
        session_id = nullIf(joinGet('${joinTableFq}', 'session_id', event_id), ''),
        agent_session_id = nullIf(joinGet('${joinTableFq}', 'agent_session_id', event_id), '')
      WHERE contractor_id = '${esc(contractorId)}'
        AND timestamp >= toDateTime('${bounds.fromCh}')
        AND timestamp < toDateTime('${bounds.toExclusiveCh}')
        AND joinGet('${joinTableFq}', 'session_id', event_id) != ''
    `,
  });
  await waitMutations(ch, database, ['events_raw']);
  await ch.command({ query: `DROP TABLE IF EXISTS ${joinTableFq}` });

  return { pg: pgRes.rowCount ?? 0, mismatchedBefore: chEvents };
}

async function main(): Promise<void> {
  const usersUrl = process.env.DATABASE_URL;
  const eventsUrl = process.env.EVENTS_DATABASE_URL;
  const chHost = process.env.CLICKHOUSE_HOST || 'localhost';
  const chPort = process.env.CLICKHOUSE_PORT || '8123';
  const chUser = process.env.CLICKHOUSE_USERNAME || 'default';
  const chPass = process.env.CLICKHOUSE_PASSWORD || '';
  const chDb = process.env.CLICKHOUSE_DATABASE || 'default';

  if (!usersUrl) {
    throw new Error('DATABASE_URL is required (USER_MS Postgres)');
  }
  if (!eventsUrl) {
    throw new Error(
      'EVENTS_DATABASE_URL is required (EVENTS_MS Postgres). Add it to ADT_MS/.env',
    );
  }

  const jobs = loadJobs();
  const progress = loadProgress();
  if (!progress.startedAt) {
    progress.startedAt = new Date().toISOString();
  }

  const pending = jobs.filter((j) => !progress.completed.includes(jobKey(j)));

  console.log('=== sync Postgres → ClickHouse (RAW) ===');
  console.log(`mode=${apply ? 'APPLY' : 'DRY-RUN'} resume=${resume}`);
  console.log(`clickhouse=${chHost}:${chPort}/${chDb}`);
  console.log(`jobs total=${jobs.length} pending=${pending.length}`);
  console.log('');

  const usersPool = new Pool({ connectionString: usersUrl });
  const eventsPool = new Pool({ connectionString: eventsUrl });
  const ch = createClient({
    host: `http://${chHost}:${chPort}`,
    username: chUser,
    password: chPass,
    database: chDb,
  });

  try {
    await ch.ping();
    await usersPool.query('SELECT 1');
    await eventsPool.query('SELECT 1');
  } catch (err) {
    console.error(
      'Connection failed:',
      err instanceof Error ? err.message : err,
    );
    process.exit(1);
  }

  const users = await usersPool.connect();
  const events = await eventsPool.connect();

  let ok = 0;
  let fail = 0;

  try {
    for (let i = 0; i < pending.length; i++) {
      const job = pending[i];
      const bounds = dayBoundsUtc(job.from, job.to);
      console.log(
        `[${i + 1}/${pending.length}] contractor=${job.contractorId} ` +
          `${job.from}→${job.to}`,
      );

      try {
        await syncSessions(ch, users, job, bounds, chDb);
        await syncAgentSessions(ch, users, job, bounds, chDb);
        await syncEventFks(ch, events, job, bounds, chDb);

        progress.completed.push(jobKey(job));
        saveProgress(progress);
        ok += 1;
        console.log(`   OK (${ok} ok / ${fail} fail)\n`);
      } catch (err) {
        fail += 1;
        const message = err instanceof Error ? err.message : String(err);
        progress.failed.push({
          contractorId: job.contractorId,
          error: message,
          at: new Date().toISOString(),
        });
        saveProgress(progress);
        console.error(`   FAIL: ${message}`);
        console.error(
          '   Stopping. Re-run with --apply --resume after fixing the issue.\n',
        );
        process.exit(1);
      }
    }
  } finally {
    users.release();
    events.release();
    await usersPool.end();
    await eventsPool.end();
    await ch.close();
  }

  console.log(
    `done mode=${apply ? 'APPLY' : 'DRY-RUN'} ok=${ok} fail=${fail} ` +
      `completedTotal=${progress.completed.length}/${jobs.length}`,
  );
  console.log(
    'Next: re-ETL activity → daily metrics → session_summary for these contractors only.',
  );
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : err);
  process.exit(1);
});
