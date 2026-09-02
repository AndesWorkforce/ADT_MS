/**
 * Reprocesa el ETL de un dia operativo completo, ejercitando el camino real
 * (Nest application context -> EtlService), no SQL a mano.
 *
 * Etapas:
 *   1. events_raw -> contractor_activity_15s   (processEventsToActivityForce)
 *   2. activity   -> contractor_daily_metrics  (processActivityToDailyMetrics, force)
 *   3. activity   -> session_summary           (processActivityToSessionSummary)
 *
 * Uso:
 *   pnpm run reprocess:day -- --day=2026-09-01
 *   pnpm run reprocess:day -- --day=2026-09-01 --contractor=<id>
 *   pnpm run reprocess:day -- --day=2026-09-01 --skip-events   # solo etapas 2 y 3
 *
 * OJO --contractor: processEventsToActivityForce borra por rango de `workday`
 * derivado de los timestamps UTC, asi que un dia D sin filtro tambien borra
 * particiones del dia D+1 (se reinsertan solo los beats del dia D operativo).
 * Con contratista acotado el borrado no toca a los demas.
 */

import 'dotenv/config';

function arg(name: string): string | undefined {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.split('=')[1] : undefined;
}

async function main(): Promise<void> {
  const { NestFactory } = await import('@nestjs/core');
  const { AppModule } = await import('../src/app.module');
  const { EtlService } = await import('../src/etl/services/etl.service');
  const { parseCalendarDayStart, parseCalendarDayEnd, formatDateInTZ } =
    await import('config');

  const day = arg('day') ?? formatDateInTZ(new Date());
  const contractor = arg('contractor');

  const from = parseCalendarDayStart(day);
  const to = parseCalendarDayEnd(day);

  console.log(
    `\nReproceso ETL dia ${day} (${from.toISOString()} .. ${to.toISOString()} UTC)` +
      `${contractor ? ` | contractor=${contractor}` : ' | todos los contratistas'}\n`,
  );

  const app = await NestFactory.createApplicationContext(AppModule, {
    logger: ['error', 'warn', 'log'],
  });
  const etl = app.get(EtlService);

  try {
    if (process.argv.includes('--skip-events')) {
      console.log('[1/3] events_raw -> activity: SALTEADO (--skip-events)');
    } else {
      console.log('[1/3] events_raw -> contractor_activity_15s ...');
      const beats = await etl.processEventsToActivityForce(
        from,
        to,
        contractor,
      );
      // OJO: el DELETE previo es una mutacion ASINCRONA (sin mutations_sync=1),
      // asi que este conteo puede leer viejo + nuevo a la vez. Verificar en CH.
      console.log(
        `      beats contados por el ETL (puede incluir la mutacion en vuelo): ${beats}`,
      );
    }

    console.log('[2/3] activity -> contractor_daily_metrics ...');
    const metrics = await etl.processActivityToDailyMetrics(
      undefined,
      from,
      to,
      contractor ? [contractor] : undefined,
      true,
    );
    console.log(`      filas de daily metrics: ${metrics.length}`);
    for (const m of metrics) {
      console.log(
        `      ${m.contractor_id} score=${Number(m.productivity_score).toFixed(2)} ` +
          `beats=${m.total_beats} active=${m.active_beats} idle=${m.idle_beats}`,
      );
    }

    console.log('[3/3] activity -> session_summary ...');
    const summaries = await etl.processActivityToSessionSummary(
      undefined,
      contractor,
      from,
    );
    console.log(`      sesiones resumidas: ${summaries.length}`);
  } finally {
    await app.close();
  }

  console.log('\nReproceso terminado.');
}

main().catch((err) => {
  console.error(
    '\nFallo el reproceso:',
    err instanceof Error ? err.stack : err,
  );
  process.exit(1);
});
