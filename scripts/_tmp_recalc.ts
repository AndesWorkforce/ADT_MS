import 'dotenv/config';
async function main() {
  const { NestFactory } = await import('@nestjs/core');
  const { AppModule } = await import('../src/app.module');
  const { EtlService } = await import('../src/etl/services/etl.service');
  const app = await NestFactory.createApplicationContext(AppModule, { logger: ['error', 'warn'] });
  const etl = app.get(EtlService);
  const sid = process.argv.find((a) => a.startsWith('--session='))?.split('=')[1];
  if (!sid) throw new Error('falta --session=<id>');
  const rows = await etl.processActivityToSessionSummary(sid);
  console.log(`recalculadas ${rows.length} fila(s) para la sesion ${sid}`);
  await app.close();
}
main().catch((e) => { console.error(e instanceof Error ? e.stack : e); process.exit(1); });
