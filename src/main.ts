import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';

import {
  envs,
  getLogModeMessage,
  getMessagePattern,
  resolveLogLevels,
} from 'config';

import { AppModule } from './app.module';
import { RpcExceptionFilter } from './common/filters/rpc-exception.filter';

async function bootstrap() {
  const logLevels = resolveLogLevels();
  Logger.overrideLogger(logLevels);

  // Crear microservicio NATS puro (sin HTTP)
  const app = await NestFactory.createMicroservice<MicroserviceOptions>(
    AppModule,
    {
      transport: Transport.NATS,
      options: {
        servers: [`nats://${envs.natsHost}:${envs.natsPort}`],
        user: envs.natsUsername,
        pass: envs.natsPassword,
      },
      logger: logLevels,
    },
  );

  app.useGlobalFilters(new RpcExceptionFilter());

  const logger = new Logger('Main');
  const log = (message: string) =>
    envs.devLogsEnabled ? logger.log(message) : logger.warn(message);

  const modeMessage = getLogModeMessage();
  if (envs.devLogsEnabled) {
    logger.verbose(modeMessage);
  } else {
    logger.warn(modeMessage);
  }

  await app.listen();

  log(`ADT microservice is running on NATS`);
  log(`  - NATS: ${envs.natsHost}:${envs.natsPort}`);
  log(
    `  - ClickHouse: ${envs.clickhouse.host}:${envs.clickhouse.port}/${envs.clickhouse.database}`,
  );
  log(`  - Environment: ${envs.environment}`);

  // Log de los patrones que está escuchando
  console.log(`[ADT_MS] 📋 Patrones que está escuchando:`);
  console.log(`  - ${getMessagePattern('adt.getDailyMetrics')}`);
  console.log(`  - ${getMessagePattern('adt.getRealtimeMetrics')}`);
  console.log(`  - ${getMessagePattern('adt.getAllRealtimeMetrics')}`);
  console.log(`  - ${getMessagePattern('adt.getSessionSummaries')}`);
  console.log(`  - ${getMessagePattern('adt.getActivity')}`);
  console.log(`  - ${getMessagePattern('adt.getAppUsage')}`);
  console.log(`  - ${getMessagePattern('adt.getRanking')}`);
  log(
    `  - Listening to patterns: adt.getDailyMetrics, adt.getRealtimeMetrics, adt.getAllRealtimeMetrics, etc.`,
  );
}
bootstrap();
