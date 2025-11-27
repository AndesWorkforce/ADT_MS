import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';

import { envs, getLogModeMessage, resolveLogLevels } from 'config';

import { AppModule } from './app.module';
import { RpcExceptionFilter } from './common/filters/rpc-exception.filter';

async function bootstrap() {
  const logLevels = resolveLogLevels();
  Logger.overrideLogger(logLevels);

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
  log(`ClickHouse: ${envs.clickhouse.host}:${envs.clickhouse.port}/${envs.clickhouse.database}`);
}
bootstrap();
