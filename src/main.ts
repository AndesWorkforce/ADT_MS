import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';

import { envs, getLogModeMessage, resolveLogLevels } from 'config';

import { AppModule } from './app.module';
import { RpcExceptionFilter } from './common/filters/rpc-exception.filter';

const globalLogger = new Logger('Process');

process.on('uncaughtException', (error: Error) => {
  globalLogger.error(
    `Uncaught Exception — process will continue. Error: ${error.message}`,
    error.stack,
  );
});

process.on('unhandledRejection', (reason: unknown) => {
  const message = reason instanceof Error ? reason.message : String(reason);
  globalLogger.error(
    `Unhandled Rejection — process will continue. Reason: ${message}`,
    reason instanceof Error ? reason.stack : undefined,
  );
});

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
}
bootstrap();
