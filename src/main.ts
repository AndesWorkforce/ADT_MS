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
        // Queue group: hace que cada mensaje lo procese UNA sola instancia.
        // Sin esto NATS entrega a TODAS las suscriptas, y con dos instancias
        // (incluido el solapamiento de un rolling deploy) cada beat se guarda
        // duplicado. Medido: 2 instancias de EVENTS_MS + 2 de ADT_MS => cada
        // heartbeat 4 veces en events_raw, con teclado y mouse inflados 4x.
        // El nombre tiene que ser DISTINTO por servicio: dos servicios con el
        // mismo grupo suscriptos al mismo subject se roban los mensajes.
        queue: 'adt-ms',
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
