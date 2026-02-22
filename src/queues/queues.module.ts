import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { ClientsModule, Transport } from '@nestjs/microservices';

import { envs } from 'config/envs';
import { QUEUE_NAMES } from 'config/bullmq.config';

import { ClickHouseModule } from '../clickhouse/clickhouse.module';
import { EtlModule } from '../etl/etl.module';
import { RedisModule } from '../redis/redis.module';
import {
  SaveEventProcessor,
  InactivityScanProcessor,
  DailyMetricsProcessor,
  SessionSummaryProcessor,
  SessionCloseEtlProcessor,
} from './processors';
import {
  EventQueueService,
  InactivityScanQueueService,
  EtlQueueService,
} from './services';

/**
 * Módulo de Colas con BullMQ
 *
 * FASE 2: Cola de eventos implementada
 * FASE 2.5: Sistema de alertas de inactividad
 * FASE 4: Colas ETL para métricas diarias y resúmenes de sesión
 *
 * Este módulo gestiona todas las colas de procesamiento asíncrono:
 * - Eventos de agentes (alta frecuencia) ✅ IMPLEMENTADO
 * - Escaneo de inactividad (periódico) ✅ IMPLEMENTADO
 * - Sesiones y agent sessions (TODO FASE 3)
 * - Actualizaciones de contractors (TODO FASE 3)
 * - ETLs pesados ✅ IMPLEMENTADO (FASE 4)
 *
 * Usa Redis en una DB separada (REDIS_QUEUE_DB) para evitar conflictos con caché
 */
@Module({
  imports: [
    // Configuración global de BullMQ con Redis
    BullModule.forRoot({
      connection: {
        host: envs.redis.host,
        port: envs.redis.port,
        password: envs.redis.password || undefined,
        db: envs.queues.redisDb, // DB separada para colas (default: 1)
      },
    }),

    // ✅ FASE 2: Registrar cola de eventos
    BullModule.registerQueue({
      name: QUEUE_NAMES.EVENTS,
    }),

    // ✅ FASE 2.5: Registrar cola de escaneo de inactividad (condicional)
    ...(envs.queues.useInactivityAlerts
      ? [
          BullModule.registerQueue({
            name: QUEUE_NAMES.INACTIVITY_SCAN,
          }),
        ]
      : []),

    // ✅ FASE 4: Registrar colas ETL (condicional)
    ...(envs.queues.useEtlQueue
      ? [
          BullModule.registerQueue({
            name: QUEUE_NAMES.ETL_DAILY_METRICS,
          }),
          BullModule.registerQueue({
            name: QUEUE_NAMES.ETL_SESSION_SUMMARIES,
          }),
          BullModule.registerQueue({
            name: QUEUE_NAMES.ETL_SESSION_CLOSE,
          }),
        ]
      : []),

    // NATS Client para InactivityScanProcessor (RPC calls y eventos)
    ...(envs.queues.useInactivityAlerts
      ? [
          ClientsModule.register([
            {
              name: 'NATS_SERVICE',
              transport: Transport.NATS,
              options: {
                servers: [`nats://${envs.natsHost}:${envs.natsPort}`],
                user: envs.natsUsername,
                pass: envs.natsPassword,
              },
            },
          ]),
        ]
      : []),

    // Importar ClickHouseModule para que los processors puedan usar ClickHouseService
    ClickHouseModule,

    // Importar RedisModule para tracking de inactividad
    RedisModule,

    // ✅ FASE 4: Importar EtlModule para que los processors ETL puedan usar EtlService
    ...(envs.queues.useEtlQueue ? [EtlModule] : []),
  ],
  providers: [
    // ✅ FASE 2: Processor de eventos
    SaveEventProcessor,

    // ✅ FASE 2: Servicio de gestión de cola de eventos
    EventQueueService,

    // ✅ FASE 2.5: Processor y servicio de inactividad (condicional)
    ...(envs.queues.useInactivityAlerts
      ? [InactivityScanProcessor, InactivityScanQueueService]
      : []),

    // ✅ FASE 4: Processors y servicio ETL (condicional)
    ...(envs.queues.useEtlQueue
      ? [
          DailyMetricsProcessor,
          SessionSummaryProcessor,
          SessionCloseEtlProcessor,
          EtlQueueService,
        ]
      : []),
  ],
  exports: [
    // Exportar servicio para uso en listeners
    EventQueueService,

    // Exportar servicio de inactividad si está habilitado
    ...(envs.queues.useInactivityAlerts ? [InactivityScanQueueService] : []),

    // ✅ FASE 4: Exportar servicio ETL si está habilitado
    ...(envs.queues.useEtlQueue ? [EtlQueueService] : []),
  ],
})
export class QueuesModule {}
