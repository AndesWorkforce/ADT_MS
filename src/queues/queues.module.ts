import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';

import { envs } from 'config/envs';
import { QUEUE_NAMES } from 'config/bullmq.config';

import { ClickHouseModule } from '../clickhouse/clickhouse.module';
import { SaveEventProcessor } from './processors';
import { EventQueueService } from './services';

/**
 * Módulo de Colas con BullMQ
 *
 * FASE 2: Cola de eventos implementada
 *
 * Este módulo gestiona todas las colas de procesamiento asíncrono:
 * - Eventos de agentes (alta frecuencia) ✅ IMPLEMENTADO
 * - Sesiones y agent sessions (TODO FASE 3)
 * - Actualizaciones de contractors (TODO FASE 3)
 * - ETLs pesados (TODO FASE 4)
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

    // Importar ClickHouseModule para que los processors puedan usar ClickHouseService
    ClickHouseModule,
  ],
  providers: [
    // ✅ FASE 2: Processor de eventos
    SaveEventProcessor,

    // ✅ FASE 2: Servicio de gestión de cola de eventos
    EventQueueService,
  ],
  exports: [
    // Exportar servicio para uso en listeners
    EventQueueService,
  ],
})
export class QueuesModule {}
