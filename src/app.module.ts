import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';

import { AppService } from './app.service';
import { ClickHouseModule } from './clickhouse/clickhouse.module';
import { EtlModule } from './etl/etl.module';
import { RedisModule } from './redis/redis.module';
import { QueuesModule } from './queues/queues.module';
import { AdtListener } from './listeners/adt.listener';
import { AgentSessionsListener } from './listeners/agent-sessions.listener';
import { ContractorsListener } from './listeners/contractors.listener';
import { DimensionsListener } from './listeners/dimensions.listener';
import { EventsListener } from './listeners/events.listener';
import { SessionsListener } from './listeners/sessions.listener';
import { RawModule } from './raw/raw.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
    }),
    ClickHouseModule,
    RawModule,
    EtlModule,
    RedisModule,
    QueuesModule, // ✨ FASE 2: Módulo de colas con BullMQ (EventQueueService exportado)
  ],
  controllers: [
    EventsListener, // Usa EventQueueService de QueuesModule
    SessionsListener,
    AgentSessionsListener,
    ContractorsListener,
    DimensionsListener,
    AdtListener,
  ],
  providers: [AppService],
})
export class AppModule {}
