import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';

import { AppService } from './app.service';
import { ClickHouseModule } from './clickhouse/clickhouse.module';
import { AgentSessionsListener } from './listeners/agent-sessions.listener';
import { ContractorsListener } from './listeners/contractors.listener';
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
  ],
  controllers: [
    EventsListener,
    SessionsListener,
    AgentSessionsListener,
    ContractorsListener,
  ],
  providers: [AppService],
})
export class AppModule {}
