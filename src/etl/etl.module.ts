import { Module } from '@nestjs/common';

import { ClickHouseModule } from '../clickhouse/clickhouse.module';
import { DimensionsInitService } from './services/dimensions-init.service';
import { DimensionsService } from './services/dimensions.service';
import { EtlService } from './services/etl.service';
import { RealtimeMetricsService } from './services/realtime-metrics.service';
import { UsageDataService } from './services/usage-data.service';
import { ActivityToDailyMetricsTransformer } from './transformers/activity-to-daily-metrics.transformer';
import { ActivityToSessionSummaryTransformer } from './transformers/activity-to-session-summary.transformer';
import { EventsToActivityTransformer } from './transformers/events-to-activity.transformer';

@Module({
  imports: [ClickHouseModule],
  providers: [
    // Services
    DimensionsService,
    DimensionsInitService,
    EtlService,
    RealtimeMetricsService,
    UsageDataService,
    // Transformers
    EventsToActivityTransformer,
    ActivityToDailyMetricsTransformer,
    ActivityToSessionSummaryTransformer,
  ],
  exports: [
    DimensionsService,
    EtlService,
    RealtimeMetricsService,
    UsageDataService,
    EventsToActivityTransformer,
    ActivityToDailyMetricsTransformer,
    ActivityToSessionSummaryTransformer,
  ],
})
export class EtlModule {}
