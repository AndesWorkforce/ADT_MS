import { Module } from '@nestjs/common';

import { ClickHouseModule } from '../clickhouse/clickhouse.module';
import { DimensionsInitService } from './services/dimensions-init.service';
import { DimensionsService } from './services/dimensions.service';
import { EtlService } from './services/etl.service';
import { RealtimeMetricsService } from './services/realtime-metrics.service';
import { ActivityToDailyMetricsTransformer } from './transformers/activity-to-daily-metrics.transformer';
import { ActivityToSessionSummaryTransformer } from './transformers/activity-to-session-summary.transformer';
import { EventsToActivityTransformer } from './transformers/events-to-activity.transformer';
import { EventsToAppUsageTransformer } from './transformers/events-to-app-usage.transformer';

@Module({
  imports: [ClickHouseModule],
  providers: [
    // Services
    DimensionsService,
    DimensionsInitService,
    EtlService,
    RealtimeMetricsService,
    // Transformers
    EventsToActivityTransformer,
    EventsToAppUsageTransformer,
    ActivityToDailyMetricsTransformer,
    ActivityToSessionSummaryTransformer,
  ],
  exports: [
    DimensionsService,
    EtlService,
    RealtimeMetricsService,
    EventsToActivityTransformer,
    EventsToAppUsageTransformer,
    ActivityToDailyMetricsTransformer,
    ActivityToSessionSummaryTransformer,
  ],
})
export class EtlModule {}
