import { Module } from '@nestjs/common';

import { ClickHouseModule } from '../clickhouse/clickhouse.module';
// Services
import { ActivityService } from './services/activity.service';
import { AppUsageService } from './services/app-usage.service';
import { AppsSyncService } from './services/apps-sync.service';
import { DailyMetricsService } from './services/daily-metrics.service';
import { DimensionsInitService } from './services/dimensions-init.service';
import { DimensionsService } from './services/dimensions.service';
import { EtlService } from './services/etl.service';
import { RankingService } from './services/ranking.service';
import { RealtimeMetricsService } from './services/realtime-metrics.service';
import { ActivityRepository } from './services/activity-repository.service';
import { SessionSummariesService } from './services/session-summaries.service';
import { UsageDataService } from './services/usage-data.service';
// Transformers
import { ActivityToDailyMetricsTransformer } from './transformers/activity-to-daily-metrics.transformer';
import { ActivityToSessionSummaryTransformer } from './transformers/activity-to-session-summary.transformer';
import { EventsToActivityTransformer } from './transformers/events-to-activity.transformer';

@Module({
  imports: [ClickHouseModule],
  providers: [
    ActivityRepository,
    ActivityService,
    AppUsageService,
    AppsSyncService,
    DailyMetricsService,
    RankingService,
    SessionSummariesService,
    RealtimeMetricsService,
    DimensionsService,
    DimensionsInitService,
    EtlService,
    UsageDataService,
    EventsToActivityTransformer,
    ActivityToDailyMetricsTransformer,
    ActivityToSessionSummaryTransformer,
  ],
  exports: [
    ActivityRepository,
    ActivityService,
    AppUsageService,
    AppsSyncService,
    DailyMetricsService,
    RankingService,
    SessionSummariesService,
    RealtimeMetricsService,
    DimensionsService,
    EtlService,
    UsageDataService,
    EventsToActivityTransformer,
    ActivityToDailyMetricsTransformer,
    ActivityToSessionSummaryTransformer,
  ],
})
export class EtlModule {}
