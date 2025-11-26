import { Module } from '@nestjs/common';

import { ClickHouseModule } from '../clickhouse/clickhouse.module';
import { RawService } from './raw.service';

@Module({
  imports: [ClickHouseModule],
  providers: [RawService],
  exports: [RawService],
})
export class RawModule {}

