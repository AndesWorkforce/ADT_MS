import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { Job } from 'bullmq';

import { QUEUE_NAMES } from 'config/bullmq.config';

import { ClickHouseService } from '../../clickhouse/clickhouse.service';
import { SaveEventJobData } from '../types';

@Processor(QUEUE_NAMES.EVENTS, {
  concurrency: 5,
})
export class SaveEventProcessor extends WorkerHost {
  private readonly logger = new Logger(SaveEventProcessor.name);

  constructor(private readonly clickHouseService: ClickHouseService) {
    super();
  }

  async process(
    job: Job<SaveEventJobData | SaveEventJobData[]>,
  ): Promise<{ inserted: number }> {
    const startTime = Date.now();

    const events: SaveEventJobData[] = Array.isArray(job.data)
      ? job.data
      : [job.data];

    const count = events.length;
    const isBatch = Array.isArray(job.data);

    this.logger.debug(
      `Processing ${count} event(s) - Job ${job.id} (${isBatch ? 'BATCH' : 'SINGLE'})`,
    );

    try {
      await job.updateProgress(10);

      const rows = events.map((event) => ({
        event_id: event.event_id,
        contractor_id: event.contractor_id,
        agent_id: event.agent_id || null,
        session_id: event.session_id || null,
        agent_session_id: event.agent_session_id || null,
        timestamp:
          event.timestamp instanceof Date
            ? event.timestamp
            : new Date(event.timestamp),
        payload: event.payload,
        created_at:
          event.created_at instanceof Date
            ? event.created_at
            : new Date(event.created_at),
      }));

      await job.updateProgress(50);

      if (count === 1) {
        await this.clickHouseService.insert('events_raw', rows[0]);
      } else {
        await this.clickHouseService.insertBatch('events_raw', rows);
      }

      await job.updateProgress(100);

      const duration = Date.now() - startTime;
      const avgTime = duration / count;

      this.logger.log(
        `✅ Processed ${count} event(s) in ${duration}ms (${avgTime.toFixed(2)}ms/event) - Job ${job.id}`,
      );

      return { inserted: count };
    } catch (error) {
      this.logger.error(
        `❌ Failed to process ${count} event(s) - Job ${job.id}: ${error.message}`,
        error.stack,
      );
      throw error;
    }
  }

  async onCompleted(job: Job, result: { inserted: number }) {
    this.logger.debug(
      `Job ${job.id} completed successfully. Inserted: ${result.inserted}`,
    );
  }

  async onFailed(job: Job | undefined, error: Error) {
    if (job) {
      const events = Array.isArray(job.data) ? job.data : [job.data];
      this.logger.error(
        `Job ${job.id} failed after ${job.attemptsMade} attempts. ` +
          `${events.length} event(s) lost: ${error.message}`,
      );
    }
  }
}
