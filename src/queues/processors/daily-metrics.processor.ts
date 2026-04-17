import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { Job } from 'bullmq';

import { coerceToOperationalDayStart, jobWorkdayToYmd } from 'config';
import { QUEUE_NAMES, QUEUE_CONCURRENCY } from 'config/bullmq.config';

import { EtlService } from '../../etl/services/etl.service';
import { EtlJobData } from '../types';

/**
 * Processor para la cola adt-etl-daily-metrics
 *
 * Consume jobs de métricas diarias y delega a EtlService.processActivityToDailyMetrics().
 * - Concurrencia: 1 (ETLs pesados, un job a la vez)
 * - Reintentos: 3 con backoff exponencial (configurado en DEFAULT_JOB_OPTIONS)
 * - Idempotencia: EtlService ya verifica si el día tiene datos antes de recalcular
 */
@Processor(QUEUE_NAMES.ETL_DAILY_METRICS, {
  concurrency: QUEUE_CONCURRENCY.ETL_DAILY_METRICS,
})
export class DailyMetricsProcessor extends WorkerHost {
  private readonly logger = new Logger(DailyMetricsProcessor.name);

  constructor(private readonly etlService: EtlService) {
    super();
  }

  async process(
    job: Job<EtlJobData>,
  ): Promise<{ count: number; workday: string }> {
    const startTime = Date.now();
    const { workday, fromDate, toDate, force, contractorIds } = job.data;

    const dayStr = jobWorkdayToYmd(workday);

    this.logger.log(
      `🚀 [Job ${job.id}] Starting daily metrics ETL for ${dayStr}` +
        `${contractorIds?.length ? ` (${contractorIds.length} contractors)` : ''}` +
        `${force ? ' [FORCE]' : ''}` +
        ` | Attempt ${job.attemptsMade + 1}/${job.opts.attempts || 3}`,
    );

    try {
      await job.updateProgress(10);

      // Parsear fechas (llegan serializadas como string desde Redis)
      const workdayDate = workday
        ? coerceToOperationalDayStart(workday as Date | string)
        : undefined;
      const from = fromDate ? new Date(fromDate) : undefined;
      const to = toDate ? new Date(toDate) : undefined;

      await job.updateProgress(30);

      // Delegar al ETL service (ya maneja idempotencia internamente)
      const metrics = await this.etlService.processActivityToDailyMetrics(
        workdayDate,
        from,
        to,
        contractorIds,
      );

      await job.updateProgress(100);

      const duration = Date.now() - startTime;
      this.logger.log(
        `✅ [Job ${job.id}] Daily metrics ETL completed for ${dayStr}: ` +
          `${metrics.length} metrics generated in ${duration}ms`,
      );

      return { count: metrics.length, workday: dayStr };
    } catch (error) {
      const duration = Date.now() - startTime;
      this.logger.error(
        `❌ [Job ${job.id}] Daily metrics ETL failed for ${dayStr} ` +
          `after ${duration}ms (attempt ${job.attemptsMade + 1}): ${error.message}`,
        error.stack,
      );
      throw error;
    }
  }

  async onCompleted(
    job: Job<EtlJobData>,
    result: { count: number; workday: string },
  ) {
    this.logger.debug(
      `[Job ${job.id}] Completed: ${result.count} daily metrics for ${result.workday}`,
    );
  }

  async onFailed(job: Job<EtlJobData> | undefined, error: Error) {
    if (job) {
      const dayStr = jobWorkdayToYmd(job.data.workday);
      this.logger.error(
        `❌ [Job ${job.id}] Daily metrics ETL for ${dayStr} failed permanently ` +
          `after ${job.attemptsMade} attempts: ${error.message}`,
      );
    }
  }
}
