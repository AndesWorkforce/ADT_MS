import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { Job } from 'bullmq';

import { QUEUE_NAMES, QUEUE_CONCURRENCY } from 'config/bullmq.config';

import { EtlService } from '../../etl/services/etl.service';
import { EtlJobData } from '../types';

/**
 * Processor para la cola adt-etl-session-summaries
 *
 * Consume jobs de resúmenes de sesión y delega a EtlService.processActivityToSessionSummary().
 * - Concurrencia: 1 (ETLs pesados, un job a la vez)
 * - Reintentos: 3 con backoff exponencial (configurado en DEFAULT_JOB_OPTIONS)
 * - Idempotencia: EtlService excluye sesiones que ya tienen resumen en session_summary
 */
@Processor(QUEUE_NAMES.ETL_SESSION_SUMMARIES, {
  concurrency: QUEUE_CONCURRENCY.ETL_SESSION_SUMMARIES,
})
export class SessionSummaryProcessor extends WorkerHost {
  private readonly logger = new Logger(SessionSummaryProcessor.name);

  constructor(private readonly etlService: EtlService) {
    super();
  }

  async process(
    job: Job<EtlJobData>,
  ): Promise<{ count: number; sessionId?: string }> {
    const startTime = Date.now();
    const { sessionId, contractorId } = job.data;

    this.logger.log(
      `🚀 [Job ${job.id}] Starting session summary ETL` +
        `${sessionId ? ` for session=${sessionId}` : ' (all pending)'}` +
        `${contractorId ? ` contractor=${contractorId}` : ''}` +
        ` | Attempt ${job.attemptsMade + 1}/${job.opts.attempts || 3}`,
    );

    try {
      await job.updateProgress(10);

      // Delegar al ETL service (ya maneja idempotencia: excluye sesiones existentes)
      const summaries =
        await this.etlService.processActivityToSessionSummary(sessionId);

      await job.updateProgress(100);

      const duration = Date.now() - startTime;
      this.logger.log(
        `✅ [Job ${job.id}] Session summary ETL completed: ` +
          `${summaries.length} summaries generated` +
          `${sessionId ? ` for session=${sessionId}` : ''}` +
          ` in ${duration}ms`,
      );

      return { count: summaries.length, sessionId };
    } catch (error) {
      const duration = Date.now() - startTime;
      this.logger.error(
        `❌ [Job ${job.id}] Session summary ETL failed` +
          `${sessionId ? ` for session=${sessionId}` : ''}` +
          ` after ${duration}ms (attempt ${job.attemptsMade + 1}): ${error.message}`,
        error.stack,
      );
      throw error;
    }
  }

  async onCompleted(
    job: Job<EtlJobData>,
    result: { count: number; sessionId?: string },
  ) {
    this.logger.debug(
      `[Job ${job.id}] Completed: ${result.count} session summaries` +
        `${result.sessionId ? ` for session=${result.sessionId}` : ''}`,
    );
  }

  async onFailed(job: Job<EtlJobData> | undefined, error: Error) {
    if (job) {
      this.logger.error(
        `❌ [Job ${job.id}] Session summary ETL failed permanently` +
          `${job.data.sessionId ? ` for session=${job.data.sessionId}` : ''}` +
          ` after ${job.attemptsMade} attempts: ${error.message}`,
      );
    }
  }
}
