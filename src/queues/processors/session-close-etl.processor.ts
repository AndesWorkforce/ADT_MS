import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { Job } from 'bullmq';

import { QUEUE_NAMES, QUEUE_CONCURRENCY } from 'config/bullmq.config';

import { EtlService } from '../../etl/services/etl.service';
import { EtlJobData } from '../types';

/**
 * Processor para la cola adt-etl-session-close
 *
 * Ejecuta la orquestadora completa al cerrar una sesión:
 * 1) process-events (hoy, solo contratista)
 * 2) process-daily-metrics (hoy, solo contratista)
 * 3) process-session-summaries (solo esa sesión)
 */
@Processor(QUEUE_NAMES.ETL_SESSION_CLOSE, {
  concurrency: QUEUE_CONCURRENCY.ETL_SESSION_CLOSE,
})
export class SessionCloseEtlProcessor extends WorkerHost {
  private readonly logger = new Logger(SessionCloseEtlProcessor.name);

  constructor(private readonly etlService: EtlService) {
    super();
  }

  async process(
    job: Job<EtlJobData>,
  ): Promise<{ sessionId: string; contractorId: string }> {
    const startTime = Date.now();
    const { sessionId, contractorId } = job.data;

    if (!sessionId || !contractorId) {
      throw new Error(
        `SessionCloseEtlProcessor requires sessionId and contractorId. Got: sessionId=${sessionId} contractorId=${contractorId}`,
      );
    }

    this.logger.log(
      `🚀 [Job ${job.id}] Starting full ETL on session close for session=${sessionId} contractor=${contractorId} | Attempt ${job.attemptsMade + 1}/${job.opts.attempts || 3}`,
    );

    try {
      await job.updateProgress(10);
      await this.etlService.runFullEtlForContractorOnSessionClose(
        contractorId,
        sessionId,
      );
      await job.updateProgress(100);

      const duration = Date.now() - startTime;
      this.logger.log(
        `✅ [Job ${job.id}] Full ETL on session close completed for session=${sessionId} contractor=${contractorId} in ${duration}ms`,
      );

      return { sessionId, contractorId };
    } catch (error) {
      const duration = Date.now() - startTime;
      this.logger.error(
        `❌ [Job ${job.id}] Full ETL on session close failed for session=${sessionId} contractor=${contractorId} after ${duration}ms (attempt ${job.attemptsMade + 1}): ${error.message}`,
        error.stack,
      );
      throw error;
    }
  }

  async onFailed(job: Job<EtlJobData> | undefined, error: Error) {
    if (job) {
      this.logger.error(
        `❌ [Job ${job.id}] Full ETL on session close failed permanently for session=${job.data.sessionId} after ${job.attemptsMade} attempts: ${error.message}`,
      );
    }
  }
}
