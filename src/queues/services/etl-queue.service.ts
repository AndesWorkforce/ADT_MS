import { Injectable, Logger } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';

import { QUEUE_NAMES, DEFAULT_JOB_OPTIONS } from 'config/bullmq.config';

import { EtlJobData, JobType, JobPriority } from '../types';

/**
 * Servicio de gestión de colas ETL con BullMQ
 *
 * FASE 4: Colas dedicadas para ETLs pesados
 * - adt-etl-daily-metrics: Procesamiento de métricas diarias por contractor
 * - adt-etl-session-summaries: Procesamiento de resúmenes de sesión
 *
 * Características:
 * - Jobs idempotentes con jobId determinista (evita duplicados)
 * - Reintentos automáticos con backoff exponencial (3 intentos, 5s base)
 * - Logs estructurados por operación
 */
@Injectable()
export class EtlQueueService {
  private readonly logger = new Logger(EtlQueueService.name);

  constructor(
    @InjectQueue(QUEUE_NAMES.ETL_DAILY_METRICS)
    private readonly dailyMetricsQueue: Queue,
    @InjectQueue(QUEUE_NAMES.ETL_SESSION_SUMMARIES)
    private readonly sessionSummariesQueue: Queue,
    @InjectQueue(QUEUE_NAMES.ETL_SESSION_CLOSE)
    private readonly sessionCloseQueue: Queue,
  ) {}

  // ============================================================================
  // DAILY METRICS
  // ============================================================================

  /**
   * Encola un job para procesar métricas diarias de un día específico.
   *
   * @param workday - Día a procesar (default: hoy)
   * @param contractorIds - IDs de contractors específicos (opcional, si no se pasan procesa todos)
   * @param forceRecalculate - Si true, recalcula aunque ya existan datos
   * @returns ID del job creado
   */
  async addDailyMetricsJob(
    workday?: Date,
    contractorIds?: string[],
    forceRecalculate?: boolean,
  ): Promise<string> {
    try {
      const effectiveWorkday = workday || new Date();
      effectiveWorkday.setUTCHours(0, 0, 0, 0);

      const dayStr = effectiveWorkday.toISOString().slice(0, 10); // YYYY-MM-DD

      // JobId determinista para idempotencia:
      // Mismo día + mismos contractors + mismo force → mismo jobId → BullMQ ignora duplicado
      const contractorSuffix = contractorIds?.length
        ? `-c${contractorIds.sort().join(',')}`
        : '';
      const forceSuffix = forceRecalculate ? '-force' : '';
      const jobId = `daily-metrics-${dayStr}${contractorSuffix}${forceSuffix}`;

      const jobData: EtlJobData = {
        jobType: JobType.DAILY_METRICS,
        requestedAt: new Date(),
        workday: effectiveWorkday,
        contractorIds,
        force: forceRecalculate,
      };

      const job = await this.dailyMetricsQueue.add(
        JobType.DAILY_METRICS,
        jobData,
        {
          ...DEFAULT_JOB_OPTIONS,
          jobId,
          priority: JobPriority.LOW,
        },
      );

      this.logger.log(
        `📋 Daily metrics job queued: ${dayStr}` +
          `${contractorIds?.length ? ` (${contractorIds.length} contractors)` : ''}` +
          `${forceRecalculate ? ' [FORCE]' : ''}` +
          ` → Job ${job.id}`,
      );

      return job.id!;
    } catch (error) {
      this.logger.error(
        `❌ Failed to queue daily metrics job: ${error.message}`,
      );
      throw error;
    }
  }

  // ============================================================================
  // SESSION SUMMARIES
  // ============================================================================

  /**
   * Encola un job para procesar el resumen de una sesión específica.
   *
   * @param sessionId - ID de la sesión a procesar
   * @param contractorId - ID del contractor (para logging/tracking)
   * @returns ID del job creado
   */
  async addSessionSummaryJob(
    sessionId: string,
    contractorId: string,
  ): Promise<string> {
    try {
      // JobId determinista: misma sesión → mismo jobId → BullMQ ignora duplicado
      const jobId = `session-summary-${sessionId}`;

      const jobData: EtlJobData = {
        jobType: JobType.SESSION_SUMMARIES,
        requestedAt: new Date(),
        sessionId,
        contractorId,
      };

      const job = await this.sessionSummariesQueue.add(
        JobType.SESSION_SUMMARIES,
        jobData,
        {
          ...DEFAULT_JOB_OPTIONS,
          jobId,
          priority: JobPriority.NORMAL,
        },
      );

      this.logger.log(
        `📋 Session summary job queued: session=${sessionId} contractor=${contractorId} → Job ${job.id}`,
      );

      return job.id!;
    } catch (error) {
      this.logger.error(
        `❌ Failed to queue session summary job for session ${sessionId}: ${error.message}`,
      );
      throw error;
    }
  }

  // ============================================================================
  // FULL ETL ON SESSION CLOSE (orquestadora: process-events → daily-metrics → session-summaries)
  // ============================================================================

  /**
   * Encola un job que ejecuta la orquestadora completa para un contratista al cerrar sesión.
   * Cada cierre de sesión (cada trigger) crea un job nuevo: si usáramos solo sessionId como jobId,
   * BullMQ ignoraría los duplicados y solo se ejecutaría el ETL una vez por sesión; al tener varias
   * agent sessions en la misma sesión principal, los cierres posteriores no generarían nuevo ETL.
   *
   * @param sessionId - ID de la sesión cerrada
   * @param contractorId - ID del contratista
   * @returns ID del job creado
   */
  async addFullEtlOnSessionCloseJob(
    sessionId: string,
    contractorId: string,
  ): Promise<string> {
    try {
      const jobId = `session-etl-${sessionId}-${Date.now()}`;

      const jobData: EtlJobData = {
        jobType: JobType.FULL_ETL_ON_SESSION_CLOSE,
        requestedAt: new Date(),
        sessionId,
        contractorId,
      };

      const job = await this.sessionCloseQueue.add(
        JobType.FULL_ETL_ON_SESSION_CLOSE,
        jobData,
        {
          ...DEFAULT_JOB_OPTIONS,
          jobId,
          priority: JobPriority.NORMAL,
        },
      );

      this.logger.log(
        `📋 Full ETL on session close queued: session=${sessionId} contractor=${contractorId} → Job ${job.id}`,
      );

      return job.id!;
    } catch (error) {
      this.logger.error(
        `❌ Failed to queue full ETL on session close for session=${sessionId}: ${error.message}`,
      );
      throw error;
    }
  }

  // ============================================================================
  // GESTIÓN DE COLAS
  // ============================================================================

  /**
   * Obtiene estadísticas de las colas ETL
   */
  async getQueuesStats(): Promise<{
    dailyMetrics: {
      waiting: number;
      active: number;
      completed: number;
      failed: number;
    };
    sessionSummaries: {
      waiting: number;
      active: number;
      completed: number;
      failed: number;
    };
  }> {
    const [dmWaiting, dmActive, dmCompleted, dmFailed] = await Promise.all([
      this.dailyMetricsQueue.getWaitingCount(),
      this.dailyMetricsQueue.getActiveCount(),
      this.dailyMetricsQueue.getCompletedCount(),
      this.dailyMetricsQueue.getFailedCount(),
    ]);

    const [ssWaiting, ssActive, ssCompleted, ssFailed] = await Promise.all([
      this.sessionSummariesQueue.getWaitingCount(),
      this.sessionSummariesQueue.getActiveCount(),
      this.sessionSummariesQueue.getCompletedCount(),
      this.sessionSummariesQueue.getFailedCount(),
    ]);

    return {
      dailyMetrics: {
        waiting: dmWaiting,
        active: dmActive,
        completed: dmCompleted,
        failed: dmFailed,
      },
      sessionSummaries: {
        waiting: ssWaiting,
        active: ssActive,
        completed: ssCompleted,
        failed: ssFailed,
      },
    };
  }

  /**
   * Pausa ambas colas ETL
   */
  async pauseQueues(): Promise<void> {
    await Promise.all([
      this.dailyMetricsQueue.pause(),
      this.sessionSummariesQueue.pause(),
    ]);
    this.logger.warn('⏸️ ETL queues paused');
  }

  /**
   * Reanuda ambas colas ETL
   */
  async resumeQueues(): Promise<void> {
    await Promise.all([
      this.dailyMetricsQueue.resume(),
      this.sessionSummariesQueue.resume(),
    ]);
    this.logger.log('▶️ ETL queues resumed');
  }

  /**
   * Limpia jobs completados de ambas colas
   */
  async cleanCompletedJobs(olderThanMs: number = 86400000): Promise<void> {
    const [dmCleaned, ssCleaned] = await Promise.all([
      this.dailyMetricsQueue.clean(olderThanMs, 100, 'completed'),
      this.sessionSummariesQueue.clean(olderThanMs, 100, 'completed'),
    ]);
    this.logger.log(
      `🧹 Cleaned ${dmCleaned.length} daily-metrics + ${ssCleaned.length} session-summaries completed jobs`,
    );
  }
}
