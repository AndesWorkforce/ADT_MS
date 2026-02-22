import { Controller, Logger, Optional } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';

import { getMessagePattern, logError } from 'config';

import { EtlQueueService } from '../queues/services/etl-queue.service';

/**
 * Payload emitido por EVENTS_MS cuando una sesión finaliza
 * (por timeout de inactividad o fin de turno explícito).
 */
interface SessionEtlTriggerPayload {
  sessionId: string;
  contractorId: string;
  triggeredAt: string;
  triggerReason: 'timeout' | 'explicit';
}

/**
 * Listener NATS que recibe el evento `etl.session.trigger` y encola
 * el procesamiento ETL de resumen de sesión en BullMQ.
 *
 * Flujo:
 *   USER_MS (al cerrar sesión)
 *     → NATS event: etl.session.trigger
 *       → EtlTriggerListener (aquí)
 *         → EtlQueueService.addFullEtlOnSessionCloseJob()
 *           → SessionCloseEtlProcessor → EtlService.runFullEtlForContractorOnSessionClose()
 *             → process-events → process-daily-metrics → process-session-summaries (solo ese contratista)
 *
 * Requisitos satisfechos:
 * ✅ ETL se invoca automáticamente al cerrar una sesión
 * ✅ Incluye sessionId y contractorId
 * ✅ Logs claros de recepción / éxito / error
 * ✅ Lógica ETL desacoplada de EVENTS_MS (reside solo en ADT_MS)
 * ✅ Idempotente: BullMQ descarta el job si ya existe (jobId determinista)
 *
 * Nota: requiere USE_ETL_QUEUE=true en la configuración de ADT_MS.
 * Si la cola no está habilitada, se registra una advertencia pero no se lanza error.
 */
@Controller()
export class EtlTriggerListener {
  private readonly logger = new Logger(EtlTriggerListener.name);

  constructor(
    // Opcional: solo disponible cuando USE_ETL_QUEUE=true
    @Optional() private readonly etlQueueService?: EtlQueueService,
  ) {}

  /**
   * Escucha el evento de fin de sesión y encola el ETL de resumen.
   */
  @EventPattern(getMessagePattern('etl.session.trigger'))
  async handleSessionEtlTrigger(
    @Payload() payload: SessionEtlTriggerPayload,
  ): Promise<void> {
    const { sessionId, contractorId, triggeredAt, triggerReason } = payload;

    this.logger.log(
      `📥 [EtlTrigger] Received session end event — ` +
        `session=${sessionId} contractor=${contractorId} ` +
        `reason=${triggerReason} triggeredAt=${triggeredAt}`,
    );

    if (!this.etlQueueService) {
      this.logger.warn(
        `⚠️ [EtlTrigger] EtlQueueService not available (USE_ETL_QUEUE=false). ` +
          `No ETL job will be queued for session=${sessionId}. ` +
          `Set USE_ETL_QUEUE=true in ADT_MS to enable automatic session ETL on break/shift-end.`,
      );
      return;
    }

    try {
      const jobId = await this.etlQueueService.addFullEtlOnSessionCloseJob(
        sessionId,
        contractorId,
      );

      this.logger.log(
        `✅ [EtlTrigger] Full ETL on session close job enqueued — ` +
          `session=${sessionId} contractor=${contractorId} jobId=${jobId}`,
      );
    } catch (error) {
      logError(
        this.logger,
        `❌ [EtlTrigger] Failed to enqueue session summary — session=${sessionId} contractor=${contractorId}`,
        error,
      );
      // No relanzamos: es fire-and-forget. BullMQ maneará reintentos si el job fue creado.
    }
  }
}
