import { Controller, Logger, Optional } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';

import { getMessagePattern, logError } from 'config';

import { EtlService } from '../etl/services/etl.service';
import { EtlQueueService } from '../queues/services/etl-queue.service';

/**
 * Payload emitido por USER_MS al cerrar la Session padre (`AdtSessionInterceptor`).
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
 * ✅ Cada cierre de sesión padre en USER_MS emite **un** `etl.session.trigger` (AdtSessionInterceptor).
 * ✅ BullMQ deduplica por `sessionId` ventana corta por si llegaran disparos duplicados (cola `full-etl-on-close`).
 *
 * Si USE_ETL_QUEUE=false, ejecuta la orquestación de forma inline como fallback
 * para no perder el ETL automático en producción.
 */
@Controller()
export class EtlTriggerListener {
  private readonly logger = new Logger(EtlTriggerListener.name);

  constructor(
    private readonly etlService: EtlService,
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
          `Running ETL inline for session=${sessionId}. ` +
          `Set USE_ETL_QUEUE=true in ADT_MS to use BullMQ on break/shift-end.`,
      );

      try {
        await this.etlService.runFullEtlForContractorOnSessionClose(
          contractorId,
          sessionId,
        );

        this.logger.log(
          `✅ [EtlTrigger] Full ETL on session close completed inline — ` +
            `session=${sessionId} contractor=${contractorId}`,
        );
      } catch (error) {
        logError(
          this.logger,
          `❌ [EtlTrigger] Failed to run inline ETL — session=${sessionId} contractor=${contractorId}`,
          error,
        );
      }

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
