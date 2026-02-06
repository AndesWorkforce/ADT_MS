import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';

import { QUEUE_NAMES } from 'config/bullmq.config';
import { envs } from 'config';

@Injectable()
export class InactivityScanQueueService implements OnModuleInit {
  private readonly logger = new Logger(InactivityScanQueueService.name);

  constructor(
    @InjectQueue(QUEUE_NAMES.INACTIVITY_SCAN)
    private readonly inactivityScanQueue: Queue,
  ) {}

  async onModuleInit() {
    if (envs.queues.useInactivityAlerts) {
      await this.startPeriodicScan();
      this.logger.log(
        `✅ Inactivity scan job registered (interval: ${envs.queues.inactivityScanIntervalMinutes} minutes)`,
      );
    }
  }

  /**
   * Inicia el escaneo periódico de inactividad
   */
  async startPeriodicScan(): Promise<void> {
    try {
      // Remover jobs repetibles anteriores si existen
      const repeatableJobs = await this.inactivityScanQueue.getRepeatableJobs();
      for (const job of repeatableJobs) {
        await this.inactivityScanQueue.removeRepeatableByKey(job.key);
      }

      // Crear nuevo job repetible
      await this.inactivityScanQueue.add(
        'scan',
        {
          scheduledAt: new Date().toISOString(),
        },
        {
          repeat: {
            every: envs.queues.inactivityScanIntervalMinutes * 60 * 1000,
          },
          removeOnComplete: {
            age: 3600, // Mantener logs por 1 hora
            count: 10,
          },
          removeOnFail: {
            age: 86400, // Mantener errores por 24 horas
            count: 20,
          },
        },
      );

      this.logger.log(
        `🔄 Periodic inactivity scan started (every ${envs.queues.inactivityScanIntervalMinutes} minutes)`,
      );
    } catch (error) {
      this.logger.error('❌ Error starting periodic scan:', error);
      throw error;
    }
  }

  /**
   * Detiene el escaneo periódico de inactividad
   */
  async stopPeriodicScan(): Promise<void> {
    try {
      const repeatableJobs = await this.inactivityScanQueue.getRepeatableJobs();
      for (const job of repeatableJobs) {
        await this.inactivityScanQueue.removeRepeatableByKey(job.key);
      }
      this.logger.log('🛑 Periodic inactivity scan stopped');
    } catch (error) {
      this.logger.error('❌ Error stopping periodic scan:', error);
      throw error;
    }
  }

  /**
   * Obtiene estadísticas de la cola de escaneo
   */
  async getQueueStats() {
    const [waiting, active, completed, failed, delayed] = await Promise.all([
      this.inactivityScanQueue.getWaitingCount(),
      this.inactivityScanQueue.getActiveCount(),
      this.inactivityScanQueue.getCompletedCount(),
      this.inactivityScanQueue.getFailedCount(),
      this.inactivityScanQueue.getDelayedCount(),
    ]);

    return {
      waiting,
      active,
      completed,
      failed,
      delayed,
    };
  }
}
