import { Injectable, Logger } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';

import { QUEUE_NAMES, DEFAULT_JOB_OPTIONS } from 'config/bullmq.config';

import { SaveEventJobData, JobType, JobPriority } from '../types';

@Injectable()
export class EventQueueService {
  private readonly logger = new Logger(EventQueueService.name);

  constructor(
    @InjectQueue(QUEUE_NAMES.EVENTS)
    private readonly eventsQueue: Queue,
  ) {}

  async addEvent(
    eventData: Omit<SaveEventJobData, 'jobType' | 'requestedAt'>,
    priority: JobPriority = JobPriority.NORMAL,
  ): Promise<string> {
    try {
      const jobData: SaveEventJobData = {
        ...eventData,
        jobType: JobType.SAVE_EVENT,
        requestedAt: new Date(),
      };

      const job = await this.eventsQueue.add(JobType.SAVE_EVENT, jobData, {
        ...DEFAULT_JOB_OPTIONS,
        priority,
        // Remover trabajos completados rápidamente para eventos
        removeOnComplete: {
          age: 3600, // 1 hora
          count: 100,
        },
      });

      this.logger.debug(`Event queued: ${eventData.event_id} (Job: ${job.id})`);

      return job.id!;
    } catch (error) {
      this.logger.error(
        `Failed to queue event ${eventData.event_id}: ${error.message}`,
      );
      throw error;
    }
  }

  async addEventsBatch(
    eventsData: Array<Omit<SaveEventJobData, 'jobType' | 'requestedAt'>>,
    priority: JobPriority = JobPriority.NORMAL,
  ): Promise<string[]> {
    try {
      const jobs = eventsData.map((eventData) => ({
        name: JobType.SAVE_EVENT,
        data: {
          ...eventData,
          jobType: JobType.SAVE_EVENT,
          requestedAt: new Date(),
        } as SaveEventJobData,
        opts: {
          ...DEFAULT_JOB_OPTIONS,
          priority,
          removeOnComplete: {
            age: 3600,
            count: 100,
          },
        },
      }));

      const addedJobs = await this.eventsQueue.addBulk(jobs);
      const jobIds = addedJobs.map((job) => job.id!);

      this.logger.log(`Queued ${eventsData.length} events in batch`);

      return jobIds;
    } catch (error) {
      this.logger.error(
        `Failed to queue ${eventsData.length} events in batch: ${error.message}`,
      );
      throw error;
    }
  }

  async getQueueStats(): Promise<{
    waiting: number;
    active: number;
    completed: number;
    failed: number;
  }> {
    const [waiting, active, completed, failed] = await Promise.all([
      this.eventsQueue.getWaitingCount(),
      this.eventsQueue.getActiveCount(),
      this.eventsQueue.getCompletedCount(),
      this.eventsQueue.getFailedCount(),
    ]);

    return { waiting, active, completed, failed };
  }

  async pauseQueue(): Promise<void> {
    await this.eventsQueue.pause();
    this.logger.warn('Events queue paused');
  }

  async resumeQueue(): Promise<void> {
    await this.eventsQueue.resume();
    this.logger.log('Events queue resumed');
  }

  async cleanCompletedJobs(olderThanMs: number = 3600000): Promise<void> {
    const cleaned = await this.eventsQueue.clean(olderThanMs, 100, 'completed');
    this.logger.log(
      `Cleaned ${cleaned.length} completed jobs from events queue`,
    );
  }
}
