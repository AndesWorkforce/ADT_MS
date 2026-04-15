import { Injectable, Logger } from '@nestjs/common';

import { envs } from 'config';
import { RedisKeys, RedisService } from '../../redis';
import { ActivityRepository } from './activity-repository.service';

/**
 * Servicio para obtener actividad detallada (beats de 15s) desde contractor_activity_15s.
 */
@Injectable()
export class ActivityService {
  private readonly logger = new Logger(ActivityService.name);

  constructor(
    private readonly activityRepository: ActivityRepository,
    private readonly redisService: RedisService,
  ) {}

  /**
   * Obtiene actividad detallada (beats de 15s) de un contractor.
   * El caché será manejado por Redis.
   *
   * @param contractorId ID del contractor
   * @param from Fecha de inicio (opcional)
   * @param to Fecha de fin (opcional)
   * @param limit Límite de registros (default: 1000)
   * @returns Array de beats de actividad
   */
  async getActivity(
    contractorId: string,
    from?: string,
    to?: string,
    limit: number = 1000,
  ): Promise<any[]> {
    const cacheKey = RedisKeys.activityByContractor(
      contractorId,
      from,
      to,
      limit,
    );

    return this.redisService.getOrSet(
      cacheKey,
      async () => {
        const fromDate = from ? this.formatDateForClickHouse(from) : undefined;
        const toDate = to ? this.formatDateForClickHouse(to) : undefined;

        return this.activityRepository.getBeatsForContractor(
          contractorId,
          fromDate,
          toDate,
          limit,
        );
      },
      envs.redis.ttl,
    );
  }

  /**
   * Formatea una fecha (ISO string o Date) al formato DateTime de ClickHouse.
   * Formato esperado: 'YYYY-MM-DD HH:MM:SS'
   */
  private formatDateForClickHouse(date: string | Date): string {
    let dateObj: Date;

    if (typeof date === 'string') {
      dateObj = new Date(date);
    } else {
      dateObj = date;
    }

    if (isNaN(dateObj.getTime())) {
      throw new Error(`Invalid date: ${date}`);
    }

    return dateObj.toISOString().replace('T', ' ').slice(0, 19);
  }
}
