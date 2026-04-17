import { Injectable, Logger } from '@nestjs/common';

import {
  envs,
  instantToClickHouseUtcDateTime,
  OPERATIONAL_TIMEZONE,
  wallTimeToUtcInOperationalZone,
} from 'config';
import { DateTime } from 'luxon';
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
        const fromDate = from
          ? this.formatDateForClickHouse(from, 'start')
          : undefined;
        const toDate = to ? this.formatDateForClickHouse(to, 'end') : undefined;

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
   * Formato ClickHouse `YYYY-MM-DD HH:MM:SS` en UTC para filtros sobre timestamps almacenados en UTC.
   * Días `YYYY-MM-DD` sin hora = inicio/fin de día en la zona operativa (America/New_York).
   */
  private formatDateForClickHouse(
    date: string | Date,
    edge: 'start' | 'end',
  ): string {
    if (typeof date === 'string') {
      const t = date.trim();
      if (/^\d{4}-\d{2}-\d{2}$/.test(t)) {
        const d =
          edge === 'start'
            ? wallTimeToUtcInOperationalZone(t, 0, 0, 0)
            : DateTime.fromISO(t, { zone: OPERATIONAL_TIMEZONE })
                .endOf('day')
                .toJSDate();
        return instantToClickHouseUtcDateTime(d);
      }
    }

    const base = typeof date === 'string' ? new Date(date) : date;
    if (isNaN(base.getTime())) {
      throw new Error(`Invalid date: ${date}`);
    }

    return instantToClickHouseUtcDateTime(base);
  }
}
