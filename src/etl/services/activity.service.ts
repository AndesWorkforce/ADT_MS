import { Injectable, Logger } from '@nestjs/common';

import { envs } from 'config';
import { ClickHouseService } from '../../clickhouse/clickhouse.service';
import { RedisKeys, RedisService } from '../../redis';

/**
 * Servicio para obtener actividad detallada (beats de 15s) desde contractor_activity_15s.
 */
@Injectable()
export class ActivityService {
  private readonly logger = new Logger(ActivityService.name);

  constructor(
    private readonly clickHouseService: ClickHouseService,
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
        let query = `
          SELECT 
            contractor_id,
            agent_id,
            session_id,
            agent_session_id,
            beat_timestamp,
            is_idle,
            keyboard_count,
            mouse_clicks,
            workday
          FROM contractor_activity_15s
          WHERE contractor_id = '${contractorId}'
        `;

        if (from) {
          const fromDate = this.formatDateForClickHouse(from);
          if (
            typeof from === 'string' &&
            (!from.includes('T') || from.includes('T00:00:00'))
          ) {
            const dateOnly = fromDate.split(' ')[0];
            query += ` AND beat_timestamp >= '${dateOnly} 00:00:00'`;
          } else {
            query += ` AND beat_timestamp >= '${fromDate}'`;
          }
        }

        if (to) {
          const toDate = this.formatDateForClickHouse(to);
          if (
            typeof to === 'string' &&
            (!to.includes('T') || to.includes('T00:00:00'))
          ) {
            const dateOnly = toDate.split(' ')[0];
            query += ` AND beat_timestamp <= '${dateOnly} 23:59:59'`;
          } else {
            query += ` AND beat_timestamp <= '${toDate}'`;
          }
        }

        query += ` ORDER BY beat_timestamp DESC LIMIT ${limit}`;

        return await this.clickHouseService.query(query);
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

    const year = dateObj.getUTCFullYear();
    const month = String(dateObj.getUTCMonth() + 1).padStart(2, '0');
    const day = String(dateObj.getUTCDate()).padStart(2, '0');
    const hours = String(dateObj.getUTCHours()).padStart(2, '0');
    const minutes = String(dateObj.getUTCMinutes()).padStart(2, '0');
    const seconds = String(dateObj.getUTCSeconds()).padStart(2, '0');

    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
  }
}
