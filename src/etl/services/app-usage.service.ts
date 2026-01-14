import { Injectable, Logger } from '@nestjs/common';

import { envs } from 'config';
import { ClickHouseService } from '../../clickhouse/clickhouse.service';
import { RedisKeys, RedisService } from '../../redis';

/**
 * Servicio para obtener uso de aplicaciones de un contractor.
 * Consulta desde la tabla pre-calculada app_usage_summary (poblada por mv_app_usage_summary).
 */
@Injectable()
export class AppUsageService {
  private readonly logger = new Logger(AppUsageService.name);

  constructor(
    private readonly clickHouseService: ClickHouseService,
    private readonly redisService: RedisService,
  ) {}

  /**
   * Obtiene uso de aplicaciones de un contractor desde la tabla pre-calculada.
   * El caché será manejado por Redis.
   *
   * @param contractorId ID del contractor
   * @param from Fecha de inicio (opcional)
   * @param to Fecha de fin (opcional)
   * @param days Días hacia atrás (opcional)
   * @returns Array de uso de aplicaciones agrupado por día y app
   */
  async getAppUsage(
    contractorId: string,
    from?: string,
    to?: string,
    days?: number,
  ): Promise<any[]> {
    const cacheKey = RedisKeys.appUsageByContractor(
      contractorId,
      from,
      to,
      days,
    );

    return this.redisService.getOrSet(
      cacheKey,
      async () => {
        let where = `contractor_id = '${contractorId}'`;

        if (from) {
          const fromDay = (
            from.includes('T') ? from.split('T')[0] : from
          ).trim();
          where += ` AND workday >= toDate('${fromDay}')`;
        }

        if (to) {
          const toDay = (to.includes('T') ? to.split('T')[0] : to).trim();
          where += ` AND workday <= toDate('${toDay}')`;
        }

        // Fallback para compatibilidad: si no hay from/to pero sí days, usar days
        if (!from && !to && typeof days === 'number' && Number.isFinite(days)) {
          where += ` AND workday >= today() - ${days}`;
        }

        const query = `
          SELECT 
            contractor_id,
            app_name,
            workday,
            sum(active_beats) AS active_beats,
            max(created_at) AS created_at
          FROM app_usage_summary
          WHERE ${where}
          GROUP BY contractor_id, app_name, workday
          ORDER BY workday DESC, active_beats DESC
        `;

        return await this.clickHouseService.query(query);
      },
      envs.redis.ttl,
    );
  }
}
