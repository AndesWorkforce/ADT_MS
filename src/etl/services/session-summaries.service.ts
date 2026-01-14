import { Injectable, Logger } from '@nestjs/common';

import { envs } from 'config';
import { ClickHouseService } from '../../clickhouse/clickhouse.service';
import { RedisKeys, RedisService } from '../../redis';

/**
 * Servicio para obtener resúmenes de sesión pre-calculados desde session_summary.
 * Estas métricas son generadas por el ETL y almacenadas en ClickHouse.
 */
@Injectable()
export class SessionSummariesService {
  private readonly logger = new Logger(SessionSummariesService.name);

  constructor(
    private readonly clickHouseService: ClickHouseService,
    private readonly redisService: RedisService,
  ) {}

  /**
   * Obtiene resúmenes de sesión de un contractor.
   * Puede filtrar por rango de fechas (from/to) o por días hacia atrás (days).
   * El caché será manejado por Redis.
   *
   * @param contractorId ID del contractor
   * @param from Fecha de inicio (opcional)
   * @param to Fecha de fin (opcional)
   * @param days Días hacia atrás (default: 30)
   * @returns Array de resúmenes de sesión
   */
  async getSessionSummaries(
    contractorId: string,
    from?: string,
    to?: string,
    days: number = 30,
  ): Promise<any[]> {
    const cacheKey = RedisKeys.sessionSummariesByContractor(
      contractorId,
      from,
      to,
      days,
    );

    return this.redisService.getOrSet(
      cacheKey,
      async () => {
        // Construir filtro de fecha
        let dateFilter: string;
        if (from && to) {
          const fromDate = from.split('T')[0];
          const toDate = to.split('T')[0];
          dateFilter = `toDate(session_start) >= '${fromDate}' AND toDate(session_start) <= '${toDate}'`;
        } else {
          dateFilter = `toDate(session_start) >= today() - ${days}`;
        }

        const query = `
          SELECT 
            session_id,
            contractor_id,
            session_start,
            session_end,
            total_seconds,
            active_seconds,
            idle_seconds,
            productivity_score,
            created_at
          FROM session_summary
          WHERE contractor_id = '${contractorId}'
            AND ${dateFilter}
          ORDER BY session_start DESC
        `;

        return await this.clickHouseService.query(query);
      },
      envs.redis.ttl,
    );
  }
}
