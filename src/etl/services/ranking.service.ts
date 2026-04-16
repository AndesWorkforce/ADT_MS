import { Injectable, Logger } from '@nestjs/common';

import { envs, toDateTZ } from 'config';
import { ClickHouseService } from '../../clickhouse/clickhouse.service';
import { RedisKeys, RedisService } from '../../redis';

/**
 * Servicio para obtener rankings de productividad desde contractor_daily_metrics.
 */
@Injectable()
export class RankingService {
  private readonly logger = new Logger(RankingService.name);

  constructor(
    private readonly clickHouseService: ClickHouseService,
    private readonly redisService: RedisService,
  ) {}

  /**
   * Obtiene ranking de productividad por día desde la tabla pre-calculada.
   * El caché será manejado por Redis.
   *
   * @param workday Fecha del día (opcional, default: ayer)
   * @param limit Número de resultados (default: 10)
   * @returns Array de contractors ordenados por productivity_score
   */
  async getRanking(workday?: string, limit: number = 10): Promise<any[]> {
    const cacheKey = RedisKeys.ranking(workday, limit);

    return this.redisService.getOrSet(
      cacheKey,
      async () => {
        let query = `
          SELECT 
            contractor_id,
            workday,
            total_beats,
            active_beats,
            active_percentage,
            productivity_score,
            total_keyboard_inputs,
            total_mouse_clicks,
            effective_work_seconds
          FROM contractor_daily_metrics FINAL
          WHERE 1=1
        `;

        if (workday) {
          query += ` AND workday = '${workday.split('T')[0]}'`;
        } else {
          query += ` AND workday = ${toDateTZ('now() - INTERVAL 1 DAY')}`;
        }

        query += ` ORDER BY productivity_score DESC LIMIT ${limit}`;

        return await this.clickHouseService.query(query);
      },
      envs.redis.ttl,
    );
  }
}
