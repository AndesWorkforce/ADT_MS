import { Injectable, Logger } from '@nestjs/common';

import { envs, formatDateInTZ, toDateTZ } from 'config';
import { ClickHouseService } from '../../clickhouse/clickhouse.service';
import { RedisKeys, RedisService } from '../../redis';

/**
 * Servicio para obtener métricas diarias pre-calculadas desde contractor_daily_metrics.
 * Estas métricas son generadas por el ETL y almacenadas en ClickHouse.
 */
@Injectable()
export class DailyMetricsService {
  private readonly logger = new Logger(DailyMetricsService.name);

  constructor(
    private readonly clickHouseService: ClickHouseService,
    private readonly redisService: RedisService,
  ) {}

  /**
   * Obtiene métricas diarias de un contractor desde la tabla pre-calculada.
   * El caché será manejado por Redis.
   *
   * @param contractorId ID del contractor
   * @param days Número de días hacia atrás
   * @returns Array de métricas diarias
   */
  async getDailyMetrics(contractorId: string, days: number): Promise<any[]> {
    const cacheKey = RedisKeys.dailyMetricsByContractor(contractorId, days);

    return this.redisService.getOrSet(
      cacheKey,
      async () => {
        const query = `
          SELECT 
            contractor_id,
            workday,
            total_beats,
            active_beats,
            idle_beats,
            active_percentage,
            total_keyboard_inputs,
            total_mouse_clicks,
            avg_keyboard_per_min,
            avg_mouse_per_min,
            total_session_time_seconds,
            effective_work_seconds,
            productivity_score,
            app_usage,
            browser_usage,
            created_at
          FROM contractor_daily_metrics FINAL
          WHERE contractor_id = '${contractorId}'
            AND workday >= ${toDateTZ(`now() - INTERVAL ${days} DAY`)}
          ORDER BY workday DESC
        `;

        const results = await this.clickHouseService.query(query);

        if (results.length === 0) {
          this.logger.warn(
            `⚠️ No daily metrics found for contractor ${contractorId} in the last ${days} days. ` +
              `The table contractor_daily_metrics is empty. ` +
              `You need to run the ETL first: GET /adt/etl/process-daily-metrics?from=YYYY-MM-DD&to=YYYY-MM-DD`,
          );
        }

        return this.formatResults(results);
      },
      envs.redis.ttl,
    );
  }

  /**
   * Formatea los resultados para consistencia en la respuesta.
   * Convierte workday a string YYYY-MM-DD y Maps a Arrays.
   */
  private formatResults(results: any[]): any[] {
    return results.map((row: any) => {
      // Convertir app_usage Map a Array
      let appUsage: Array<{ appName: string; seconds: number }> = [];
      if (row.app_usage && typeof row.app_usage === 'object') {
        appUsage = Object.entries(row.app_usage).map(([appName, seconds]) => ({
          appName,
          seconds: Number(seconds) || 0,
        }));
      }

      // Convertir browser_usage Map a Array
      let browserUsage: Array<{ domain: string; seconds: number }> = [];
      if (row.browser_usage && typeof row.browser_usage === 'object') {
        browserUsage = Object.entries(row.browser_usage).map(
          ([domain, seconds]) => ({
            domain,
            seconds: Number(seconds) || 0,
          }),
        );
      }

      return {
        ...row,
        workday:
          typeof row.workday === 'string'
            ? row.workday.split('T')[0]
            : row.workday instanceof Date
              ? formatDateInTZ(row.workday)
              : row.workday,
        app_usage: appUsage,
        browser_usage: browserUsage,
      };
    });
  }
}
