import { Injectable, Logger } from '@nestjs/common';

import { ClickHouseService } from '../../clickhouse/clickhouse.service';
import {
  AppUsageData,
  BrowserUsageData,
} from '../transformers/activity-to-daily-metrics.transformer';

/**
 * Servicio compartido para obtener datos de uso de aplicaciones y navegador.
 * Centraliza la lógica de queries para evitar duplicación entre EtlService y RealtimeMetricsService.
 */
@Injectable()
export class UsageDataService {
  private readonly logger = new Logger(UsageDataService.name);

  constructor(private readonly clickHouseService: ClickHouseService) {}

  /**
   * Obtiene datos de AppUsage para un contractor y día específico.
   *
   * @param contractorId ID del contractor
   * @param workday Fecha del día
   * @returns Array de AppUsageData con {appName, seconds}
   */
  async getAppUsageForDay(
    contractorId: string,
    workday: Date,
  ): Promise<AppUsageData[]> {
    const workdayStr = workday.toISOString().split('T')[0];

    try {
      const query = `
        SELECT 
          JSONExtractString(payload, 'AppUsage') AS app_usage_json,
          timestamp
        FROM events_raw
        WHERE contractor_id = '${contractorId}'
          AND toDate(timestamp) = '${workdayStr}'
          AND JSONHas(payload, 'AppUsage')
        ORDER BY timestamp
      `;

      const events = await this.clickHouseService.query<any>(query);

      const appUsageMap: Record<string, number> = {};

      for (const event of events) {
        try {
          const appUsageObj = JSON.parse(event.app_usage_json || '{}');
          for (const [appName, seconds] of Object.entries(appUsageObj)) {
            if (typeof seconds === 'number') {
              const safe = seconds < 0 ? 0 : seconds;
              appUsageMap[appName] = (appUsageMap[appName] || 0) + safe;
            }
          }
        } catch {
          // Ignorar errores de parsing
        }
      }

      return Object.entries(appUsageMap)
        .map(([appName, seconds]) => ({
          appName,
          seconds: seconds < 0 ? 0 : seconds,
        }))
        .filter((u) => u.seconds > 0);
    } catch (error) {
      this.logger.warn(
        `Error getting AppUsage for day: ${error.message}. Returning empty array.`,
      );
      return [];
    }
  }

  /**
   * Obtiene datos de Browser para un contractor y día específico.
   *
   * @param contractorId ID del contractor
   * @param workday Fecha del día
   * @returns Array de BrowserUsageData con {domain, seconds}
   */
  async getBrowserUsageForDay(
    contractorId: string,
    workday: Date,
  ): Promise<BrowserUsageData[]> {
    const workdayStr = workday.toISOString().split('T')[0];

    try {
      const query = `
        SELECT 
          JSONExtractString(payload, 'browser') AS browser_json,
          timestamp
        FROM events_raw
        WHERE contractor_id = '${contractorId}'
          AND toDate(timestamp) = '${workdayStr}'
          AND JSONHas(payload, 'browser')
        ORDER BY timestamp
      `;

      const events = await this.clickHouseService.query<any>(query);

      const browserUsageMap: Record<string, number> = {};

      for (const event of events) {
        try {
          const browserObj = JSON.parse(event.browser_json || '{}');
          for (const [domain, seconds] of Object.entries(browserObj)) {
            if (typeof seconds === 'number') {
              const safe = seconds < 0 ? 0 : seconds;
              browserUsageMap[domain] = (browserUsageMap[domain] || 0) + safe;
            }
          }
        } catch {
          // Ignorar errores de parsing
        }
      }

      return Object.entries(browserUsageMap)
        .map(([domain, seconds]) => ({
          domain,
          seconds: seconds < 0 ? 0 : seconds,
        }))
        .filter((u) => u.seconds > 0);
    } catch (error) {
      this.logger.warn(
        `Error getting Browser usage for day: ${error.message}. Returning empty array.`,
      );
      return [];
    }
  }

  /**
   * Obtiene datos de AppUsage para un contractor en un rango de fechas.
   * Optimizado para evitar timeouts con grandes volúmenes de datos.
   *
   * @param contractorId ID del contractor
   * @param fromDate Fecha de inicio
   * @param toDate Fecha de fin
   * @param limit Límite de registros (opcional, default: 100000 para rangos > 7 días)
   * @returns Array de AppUsageData con {appName, seconds}
   */
  async getAppUsageForDateRange(
    contractorId: string,
    fromDate: Date,
    toDate: Date,
    limit?: number,
  ): Promise<AppUsageData[]> {
    const fromStr = fromDate.toISOString().split('T')[0];
    const toStr = toDate.toISOString().split('T')[0];

    // Calcular si usar LIMIT basado en el rango
    const daysDiff =
      Math.ceil(
        (toDate.getTime() - fromDate.getTime()) / (1000 * 60 * 60 * 24),
      ) + 1;
    const useLimit = limit ?? (daysDiff > 7 ? 100000 : undefined);

    try {
      const query = `
        SELECT 
          JSONExtractString(payload, 'AppUsage') AS app_usage_json,
          timestamp
        FROM events_raw
        WHERE contractor_id = '${contractorId}'
          AND toDate(timestamp) >= '${fromStr}'
          AND toDate(timestamp) <= '${toStr}'
          AND JSONHas(payload, 'AppUsage')
        ORDER BY timestamp
        ${useLimit ? `LIMIT ${useLimit}` : ''}
      `;

      const events = await this.clickHouseService.query<any>(query);

      const appUsageMap: Record<string, number> = {};

      for (const event of events) {
        try {
          const appUsageObj = JSON.parse(event.app_usage_json || '{}');
          for (const [appName, seconds] of Object.entries(appUsageObj)) {
            if (typeof seconds === 'number') {
              const safe = seconds < 0 ? 0 : seconds;
              appUsageMap[appName] = (appUsageMap[appName] || 0) + safe;
            }
          }
        } catch {
          // Ignorar errores de parsing
        }
      }

      return Object.entries(appUsageMap)
        .map(([appName, seconds]) => ({
          appName,
          seconds: seconds < 0 ? 0 : seconds,
        }))
        .filter((u) => u.seconds > 0);
    } catch (error) {
      this.logger.warn(
        `Error getting AppUsage for date range: ${error.message}. Returning empty array.`,
      );
      return [];
    }
  }

  /**
   * Obtiene datos de Browser para un contractor en un rango de fechas.
   * Optimizado para evitar timeouts con grandes volúmenes de datos.
   *
   * @param contractorId ID del contractor
   * @param fromDate Fecha de inicio
   * @param toDate Fecha de fin
   * @param limit Límite de registros (opcional, default: 100000 para rangos > 7 días)
   * @returns Array de BrowserUsageData con {domain, seconds}
   */
  async getBrowserUsageForDateRange(
    contractorId: string,
    fromDate: Date,
    toDate: Date,
    limit?: number,
  ): Promise<BrowserUsageData[]> {
    const fromStr = fromDate.toISOString().split('T')[0];
    const toStr = toDate.toISOString().split('T')[0];

    // Calcular si usar LIMIT basado en el rango
    const daysDiff =
      Math.ceil(
        (toDate.getTime() - fromDate.getTime()) / (1000 * 60 * 60 * 24),
      ) + 1;
    const useLimit = limit ?? (daysDiff > 7 ? 100000 : undefined);

    try {
      const query = `
        SELECT 
          JSONExtractString(payload, 'browser') AS browser_json,
          timestamp
        FROM events_raw
        WHERE contractor_id = '${contractorId}'
          AND toDate(timestamp) >= '${fromStr}'
          AND toDate(timestamp) <= '${toStr}'
          AND JSONHas(payload, 'browser')
        ORDER BY timestamp
        ${useLimit ? `LIMIT ${useLimit}` : ''}
      `;

      const events = await this.clickHouseService.query<any>(query);

      const browserUsageMap: Record<string, number> = {};

      for (const event of events) {
        try {
          const browserObj = JSON.parse(event.browser_json || '{}');
          for (const [domain, seconds] of Object.entries(browserObj)) {
            if (typeof seconds === 'number') {
              const safe = seconds < 0 ? 0 : seconds;
              browserUsageMap[domain] = (browserUsageMap[domain] || 0) + safe;
            }
          }
        } catch {
          // Ignorar errores de parsing
        }
      }

      return Object.entries(browserUsageMap)
        .map(([domain, seconds]) => ({
          domain,
          seconds: seconds < 0 ? 0 : seconds,
        }))
        .filter((u) => u.seconds > 0);
    } catch (error) {
      this.logger.warn(
        `Error getting Browser usage for date range: ${error.message}. Returning empty array.`,
      );
      return [];
    }
  }

  /**
   * Obtiene datos de AppUsage agregados usando SQL nativo (más eficiente).
   * Útil para dashboards que necesitan datos resumidos sin cargar en memoria.
   *
   * @param contractorId ID del contractor
   * @param fromDate Fecha de inicio
   * @param toDate Fecha de fin
   * @returns Array de AppUsageData ordenado por segundos DESC
   */
  async getAppUsageAggregated(
    contractorId: string,
    fromDate: Date,
    toDate: Date,
  ): Promise<AppUsageData[]> {
    const fromStr = fromDate.toISOString().split('T')[0];
    const toStr = toDate.toISOString().split('T')[0];

    try {
      const query = `
        SELECT 
          app_name AS appName,
          sum(JSONExtractFloat(payload, 'AppUsage', app_name)) AS seconds
        FROM events_raw
        ARRAY JOIN JSONExtractKeys(payload, 'AppUsage') AS app_name
        WHERE contractor_id = '${contractorId}'
          AND toDate(timestamp) >= '${fromStr}'
          AND toDate(timestamp) <= '${toStr}'
          AND JSONHas(payload, 'AppUsage')
        GROUP BY app_name
        HAVING seconds > 0
        ORDER BY seconds DESC
      `;

      const results = await this.clickHouseService.query<{
        appName: string;
        seconds: number;
      }>(query);

      return results.map((r) => ({
        appName: r.appName,
        seconds: Number(r.seconds) || 0,
      }));
    } catch (error) {
      this.logger.warn(
        `Error getting aggregated AppUsage: ${error.message}. Returning empty array.`,
      );
      return [];
    }
  }

  /**
   * Obtiene datos de Browser agregados usando SQL nativo (más eficiente).
   * Útil para dashboards que necesitan datos resumidos sin cargar en memoria.
   *
   * @param contractorId ID del contractor
   * @param fromDate Fecha de inicio
   * @param toDate Fecha de fin
   * @returns Array de BrowserUsageData ordenado por segundos DESC
   */
  async getBrowserUsageAggregated(
    contractorId: string,
    fromDate: Date,
    toDate: Date,
  ): Promise<BrowserUsageData[]> {
    const fromStr = fromDate.toISOString().split('T')[0];
    const toStr = toDate.toISOString().split('T')[0];

    try {
      const query = `
        SELECT 
          domain,
          sum(JSONExtractFloat(payload, 'browser', domain)) AS seconds
        FROM events_raw
        ARRAY JOIN JSONExtractKeys(payload, 'browser') AS domain
        WHERE contractor_id = '${contractorId}'
          AND toDate(timestamp) >= '${fromStr}'
          AND toDate(timestamp) <= '${toStr}'
          AND JSONHas(payload, 'browser')
        GROUP BY domain
        HAVING seconds > 0
        ORDER BY seconds DESC
      `;

      const results = await this.clickHouseService.query<{
        domain: string;
        seconds: number;
      }>(query);

      return results.map((r) => ({
        domain: r.domain,
        seconds: Number(r.seconds) || 0,
      }));
    } catch (error) {
      this.logger.warn(
        `Error getting aggregated Browser usage: ${error.message}. Returning empty array.`,
      );
      return [];
    }
  }
}
