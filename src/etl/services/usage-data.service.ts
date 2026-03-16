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
   * Obtiene los tipos de aplicaciones desde apps_dimension.
   * Método público reutilizable para evitar duplicación de código.
   *
   * @param appNames Array de nombres de aplicaciones
   * @returns Map con appName -> type
   */
  async getAppTypesFromDimension(
    appNames: string[],
  ): Promise<Record<string, string>> {
    const typeMap: Record<string, string> = {};

    if (appNames.length === 0) {
      return typeMap;
    }

    try {
      const appNamesList = appNames
        .map((name) => `'${name.replace(/'/g, "''")}'`)
        .join(',');
      const typeQuery = `
        SELECT name, type
        FROM apps_dimension
        WHERE name IN (${appNamesList})
      `;
      const typeResults = await this.clickHouseService.query<{
        name: string;
        type: string;
      }>(typeQuery);

      typeResults.forEach((row) => {
        typeMap[row.name] = row.type;
      });
    } catch (error) {
      this.logger.warn(
        `Error getting app types from apps_dimension: ${error.message}. Continuing without types.`,
      );
    }

    return typeMap;
  }

  /**
   * Obtiene datos de AppUsage para un contractor y día específico.
   *
   * @param contractorId ID del contractor
   * @param workday Fecha del día
   * @param agentId ID del agente (opcional, para filtrar por agente específico)
   * @returns Array de AppUsageData con {appName, seconds}
   */
  async getAppUsageForDay(
    contractorId: string,
    workday: Date,
    agentId?: string,
  ): Promise<AppUsageData[]> {
    const workdayStr = workday.toLocaleDateString('en-CA');
    const agentFilter = agentId ? `AND agent_id = '${agentId}'` : '';

    try {
      const query = `
        SELECT 
          JSONExtractString(payload, 'AppUsage') AS app_usage_json,
          timestamp
        FROM events_raw
        WHERE contractor_id = '${contractorId}'
          AND toDate(timestamp) = '${workdayStr}'
          AND JSONHas(payload, 'AppUsage')
          ${agentFilter}
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

      // Obtener tipos desde apps_dimension
      const appNames = Object.keys(appUsageMap);
      const typeMap = await this.getAppTypesFromDimension(appNames);

      return Object.entries(appUsageMap)
        .map(([appName, seconds]) => ({
          appName,
          seconds: seconds < 0 ? 0 : seconds,
          type: typeMap[appName] || undefined,
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
   * @param agentId ID del agente (opcional, para filtrar por agente específico)
   * @returns Array de BrowserUsageData con {domain, seconds}
   */
  async getBrowserUsageForDay(
    contractorId: string,
    workday: Date,
    agentId?: string,
  ): Promise<BrowserUsageData[]> {
    const workdayStr = workday.toLocaleDateString('en-CA');
    const agentFilter = agentId ? `AND agent_id = '${agentId}'` : '';

    try {
      const query = `
        SELECT 
          JSONExtractString(payload, 'browser') AS browser_json,
          timestamp
        FROM events_raw
        WHERE contractor_id = '${contractorId}'
          AND toDate(timestamp) = '${workdayStr}'
          AND JSONHas(payload, 'browser')
          ${agentFilter}
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
    agentId?: string,
  ): Promise<AppUsageData[]> {
    const fromStr = fromDate.toLocaleDateString('en-CA');
    const toStr = toDate.toLocaleDateString('en-CA');

    // Calcular si usar LIMIT basado en el rango
    const daysDiff =
      Math.ceil(
        (toDate.getTime() - fromDate.getTime()) / (1000 * 60 * 60 * 24),
      ) + 1;
    const useLimit = limit ?? (daysDiff > 7 ? 100000 : undefined);

    const agentFilter = agentId ? `AND agent_id = '${agentId}'` : '';

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
          ${agentFilter}
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

      // Obtener tipos desde apps_dimension
      const appNames = Object.keys(appUsageMap);
      const typeMap = await this.getAppTypesFromDimension(appNames);

      return Object.entries(appUsageMap)
        .map(([appName, seconds]) => ({
          appName,
          seconds: seconds < 0 ? 0 : seconds,
          type: typeMap[appName] || undefined,
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
    agentId?: string,
  ): Promise<BrowserUsageData[]> {
    const fromStr = fromDate.toLocaleDateString('en-CA');
    const toStr = toDate.toLocaleDateString('en-CA');

    // Calcular si usar LIMIT basado en el rango
    const daysDiff =
      Math.ceil(
        (toDate.getTime() - fromDate.getTime()) / (1000 * 60 * 60 * 24),
      ) + 1;
    const useLimit = limit ?? (daysDiff > 7 ? 100000 : undefined);

    const agentFilter = agentId ? `AND agent_id = '${agentId}'` : '';

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
          ${agentFilter}
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
    const fromStr = fromDate.toLocaleDateString('en-CA');
    const toStr = toDate.toLocaleDateString('en-CA');

    try {
      const query = `
        SELECT 
          app_name AS appName,
          sum(JSONExtractFloat(payload, 'AppUsage', app_name)) AS seconds,
          any(d.type) AS type
        FROM events_raw
        ARRAY JOIN JSONExtractKeys(payload, 'AppUsage') AS app_name
        LEFT JOIN apps_dimension d ON d.name = app_name
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
        type?: string;
      }>(query);

      return results.map((r) => {
        const seconds = Number(r.seconds);
        return {
          appName: r.appName,
          seconds: isNaN(seconds) || !isFinite(seconds) ? 0 : seconds,
          type: r.type || undefined,
        };
      });
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
    const fromStr = fromDate.toLocaleDateString('en-CA');
    const toStr = toDate.toLocaleDateString('en-CA');

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

      return results.map((r) => {
        const seconds = Number(r.seconds);
        return {
          domain: r.domain,
          seconds: isNaN(seconds) || !isFinite(seconds) ? 0 : seconds,
        };
      });
    } catch (error) {
      this.logger.warn(
        `Error getting aggregated Browser usage: ${error.message}. Returning empty array.`,
      );
      return [];
    }
  }

  /**
   * Obtiene datos de AppUsage agregados para múltiples contractors en una sola query.
   * Optimizado para evitar N+1 queries cuando se necesita app_usage de muchos contractors.
   *
   * @param contractorIds Array de IDs de contractors
   * @param fromDate Fecha de inicio
   * @param toDate Fecha de fin
   * @returns Map con contractor_id como key y array de AppUsageData como value
   */
  async getAppUsageAggregatedForMultiple(
    contractorIds: string[],
    fromDate: Date,
    toDate: Date,
  ): Promise<Map<string, AppUsageData[]>> {
    if (contractorIds.length === 0) {
      return new Map();
    }

    const fromStr = fromDate.toLocaleDateString('en-CA');
    const toStr = toDate.toLocaleDateString('en-CA');
    const contractorIdsList = contractorIds.map((id) => `'${id}'`).join(',');

    try {
      const query = `
        SELECT 
          contractor_id,
          app_name AS appName,
          sum(JSONExtractFloat(payload, 'AppUsage', app_name)) AS seconds,
          any(d.type) AS type
        FROM events_raw
        ARRAY JOIN JSONExtractKeys(payload, 'AppUsage') AS app_name
        LEFT JOIN apps_dimension d ON d.name = app_name
        WHERE contractor_id IN (${contractorIdsList})
          AND toDate(timestamp) >= '${fromStr}'
          AND toDate(timestamp) <= '${toStr}'
          AND JSONHas(payload, 'AppUsage')
        GROUP BY contractor_id, app_name
        HAVING seconds > 0
        ORDER BY contractor_id, seconds DESC
      `;

      const results = await this.clickHouseService.query<{
        contractor_id: string;
        appName: string;
        seconds: number;
        type?: string;
      }>(query);

      // Inicializar Map con arrays vacíos para todos los contractors
      const usageMap = new Map<string, AppUsageData[]>();
      contractorIds.forEach((id) => usageMap.set(id, []));

      // Procesar resultados (ya vienen agrupados por contractor_id desde SQL)
      // Un solo ciclo, sin verificaciones de has/get
      results.forEach((row) => {
        const existing = usageMap.get(row.contractor_id);
        if (existing) {
          const seconds = Number(row.seconds);
          existing.push({
            appName: row.appName,
            seconds: isNaN(seconds) || !isFinite(seconds) ? 0 : seconds,
            type: row.type || undefined,
          });
        }
      });

      return usageMap;
    } catch (error) {
      this.logger.warn(
        `Error getting aggregated AppUsage for multiple contractors: ${error.message}. Returning empty map.`,
      );
      // Retornar map vacío para todos los contractors
      const emptyMap = new Map<string, AppUsageData[]>();
      contractorIds.forEach((id) => emptyMap.set(id, []));
      return emptyMap;
    }
  }

  /**
   * Obtiene datos de Browser agregados para múltiples contractors en una sola query.
   * Optimizado para evitar N+1 queries cuando se necesita browser_usage de muchos contractors.
   *
   * @param contractorIds Array de IDs de contractors
   * @param fromDate Fecha de inicio
   * @param toDate Fecha de fin
   * @returns Map con contractor_id como key y array de BrowserUsageData como value
   */
  async getBrowserUsageAggregatedForMultiple(
    contractorIds: string[],
    fromDate: Date,
    toDate: Date,
  ): Promise<Map<string, BrowserUsageData[]>> {
    if (contractorIds.length === 0) {
      return new Map();
    }

    const fromStr = fromDate.toLocaleDateString('en-CA');
    const toStr = toDate.toLocaleDateString('en-CA');
    const contractorIdsList = contractorIds.map((id) => `'${id}'`).join(',');

    try {
      const query = `
        SELECT 
          contractor_id,
          domain,
          sum(JSONExtractFloat(payload, 'browser', domain)) AS seconds
        FROM events_raw
        ARRAY JOIN JSONExtractKeys(payload, 'browser') AS domain
        WHERE contractor_id IN (${contractorIdsList})
          AND toDate(timestamp) >= '${fromStr}'
          AND toDate(timestamp) <= '${toStr}'
          AND JSONHas(payload, 'browser')
        GROUP BY contractor_id, domain
        HAVING seconds > 0
        ORDER BY contractor_id, seconds DESC
      `;

      const results = await this.clickHouseService.query<{
        contractor_id: string;
        domain: string;
        seconds: number;
      }>(query);

      // Inicializar Map con arrays vacíos para todos los contractors
      const usageMap = new Map<string, BrowserUsageData[]>();
      contractorIds.forEach((id) => usageMap.set(id, []));

      // Procesar resultados (ya vienen agrupados por contractor_id desde SQL)
      // Un solo ciclo, sin verificaciones de has/get
      results.forEach((row) => {
        const existing = usageMap.get(row.contractor_id);
        if (existing) {
          const seconds = Number(row.seconds);
          existing.push({
            domain: row.domain,
            seconds: isNaN(seconds) || !isFinite(seconds) ? 0 : seconds,
          });
        }
      });

      return usageMap;
    } catch (error) {
      this.logger.warn(
        `Error getting aggregated Browser usage for multiple contractors: ${error.message}. Returning empty map.`,
      );
      // Retornar map vacío para todos los contractors
      const emptyMap = new Map<string, BrowserUsageData[]>();
      contractorIds.forEach((id) => emptyMap.set(id, []));
      return emptyMap;
    }
  }
}
