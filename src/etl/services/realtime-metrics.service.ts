import { Injectable, Logger } from '@nestjs/common';

import { ClickHouseService } from '../../clickhouse/clickhouse.service';
import { DimensionsService } from './dimensions.service';
import { ContractorActivity15sDto } from '../dto/contractor-activity-15s.dto';
import {
  AppUsageData,
  BrowserUsageData,
} from '../transformers/activity-to-daily-metrics.transformer';
import { ActivityToDailyMetricsTransformer } from '../transformers/activity-to-daily-metrics.transformer';

/**
 * Servicio para calcular métricas de productividad en tiempo real.
 * Lee directamente desde contractor_activity_15s sin depender de contractor_daily_metrics.
 * Optimizado para dashboards que necesitan actualización frecuente.
 */
@Injectable()
export class RealtimeMetricsService {
  private readonly logger = new Logger(RealtimeMetricsService.name);
  private readonly cache = new Map<string, { data: any; expires: number }>();
  private readonly CACHE_TTL_MS = 30 * 1000; // 30 segundos

  constructor(
    private readonly clickHouseService: ClickHouseService,
    private readonly dimensionsService: DimensionsService,
    private readonly activityToDailyMetricsTransformer: ActivityToDailyMetricsTransformer,
  ) {}

  /**
   * Calcula métricas de productividad en tiempo real para un contractor.
   * Usa caché para evitar recalcular constantemente.
   *
   * @param contractorId ID del contractor
   * @param workday Fecha del día (por defecto: hoy)
   * @param useCache Si usar caché (default: true)
   * @returns Métricas de productividad del día
   */
  async getRealtimeMetrics(
    contractorId: string,
    workday?: Date,
    useCache: boolean = true,
  ) {
    // Crear una copia del Date para no modificar el original
    const workdayDate = workday ? new Date(workday) : new Date();
    workdayDate.setUTCHours(0, 0, 0, 0); // Usar UTC para evitar problemas de zona horaria
    const workdayStr = workdayDate.toISOString().split('T')[0];

    const cacheKey = `realtime:${contractorId}:${workdayStr}`;

    // Verificar caché
    if (useCache) {
      const cached = this.cache.get(cacheKey);
      if (cached && cached.expires > Date.now()) {
        this.logger.debug(`Cache hit for ${cacheKey}`);
        return cached.data;
      }
    }

    // Calcular métricas desde contractor_activity_15s
    const metrics = await this.calculateMetrics(contractorId, workdayDate);

    // Asegurar que workday se devuelva como string YYYY-MM-DD
    const result = {
      ...metrics,
      workday: workdayStr, // Usar el string formateado en lugar del Date
      is_realtime: true,
      calculated_at: new Date().toISOString(),
    };

    // Guardar en caché
    if (useCache) {
      this.cache.set(cacheKey, {
        data: result,
        expires: Date.now() + this.CACHE_TTL_MS,
      });
    }

    return result;
  }

  /**
   * Calcula métricas desde contractor_activity_15s para un contractor y día.
   */
  private async calculateMetrics(
    contractorId: string,
    workday: Date,
  ): Promise<any> {
    const workdayStr = workday.toISOString().split('T')[0];

    // Leer beats del día
    const beatsQuery = `
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
        AND toDate(beat_timestamp) = '${workdayStr}'
      ORDER BY beat_timestamp
    `;

    const beats =
      await this.clickHouseService.query<ContractorActivity15sDto>(beatsQuery);

    if (beats.length === 0) {
      return {
        contractor_id: contractorId,
        workday: workdayStr, // Ya es string YYYY-MM-DD
        total_beats: 0,
        active_beats: 0,
        idle_beats: 0,
        active_percentage: 0,
        total_keyboard_inputs: 0,
        total_mouse_clicks: 0,
        avg_keyboard_per_min: 0,
        avg_mouse_per_min: 0,
        total_session_time_seconds: 0,
        effective_work_seconds: 0,
        productivity_score: 0,
        is_realtime: true,
      };
    }

    // Convertir timestamps
    for (const beat of beats) {
      if (typeof beat.beat_timestamp === 'string') {
        beat.beat_timestamp = new Date(beat.beat_timestamp);
      }
    }

    // Obtener AppUsage y Browser para este día
    const appUsage = await this.getAppUsageForDay(contractorId, workday);
    const browserUsage = await this.getBrowserUsageForDay(
      contractorId,
      workday,
    );

    // Calcular métricas usando el transformer
    const metrics = this.activityToDailyMetricsTransformer.aggregate(
      contractorId,
      workday,
      beats,
      appUsage,
      browserUsage,
    );

    return {
      ...metrics,
      workday: workdayStr, // Devolver como string en lugar de Date
      app_usage: appUsage.map((a) => ({
        appName: a.appName,
        seconds: a.seconds,
      })),
      browser_usage: browserUsage.map((b) => ({
        domain: b.domain,
        seconds: b.seconds,
      })),
      is_realtime: true,
      calculated_at: new Date().toISOString(),
    };
  }

  /**
   * Obtiene datos de AppUsage para un contractor y día.
   */
  private async getAppUsageForDay(
    contractorId: string,
    workday: Date,
  ): Promise<AppUsageData[]> {
    const workdayStrForQuery = workday.toISOString().split('T')[0];

    const query = `
      SELECT 
        JSONExtractString(payload, 'AppUsage') AS app_usage_json,
        timestamp
      FROM events_raw
      WHERE contractor_id = '${contractorId}'
        AND toDate(timestamp) = '${workdayStrForQuery}'
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

    // Convertir a array de AppUsageData
    const appUsage: AppUsageData[] = Object.entries(appUsageMap)
      .map(([appName, seconds]) => ({
        appName,
        seconds: seconds < 0 ? 0 : seconds,
      }))
      .filter((u) => u.seconds > 0);

    return appUsage;
  }

  /**
   * Obtiene datos de Browser para un contractor y día.
   */
  private async getBrowserUsageForDay(
    contractorId: string,
    workday: Date,
  ): Promise<BrowserUsageData[]> {
    const workdayStrForQuery = workday.toISOString().split('T')[0];

    const query = `
      SELECT 
        JSONExtractString(payload, 'browser') AS browser_json,
        timestamp
      FROM events_raw
      WHERE contractor_id = '${contractorId}'
        AND toDate(timestamp) = '${workdayStrForQuery}'
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

    // Convertir a array de BrowserUsageData
    const browserUsage: BrowserUsageData[] = Object.entries(browserUsageMap)
      .map(([domain, seconds]) => ({
        domain,
        seconds: seconds < 0 ? 0 : seconds,
      }))
      .filter((u) => u.seconds > 0);

    return browserUsage;
  }

  /**
   * Obtiene métricas en tiempo real de todos los contratistas que tienen métricas para un día específico.
   * Solo devuelve contratistas que tienen datos (total_beats > 0).
   * Enriquece los datos con información del contractor (nombre, email, job_position, country, client, team).
   *
   * @param workday Fecha del día (por defecto: hoy)
   * @param useCache Si usar caché (default: true)
   * @returns Array de métricas de productividad por contractor con información enriquecida
   */
  async getAllRealtimeMetrics(
    workday?: Date,
    useCache: boolean = true,
  ): Promise<any[]> {
    // Crear una copia del Date para no modificar el original
    const workdayDate = workday ? new Date(workday) : new Date();
    workdayDate.setUTCHours(0, 0, 0, 0); // Usar UTC para evitar problemas de zona horaria
    const workdayStr = workdayDate.toISOString().split('T')[0];

    // Obtener todos los contractor_id únicos que tienen datos para este día
    const contractorsQuery = `
      SELECT DISTINCT contractor_id
      FROM contractor_activity_15s
      WHERE toDate(beat_timestamp) = '${workdayStr}'
    `;

    const contractors = await this.clickHouseService.query<{
      contractor_id: string;
    }>(contractorsQuery);

    if (contractors.length === 0) {
      return [];
    }

    // Calcular métricas para cada contractor en paralelo
    const metricsPromises = contractors.map((contractor) =>
      this.getRealtimeMetrics(contractor.contractor_id, workdayDate, useCache),
    );

    const allMetrics = await Promise.all(metricsPromises);

    // Filtrar solo aquellos que tienen métricas (total_beats > 0)
    const metricsWithData = allMetrics.filter(
      (metric) => metric.total_beats > 0,
    );

    if (metricsWithData.length === 0) {
      return [];
    }

    // Enriquecer con información del contractor usando JOINs
    const enrichedMetrics =
      await this.enrichMetricsWithContractorInfo(metricsWithData);

    return enrichedMetrics;
  }

  /**
   * Enriquece las métricas con información del contractor, client y team usando JOINs.
   */
  private async enrichMetricsWithContractorInfo(
    metrics: any[],
  ): Promise<any[]> {
    if (metrics.length === 0) {
      return [];
    }

    // Extraer los contractor_ids únicos
    const contractorIds = [...new Set(metrics.map((m) => m.contractor_id))];
    const contractorIdsList = contractorIds.map((id) => `'${id}'`).join(',');

    // Query para obtener información del contractor con JOINs a teams y clients
    const enrichmentQuery = `
      SELECT 
        c.contractor_id,
        c.name,
        c.email,
        c.job_position,
        c.country,
        c.client_id,
        c.team_id,
        COALESCE(cl.client_name, 'N/A') as client_name,
        COALESCE(t.team_name, 'N/A') as team_name
      FROM (
        SELECT 
          contractor_id,
          name,
          email,
          job_position,
          country,
          client_id,
          team_id
        FROM contractor_info_raw FINAL
        WHERE contractor_id IN (${contractorIdsList})
      ) c
      LEFT JOIN (
        SELECT 
          client_id,
          client_name
        FROM clients_dimension FINAL
      ) cl ON c.client_id = cl.client_id
      LEFT JOIN (
        SELECT 
          team_id,
          team_name
        FROM teams_dimension FINAL
      ) t ON c.team_id = t.team_id
    `;

    const contractorInfo =
      await this.clickHouseService.query<any>(enrichmentQuery);

    // Crear un mapa para acceso rápido
    const infoMap = new Map<string, any>();
    contractorInfo.forEach((info) => {
      infoMap.set(info.contractor_id, info);
    });

    // Enriquecer cada métrica con la información del contractor
    return metrics.map((metric) => {
      const info = infoMap.get(metric.contractor_id) || {};
      return {
        ...metric,
        contractor_name: info.name || 'N/A',
        contractor_email: info.email || null,
        job_position: info.job_position || 'N/A',
        country: info.country || 'N/A',
        client_id: info.client_id || 'N/A',
        client_name: info.client_name || 'N/A',
        team_id: info.team_id || null,
        team_name: info.team_name || 'N/A',
      };
    });
  }

  /**
   * Limpia el caché expirado.
   */
  clearExpiredCache() {
    const now = Date.now();
    for (const [key, value] of this.cache.entries()) {
      if (value.expires <= now) {
        this.cache.delete(key);
      }
    }
  }

  /**
   * Limpia todo el caché.
   */
  clearCache() {
    this.cache.clear();
  }
}
