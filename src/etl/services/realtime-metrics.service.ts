import { Injectable, Logger } from '@nestjs/common';

import { envs } from 'config';
import { ClickHouseService } from '../../clickhouse/clickhouse.service';
import { DimensionsService } from './dimensions.service';
import { UsageDataService } from './usage-data.service';
import { ContractorActivity15sDto } from '../dto/contractor-activity-15s.dto';
import {
  AppUsageData,
  BrowserUsageData,
} from '../transformers/activity-to-daily-metrics.transformer';
import { ActivityToDailyMetricsTransformer } from '../transformers/activity-to-daily-metrics.transformer';

/**
 * Servicio para calcular métricas de productividad en tiempo real.
 * OPTIMIZADO: Usa contractor_daily_metrics para rangos históricos.
 * Solo calcula en tiempo real para el día actual.
 */
@Injectable()
export class RealtimeMetricsService {
  private readonly logger = new Logger(RealtimeMetricsService.name);
  private readonly cache = new Map<string, { data: any; expires: number }>();
  private readonly CACHE_TTL_MS = 30 * 1000; // 30 segundos

  constructor(
    private readonly clickHouseService: ClickHouseService,
    private readonly dimensionsService: DimensionsService,
    private readonly usageDataService: UsageDataService,
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
        app_usage: [],
        browser_usage: [],
        is_realtime: true,
      };
    }

    // Convertir timestamps
    for (const beat of beats) {
      if (typeof beat.beat_timestamp === 'string') {
        beat.beat_timestamp = new Date(beat.beat_timestamp);
      }
    }

    // Obtener AppUsage y Browser para este día (usando servicio compartido)
    const appUsage = await this.usageDataService.getAppUsageForDay(
      contractorId,
      workday,
    );
    const browserUsage = await this.usageDataService.getBrowserUsageForDay(
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
   * @param filters Filtros opcionales para filtrar contractors
   * @returns Array de métricas de productividad por contractor con información enriquecida
   */
  async getAllRealtimeMetrics(
    workday?: Date,
    useCache: boolean = true,
    filters?: {
      name?: string;
      job_position?: string;
      country?: string;
      client_id?: string;
      team_id?: string;
    },
  ): Promise<any[]> {
    // Crear una copia del Date para no modificar el original
    const workdayDate = workday ? new Date(workday) : new Date();
    workdayDate.setUTCHours(0, 0, 0, 0); // Usar UTC para evitar problemas de zona horaria
    const workdayStr = workdayDate.toISOString().split('T')[0];

    // Obtener contractor_ids que cumplen con los filtros (si se proporcionan)
    let contractorIds: string[] = [];
    if (
      filters &&
      Object.values(filters).some((v) => v !== undefined && v !== null)
    ) {
      contractorIds = await this.getFilteredContractorIds(filters);
      if (contractorIds.length === 0) {
        return [];
      }
    }

    // Construir query para obtener contractors con datos del día
    let contractorsQuery = `
      SELECT DISTINCT contractor_id
      FROM contractor_activity_15s
      WHERE toDate(beat_timestamp) = '${workdayStr}'
    `;

    // Si hay filtros, agregar condición para filtrar por contractor_ids
    if (contractorIds.length > 0) {
      const contractorIdsList = contractorIds.map((id) => `'${id}'`).join(',');
      contractorsQuery += ` AND contractor_id IN (${contractorIdsList})`;
    }

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
   * Obtiene métricas en tiempo real agregadas por rango de fechas para todos los contratistas.
   * OPTIMIZADO: Usa contractor_daily_metrics para rangos históricos (95% más rápido).
   * Solo calcula en tiempo real para el día actual.
   *
   * @param fromDate Fecha de inicio del rango
   * @param toDate Fecha de fin del rango
   * @param useCache Si usar caché (default: true)
   * @param filters Filtros opcionales para filtrar contractors
   * @returns Array de métricas agregadas por contractor con información enriquecida
   */
  async getAllRealtimeMetricsByDateRange(
    fromDate: Date,
    toDate: Date,
    useCache: boolean = true,
    filters?: {
      name?: string;
      job_position?: string;
      country?: string;
      client_id?: string;
      team_id?: string;
    },
  ): Promise<any[]> {
    // Normalizar fechas a UTC
    const from = new Date(fromDate);
    from.setUTCHours(0, 0, 0, 0);
    const to = new Date(toDate);
    to.setUTCHours(23, 59, 59, 999);

    const fromStr = from.toISOString().split('T')[0];
    const toStr = to.toISOString().split('T')[0];

    // Construir clave de caché incluyendo filtros
    const filtersKey = filters ? JSON.stringify(filters) : '';
    const cacheKey = `realtime-metrics-range:${fromStr}:${toStr}:${filtersKey}`;
    if (useCache) {
      const cached = this.cache.get(cacheKey);
      if (cached && cached.expires > Date.now()) {
        this.logger.debug(`Cache hit for ${cacheKey}`);
        return cached.data;
      }
    }

    // Obtener contractor_ids que cumplen con los filtros (si se proporcionan)
    let contractorIds: string[] = [];
    if (
      filters &&
      Object.values(filters).some((v) => v !== undefined && v !== null)
    ) {
      contractorIds = await this.getFilteredContractorIds(filters);
      if (contractorIds.length === 0) {
        return [];
      }
    }

    // ✅ OPTIMIZACIÓN: Usar contractor_daily_metrics para rangos históricos
    // En lugar de calcular desde events_raw para cada contractor, usamos datos pre-calculados
    const allMetrics = await this.getAggregatedMetricsFromDailyTable(
      fromStr,
      toStr,
      contractorIds,
    );

    if (allMetrics.length === 0) {
      return [];
    }

    // Enriquecer con información del contractor usando JOINs
    const enrichedMetrics =
      await this.enrichMetricsWithContractorInfo(allMetrics);

    // Guardar en caché (TTL 30 segundos)
    if (useCache) {
      this.cache.set(cacheKey, {
        data: enrichedMetrics,
        expires: Date.now() + 30000,
      });
    }

    return enrichedMetrics;
  }

  /**
   * Obtiene métricas agregadas directamente desde contractor_daily_metrics.
   * Esta es la función optimizada que reemplaza el cálculo en tiempo real.
   *
   * @param fromStr Fecha inicio (YYYY-MM-DD)
   * @param toStr Fecha fin (YYYY-MM-DD)
   * @param contractorIds Lista de IDs a filtrar (opcional)
   * @returns Métricas agregadas por contractor
   */
  private async getAggregatedMetricsFromDailyTable(
    fromStr: string,
    toStr: string,
    contractorIds: string[] = [],
  ): Promise<any[]> {
    // Construir filtro de contractors si se proporciona
    const contractorFilter =
      contractorIds.length > 0
        ? `AND contractor_id IN (${contractorIds.map((id) => `'${id}'`).join(',')})`
        : '';

    // Query optimizada: agregar métricas desde contractor_daily_metrics
    // Una sola query en lugar de N queries por contractor
    // NOTA: Usamos subquery para evitar conflictos de alias en ClickHouse
    const query = `
      SELECT 
        contractor_id,
        '${fromStr} to ${toStr}' AS workday,
        _total_beats AS total_beats,
        _active_beats AS active_beats,
        _idle_beats AS idle_beats,
        -- Calcular active_percentage como promedio ponderado
        100.0 * _active_beats / nullIf(_total_beats, 0) AS active_percentage,
        _total_keyboard_inputs AS total_keyboard_inputs,
        _total_mouse_clicks AS total_mouse_clicks,
        -- Promedios ponderados por total_beats
        _sum_keyboard_weighted / nullIf(_total_beats, 0) AS avg_keyboard_per_min,
        _sum_mouse_weighted / nullIf(_total_beats, 0) AS avg_mouse_per_min,
        _total_session_time_seconds AS total_session_time_seconds,
        _effective_work_seconds AS effective_work_seconds,
        -- Productivity score: promedio ponderado por total_beats
        _sum_productivity_weighted / nullIf(_total_beats, 0) AS productivity_score,
        _days_count AS days_count
      FROM (
        SELECT 
          contractor_id,
          sum(total_beats) AS _total_beats,
          sum(active_beats) AS _active_beats,
          sum(idle_beats) AS _idle_beats,
          sum(total_keyboard_inputs) AS _total_keyboard_inputs,
          sum(total_mouse_clicks) AS _total_mouse_clicks,
          sum(avg_keyboard_per_min * total_beats) AS _sum_keyboard_weighted,
          sum(avg_mouse_per_min * total_beats) AS _sum_mouse_weighted,
          sum(total_session_time_seconds) AS _total_session_time_seconds,
          sum(effective_work_seconds) AS _effective_work_seconds,
          sum(productivity_score * total_beats) AS _sum_productivity_weighted,
          count() AS _days_count
        FROM contractor_daily_metrics
        WHERE workday >= '${fromStr}'
          AND workday <= '${toStr}'
          ${contractorFilter}
        GROUP BY contractor_id
        HAVING sum(total_beats) > 0
      )
    `;

    const results = await this.clickHouseService.query<any>(query);

    this.logger.debug(
      `📊 Fetched ${results.length} contractors from contractor_daily_metrics (${fromStr} to ${toStr})`,
    );

    // ✅ OPTIMIZACIÓN: Obtener app_usage y browser_usage para TODOS los contractors en 2 queries
    // en lugar de 2 queries por cada contractor (N+1 problem)
    const fromDate = new Date(fromStr);
    const toDate = new Date(toStr);
    toDate.setUTCHours(23, 59, 59, 999);

    const resultContractorIds = results.map((r) => r.contractor_id);

    // Obtener todos los app_usage y browser_usage en paralelo (solo 2 queries totales)
    const [appUsageMap, browserUsageMap] = await Promise.all([
      this.usageDataService.getAppUsageAggregatedForMultiple(
        resultContractorIds,
        fromDate,
        toDate,
      ),
      this.usageDataService.getBrowserUsageAggregatedForMultiple(
        resultContractorIds,
        fromDate,
        toDate,
      ),
    ]);

    // Formatear resultados usando los maps obtenidos
    const enrichedResults = results.map((row) => {
      const appUsage = appUsageMap.get(row.contractor_id) || [];
      const browserUsage = browserUsageMap.get(row.contractor_id) || [];

      return {
        contractor_id: row.contractor_id,
        workday: row.workday,
        total_beats: Number(row.total_beats) || 0,
        active_beats: Number(row.active_beats) || 0,
        idle_beats: Number(row.idle_beats) || 0,
        active_percentage: Number(row.active_percentage) || 0,
        total_keyboard_inputs: Number(row.total_keyboard_inputs) || 0,
        total_mouse_clicks: Number(row.total_mouse_clicks) || 0,
        avg_keyboard_per_min: Number(row.avg_keyboard_per_min) || 0,
        avg_mouse_per_min: Number(row.avg_mouse_per_min) || 0,
        total_session_time_seconds: Number(row.total_session_time_seconds) || 0,
        effective_work_seconds: Number(row.effective_work_seconds) || 0,
        productivity_score: Number(row.productivity_score) || 0,
        days_count: Number(row.days_count) || 0,
        app_usage: appUsage.map((a) => ({
          appName: a.appName,
          seconds: a.seconds,
        })),
        browser_usage: browserUsage.map((b) => ({
          domain: b.domain,
          seconds: b.seconds,
        })),
        is_realtime: false, // Indica que viene de datos pre-calculados
        calculated_at: new Date().toISOString(),
      };
    });

    return enrichedResults;
  }

  /**
   * Obtiene los contractor_ids que cumplen con los filtros especificados.
   * En ClickHouse, cuando usas FINAL, necesitas usar subconsultas para los JOINs.
   */
  private async getFilteredContractorIds(filters: {
    name?: string;
    job_position?: string;
    country?: string;
    client_id?: string;
    team_id?: string;
  }): Promise<string[]> {
    const conditions: string[] = [];
    const dbName = envs.clickhouse.database;

    if (filters.name) {
      conditions.push(`c.name = '${filters.name.replace(/'/g, "''")}'`);
    }
    if (filters.job_position) {
      conditions.push(
        `c.job_position = '${filters.job_position.replace(/'/g, "''")}'`,
      );
    }
    if (filters.country) {
      conditions.push(`c.country = '${filters.country.replace(/'/g, "''")}'`);
    }
    if (filters.client_id) {
      conditions.push(
        `c.client_id = '${filters.client_id.replace(/'/g, "''")}'`,
      );
    }
    if (filters.team_id) {
      conditions.push(`c.team_id = '${filters.team_id.replace(/'/g, "''")}'`);
    }

    if (conditions.length === 0) {
      return [];
    }

    const whereClause = conditions.join(' AND ');

    // En ClickHouse, cuando usas FINAL, necesitas envolver las tablas en subconsultas para hacer JOINs
    const query = `
      SELECT DISTINCT c.contractor_id
      FROM (
        SELECT * FROM ${dbName}.contractor_info_raw FINAL
      ) AS c
      LEFT JOIN (
        SELECT * FROM ${dbName}.clients_dimension FINAL
      ) AS cl ON c.client_id = cl.client_id
      LEFT JOIN (
        SELECT * FROM ${dbName}.teams_dimension FINAL
      ) AS t ON c.team_id = t.team_id
      WHERE ${whereClause}
    `;

    try {
      const results = await this.clickHouseService.query<{
        contractor_id: string;
      }>(query);

      return results.map((r) => r.contractor_id);
    } catch (error) {
      this.logger.error(`Error in getFilteredContractorIds: ${error}`);
      this.logger.error(`Query: ${query}`);
      throw error;
    }
  }

  /**
   * Obtiene métricas de productividad en tiempo real para un contractor en un rango de fechas.
   * Usa caché para evitar recalcular constantemente.
   *
   * @param contractorId ID del contractor
   * @param fromDate Fecha de inicio del rango
   * @param toDate Fecha de fin del rango
   * @param useCache Si usar caché (default: true)
   * @returns Métricas de productividad agregadas para el rango
   */
  async getRealtimeMetricsForDateRange(
    contractorId: string,
    fromDate: Date,
    toDate: Date,
    useCache: boolean = true,
  ): Promise<any> {
    // Normalizar fechas a UTC
    const from = new Date(fromDate);
    from.setUTCHours(0, 0, 0, 0);
    const to = new Date(toDate);
    to.setUTCHours(23, 59, 59, 999);

    const fromStr = from.toISOString().split('T')[0];
    const toStr = to.toISOString().split('T')[0];

    const cacheKey = `realtime-range:${contractorId}:${fromStr}:${toStr}`;

    // Verificar caché
    if (useCache) {
      const cached = this.cache.get(cacheKey);
      if (cached && cached.expires > Date.now()) {
        this.logger.debug(`Cache hit for ${cacheKey}`);
        return cached.data;
      }
    }

    // Calcular métricas agregadas para el rango
    const metrics = await this.calculateMetricsForDateRange(
      contractorId,
      from,
      to,
    );

    // Enriquecer con información del contractor
    const enrichedMetrics = await this.enrichMetricsWithContractorInfo([
      metrics,
    ]);

    const result = enrichedMetrics.length > 0 ? enrichedMetrics[0] : metrics;

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
   * Calcula métricas agregadas para un contractor en un rango de fechas.
   * OPTIMIZADO: Usa contractor_daily_metrics para rangos históricos.
   */
  private async calculateMetricsForDateRange(
    contractorId: string,
    fromDate: Date,
    toDate: Date,
  ): Promise<any> {
    const fromStr = fromDate.toISOString().split('T')[0];
    const toStr = toDate.toISOString().split('T')[0];

    // ✅ OPTIMIZACIÓN: Usar contractor_daily_metrics en lugar de calcular desde beats
    // Usar subquery para evitar conflictos de agregaciones anidadas en ClickHouse
    const query = `
      SELECT 
        contractor_id,
        '${fromStr} to ${toStr}' AS workday,
        _total_beats AS total_beats,
        _active_beats AS active_beats,
        _idle_beats AS idle_beats,
        100.0 * _active_beats / nullIf(_total_beats, 0) AS active_percentage,
        _total_keyboard_inputs AS total_keyboard_inputs,
        _total_mouse_clicks AS total_mouse_clicks,
        _sum_keyboard_weighted / nullIf(_total_beats, 0) AS avg_keyboard_per_min,
        _sum_mouse_weighted / nullIf(_total_beats, 0) AS avg_mouse_per_min,
        _total_session_time_seconds AS total_session_time_seconds,
        _effective_work_seconds AS effective_work_seconds,
        _sum_productivity_weighted / nullIf(_total_beats, 0) AS productivity_score,
        _days_count AS days_count
      FROM (
        SELECT 
          contractor_id,
          sum(total_beats) AS _total_beats,
          sum(active_beats) AS _active_beats,
          sum(idle_beats) AS _idle_beats,
          sum(total_keyboard_inputs) AS _total_keyboard_inputs,
          sum(total_mouse_clicks) AS _total_mouse_clicks,
          sum(avg_keyboard_per_min * total_beats) AS _sum_keyboard_weighted,
          sum(avg_mouse_per_min * total_beats) AS _sum_mouse_weighted,
          sum(total_session_time_seconds) AS _total_session_time_seconds,
          sum(effective_work_seconds) AS _effective_work_seconds,
          sum(productivity_score * total_beats) AS _sum_productivity_weighted,
          count() AS _days_count
        FROM contractor_daily_metrics
        WHERE contractor_id = '${contractorId}'
          AND workday >= '${fromStr}'
          AND workday <= '${toStr}'
        GROUP BY contractor_id
      )
    `;

    const results = await this.clickHouseService.query<any>(query);

    if (results.length === 0 || !results[0].total_beats) {
      return {
        contractor_id: contractorId,
        workday: `${fromStr} to ${toStr}`,
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
        app_usage: [],
        browser_usage: [],
        is_realtime: false,
      };
    }

    const row = results[0];

    // Obtener app_usage y browser_usage para el rango de fechas
    const [appUsage, browserUsage] = await Promise.all([
      this.usageDataService.getAppUsageAggregated(
        contractorId,
        fromDate,
        toDate,
      ),
      this.usageDataService.getBrowserUsageAggregated(
        contractorId,
        fromDate,
        toDate,
      ),
    ]);

    return {
      contractor_id: row.contractor_id,
      workday: row.workday,
      total_beats: Number(row.total_beats) || 0,
      active_beats: Number(row.active_beats) || 0,
      idle_beats: Number(row.idle_beats) || 0,
      active_percentage: Number(row.active_percentage) || 0,
      total_keyboard_inputs: Number(row.total_keyboard_inputs) || 0,
      total_mouse_clicks: Number(row.total_mouse_clicks) || 0,
      avg_keyboard_per_min: Number(row.avg_keyboard_per_min) || 0,
      avg_mouse_per_min: Number(row.avg_mouse_per_min) || 0,
      total_session_time_seconds: Number(row.total_session_time_seconds) || 0,
      effective_work_seconds: Number(row.effective_work_seconds) || 0,
      productivity_score: Number(row.productivity_score) || 0,
      days_count: Number(row.days_count) || 0,
      app_usage: appUsage.map((a) => ({
        appName: a.appName,
        seconds: a.seconds,
      })),
      browser_usage: browserUsage.map((b) => ({
        domain: b.domain,
        seconds: b.seconds,
      })),
      is_realtime: false,
      calculated_at: new Date().toISOString(),
    };
  }

  /**
   * Obtiene datos de AppUsage para un contractor en un rango de fechas.
   * Optimizado para evitar timeouts con grandes volúmenes de datos.
   */
  private async getAppUsageForDateRange(
    contractorId: string,
    fromDate: Date,
    toDate: Date,
  ): Promise<AppUsageData[]> {
    const fromStr = fromDate.toISOString().split('T')[0];
    const toStr = toDate.toISOString().split('T')[0];

    // Optimizar query: usar LIMIT si el rango es muy grande (más de 7 días)
    const daysDiff =
      Math.ceil(
        (toDate.getTime() - fromDate.getTime()) / (1000 * 60 * 60 * 24),
      ) + 1;
    const useLimit = daysDiff > 7;

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
      ${useLimit ? 'LIMIT 100000' : ''}
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
   * Obtiene datos de Browser para un contractor en un rango de fechas.
   * Optimizado para evitar timeouts con grandes volúmenes de datos.
   */
  private async getBrowserUsageForDateRange(
    contractorId: string,
    fromDate: Date,
    toDate: Date,
  ): Promise<BrowserUsageData[]> {
    const fromStr = fromDate.toISOString().split('T')[0];
    const toStr = toDate.toISOString().split('T')[0];

    // Optimizar query: usar LIMIT si el rango es muy grande (más de 7 días)
    const daysDiff =
      Math.ceil(
        (toDate.getTime() - fromDate.getTime()) / (1000 * 60 * 60 * 24),
      ) + 1;
    const useLimit = daysDiff > 7;

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
      ${useLimit ? 'LIMIT 100000' : ''}
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

    const dbName = envs.clickhouse.database;

    // Query para obtener información del contractor con JOINs a teams y clients
    // Asegurar que client_id y team_id se obtengan directamente de contractor_info_raw
    // Usar alias explícitos para evitar prefijos en los nombres de columnas
    const enrichmentQuery = `
      SELECT 
        c.contractor_id AS contractor_id,
        c.name AS name,
        c.email AS email,
        c.job_position AS job_position,
        c.country AS country,
        c.client_id AS client_id,
        c.team_id AS team_id,
        COALESCE(cl.client_name, 'N/A') AS client_name,
        COALESCE(t.team_name, 'N/A') AS team_name
      FROM (
        SELECT 
          contractor_id,
          name,
          email,
          job_position,
          country,
          client_id,
          team_id
        FROM ${dbName}.contractor_info_raw FINAL
        WHERE contractor_id IN (${contractorIdsList})
      ) c
      LEFT JOIN (
        SELECT 
          client_id,
          client_name
        FROM ${dbName}.clients_dimension FINAL
      ) cl ON c.client_id = cl.client_id
      LEFT JOIN (
        SELECT 
          team_id,
          team_name
        FROM ${dbName}.teams_dimension FINAL
      ) t ON c.team_id = t.team_id
    `;

    const contractorInfo =
      await this.clickHouseService.query<any>(enrichmentQuery);

    this.logger.debug(
      `Enriching ${metrics.length} metrics with info from ${contractorInfo.length} contractors`,
    );

    // Log para depuración: mostrar algunos ejemplos de lo que viene de la query
    if (contractorInfo.length > 0) {
      this.logger.debug(
        `Sample contractor info: ${JSON.stringify(contractorInfo[0])}`,
      );
    }

    // Crear un mapa para acceso rápido
    const infoMap = new Map<string, any>();
    contractorInfo.forEach((info) => {
      infoMap.set(info.contractor_id, info);
    });

    // Enriquecer cada métrica con la información del contractor
    return metrics.map((metric) => {
      const info = infoMap.get(metric.contractor_id);

      // Si no hay información del contractor, usar valores por defecto
      if (!info) {
        this.logger.warn(
          `No contractor info found for contractor_id: ${metric.contractor_id}`,
        );
        return {
          ...metric,
          contractor_name: 'N/A',
          contractor_email: null,
          job_position: 'N/A',
          country: 'N/A',
          client_id: 'N/A',
          client_name: 'N/A',
          team_id: null,
          team_name: 'N/A',
        };
      }

      // ClickHouse puede devolver los campos con prefijo del alias (c.client_id) o sin prefijo
      // Intentar ambos formatos para compatibilidad
      const clientId = info.client_id || info['c.client_id'] || null;
      const teamId = info.team_id || info['c.team_id'] || null;
      const name = info.name || info['c.name'] || null;
      const email = info.email || info['c.email'] || null;
      const jobPosition = info.job_position || info['c.job_position'] || null;
      const country = info.country || info['c.country'] || null;

      // client_name y team_name vienen de los JOINs con las tablas de dimensiones
      const clientName = info.client_name || 'N/A';
      const teamName = info.team_name || 'N/A';

      return {
        ...metric,
        contractor_name: name || 'N/A',
        contractor_email: email || null,
        job_position: jobPosition || 'N/A',
        country: country || 'N/A',
        // Usar el client_id directamente de contractor_info_raw
        client_id: clientId && clientId !== '' ? clientId : 'N/A',
        client_name: clientName,
        // Usar el team_id directamente de contractor_info_raw (puede ser null)
        team_id: teamId && teamId !== '' ? teamId : null,
        team_name: teamName,
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

  /**
   * Obtiene el top 5 de mejores rankings de productividad.
   * @param period Período: 'day' (día actual), 'week' (última semana), 'month' (mes actual)
   * @param useCache Si usar caché (default: true)
   * @returns Top 5 contractors con mejor productividad_score del período
   */
  async getTop5BestRanking(
    period: 'day' | 'week' | 'month' = 'day',
    useCache: boolean = true,
  ): Promise<any[]> {
    let allMetrics: any[];

    if (period === 'day') {
      // Día actual
      const today = new Date();
      allMetrics = await this.getAllRealtimeMetrics(today, useCache, undefined);
    } else {
      // Semana o mes: calcular rango de fechas
      const today = new Date();
      today.setUTCHours(23, 59, 59, 999);
      let fromDate: Date;

      if (period === 'week') {
        // Última semana (7 días incluyendo hoy)
        fromDate = new Date(today);
        fromDate.setUTCDate(fromDate.getUTCDate() - 6);
        fromDate.setUTCHours(0, 0, 0, 0);
      } else {
        // Mes actual (primer día del mes hasta hoy)
        fromDate = new Date(today);
        fromDate.setUTCDate(1);
        fromDate.setUTCHours(0, 0, 0, 0);
      }

      // ✅ OPTIMIZACIÓN: getAllRealtimeMetricsByDateRange ya devuelve datos agregados
      // (un registro por contractor con productivity_score calculado como promedio ponderado)
      // No necesitamos hacer agregación adicional
      allMetrics = await this.getAllRealtimeMetricsByDateRange(
        fromDate,
        today,
        useCache,
        undefined,
      );
    }

    // Ordenar por productivity_score descendente y tomar top 5
    const sorted = allMetrics
      .filter(
        (m) =>
          m.productivity_score !== null &&
          m.productivity_score !== undefined &&
          !isNaN(Number(m.productivity_score)) &&
          Number(m.productivity_score) > 0,
      )
      .sort((a, b) => (b.productivity_score || 0) - (a.productivity_score || 0))
      .slice(0, 5);

    this.logger.debug(
      `getTop5BestRanking(${period}): Found ${sorted.length} contractors from ${allMetrics.length} total`,
    );

    return sorted;
  }

  /**
   * Obtiene el top 5 de peores rankings de productividad.
   * @param period Período: 'day' (día actual), 'week' (última semana), 'month' (mes actual)
   * @param useCache Si usar caché (default: true)
   * @returns Top 5 contractors con peor productividad_score del período
   */
  async getTop5WorstRanking(
    period: 'day' | 'week' | 'month' = 'day',
    useCache: boolean = true,
  ): Promise<any[]> {
    let allMetrics: any[];

    if (period === 'day') {
      // Día actual
      const today = new Date();
      allMetrics = await this.getAllRealtimeMetrics(today, useCache, undefined);
    } else {
      // Semana o mes: calcular rango de fechas
      const today = new Date();
      today.setUTCHours(23, 59, 59, 999);
      let fromDate: Date;

      if (period === 'week') {
        // Última semana (7 días incluyendo hoy)
        fromDate = new Date(today);
        fromDate.setUTCDate(fromDate.getUTCDate() - 6);
        fromDate.setUTCHours(0, 0, 0, 0);
      } else {
        // Mes actual (primer día del mes hasta hoy)
        fromDate = new Date(today);
        fromDate.setUTCDate(1);
        fromDate.setUTCHours(0, 0, 0, 0);
      }

      // ✅ OPTIMIZACIÓN: getAllRealtimeMetricsByDateRange ya devuelve datos agregados
      // (un registro por contractor con productivity_score calculado como promedio ponderado)
      // No necesitamos hacer agregación adicional
      allMetrics = await this.getAllRealtimeMetricsByDateRange(
        fromDate,
        today,
        useCache,
        undefined,
      );
    }

    // Ordenar por productivity_score ascendente y tomar top 5
    const sorted = allMetrics
      .filter(
        (m) =>
          m.productivity_score !== null &&
          m.productivity_score !== undefined &&
          !isNaN(Number(m.productivity_score)) &&
          Number(m.productivity_score) >= 0,
      )
      .sort((a, b) => (a.productivity_score || 0) - (b.productivity_score || 0))
      .slice(0, 5);

    this.logger.debug(
      `getTop5WorstRanking(${period}): Found ${sorted.length} contractors from ${allMetrics.length} total`,
    );

    return sorted;
  }

  /**
   * Calcula el porcentaje de talento activo vs inactivo en un período.
   * Un contractor se considera "activo" si tiene métricas (beats) en el período.
   *
   * Para 'day': % de contractors activos ese día específico.
   * Para 'week'/'month': PROMEDIO de % de asistencia diaria en el período.
   *   - Esto refleja la presencialidad real: si un contractor faltó algunos días,
   *     el porcentaje baja proporcionalmente.
   *
   * @param period 'day' (día actual), 'week' (última semana), 'month' (mes actual)
   * @param useCache Si usar caché (default: true)
   * @returns Objeto con porcentajes y conteos de contractors activos/inactivos
   */
  async getActiveTalentPercentage(
    period: 'day' | 'week' | 'month' = 'day',
    useCache: boolean = true,
  ): Promise<{
    active_percentage: number;
    inactive_percentage: number;
    total_contractors: number;
    active_contractors: number;
    inactive_contractors: number;
    period: string;
    daily_breakdown?: Array<{
      date: string;
      active: number;
      percentage: number;
    }>;
  }> {
    const dbName = envs.clickhouse.database;

    // Calcular rango de fechas según el período
    const today = new Date();
    today.setUTCHours(23, 59, 59, 999);
    let fromDate: Date;
    let periodStr: string;

    if (period === 'day') {
      fromDate = new Date(today);
      fromDate.setUTCHours(0, 0, 0, 0);
      periodStr = fromDate.toISOString().split('T')[0];
    } else if (period === 'week') {
      fromDate = new Date(today);
      fromDate.setUTCDate(fromDate.getUTCDate() - 6); // 7 días incluyendo hoy
      fromDate.setUTCHours(0, 0, 0, 0);
      periodStr = `${fromDate.toISOString().split('T')[0]} to ${today.toISOString().split('T')[0]}`;
    } else {
      // month
      fromDate = new Date(today);
      fromDate.setUTCDate(1);
      fromDate.setUTCHours(0, 0, 0, 0);
      periodStr = `${fromDate.toISOString().split('T')[0]} to ${today.toISOString().split('T')[0]}`;
    }

    const fromStr = fromDate.toISOString().split('T')[0];
    const toStr = today.toISOString().split('T')[0];

    // Verificar caché
    const cacheKey = `active-talent:${period}:${fromStr}:${toStr}`;
    if (useCache) {
      const cached = this.cache.get(cacheKey);
      if (cached && cached.expires > Date.now()) {
        this.logger.debug(`Cache hit for ${cacheKey}`);
        return cached.data;
      }
    }

    try {
      // 1. Obtener el total de contractors desde contractor_info_raw
      const totalContractorsQuery = `
        SELECT COUNT(DISTINCT contractor_id) AS total
        FROM ${dbName}.contractor_info_raw FINAL
      `;

      const totalResult = await this.clickHouseService.query<{ total: number }>(
        totalContractorsQuery,
      );
      const totalContractors = totalResult[0]?.total || 0;

      if (totalContractors === 0) {
        const result = {
          active_percentage: 0,
          inactive_percentage: 100,
          total_contractors: 0,
          active_contractors: 0,
          inactive_contractors: 0,
          period: periodStr,
        };

        if (useCache) {
          this.cache.set(cacheKey, {
            data: result,
            expires: Date.now() + this.CACHE_TTL_MS,
          });
        }

        return result;
      }

      if (period === 'day') {
        // Para 'day': lógica simple - % de contractors activos hoy
        const activeContractorsQuery = `
          SELECT COUNT(DISTINCT contractor_id) AS active
          FROM contractor_activity_15s
          WHERE toDate(beat_timestamp) = '${fromStr}'
        `;

        const activeResult = await this.clickHouseService.query<{
          active: number;
        }>(activeContractorsQuery);
        const activeContractors = activeResult[0]?.active || 0;

        const activePercentage =
          totalContractors > 0
            ? Math.round((activeContractors / totalContractors) * 100 * 100) /
              100
            : 0;
        const inactiveContractors = totalContractors - activeContractors;
        const inactivePercentage =
          totalContractors > 0
            ? Math.round((inactiveContractors / totalContractors) * 100 * 100) /
              100
            : 100;

        const result = {
          active_percentage: activePercentage,
          inactive_percentage: inactivePercentage,
          total_contractors: totalContractors,
          active_contractors: activeContractors,
          inactive_contractors: inactiveContractors,
          period: periodStr,
        };

        if (useCache) {
          this.cache.set(cacheKey, {
            data: result,
            expires: Date.now() + this.CACHE_TTL_MS,
          });
        }

        this.logger.debug(
          `Active talent percentage for ${period}: ${activePercentage}% (${activeContractors}/${totalContractors})`,
        );

        return result;
      }

      // Para 'week' y 'month': calcular PROMEDIO de asistencia diaria
      // 2. Obtener contractors activos POR DÍA en el período
      const dailyActiveQuery = `
        SELECT 
          toDate(beat_timestamp) AS day,
          COUNT(DISTINCT contractor_id) AS active_count
        FROM contractor_activity_15s
        WHERE toDate(beat_timestamp) >= '${fromStr}'
          AND toDate(beat_timestamp) <= '${toStr}'
        GROUP BY day
        ORDER BY day
      `;

      const dailyResults = await this.clickHouseService.query<{
        day: string;
        active_count: number;
      }>(dailyActiveQuery);

      // Calcular el número de días en el período
      const daysDiff =
        Math.ceil(
          (today.getTime() - fromDate.getTime()) / (1000 * 60 * 60 * 24),
        ) + 1;

      // Crear mapa de días con actividad
      const dailyMap = new Map<string, number>();
      for (const row of dailyResults) {
        // Normalizar fecha a string YYYY-MM-DD
        const dayStr =
          typeof row.day === 'string'
            ? row.day.split('T')[0]
            : new Date(row.day).toISOString().split('T')[0];
        dailyMap.set(dayStr, Number(row.active_count) || 0);
      }

      // Calcular promedio de asistencia diaria
      // Incluir días sin actividad como 0
      let totalDailyPercentages = 0;
      const dailyBreakdown: Array<{
        date: string;
        active: number;
        percentage: number;
      }> = [];

      for (
        let d = new Date(fromDate);
        d <= today;
        d.setUTCDate(d.getUTCDate() + 1)
      ) {
        const dayStr = d.toISOString().split('T')[0];
        const activeOnDay = dailyMap.get(dayStr) || 0;
        const percentageOnDay =
          totalContractors > 0 ? (activeOnDay / totalContractors) * 100 : 0;
        totalDailyPercentages += percentageOnDay;
        dailyBreakdown.push({
          date: dayStr,
          active: activeOnDay,
          percentage: Math.round(percentageOnDay * 100) / 100,
        });
      }

      // Promedio de porcentajes diarios
      const avgActivePercentage =
        daysDiff > 0
          ? Math.round((totalDailyPercentages / daysDiff) * 100) / 100
          : 0;
      const avgInactivePercentage =
        Math.round((100 - avgActivePercentage) * 100) / 100;

      // Para los contadores, usar el promedio de contractors activos por día
      const avgActiveContractors =
        daysDiff > 0
          ? Math.round(
              dailyBreakdown.reduce((sum, d) => sum + d.active, 0) / daysDiff,
            )
          : 0;
      const avgInactiveContractors = totalContractors - avgActiveContractors;

      const result = {
        active_percentage: avgActivePercentage,
        inactive_percentage: avgInactivePercentage,
        total_contractors: totalContractors,
        active_contractors: avgActiveContractors,
        inactive_contractors: avgInactiveContractors,
        period: periodStr,
        daily_breakdown: dailyBreakdown,
      };

      if (useCache) {
        this.cache.set(cacheKey, {
          data: result,
          expires: Date.now() + this.CACHE_TTL_MS,
        });
      }

      this.logger.debug(
        `Active talent percentage for ${period}: ${avgActivePercentage}% avg over ${daysDiff} days (breakdown: ${dailyBreakdown.map((d) => `${d.date}:${d.percentage}%`).join(', ')})`,
      );

      return result;
    } catch (error) {
      this.logger.error(`Error calculating active talent percentage: ${error}`);
      throw error;
    }
  }
}
