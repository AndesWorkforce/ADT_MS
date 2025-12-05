import { Injectable, Logger } from '@nestjs/common';

import { envs } from 'config';
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
   * Agrega métricas de todos los días del rango en una sola métrica por contractor.
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

    // Construir query para obtener contractors con datos en el rango
    let contractorsQuery = `
      SELECT DISTINCT contractor_id
      FROM contractor_activity_15s
      WHERE toDate(beat_timestamp) >= '${fromStr}'
        AND toDate(beat_timestamp) <= '${toStr}'
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

    // Calcular métricas agregadas para cada contractor en paralelo
    const metricsPromises = contractors.map((contractor) =>
      this.calculateMetricsForDateRange(contractor.contractor_id, from, to),
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
   */
  private async calculateMetricsForDateRange(
    contractorId: string,
    fromDate: Date,
    toDate: Date,
  ): Promise<any> {
    const fromStr = fromDate.toISOString().split('T')[0];
    const toStr = toDate.toISOString().split('T')[0];

    // Leer todos los beats del rango
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
        AND toDate(beat_timestamp) >= '${fromStr}'
        AND toDate(beat_timestamp) <= '${toStr}'
      ORDER BY beat_timestamp
    `;

    const beats =
      await this.clickHouseService.query<ContractorActivity15sDto>(beatsQuery);

    if (beats.length === 0) {
      return {
        contractor_id: contractorId,
        workday: `${fromStr} to ${toStr}`, // Rango de fechas
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

    // Obtener AppUsage y Browser para todo el rango
    const appUsage = await this.getAppUsageForDateRange(
      contractorId,
      fromDate,
      toDate,
    );
    const browserUsage = await this.getBrowserUsageForDateRange(
      contractorId,
      fromDate,
      toDate,
    );

    // Calcular métricas agregadas
    // Usar el primer día del rango como referencia para el transformer
    const aggregatedMetrics = this.activityToDailyMetricsTransformer.aggregate(
      contractorId,
      fromDate,
      beats,
      appUsage,
      browserUsage,
    );

    return {
      ...aggregatedMetrics,
      workday: `${fromStr} to ${toStr}`, // Rango de fechas como string
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
   * Obtiene datos de AppUsage para un contractor en un rango de fechas.
   */
  private async getAppUsageForDateRange(
    contractorId: string,
    fromDate: Date,
    toDate: Date,
  ): Promise<AppUsageData[]> {
    const fromStr = fromDate.toISOString().split('T')[0];
    const toStr = toDate.toISOString().split('T')[0];

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
   */
  private async getBrowserUsageForDateRange(
    contractorId: string,
    fromDate: Date,
    toDate: Date,
  ): Promise<BrowserUsageData[]> {
    const fromStr = fromDate.toISOString().split('T')[0];
    const toStr = toDate.toISOString().split('T')[0];

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
}
