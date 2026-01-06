import { Controller, Logger } from '@nestjs/common';
import { MessagePattern, Payload } from '@nestjs/microservices';

import { getMessagePattern, logError } from 'config';

import { ClickHouseService } from '../clickhouse/clickhouse.service';
import { EtlService } from '../etl/services/etl.service';
import { RealtimeMetricsService } from '../etl/services/realtime-metrics.service';

/**
 * Listener NATS para responder peticiones de ADT desde API_GATEWAY.
 * Todos los endpoints HTTP del API_GATEWAY se resuelven aquí vía NATS.
 */
@Controller()
export class AdtListener {
  private readonly logger = new Logger(AdtListener.name);

  constructor(
    private readonly clickHouseService: ClickHouseService,
    private readonly etlService: EtlService,
    private readonly realtimeMetricsService: RealtimeMetricsService,
  ) {}

  /**
   * Obtiene métricas diarias de un contractor (desde tabla ADT).
   */
  @MessagePattern(getMessagePattern('adt.getDailyMetrics'))
  async getDailyMetrics(
    @Payload() data: { contractorId: string; days: number },
  ) {
    try {
      const { contractorId, days } = data;
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
          AND workday >= today() - ${days}
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

      // Convertir workday de Date a string YYYY-MM-DD para consistencia
      // Convertir app_usage y browser_usage de Map a Array
      return results.map((row: any) => {
        // Convertir app_usage Map a Array
        let appUsage: Array<{ appName: string; seconds: number }> = [];
        if (row.app_usage && typeof row.app_usage === 'object') {
          appUsage = Object.entries(row.app_usage).map(
            ([appName, seconds]) => ({
              appName,
              seconds: Number(seconds) || 0,
            }),
          );
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
                ? row.workday.toISOString().split('T')[0]
                : row.workday,
          app_usage: appUsage,
          browser_usage: browserUsage,
        };
      });
    } catch (error) {
      logError(this.logger, 'Error in getDailyMetrics', error);
      throw error;
    }
  }

  /**
   * Obtiene métricas de productividad en tiempo real para un contractor.
   * Puede recibir:
   * - workday: un día específico (YYYY-MM-DD)
   * - from y to: un rango de fechas (YYYY-MM-DD) - devuelve métricas agregadas
   */
  @MessagePattern(getMessagePattern('adt.getRealtimeMetrics'))
  async getRealtimeMetrics(
    @Payload()
    data: {
      contractorId: string;
      workday?: string;
      from?: string;
      to?: string;
      useCache?: boolean;
    },
  ) {
    try {
      const { contractorId, workday, from, to, useCache = true } = data;

      // Si se proporciona from y to, usar rango de fechas
      if (from && to) {
        const fromDate = new Date(from);
        const toDate = new Date(to);
        return await this.realtimeMetricsService.getRealtimeMetricsForDateRange(
          contractorId,
          fromDate,
          toDate,
          useCache,
        );
      }

      // Si solo se proporciona workday o ninguno, usar comportamiento original
      const workdayDate = workday ? new Date(workday) : undefined;
      return await this.realtimeMetricsService.getRealtimeMetrics(
        contractorId,
        workdayDate,
        useCache,
      );
    } catch (error) {
      logError(this.logger, 'Error in getRealtimeMetrics', error);
      throw error;
    }
  }

  /**
   * Obtiene métricas de productividad en tiempo real de todos los contratistas que tienen métricas.
   * Solo devuelve contratistas que tienen datos (total_beats > 0).
   *
   * Puede recibir:
   * - workday: un día específico (YYYY-MM-DD)
   * - from y to: un rango de fechas (YYYY-MM-DD) - devuelve métricas agregadas
   * - Filtros opcionales: name, job_position, country, client_id, team_id
   */
  @MessagePattern(getMessagePattern('adt.getAllRealtimeMetrics'))
  async getAllRealtimeMetrics(
    @Payload()
    data: {
      workday?: string;
      from?: string;
      to?: string;
      name?: string;
      job_position?: string;
      country?: string;
      client_id?: string;
      team_id?: string;
      useCache?: boolean;
    },
  ) {
    const pattern = getMessagePattern('adt.getAllRealtimeMetrics');
    this.logger.log(`📥 Mensaje recibido en ADT_MS: ${pattern}`);
    this.logger.debug(`📦 Payload recibido: ${JSON.stringify(data)}`);

    try {
      const {
        workday,
        from,
        to,
        name,
        job_position,
        country,
        client_id,
        team_id,
        useCache = true,
      } = data;

      const filters = {
        name,
        job_position,
        country,
        client_id,
        team_id,
      };

      // Si se proporciona from y to, usar rango de fechas
      if (from && to) {
        const fromDate = new Date(from);
        const toDate = new Date(to);
        return await this.realtimeMetricsService.getAllRealtimeMetricsByDateRange(
          fromDate,
          toDate,
          useCache,
          filters,
        );
      }

      // Si solo se proporciona workday o ninguno, usar comportamiento original
      const workdayDate = workday ? new Date(workday) : undefined;
      return await this.realtimeMetricsService.getAllRealtimeMetrics(
        workdayDate,
        useCache,
        filters,
      );
    } catch (error) {
      logError(this.logger, 'Error in getAllRealtimeMetrics', error);
      throw error;
    }
  }

  /**
   * Obtiene resúmenes de sesión de un contractor.
   * Puede filtrar por rango de fechas (from/to) o por días hacia atrás (days).
   */
  @MessagePattern(getMessagePattern('adt.getSessionSummaries'))
  async getSessionSummaries(
    @Payload()
    data: {
      contractorId: string;
      from?: string;
      to?: string;
      days?: number;
    },
  ) {
    try {
      const { contractorId, from, to, days = 30 } = data;

      // Construir filtro de fecha
      let dateFilter: string;
      if (from && to) {
        // Si se proporcionan from y to, usar rango de fechas
        const fromDate = from.split('T')[0]; // Extraer solo YYYY-MM-DD
        const toDate = to.split('T')[0];
        dateFilter = `toDate(session_start) >= '${fromDate}' AND toDate(session_start) <= '${toDate}'`;
      } else {
        // Si no, usar days (por defecto 30)
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
    } catch (error) {
      logError(this.logger, 'Error in getSessionSummaries', error);
      throw error;
    }
  }

  /**
   * Obtiene actividad detallada (beats de 15s) de un contractor.
   */
  @MessagePattern(getMessagePattern('adt.getActivity'))
  async getActivity(
    @Payload()
    data: {
      contractorId: string;
      from?: string;
      to?: string;
      limit?: number;
    },
  ) {
    try {
      const { contractorId, from, to, limit = 1000 } = data;
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
        // Si from es solo fecha (sin hora) o tiene hora 00:00:00, usar inicio del día
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
        // Si to es solo fecha (sin hora) o tiene hora 00:00:00, usar fin del día
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
    } catch (error) {
      logError(this.logger, 'Error in getActivity', error);
      throw error;
    }
  }

  /**
   * Formatea una fecha (ISO string o Date) al formato DateTime de ClickHouse.
   * Formato esperado: 'YYYY-MM-DD HH:MM:SS'
   */
  private formatDateForClickHouse(date: string | Date): string {
    let dateObj: Date;

    if (typeof date === 'string') {
      // Si es string ISO, parsearlo
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

  /**
   * Obtiene uso de aplicaciones de un contractor.
   * Consulta directamente desde events_raw para obtener datos de AppUsage.
   */
  @MessagePattern(getMessagePattern('adt.getAppUsage'))
  async getAppUsage(
    @Payload()
    data: {
      contractorId: string;
      from?: string;
      to?: string;
      days?: number;
    },
  ) {
    try {
      const { contractorId, from, to, days } = data;

      let where = `contractor_id = '${contractorId}'`;
      if (from) {
        const fromDay = (from.includes('T') ? from.split('T')[0] : from).trim();
        where += ` AND toDate(timestamp) >= toDate('${fromDay}')`;
      }
      if (to) {
        const toDay = (to.includes('T') ? to.split('T')[0] : to).trim();
        where += ` AND toDate(timestamp) <= toDate('${toDay}')`;
      }
      // Fallback para compatibilidad: si no hay from/to pero sí days, usar days
      if (!from && !to && typeof days === 'number' && Number.isFinite(days)) {
        where += ` AND toDate(timestamp) >= today() - ${days}`;
      }

      // Consulta directa desde events_raw - agrupa por día y app
      const query = `
        SELECT 
          contractor_id,
          app_name,
          toDate(timestamp) AS workday,
          toUInt32(sum(JSONExtractFloat(payload, 'AppUsage', app_name)) / 15) AS active_beats,
          max(timestamp) AS created_at
        FROM events_raw
        ARRAY JOIN JSONExtractKeys(payload, 'AppUsage') AS app_name
        WHERE ${where} AND JSONHas(payload, 'AppUsage')
        GROUP BY contractor_id, app_name, workday
        ORDER BY workday DESC, active_beats DESC
      `;

      return await this.clickHouseService.query(query);
    } catch (error) {
      logError(this.logger, 'Error in getAppUsage', error);
      throw error;
    }
  }

  /**
   * Obtiene ranking de productividad por día.
   */
  @MessagePattern(getMessagePattern('adt.getRanking'))
  async getRanking(@Payload() data: { workday?: string; limit?: number }) {
    try {
      const { workday, limit = 10 } = data;
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
        query += ` AND workday = today() - 1`;
      }

      query += ` ORDER BY productivity_score DESC LIMIT ${limit}`;

      return await this.clickHouseService.query(query);
    } catch (error) {
      logError(this.logger, 'Error in getRanking', error);
      throw error;
    }
  }

  /**
   * Obtiene top 5 mejores rankings de productividad.
   * @param period 'day' (día actual), 'week' (última semana), 'month' (mes actual)
   */
  @MessagePattern(getMessagePattern('adt.getTop5BestRanking'))
  async getTop5BestRanking(
    @Payload()
    data: {
      period?: 'day' | 'week' | 'month';
      useCache?: boolean;
    },
  ) {
    try {
      const { period = 'day', useCache = true } = data;

      return await this.realtimeMetricsService.getTop5BestRanking(
        period,
        useCache,
      );
    } catch (error) {
      logError(this.logger, 'Error in getTop5BestRanking', error);
      throw error;
    }
  }

  /**
   * Obtiene top 5 peores rankings de productividad.
   * @param period 'day' (día actual), 'week' (última semana), 'month' (mes actual)
   */
  @MessagePattern(getMessagePattern('adt.getTop5WorstRanking'))
  async getTop5WorstRanking(
    @Payload()
    data: {
      period?: 'day' | 'week' | 'month';
      useCache?: boolean;
    },
  ) {
    try {
      const { period = 'day', useCache = true } = data;

      return await this.realtimeMetricsService.getTop5WorstRanking(
        period,
        useCache,
      );
    } catch (error) {
      logError(this.logger, 'Error in getTop5WorstRanking', error);
      throw error;
    }
  }

  /**
   * Obtiene el porcentaje de talento activo vs inactivo en un período.
   * Un contractor se considera "activo" si tiene métricas (beats) en el período.
   * @param period 'day' (día actual), 'week' (última semana), 'month' (mes actual)
   */
  @MessagePattern(getMessagePattern('adt.getActiveTalentPercentage'))
  async getActiveTalentPercentage(
    @Payload()
    data: {
      period?: 'day' | 'week' | 'month';
      useCache?: boolean;
    },
  ) {
    try {
      const { period = 'day', useCache = true } = data;

      return await this.realtimeMetricsService.getActiveTalentPercentage(
        period,
        useCache,
      );
    } catch (error) {
      logError(this.logger, 'Error in getActiveTalentPercentage', error);
      throw error;
    }
  }

  /**
   * Ejecuta ETL para procesar eventos.
   */
  @MessagePattern(getMessagePattern('adt.processEvents'))
  async processEvents(@Payload() data: { from?: string; to?: string }) {
    try {
      const { from, to } = data;
      const fromDate = from ? new Date(from) : undefined;
      const toDate = to ? new Date(to) : undefined;

      const count = await this.etlService.processEventsToActivity(
        fromDate,
        toDate,
      );

      return {
        message: 'Events processed successfully',
        count,
      };
    } catch (error) {
      logError(this.logger, 'Error in processEvents', error);
      throw error;
    }
  }

  /**
   * Ejecuta ETL para procesar eventos (FORCE: usa DELETE + INSERT).
   */
  @MessagePattern(getMessagePattern('adt.processEventsForce'))
  async processEventsForce(@Payload() data: { from?: string; to?: string }) {
    try {
      const { from, to } = data;
      const fromDate = from ? new Date(from) : undefined;
      const toDate = to ? new Date(to) : undefined;

      const count = await this.etlService.processEventsToActivityForce(
        fromDate,
        toDate,
      );

      return {
        message: 'Events processed (force) successfully',
        count,
      };
    } catch (error) {
      logError(this.logger, 'Error in processEventsForce', error);
      throw error;
    }
  }

  /**
   * Ejecuta ETL para procesar métricas diarias.
   * Puede procesar un día específico o un rango de fechas.
   */
  @MessagePattern(getMessagePattern('adt.processDailyMetrics'))
  async processDailyMetrics(
    @Payload() data: { workday?: string; from?: string; to?: string },
  ) {
    try {
      const { workday, from, to } = data;
      const workdayDate = workday ? new Date(workday) : undefined;
      const fromDate = from ? new Date(from) : undefined;
      const toDate = to ? new Date(to) : undefined;

      const metrics = await this.etlService.processActivityToDailyMetrics(
        workdayDate,
        fromDate,
        toDate,
      );

      return {
        message: 'Daily metrics processed successfully',
        count: metrics.length,
        metrics,
      };
    } catch (error) {
      logError(this.logger, 'Error in processDailyMetrics', error);
      throw error;
    }
  }

  /**
   * Ejecuta ETL para procesar resúmenes de sesión.
   */
  @MessagePattern(getMessagePattern('adt.processSessionSummaries'))
  async processSessionSummaries(@Payload() data: { sessionId?: string }) {
    try {
      const { sessionId } = data;

      const summaries =
        await this.etlService.processActivityToSessionSummary(sessionId);

      return {
        message: 'Session summaries processed successfully',
        count: summaries.length,
        summaries,
      };
    } catch (error) {
      logError(this.logger, 'Error in processSessionSummaries', error);
      throw error;
    }
  }
}
