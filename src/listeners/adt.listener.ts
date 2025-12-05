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
      return results.map((row: any) => ({
        ...row,
        workday:
          typeof row.workday === 'string'
            ? row.workday.split('T')[0]
            : row.workday instanceof Date
              ? row.workday.toISOString().split('T')[0]
              : row.workday,
      }));
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
   */
  @MessagePattern(getMessagePattern('adt.getSessionSummaries'))
  async getSessionSummaries(
    @Payload() data: { contractorId: string; days: number },
  ) {
    try {
      const { contractorId, days } = data;
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
          AND toDate(session_start) >= today() - ${days}
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
          active_beats,
          created_at
        FROM app_usage_summary FINAL
        WHERE ${where}
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

  /**
   * Ejecuta ETL para procesar uso de aplicaciones.
   */
  @MessagePattern(getMessagePattern('adt.processAppUsage'))
  async processAppUsage(@Payload() data: { from?: string; to?: string }) {
    try {
      const { from, to } = data;
      const fromDate = from ? new Date(from) : undefined;
      const toDate = to ? new Date(to) : undefined;

      const count = await this.etlService.processEventsToAppUsage(
        fromDate,
        toDate,
      );

      return {
        message: 'App usage processed successfully',
        count,
      };
    } catch (error) {
      logError(this.logger, 'Error in processAppUsage', error);
      throw error;
    }
  }

  /**
   * Ejecuta ETL FORCE para procesar uso de aplicaciones (DELETE + INSERT).
   */
  @MessagePattern(getMessagePattern('adt.processAppUsageForce'))
  async processAppUsageForce(@Payload() data: { from?: string; to?: string }) {
    try {
      const { from, to } = data;
      const fromDate = from ? new Date(from) : undefined;
      const toDate = to ? new Date(to) : undefined;

      const count = await this.etlService.processEventsToAppUsageForce(
        fromDate,
        toDate,
      );

      return {
        message: 'App usage processed (force) successfully',
        count,
      };
    } catch (error) {
      logError(this.logger, 'Error in processAppUsageForce', error);
      throw error;
    }
  }
}
