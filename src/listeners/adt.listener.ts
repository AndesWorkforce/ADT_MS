import { Controller, Logger } from '@nestjs/common';
import { MessagePattern, Payload } from '@nestjs/microservices';

import { getMessagePattern, logError } from 'config';

import { ActivityService } from '../etl/services/activity.service';
import { AppUsageService } from '../etl/services/app-usage.service';
import { AppsSyncService } from '../etl/services/apps-sync.service';
import { DailyMetricsService } from '../etl/services/daily-metrics.service';
import { AppType } from '../etl/dto/app-dimension.dto';
import { EtlService } from '../etl/services/etl.service';
import { RankingService } from '../etl/services/ranking.service';
import { RealtimeMetricsService } from '../etl/services/realtime-metrics.service';
import { SessionSummariesService } from '../etl/services/session-summaries.service';

/**
 * Listener NATS para responder peticiones de ADT desde API_GATEWAY.
 * Todos los endpoints HTTP del API_GATEWAY se resuelven aquí vía NATS.
 *
 * Este listener es delgado: solo recibe requests, delega a servicios y devuelve respuestas.
 * La lógica de negocio, queries y cache están en los servicios correspondientes.
 */
@Controller()
export class AdtListener {
  private readonly logger = new Logger(AdtListener.name);

  constructor(
    // Servicios de lectura (datos pre-calculados)
    private readonly dailyMetricsService: DailyMetricsService,
    private readonly sessionSummariesService: SessionSummariesService,
    private readonly activityService: ActivityService,
    private readonly appUsageService: AppUsageService,
    private readonly rankingService: RankingService,
    // Servicios de tiempo real
    private readonly realtimeMetricsService: RealtimeMetricsService,
    // Servicios ETL
    private readonly etlService: EtlService,
    // Servicios de sincronización
    private readonly appsSyncService: AppsSyncService,
  ) {}

  // ============================================================================
  // ENDPOINTS DE LECTURA - Datos Pre-calculados
  // ============================================================================

  /**
   * Obtiene métricas diarias de un contractor (desde tabla pre-calculada).
   */
  @MessagePattern(getMessagePattern('adt.getDailyMetrics'))
  async getDailyMetrics(
    @Payload() data: { contractorId: string; days: number },
  ) {
    try {
      const { contractorId, days } = data;
      return await this.dailyMetricsService.getDailyMetrics(contractorId, days);
    } catch (error) {
      logError(this.logger, 'Error in getDailyMetrics', error);
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
      return await this.sessionSummariesService.getSessionSummaries(
        contractorId,
        from,
        to,
        days,
      );
    } catch (error) {
      logError(this.logger, 'Error in getSessionSummaries', error);
      throw error;
    }
  }

  /**
   * Obtiene resúmenes de sesión de un contractor agrupados por día.
   * Puede filtrar por rango de fechas (from/to) o por días hacia atrás (days).
   */
  @MessagePattern(getMessagePattern('adt.getSessionSummariesByDay'))
  async getSessionSummariesByDay(
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
      return await this.sessionSummariesService.getSessionSummariesByDay(
        contractorId,
        from,
        to,
        days,
      );
    } catch (error) {
      logError(this.logger, 'Error in getSessionSummariesByDay', error);
      throw error;
    }
  }

  /**
   * Obtiene duración de actividad por hora para un contractor.
   * Útil para gráficos de duración horaria durante la jornada laboral.
   */
  @MessagePattern(getMessagePattern('adt.getHourlyActivity'))
  async getHourlyActivity(
    @Payload()
    data: {
      contractorId: string;
      from?: string;
      to?: string;
      days?: number;
      startHour?: number;
      endHour?: number;
    },
  ) {
    try {
      const {
        contractorId,
        from,
        to,
        days = 30,
        startHour = 8,
        endHour = 17,
      } = data;
      return await this.sessionSummariesService.getHourlyActivity(
        contractorId,
        from,
        to,
        days,
        startHour,
        endHour,
      );
    } catch (error) {
      logError(this.logger, 'Error in getHourlyActivity', error);
      throw error;
    }
  }

  /**
   * Obtiene duración REAL de sesiones por hora para un contractor.
   * Calcula cuánto tiempo de sesión hubo activo DURANTE cada hora específica.
   * Si una sesión cruza varias horas, cada hora muestra solo la porción correspondiente.
   */
  @MessagePattern(getMessagePattern('adt.getHourlySessionDuration'))
  async getHourlySessionDuration(
    @Payload()
    data: {
      contractorId: string;
      from?: string;
      to?: string;
      days?: number;
      startHour?: number;
      endHour?: number;
    },
  ) {
    try {
      const {
        contractorId,
        from,
        to,
        days = 30,
        startHour = 8,
        endHour = 17,
      } = data;
      return await this.sessionSummariesService.getHourlySessionDuration(
        contractorId,
        from,
        to,
        days,
        startHour,
        endHour,
      );
    } catch (error) {
      logError(this.logger, 'Error in getHourlySessionDuration', error);
      throw error;
    }
  }

  /**
   * Obtiene productividad promedio por hora para un contractor.
   * Usa la misma fórmula del ETL con apps y browser (opción B).
   */
  @MessagePattern(getMessagePattern('adt.getHourlyProductivity'))
  async getHourlyProductivity(
    @Payload()
    data: {
      contractorId: string;
      from?: string;
      to?: string;
      days?: number;
      startHour?: number;
      endHour?: number;
    },
  ) {
    try {
      const {
        contractorId,
        from,
        to,
        days = 30,
        startHour = 8,
        endHour = 17,
      } = data;
      return await this.sessionSummariesService.getHourlyProductivity(
        contractorId,
        from,
        to,
        days,
        startHour,
        endHour,
      );
    } catch (error) {
      logError(this.logger, 'Error in getHourlyProductivity', error);
      throw error;
    }
  }

  /**
   * Obtiene la duración promedio de sesiones agrupada dinámicamente.
   * - Sin cliente: Agrupa por cliente
   * - Con cliente: Agrupa por equipo
   * - Con cliente + equipo: Agrupa por contratista individual
   * - Con cliente + equipo + job: Igual, filtrado por job position
   */
  @MessagePattern(getMessagePattern('adt.getGroupedAvgSessionDuration'))
  async getGroupedAvgSessionDuration(
    @Payload()
    data: {
      from?: string;
      to?: string;
      clientId?: string;
      teamId?: string;
      jobPosition?: string;
      country?: string;
      days?: number;
    },
  ) {
    try {
      const {
        from,
        to,
        clientId,
        teamId,
        jobPosition,
        country,
        days = 30,
      } = data;
      return await this.sessionSummariesService.getGroupedAvgSessionDuration(
        from,
        to,
        clientId,
        teamId,
        jobPosition,
        country,
        days,
      );
    } catch (error) {
      logError(this.logger, 'Error in getGroupedAvgSessionDuration', error);
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
      return await this.activityService.getActivity(
        contractorId,
        from,
        to,
        limit,
      );
    } catch (error) {
      logError(this.logger, 'Error in getActivity', error);
      throw error;
    }
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
      return await this.appUsageService.getAppUsage(
        contractorId,
        from,
        to,
        days,
      );
    } catch (error) {
      logError(this.logger, 'Error in getAppUsage', error);
      throw error;
    }
  }

  /**
   * Obtiene ranking de productividad por día (desde tabla pre-calculada).
   */
  @MessagePattern(getMessagePattern('adt.getRanking'))
  async getRanking(@Payload() data: { workday?: string; limit?: number }) {
    try {
      const { workday, limit = 10 } = data;
      return await this.rankingService.getRanking(workday, limit);
    } catch (error) {
      logError(this.logger, 'Error in getRanking', error);
      throw error;
    }
  }

  // ============================================================================
  // ENDPOINTS DE TIEMPO REAL - Calculados on-demand
  // ============================================================================

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
    },
  ) {
    try {
      const { contractorId, workday, from, to } = data;

      if (from && to) {
        const fromDate = new Date(from);
        const toDate = new Date(to);
        return await this.realtimeMetricsService.getRealtimeMetricsForDateRange(
          contractorId,
          fromDate,
          toDate,
        );
      }

      const workdayDate = workday ? new Date(workday) : undefined;
      return await this.realtimeMetricsService.getRealtimeMetrics(
        contractorId,
        workdayDate,
      );
    } catch (error) {
      logError(this.logger, 'Error in getRealtimeMetrics', error);
      throw error;
    }
  }

  /**
   * Obtiene métricas de productividad en tiempo real de todos los contratistas.
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
      } = data;

      const filters = {
        name,
        job_position,
        country,
        client_id,
        team_id,
      };

      if (from && to) {
        const fromDate = new Date(from);
        const toDate = new Date(to);
        return await this.realtimeMetricsService.getAllRealtimeMetricsByDateRange(
          fromDate,
          toDate,
          filters,
        );
      }

      const workdayDate = workday ? new Date(workday) : undefined;
      return await this.realtimeMetricsService.getAllRealtimeMetrics(
        workdayDate,
        filters,
      );
    } catch (error) {
      logError(this.logger, 'Error in getAllRealtimeMetrics', error);
      throw error;
    }
  }

  /**
   * Obtiene top 5 rankings de productividad (mejores o peores).
   * @param period 'day' (día actual), 'week' (última semana), 'month' (mes actual)
   * @param order 'best' (mejores) o 'worst' (peores)
   */
  @MessagePattern(getMessagePattern('adt.getTopRanking'))
  async getTopRanking(
    @Payload()
    data: {
      period?: 'day' | 'week' | 'month';
      order?: 'best' | 'worst';
    },
  ) {
    try {
      const { period = 'day', order = 'best' } = data;
      return await this.realtimeMetricsService.getTopRanking(period, order);
    } catch (error) {
      logError(this.logger, 'Error in getTopRanking', error);
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
    },
  ) {
    try {
      const { period = 'day' } = data;
      return await this.realtimeMetricsService.getActiveTalentPercentage(
        period,
      );
    } catch (error) {
      logError(this.logger, 'Error in getActiveTalentPercentage', error);
      throw error;
    }
  }

  /**
   * Obtiene métricas de productividad consolidadas para un contractor (todos los agentes juntos).
   * Usa la consolidación multi-agente implementada.
   */
  @MessagePattern(getMessagePattern('adt.getConsolidatedProductivity'))
  async getConsolidatedProductivity(
    @Payload()
    data: {
      contractorId: string;
      workday?: string;
    },
  ) {
    try {
      const { contractorId, workday } = data;
      const workdayDate = workday ? new Date(workday) : undefined;
      return await this.realtimeMetricsService.getConsolidatedProductivity(
        contractorId,
        workdayDate,
      );
    } catch (error) {
      logError(this.logger, 'Error in getConsolidatedProductivity', error);
      throw error;
    }
  }

  /**
   * Obtiene métricas de productividad granuladas por agente para un contractor.
   * Cada agente tiene sus propias métricas calculadas independientemente.
   */
  @MessagePattern(getMessagePattern('adt.getProductivityByAgent'))
  async getProductivityByAgent(
    @Payload()
    data: {
      contractorId: string;
      workday?: string;
    },
  ) {
    try {
      const { contractorId, workday } = data;
      const workdayDate = workday ? new Date(workday) : undefined;
      return await this.realtimeMetricsService.getProductivityByAgent(
        contractorId,
        workdayDate,
      );
    } catch (error) {
      logError(this.logger, 'Error in getProductivityByAgent', error);
      throw error;
    }
  }

  // ============================================================================
  // ENDPOINTS ETL - Procesamiento de datos
  // ============================================================================

  /**
   * Ejecuta ETL para procesar eventos RAW → beats de 15s.
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

  // ============================================================================
  // ENDPOINTS DE SINCRONIZACIÓN - Apps desde USER_MS
  // ============================================================================

  /**
   * Sincroniza una aplicación desde USER_MS a ClickHouse.
   * Se llama cuando se crea o actualiza una app en Prisma.
   */
  @MessagePattern(getMessagePattern('adt.syncApp'))
  async syncApp(
    @Payload()
    app: {
      id: string;
      name: string;
      category?: string | null;
      type?: AppType | null;
      weight?: number | null;
      created_at: Date;
      updated_at: Date;
    },
  ) {
    try {
      await this.appsSyncService.syncApp(app);
      return {
        message: 'App synchronized successfully',
        appId: app.id,
      };
    } catch (error) {
      logError(this.logger, 'Error in syncApp', error);
      throw error;
    }
  }

  /**
   * Elimina una aplicación de ClickHouse cuando se elimina en Prisma.
   */
  @MessagePattern(getMessagePattern('adt.deleteApp'))
  async deleteApp(@Payload() appId: string) {
    try {
      await this.appsSyncService.deleteApp(appId);
      return {
        message: 'App deleted successfully',
        appId,
      };
    } catch (error) {
      logError(this.logger, 'Error in deleteApp', error);
      throw error;
    }
  }

  /**
   * Sincroniza todas las apps desde USER_MS a ClickHouse.
   * Útil para sincronización inicial o resincronización completa.
   */
  @MessagePattern(getMessagePattern('adt.syncAllApps'))
  async syncAllApps(
    @Payload()
    apps: Array<{
      id: string;
      name: string;
      category?: string | null;
      type?: string | null;
      weight?: number | null;
      created_at: Date;
      updated_at: Date;
    }>,
  ) {
    try {
      await this.appsSyncService.syncAllApps(apps);
      return {
        message: 'All apps synchronized successfully',
        count: apps.length,
      };
    } catch (error) {
      logError(this.logger, 'Error in syncAllApps', error);
      throw error;
    }
  }
}
