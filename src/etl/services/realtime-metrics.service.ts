import { Injectable, Logger } from '@nestjs/common';

import { envs } from 'config';
import { ClickHouseService } from '../../clickhouse/clickhouse.service';
import { UsageDataService } from './usage-data.service';
import { ContractorActivity15sDto } from '../dto/contractor-activity-15s.dto';
import { ActivityToDailyMetricsTransformer } from '../transformers/activity-to-daily-metrics.transformer';
import { RedisKeys, RedisService } from 'src/redis';

@Injectable()
export class RealtimeMetricsService {
  private readonly logger = new Logger(RealtimeMetricsService.name);

  constructor(
    private readonly clickHouseService: ClickHouseService,
    private readonly redisService: RedisService,
    private readonly usageDataService: UsageDataService,
    private readonly activityToDailyMetricsTransformer: ActivityToDailyMetricsTransformer,
  ) {}

  /**
   * Calcula métricas de productividad en tiempo real para un contractor.
   * El caché será manejado por Redis.
   *
   * @param contractorId ID del contractor
   * @param workday Fecha del día (por defecto: hoy)
   * @returns Métricas de productividad del día
   */
  async getRealtimeMetrics(contractorId: string, workday?: Date) {
    // Crear una copia del Date para no modificar el original
    const workdayDate = workday ? new Date(workday) : new Date();
    workdayDate.setUTCHours(0, 0, 0, 0); // Usar UTC para evitar problemas de zona horaria
    const workdayStr = workdayDate.toISOString().split('T')[0];

    const cacheKey = RedisKeys.realTimeMetricsByContractor(
      contractorId,
      workdayStr,
    );

    return this.redisService.getOrSet(
      cacheKey,
      async () => {
        // Calcular métricas desde contractor_activity_15s
        const metrics = await this.calculateMetrics(contractorId, workdayDate);

        // Asegurar que workday se devuelva como string YYYY-MM-DD
        const result = {
          ...metrics,
          workday: workdayStr, // Usar el string formateado en lugar del Date
          is_realtime: true,
          calculated_at: new Date().toISOString(),
        };

        return result;
      },
      envs.redis.ttl,
    );
  }

  /**
   * Consolidar beats por timestamp para contratistas con múltiples agentes.
   * Si al menos un agente está activo en un timestamp → el contratista está activo.
   * Suma los inputs de todos los agentes en el mismo timestamp.
   */
  private consolidateBeatsByTimestamp(
    beats: ContractorActivity15sDto[],
  ): ContractorActivity15sDto[] {
    // Agrupar por beat_timestamp (usando getTime() para comparación)
    const beatsByTimestamp = new Map<number, ContractorActivity15sDto[]>();

    for (const beat of beats) {
      const timestamp =
        beat.beat_timestamp instanceof Date
          ? beat.beat_timestamp.getTime()
          : new Date(beat.beat_timestamp).getTime();
      if (!beatsByTimestamp.has(timestamp)) {
        beatsByTimestamp.set(timestamp, []);
      }
      beatsByTimestamp.get(timestamp)!.push(beat);
    }

    // Consolidar cada grupo
    const consolidated: ContractorActivity15sDto[] = [];
    for (const [timestamp, group] of beatsByTimestamp) {
      const consolidatedBeat: ContractorActivity15sDto = {
        contractor_id: group[0].contractor_id,
        agent_id: null, // Ya no es relevante a nivel consolidado
        session_id: group[0].session_id, // Tomar el primero (o hacer merge lógico)
        agent_session_id: null,
        beat_timestamp: new Date(timestamp),
        is_idle: group.every((b) => b.is_idle), // true solo si TODOS están idle
        keyboard_count: group.reduce(
          (sum, b) => sum + (b.keyboard_count || 0),
          0,
        ),
        mouse_clicks: group.reduce((sum, b) => sum + (b.mouse_clicks || 0), 0),
      };
      consolidated.push(consolidatedBeat);
    }

    return consolidated.sort(
      (a, b) =>
        (a.beat_timestamp instanceof Date
          ? a.beat_timestamp.getTime()
          : new Date(a.beat_timestamp).getTime()) -
        (b.beat_timestamp instanceof Date
          ? b.beat_timestamp.getTime()
          : new Date(b.beat_timestamp).getTime()),
    );
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

    // ✅ CONSOLIDACIÓN MULTI-AGENTE: Consolidar beats por timestamp antes de calcular métricas
    // Esto evita que agentes idle en segundo plano penalicen la productividad
    // cuando otro agente está activo en el mismo intervalo de 15s
    const consolidatedBeats = this.consolidateBeatsByTimestamp(beats);

    // Obtener AppUsage y Browser para este día (usando servicio compartido)
    const appUsage = await this.usageDataService.getAppUsageForDay(
      contractorId,
      workday,
    );
    const browserUsage = await this.usageDataService.getBrowserUsageForDay(
      contractorId,
      workday,
    );

    // Calcular métricas usando el transformer con beats consolidados
    const metrics = this.activityToDailyMetricsTransformer.aggregate(
      contractorId,
      workday,
      consolidatedBeats, // ← Pasar beats consolidados
      appUsage,
      browserUsage,
    );

    return {
      ...metrics,
      workday: workdayStr, // Devolver como string en lugar de Date
      app_usage: appUsage.map((a) => ({
        appName: a.appName,
        seconds: a.seconds,
        type: a.type,
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
   * Obtiene métricas de productividad consolidadas para un contractor (todos los agentes juntos).
   * Este método usa la consolidación multi-agente implementada.
   *
   * @param contractorId ID del contractor
   * @param workday Fecha del día (por defecto: hoy)
   * @returns Métricas consolidadas de productividad
   */
  async getConsolidatedProductivity(
    contractorId: string,
    workday?: Date,
  ): Promise<any> {
    // Reutilizar getRealtimeMetrics que ya consolida beats por timestamp
    return await this.getRealtimeMetrics(contractorId, workday);
  }

  /**
   * Obtiene métricas de productividad granuladas por agente para un contractor.
   * Cada agente tiene sus propias métricas calculadas independientemente.
   *
   * @param contractorId ID del contractor
   * @param workday Fecha del día (por defecto: hoy)
   * @returns Objeto con métricas por agente: { [agentId]: metrics }
   */
  async getProductivityByAgent(
    contractorId: string,
    workday?: Date,
  ): Promise<{
    contractor_id: string;
    workday: string;
    agents: Record<
      string,
      {
        agent_id: string;
        total_beats: number;
        active_beats: number;
        idle_beats: number;
        active_percentage: number;
        total_keyboard_inputs: number;
        total_mouse_clicks: number;
        avg_keyboard_per_min: number;
        avg_mouse_per_min: number;
        total_session_time_seconds: number;
        effective_work_seconds: number;
        productivity_score: number;
        app_usage?: {
          appName: string;
          seconds: number;
          type?: string;
        }[];
        browser_usage?: {
          domain: string;
          seconds: number;
        }[];
      }
    >;
  }> {
    const workdayDate = workday ? new Date(workday) : new Date();
    workdayDate.setUTCHours(0, 0, 0, 0);
    const workdayStr = workdayDate.toISOString().split('T')[0];

    // Obtener todos los agent_ids únicos para este contractor en este día
    const agentsQuery = `
      SELECT DISTINCT agent_id
      FROM contractor_activity_15s
      WHERE contractor_id = '${contractorId}'
        AND toDate(beat_timestamp) = '${workdayStr}'
        AND agent_id IS NOT NULL
      ORDER BY agent_id
    `;

    const agentsResult = await this.clickHouseService.query<{
      agent_id: string;
    }>(agentsQuery);

    if (agentsResult.length === 0) {
      return {
        contractor_id: contractorId,
        workday: workdayStr,
        agents: {},
      };
    }

    const agents: Record<string, any> = {};

    // Calcular métricas para cada agente
    for (const { agent_id } of agentsResult) {
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
          AND agent_id = '${agent_id}'
          AND toDate(beat_timestamp) = '${workdayStr}'
        ORDER BY beat_timestamp
      `;

      const beats =
        await this.clickHouseService.query<ContractorActivity15sDto>(
          beatsQuery,
        );

      if (beats.length === 0) {
        continue;
      }

      // Convertir timestamps
      for (const beat of beats) {
        if (typeof beat.beat_timestamp === 'string') {
          beat.beat_timestamp = new Date(beat.beat_timestamp);
        }
      }

      // Obtener AppUsage y Browser para este agente (filtrar por agent_id en events_raw)
      const appUsage = await this.usageDataService.getAppUsageForDay(
        contractorId,
        workdayDate,
        agent_id, // Filtrar por agente
      );
      const browserUsage = await this.usageDataService.getBrowserUsageForDay(
        contractorId,
        workdayDate,
        agent_id, // Filtrar por agente
      );

      // Calcular métricas usando el transformer (SIN consolidación, cada agente por separado)
      const metrics = this.activityToDailyMetricsTransformer.aggregate(
        contractorId,
        workdayDate,
        beats, // Beats sin consolidar (solo de este agente)
        appUsage,
        browserUsage,
      );

      agents[agent_id] = {
        agent_id,
        total_beats: metrics.total_beats,
        active_beats: metrics.active_beats,
        idle_beats: metrics.idle_beats,
        active_percentage: metrics.active_percentage,
        total_keyboard_inputs: metrics.total_keyboard_inputs,
        total_mouse_clicks: metrics.total_mouse_clicks,
        avg_keyboard_per_min: metrics.avg_keyboard_per_min,
        avg_mouse_per_min: metrics.avg_mouse_per_min,
        total_session_time_seconds: metrics.total_session_time_seconds,
        effective_work_seconds: metrics.effective_work_seconds,
        productivity_score: metrics.productivity_score,
        app_usage: appUsage.map((a) => ({
          appName: a.appName,
          seconds: a.seconds,
          type: a.type,
        })),
        browser_usage: browserUsage.map((b) => ({
          domain: b.domain,
          seconds: b.seconds,
        })),
      };
    }

    return {
      contractor_id: contractorId,
      workday: workdayStr,
      agents,
    };
  }

  /**
   * Obtiene métricas de productividad granuladas por agente para un contractor en un rango de fechas.
   * Agrega todos los beats del rango y calcula las métricas por agente.
   *
   * @param contractorId ID del contractor
   * @param fromDate Fecha de inicio (inclusive)
   * @param toDate Fecha de fin (inclusive)
   */
  async getProductivityByAgentForDateRange(
    contractorId: string,
    fromDate: Date,
    toDate: Date,
  ): Promise<{
    contractor_id: string;
    from: string;
    to: string;
    agents: Record<
      string,
      {
        agent_id: string;
        total_beats: number;
        active_beats: number;
        idle_beats: number;
        active_percentage: number;
        total_keyboard_inputs: number;
        total_mouse_clicks: number;
        avg_keyboard_per_min: number;
        avg_mouse_per_min: number;
        total_session_time_seconds: number;
        effective_work_seconds: number;
        productivity_score: number;
        app_usage?: {
          appName: string;
          seconds: number;
          type?: string;
        }[];
        browser_usage?: {
          domain: string;
          seconds: number;
        }[];
      }
    >;
  }> {
    const from = new Date(fromDate);
    from.setUTCHours(0, 0, 0, 0);
    const to = new Date(toDate);
    to.setUTCHours(23, 59, 59, 999);

    const fromStr = from.toISOString().split('T')[0];
    const toStr = to.toISOString().split('T')[0];

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
        AND workday >= '${fromStr}'
        AND workday <= '${toStr}'
        AND agent_id IS NOT NULL
      ORDER BY agent_id, beat_timestamp
    `;

    const beats =
      await this.clickHouseService.query<ContractorActivity15sDto>(beatsQuery);

    if (beats.length === 0) {
      return {
        contractor_id: contractorId,
        from: fromStr,
        to: toStr,
        agents: {},
      };
    }

    // Agrupar beats por agent_id
    const beatsByAgent = new Map<string, ContractorActivity15sDto[]>();
    for (const beat of beats) {
      const agentId = String(beat.agent_id);
      if (!beatsByAgent.has(agentId)) {
        beatsByAgent.set(agentId, []);
      }
      beatsByAgent.get(agentId)!.push(beat);
    }

    const agents: Record<string, any> = {};

    for (const [agentId, agentBeats] of beatsByAgent) {
      // Convertir timestamps
      for (const beat of agentBeats) {
        if (typeof beat.beat_timestamp === 'string') {
          beat.beat_timestamp = new Date(beat.beat_timestamp);
        }
      }

      // AppUsage y Browser por agente en el rango
      const appUsage = await this.usageDataService.getAppUsageForDateRange(
        contractorId,
        from,
        to,
        undefined,
        agentId,
      );
      const browserUsage =
        await this.usageDataService.getBrowserUsageForDateRange(
          contractorId,
          from,
          to,
          undefined,
          agentId,
        );

      // Calcular métricas usando el transformer sobre todos los beats del rango
      const metrics = this.activityToDailyMetricsTransformer.aggregate(
        contractorId,
        from,
        agentBeats,
        appUsage,
        browserUsage,
      );

      agents[agentId] = {
        agent_id: agentId,
        total_beats: metrics.total_beats,
        active_beats: metrics.active_beats,
        idle_beats: metrics.idle_beats,
        active_percentage: metrics.active_percentage,
        total_keyboard_inputs: metrics.total_keyboard_inputs,
        total_mouse_clicks: metrics.total_mouse_clicks,
        avg_keyboard_per_min: metrics.avg_keyboard_per_min,
        avg_mouse_per_min: metrics.avg_mouse_per_min,
        total_session_time_seconds: metrics.total_session_time_seconds,
        effective_work_seconds: metrics.effective_work_seconds,
        productivity_score: metrics.productivity_score,
        app_usage: appUsage.map((a) => ({
          appName: a.appName,
          seconds: a.seconds,
          type: a.type,
        })),
        browser_usage: browserUsage.map((b) => ({
          domain: b.domain,
          seconds: b.seconds,
        })),
      };
    }

    return {
      contractor_id: contractorId,
      from: fromStr,
      to: toStr,
      agents,
    };
  }

  /**
   * Calcula métricas de productividad consolidadas (multi-agente) para un contractor en un rango de fechas,
   * usando directamente los beats de contractor_activity_15s y la misma fórmula del transformer.
   * Esto mantiene consistencia con las métricas por agente, especialmente cuando solo hay un agente.
   */
  async getConsolidatedProductivityForDateRange(
    contractorId: string,
    fromDate: Date,
    toDate: Date,
  ): Promise<any> {
    const from = new Date(fromDate);
    from.setUTCHours(0, 0, 0, 0);
    const to = new Date(toDate);
    to.setUTCHours(23, 59, 59, 999);

    const fromStr = from.toISOString().split('T')[0];
    const toStr = to.toISOString().split('T')[0];

    // Leer beats consolidados por timestamp (multi-agente) en el rango
    const beatsQuery = `
      SELECT
        contractor_id,
        toDate(beat_timestamp) AS workday,
        beat_timestamp,
        min(is_idle) AS is_idle,
        sum(keyboard_count) AS keyboard_count,
        sum(mouse_clicks) AS mouse_clicks
      FROM contractor_activity_15s
      WHERE contractor_id = '${contractorId}'
        AND workday >= toDate('${fromStr}')
        AND workday <= toDate('${toStr}')
      GROUP BY contractor_id, workday, beat_timestamp
      ORDER BY beat_timestamp
    `;

    const rows = await this.clickHouseService.query<{
      contractor_id: string;
      workday: string;
      beat_timestamp: string;
      is_idle: number;
      keyboard_count: number;
      mouse_clicks: number;
    }>(beatsQuery);

    if (rows.length === 0) {
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

    const beats: ContractorActivity15sDto[] = rows.map((row) => ({
      contractor_id: row.contractor_id,
      agent_id: null,
      session_id: null,
      agent_session_id: null,
      beat_timestamp: new Date(row.beat_timestamp),
      is_idle: Boolean(row.is_idle),
      keyboard_count: row.keyboard_count,
      mouse_clicks: row.mouse_clicks,
      workday: new Date(row.workday),
    }));

    // AppUsage y Browser para todo el contractor en el rango (todos los agentes)
    const appUsage = await this.usageDataService.getAppUsageForDateRange(
      contractorId,
      from,
      to,
    );
    const browserUsage =
      await this.usageDataService.getBrowserUsageForDateRange(
        contractorId,
        from,
        to,
      );

    const metrics = this.activityToDailyMetricsTransformer.aggregate(
      contractorId,
      from,
      beats,
      appUsage,
      browserUsage,
    );

    return {
      contractor_id: contractorId,
      workday: `${fromStr} to ${toStr}`,
      total_beats: metrics.total_beats,
      active_beats: metrics.active_beats,
      idle_beats: metrics.idle_beats,
      active_percentage: metrics.active_percentage,
      total_keyboard_inputs: metrics.total_keyboard_inputs,
      total_mouse_clicks: metrics.total_mouse_clicks,
      avg_keyboard_per_min: metrics.avg_keyboard_per_min,
      avg_mouse_per_min: metrics.avg_mouse_per_min,
      total_session_time_seconds: metrics.total_session_time_seconds,
      effective_work_seconds: metrics.effective_work_seconds,
      productivity_score: metrics.productivity_score,
      app_usage: appUsage.map((a) => ({
        appName: a.appName,
        seconds: a.seconds,
        type: a.type,
      })),
      browser_usage: browserUsage.map((b) => ({
        domain: b.domain,
        seconds: b.seconds,
      })),
      is_realtime: false,
    };
  }

  /**
   * Obtiene métricas en tiempo real de todos los contratistas que tienen métricas para un día específico.
   * Solo devuelve contratistas que tienen datos (total_beats > 0).
   * Enriquece los datos con información del contractor (nombre, email, job_position, country, client, team).
   * El caché será manejado por Redis.
   *
   * @param workday Fecha del día (por defecto: hoy)
   * @param filters Filtros opcionales para filtrar contractors
   * @returns Array de métricas de productividad por contractor con información enriquecida
   */
  async getAllRealtimeMetrics(
    workday?: Date,
    filters?: {
      name?: string;
      job_position?: string;
      country?: string;
      client_id?: string;
      team_id?: string;
    },
  ): Promise<any[]> {
    const workdayDate = workday ? new Date(workday) : new Date();
    workdayDate.setUTCHours(0, 0, 0, 0);
    const workdayStr = workdayDate.toISOString().split('T')[0];

    const cacheKey = RedisKeys.allRealTimeMetricsByWorkday(workdayStr, filters);

    return this.redisService.getOrSet(
      cacheKey,
      async () => {
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

        let contractorsQuery = `
          SELECT DISTINCT contractor_id
          FROM contractor_activity_15s
          WHERE toDate(beat_timestamp) = '${workdayStr}'
        `;

        if (contractorIds.length > 0) {
          const contractorIdsList = contractorIds
            .map((id) => `'${id}'`)
            .join(',');
          contractorsQuery += ` AND contractor_id IN (${contractorIdsList})`;
        }

        const contractors = await this.clickHouseService.query<{
          contractor_id: string;
        }>(contractorsQuery);

        if (contractors.length === 0) {
          return [];
        }

        const metricsPromises = contractors.map((contractor) =>
          this.getRealtimeMetrics(contractor.contractor_id, workdayDate),
        );

        const allMetrics = await Promise.all(metricsPromises);

        const metricsWithData = allMetrics.filter(
          (metric) => metric.total_beats > 0,
        );

        if (metricsWithData.length === 0) {
          return [];
        }

        const enrichedMetrics =
          await this.enrichMetricsWithContractorInfo(metricsWithData);

        return enrichedMetrics;
      },
      envs.redis.ttl,
    );
  }

  /**
   * Obtiene métricas en tiempo real agregadas por rango de fechas para todos los contratistas.
   * OPTIMIZADO: Usa contractor_daily_metrics para rangos históricos (95% más rápido).
   * Solo calcula en tiempo real para el día actual.
   * El caché será manejado por Redis.
   *
   * @param fromDate Fecha de inicio del rango
   * @param toDate Fecha de fin del rango
   * @param filters Filtros opcionales para filtrar contractors
   * @returns Array de métricas agregadas por contractor con información enriquecida
   */
  async getAllRealtimeMetricsByDateRange(
    fromDate: Date,
    toDate: Date,
    filters?: {
      name?: string;
      job_position?: string;
      country?: string;
      client_id?: string;
      team_id?: string;
    },
  ): Promise<any[]> {
    const from = new Date(fromDate);
    from.setUTCHours(0, 0, 0, 0);
    const to = new Date(toDate);
    to.setUTCHours(23, 59, 59, 999);

    const fromStr = from.toISOString().split('T')[0];
    const toStr = to.toISOString().split('T')[0];
    const cacheKey = RedisKeys.allRealTimeMetricsByDateRange(
      fromStr,
      toStr,
      filters,
    );

    return this.redisService.getOrSet(
      cacheKey,
      async () => {
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

        const allMetrics = await this.getAggregatedMetricsFromDailyTable(
          fromStr,
          toStr,
          contractorIds,
        );

        if (allMetrics.length === 0) {
          return [];
        }

        const enrichedMetrics =
          await this.enrichMetricsWithContractorInfo(allMetrics);

        return enrichedMetrics;
      },
      envs.redis.ttl,
    );
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
    const fromDate = new Date(fromStr);
    const toDate = new Date(toStr);
    toDate.setUTCHours(23, 59, 59, 999);

    const resultContractorIds = results.map((r) => r.contractor_id);

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

  async getRealtimeMetricsForDateRange(
    contractorId: string,
    fromDate: Date,
    toDate: Date,
  ): Promise<any> {
    const from = new Date(fromDate);
    from.setUTCHours(0, 0, 0, 0);
    const to = new Date(toDate);
    to.setUTCHours(23, 59, 59, 999);

    const fromStr = from.toISOString().split('T')[0];
    const toStr = to.toISOString().split('T')[0];
    const cacheKey = RedisKeys.realTimeMetricsByContractorRange(
      contractorId,
      fromStr,
      toStr,
    );

    return this.redisService.getOrSet(
      cacheKey,
      async () => {
        const metrics = await this.calculateMetricsForDateRange(
          contractorId,
          from,
          to,
        );

        // Enriquecer con información del contractor
        const enrichedMetrics = await this.enrichMetricsWithContractorInfo([
          metrics,
        ]);

        const result =
          enrichedMetrics.length > 0 ? enrichedMetrics[0] : metrics;

        return result;
      },
      envs.redis.ttl,
    );
  }

  private async calculateMetricsForDateRange(
    contractorId: string,
    fromDate: Date,
    toDate: Date,
  ): Promise<any> {
    const fromStr = fromDate.toISOString().split('T')[0];
    const toStr = toDate.toISOString().split('T')[0];

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
        type: a.type,
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
   * Obtiene el top 5 de rankings de productividad (mejores o peores).
   * Función unificada que combina getTop5BestRanking y getTop5WorstRanking.
   * El caché será manejado por Redis.
   *
   * @param period Período: 'day' (día actual), 'week' (última semana), 'month' (mes actual)
   * @param order Orden: 'best' (mejores) o 'worst' (peores)
   * @returns Top 5 contractors según el orden especificado
   */
  async getTopRanking(
    period: 'day' | 'week' | 'month' = 'day',
    order: 'best' | 'worst' = 'best',
  ): Promise<any[]> {
    const cacheKey = RedisKeys.topRanking(period, order);

    return this.redisService.getOrSet(
      cacheKey,
      async () => {
        let allMetrics: any[];

        if (period === 'day') {
          const today = new Date();
          allMetrics = await this.getAllRealtimeMetrics(today, undefined);
        } else {
          const today = new Date();
          today.setUTCHours(23, 59, 59, 999);
          let fromDate: Date;

          if (period === 'week') {
            fromDate = new Date(today);
            fromDate.setUTCDate(fromDate.getUTCDate() - 6);
            fromDate.setUTCHours(0, 0, 0, 0);
          } else {
            fromDate = new Date(today);
            fromDate.setUTCDate(1);
            fromDate.setUTCHours(0, 0, 0, 0);
          }

          allMetrics = await this.getAllRealtimeMetricsByDateRange(
            fromDate,
            today,
            undefined,
          );
        }

        const sorted = allMetrics
          .filter(
            (m) =>
              m.productivity_score !== null &&
              m.productivity_score !== undefined &&
              !isNaN(Number(m.productivity_score)) &&
              (order === 'best'
                ? Number(m.productivity_score) > 0
                : Number(m.productivity_score) >= 0),
          )
          .sort((a, b) =>
            order === 'best'
              ? (b.productivity_score || 0) - (a.productivity_score || 0)
              : (a.productivity_score || 0) - (b.productivity_score || 0),
          )
          .slice(0, 5);

        this.logger.debug(
          `getTopRanking(${period}, ${order}): Found ${sorted.length} contractors from ${allMetrics.length} total`,
        );

        return sorted;
      },
      envs.redis.ttl,
    );
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
   * El caché será manejado por Redis.
   * @param period 'day' (día actual), 'week' (última semana), 'month' (mes actual)
   * @returns Objeto con porcentajes y conteos de contractors activos/inactivos
   */
  async getActiveTalentPercentage(
    period: 'day' | 'week' | 'month' = 'day',
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
    const cacheKey = RedisKeys.activeTalentPercentage(period);

    return this.redisService.getOrSet(
      cacheKey,
      async () => {
        const dbName = envs.clickhouse.database;

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

        try {
          const totalContractorsQuery = `
        SELECT COUNT(DISTINCT contractor_id) AS total
        FROM ${dbName}.contractor_info_raw FINAL
      `;

          const totalResult = await this.clickHouseService.query<{
            total: number;
          }>(totalContractorsQuery);
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

            return result;
          }

          if (period === 'day') {
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
                ? Math.round(
                    (activeContractors / totalContractors) * 100 * 100,
                  ) / 100
                : 0;
            const inactiveContractors = totalContractors - activeContractors;
            const inactivePercentage =
              totalContractors > 0
                ? Math.round(
                    (inactiveContractors / totalContractors) * 100 * 100,
                  ) / 100
                : 100;

            const result = {
              active_percentage: activePercentage,
              inactive_percentage: inactivePercentage,
              total_contractors: totalContractors,
              active_contractors: activeContractors,
              inactive_contractors: inactiveContractors,
              period: periodStr,
            };

            this.logger.debug(
              `Active talent percentage for ${period}: ${activePercentage}% (${activeContractors}/${totalContractors})`,
            );

            return result;
          }

          // Para 'week' y 'month': obtener datos y calcular todo en SQL
          // Query optimizada que genera todas las fechas del rango y calcula porcentajes
          const dailyBreakdownQuery = `
        WITH 
          toDate('${fromStr}') AS start_date,
          toDate('${toStr}') AS end_date,
          dateDiff('day', start_date, end_date) + 1 AS total_days,
          ${totalContractors} AS total_contractors
        SELECT 
          toString(day) AS date,
          active_count AS active,
          round(active_count * 100.0 / total_contractors, 2) AS percentage
        FROM (
          SELECT 
            arrayJoin(arrayMap(x -> addDays(start_date, x), range(toUInt32(total_days)))) AS day
        ) dates
        LEFT JOIN (
          SELECT 
            toDate(beat_timestamp) AS activity_day,
            COUNT(DISTINCT contractor_id) AS active_count
          FROM contractor_activity_15s
          WHERE toDate(beat_timestamp) >= '${fromStr}'
            AND toDate(beat_timestamp) <= '${toStr}'
          GROUP BY activity_day
        ) activity ON dates.day = activity.activity_day
        ORDER BY day
      `;

          const dailyBreakdown = await this.clickHouseService.query<{
            date: string;
            active: number;
            percentage: number;
          }>(dailyBreakdownQuery);

          // Formatear fechas y asegurar valores numéricos
          const formattedBreakdown = dailyBreakdown.map((row) => ({
            date:
              typeof row.date === 'string' ? row.date.split('T')[0] : row.date,
            active: Number(row.active) || 0,
            percentage: Number(row.percentage) || 0,
          }));

          // Calcular promedios desde los resultados
          const daysDiff = formattedBreakdown.length;
          const totalActiveSum = formattedBreakdown.reduce(
            (sum, d) => sum + d.active,
            0,
          );
          const totalPercentageSum = formattedBreakdown.reduce(
            (sum, d) => sum + d.percentage,
            0,
          );

          const avgActivePercentage =
            daysDiff > 0
              ? Math.round((totalPercentageSum / daysDiff) * 100) / 100
              : 0;
          const avgInactivePercentage =
            Math.round((100 - avgActivePercentage) * 100) / 100;

          const avgActiveContractors =
            daysDiff > 0 ? Math.round(totalActiveSum / daysDiff) : 0;
          const avgInactiveContractors =
            totalContractors - avgActiveContractors;

          const result = {
            active_percentage: avgActivePercentage,
            inactive_percentage: avgInactivePercentage,
            total_contractors: totalContractors,
            active_contractors: avgActiveContractors,
            inactive_contractors: avgInactiveContractors,
            period: periodStr,
            daily_breakdown: formattedBreakdown,
          };

          this.logger.debug(
            `Active talent percentage for ${period}: ${avgActivePercentage}% avg over ${daysDiff} days`,
          );

          return result;
        } catch (error) {
          this.logger.error(
            `Error calculating active talent percentage: ${error}`,
          );
          throw error;
        }
      },
      envs.redis.ttl,
    );
  }
}
