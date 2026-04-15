import { Injectable, Logger } from '@nestjs/common';

import { envs } from 'config';
import { ClickHouseService } from '../../clickhouse/clickhouse.service';
import { RedisKeys, RedisService } from '../../redis';
import { SessionSummaryDto } from '../dto/session-summary.dto';

/**
 * Tipo de agrupación para métricas de sesión
 */
export type GroupByLevel = 'client' | 'team' | 'contractor';

/**
 * Resultado de duración promedio de sesiones agrupada
 */
export interface GroupedAvgDuration {
  group_id: string;
  group_name: string;
  contractor_count: number;
  avg_duration_hours: number;
}

/**
 * Servicio para obtener resúmenes de sesión pre-calculados desde session_summary.
 * Estas métricas son generadas por el ETL y almacenadas en ClickHouse.
 */
@Injectable()
export class SessionSummariesService {
  private readonly logger = new Logger(SessionSummariesService.name);

  constructor(
    private readonly clickHouseService: ClickHouseService,
    private readonly redisService: RedisService,
  ) {}

  private buildDateFilter(
    from?: string,
    to?: string,
    days: number = 30,
  ): string {
    if (from && to) {
      const fromDate = from.split('T')[0];
      const toDate = to.split('T')[0];
      return `toDate(session_start) >= '${fromDate}' AND toDate(session_start) <= '${toDate}'`;
    }
    return `toDate(session_start) >= today() - ${days}`;
  }

  private buildAgentFilter(agentId?: string): {
    effectiveAgentId?: string;
    agentFilterSql: string;
  } {
    const effectiveAgentId =
      agentId && agentId !== 'consolidated' ? agentId : undefined;
    const agentFilterSql = effectiveAgentId
      ? `AND agent_id = '${effectiveAgentId}'`
      : '';
    return { effectiveAgentId, agentFilterSql };
  }

  private buildAgentViewQuery(
    contractorId: string,
    dateFilter: string,
    agentFilterSql: string,
  ): string {
    // Vista por agente: usar duración calendario, escalando active/idle
    return `
      SELECT 
        session_id,
        contractor_id,
        agent_id,
        session_start,
        session_end,
        dateDiff('second', session_start, session_end) AS total_seconds,
        round(
          dateDiff('second', session_start, session_end)
          * active_seconds
          / nullIf(total_seconds, 0)
        ) AS active_seconds,
        greatest(
          0,
          dateDiff('second', session_start, session_end) -
          round(
            dateDiff('second', session_start, session_end)
            * active_seconds
            / nullIf(total_seconds, 0)
          )
        ) AS idle_seconds,
        productivity_score,
        created_at
      FROM session_summary
      WHERE contractor_id = '${contractorId}'
        AND ${dateFilter}
        ${agentFilterSql}
      ORDER BY session_start DESC
    `;
  }

  private buildConsolidatedViewQuery(
    contractorId: string,
    dateFilter: string,
  ): string {
    // Consolidado: una fila por session_id
    return `
      SELECT 
        session_id,
        contractor_id,
        agent_id,
        session_start,
        session_end,
        dateDiff('second', session_start, session_end) AS total_seconds,
        round(
          dateDiff('second', session_start, session_end)
          * active_seconds_sum
          / nullIf(total_seconds_sum, 0)
        ) AS active_seconds,
        greatest(
          0,
          dateDiff('second', session_start, session_end) -
          round(
            dateDiff('second', session_start, session_end)
            * active_seconds_sum
            / nullIf(total_seconds_sum, 0)
          )
        ) AS idle_seconds,
        round(
          productivity_numerator / nullIf(total_seconds_sum, 0),
          2
        ) AS productivity_score,
        created_at
      FROM (
        SELECT 
          session_id,
          contractor_id,
          any(agent_id) AS agent_id,
          min(session_start) AS session_start,
          max(session_end) AS session_end,
          sum(total_seconds) AS total_seconds_sum,
          sum(active_seconds) AS active_seconds_sum,
          sum(productivity_score * total_seconds) AS productivity_numerator,
          max(created_at) AS created_at
        FROM (
          SELECT session_id, contractor_id, agent_id, session_start, session_end,
            total_seconds, active_seconds, idle_seconds, productivity_score, created_at
          FROM session_summary
          WHERE contractor_id = '${contractorId}' AND ${dateFilter}
        ) AS t
        GROUP BY session_id, contractor_id
      ) AS g
      ORDER BY session_start DESC
    `;
  }

  /**
   * Obtiene resúmenes de sesión de un contractor.
   * Con agentId: solo sesiones de ese agente. Sin agentId (consolidado): una fila por session_id (métricas agregadas).
   *
   * @param contractorId ID del contractor
   * @param from Fecha de inicio (opcional)
   * @param to Fecha de fin (opcional)
   * @param days Días hacia atrás (default: 30)
   * @param agentId Si se informa (y no es 'consolidated'), solo sesiones de ese agente; si no, consolidado (una fila por sesión).
   */
  async getSessionSummaries(
    contractorId: string,
    from?: string,
    to?: string,
    days: number = 30,
    agentId?: string,
  ): Promise<SessionSummaryDto[]> {
    const { effectiveAgentId, agentFilterSql } = this.buildAgentFilter(agentId);
    const cacheKey =
      RedisKeys.sessionSummariesByContractor(contractorId, from, to, days) +
      (effectiveAgentId ? `:agent:${effectiveAgentId}` : ':consolidated');

    return this.redisService.getOrSet(
      cacheKey,
      async () => {
        const dateFilter = this.buildDateFilter(from, to, days);

        const query = effectiveAgentId
          ? this.buildAgentViewQuery(contractorId, dateFilter, agentFilterSql)
          : this.buildConsolidatedViewQuery(contractorId, dateFilter);

        return await this.clickHouseService.query<SessionSummaryDto>(query);
      },
      envs.redis.ttl,
    );
  }

  /**
   * Obtiene resúmenes de sesión de un contractor agrupados por día.
   * Con agentId: solo sesiones de ese agente. Sin agentId: consolidado (una fila por session_id por día).
   *
   * @param contractorId ID del contractor
   * @param from Fecha de inicio (opcional)
   * @param to Fecha de fin (opcional)
   * @param days Días hacia atrás (default: 30)
   * @param agentId Si se informa (y no es 'consolidated'), solo sesiones de ese agente; si no, consolidado.
   */
  async getSessionSummariesByDay(
    contractorId: string,
    from?: string,
    to?: string,
    days: number = 30,
    agentId?: string,
  ): Promise<Array<{ session_day: string; sessions: SessionSummaryDto[] }>> {
    const { effectiveAgentId, agentFilterSql } = this.buildAgentFilter(agentId);
    const cacheKey =
      RedisKeys.sessionSummariesByContractor(contractorId, from, to, days) +
      ':by-day' +
      (effectiveAgentId ? `:agent:${effectiveAgentId}` : ':consolidated');

    return this.redisService.getOrSet(
      cacheKey,
      async () => {
        const dateFilter = this.buildDateFilter(from, to, days);

        const query = effectiveAgentId
          ? this.buildAgentViewQuery(contractorId, dateFilter, agentFilterSql)
          : this.buildConsolidatedViewQuery(contractorId, dateFilter);

        const sessions =
          await this.clickHouseService.query<SessionSummaryDto>(query);

        const groupedByDay = new Map<string, SessionSummaryDto[]>();
        sessions.forEach((session) => {
          const sessionDay = (session.session_start as unknown as string).split(
            ' ',
          )[0];
          if (!groupedByDay.has(sessionDay)) {
            groupedByDay.set(sessionDay, []);
          }
          groupedByDay.get(sessionDay)!.push(session);
        });

        const result = Array.from(groupedByDay.entries())
          .map(([session_day, sess]) => ({ session_day, sessions: sess }))
          .sort((a, b) => b.session_day.localeCompare(a.session_day));

        return result;
      },
      envs.redis.ttl,
    );
  }

  /**
   * Obtiene la duración PROMEDIO de actividad por hora para un contractor.
   * Consulta contractor_activity_15s y agrupa por hora, calculando el promedio
   * dividiendo por la cantidad de días únicos con datos en cada hora.
   * Útil para gráficos que muestran el patrón típico de sesiones a lo largo del tiempo.
   *
   * @param contractorId ID del contractor
   * @param from Fecha de inicio (opcional)
   * @param to Fecha de fin (opcional)
   * @param days Días hacia atrás (default: 30)
   * @param startHour Hora de inicio de jornada (default: 8)
   * @param endHour Hora de fin de jornada (default: 17)
   * @returns Array de objetos con promedios por hora
   */
  /**
   * Obtiene la duración de sesiones por hora para un contractor.
   * Para cada hora del día, muestra el tiempo acumulado de sesiones MIENTRAS haya
   * sesión activa o recién terminada. Si no hubo sesión entre la hora anterior
   * y la actual, se resetea a 0.
   *
   * Ejemplo: Si una sesión va de 11:00 a 12:52:
   * - A las 11:00: 0 (la sesión empieza)
   * - A las 12:00: 1h (sesión activa, acumulado desde 11:00)
   * - A las 13:00: 1h 52m (sesión terminó 12:52, se muestra el total)
   * - A las 14:00: 0 (no hubo sesión entre 13:00-14:00, RESET)
   *
   * @param contractorId ID del contractor
   * @param from Fecha de inicio (opcional)
   * @param to Fecha de fin (opcional)
   * @param days Días hacia atrás (default: 30)
   * @param startHour Hora de inicio de jornada (default: 8)
   * @param endHour Hora de fin de jornada (default: 17)
   * @param agentId Si se informa, solo sesiones de ese agente; si no, consolidado (merge de intervalos de todos los agentes).
   * @returns Array de objetos con duración de sesiones por hora
   */
  async getHourlySessionDuration(
    contractorId: string,
    from?: string,
    to?: string,
    days: number = 30,
    startHour: number = 8,
    endHour: number = 17,
    agentId?: string,
  ): Promise<
    Array<{
      hour: number;
      hour_label: string;
      days_with_data: number;
      avg_duration_seconds: number;
    }>
  > {
    const cacheKey =
      RedisKeys.hourlyActivityByContractor(contractorId, from, to, days) +
      `:hours:${startHour}-${endHour}:session-duration-v3` +
      (agentId && agentId !== 'consolidated' ? `:agent:${agentId}` : '');

    return this.redisService.getOrSet(
      cacheKey,
      async () => {
        // Construir filtro de fecha
        let dateFilter: string;
        if (from && to) {
          const fromDate = from.split('T')[0];
          const toDate = to.split('T')[0];
          dateFilter = `toDate(session_start) >= '${fromDate}' AND toDate(session_start) <= '${toDate}'`;
        } else {
          dateFilter = `toDate(session_start) >= today() - ${days}`;
        }

        const agentFilter =
          agentId && agentId !== 'consolidated'
            ? `AND agent_id = '${agentId}'`
            : '';

        // Obtener sesiones del contractor (opcionalmente por agent_id)
        const sessionsQuery = `
          SELECT 
            toDate(session_start) AS session_day,
            toDateTime(session_start) AS start_dt,
            toDateTime(session_end) AS end_dt
          FROM session_summary
          WHERE contractor_id = '${contractorId}'
            AND ${dateFilter}
            ${agentFilter}
          ORDER BY session_day, start_dt
        `;

        const sessions = (await this.clickHouseService.query(
          sessionsQuery,
        )) as Array<{
          session_day: string;
          start_dt: string;
          end_dt: string;
        }>;

        // Normalizar fecha: ClickHouse puede devolver "YYYY-MM-DD HH:mm:ss"; ISO usa "T" para parseo consistente
        const parseDt = (dt: string): Date => {
          const normalized = String(dt).replace(' ', 'T');
          return new Date(normalized);
        };

        // Agrupar sesiones por día
        const sessionsByDay = new Map<
          string,
          Array<{ start: Date; end: Date }>
        >();
        for (const s of sessions) {
          const day = s.session_day;
          if (!sessionsByDay.has(day)) {
            sessionsByDay.set(day, []);
          }
          sessionsByDay.get(day)!.push({
            start: parseDt(s.start_dt),
            end: parseDt(s.end_dt),
          });
        }

        // Sin agentId: fusionar intervalos por día para no duplicar tiempo (varios agentes)
        const mergeIntervals = (
          intervals: Array<{ start: Date; end: Date }>,
        ) => {
          if (intervals.length === 0) return [];
          const sorted = [...intervals].sort(
            (a, b) => a.start.getTime() - b.start.getTime(),
          );
          const merged: Array<{ start: Date; end: Date }> = [{ ...sorted[0] }];
          for (let i = 1; i < sorted.length; i++) {
            const cur = sorted[i];
            const last = merged[merged.length - 1];
            if (cur.start.getTime() <= last.end.getTime()) {
              last.end = new Date(
                Math.max(last.end.getTime(), cur.end.getTime()),
              );
            } else {
              merged.push({ ...cur });
            }
          }
          return merged;
        };

        // Para cada día, calcular la duración por hora con la lógica correcta
        const hourlyByDay = new Map<number, number[]>();

        for (const [day, daySessions] of sessionsByDay) {
          const intervals = agentId ? daySessions : mergeIntervals(daySessions);
          intervals.sort((a, b) => a.start.getTime() - b.start.getTime());

          for (let h = startHour; h <= endHour; h++) {
            // hourPrev = (H-1):00 - la hora anterior en punto
            // hourCurrent = H:00 - la hora actual en punto
            const hourPrev = new Date(
              `${day}T${String(h - 1).padStart(2, '0')}:00:00`,
            );
            const hourCurrent = new Date(
              `${day}T${String(h).padStart(2, '0')}:00:00`,
            );

            // Buscar sesiones que:
            // 1. NO habían terminado para la hora anterior (session_end > hourPrev)
            // 2. Empezaron ANTES de la hora actual (session_start < hourCurrent)
            const activeSessions = intervals.filter(
              (s) => s.end > hourPrev && s.start < hourCurrent,
            );

            let durationSeconds = 0;

            if (activeSessions.length > 0) {
              // Calcular el tiempo total de todas las sesiones activas
              for (const session of activeSessions) {
                // El tiempo que contribuye esta sesión es:
                // desde su inicio hasta min(su fin, la hora actual)
                const effectiveEnd =
                  session.end < hourCurrent ? session.end : hourCurrent;
                const sessionDuration =
                  (effectiveEnd.getTime() - session.start.getTime()) / 1000;
                durationSeconds += sessionDuration;
              }
            }
            // Si no hay sesiones activas, durationSeconds queda en 0

            if (!hourlyByDay.has(h)) {
              hourlyByDay.set(h, []);
            }
            hourlyByDay.get(h)!.push(durationSeconds);
          }
        }

        // Calcular promedios por hora
        const hourlyData: Array<{
          hour: number;
          hour_label: string;
          days_with_data: number;
          avg_duration_seconds: number;
        }> = [];

        for (let h = startHour; h <= endHour; h++) {
          const durations = hourlyByDay.get(h) || [];
          const daysWithData = durations.filter((d) => d > 0).length;
          const avgDuration =
            durations.length > 0
              ? durations.reduce((a, b) => a + b, 0) / durations.length
              : 0;

          hourlyData.push({
            hour: h,
            hour_label: `${String(h).padStart(2, '0')}:00`,
            days_with_data: daysWithData,
            avg_duration_seconds: Math.round(avgDuration * 100) / 100,
          });
        }

        return hourlyData;
      },
      envs.redis.ttl,
    );
  }

  async getHourlyActivity(
    contractorId: string,
    from?: string,
    to?: string,
    days: number = 30,
    startHour: number = 8,
    endHour: number = 17,
  ): Promise<
    Array<{
      hour: number;
      hour_label: string;
      days_with_data: number;
      total_beat_count: number;
      avg_beat_count: number;
      avg_duration_seconds: number;
      avg_active_seconds: number;
      avg_idle_seconds: number;
      avg_keyboard_inputs: number;
      avg_mouse_clicks: number;
    }>
  > {
    const cacheKey =
      RedisKeys.hourlyActivityByContractor(contractorId, from, to, days) +
      `:hours:${startHour}-${endHour}:avg`;

    return this.redisService.getOrSet(
      cacheKey,
      async () => {
        // Construir filtro de fecha
        let dateFilter: string;
        if (from && to) {
          const fromDate = from.split('T')[0];
          const toDate = to.split('T')[0];
          dateFilter = `toDate(beat_timestamp, 'America/New_York') >= '${fromDate}' AND toDate(beat_timestamp, 'America/New_York') <= '${toDate}'`;
        } else {
          dateFilter = `toDate(beat_timestamp, 'America/New_York') >= today() - ${days}`;
        }

        // Query que calcula totales y cuenta días únicos para promediar
        const query = `
          SELECT 
            toHour(beat_timestamp) AS hour,
            uniqExact(toDate(beat_timestamp, 'America/New_York')) AS days_with_data,
            count(*) AS total_beat_count,
            round(count(*) / uniqExact(toDate(beat_timestamp, 'America/New_York')), 2) AS avg_beat_count,
            round((count(*) * 15) / uniqExact(toDate(beat_timestamp, 'America/New_York')), 2) AS avg_duration_seconds,
            round((countIf(is_idle = 0) * 15) / uniqExact(toDate(beat_timestamp, 'America/New_York')), 2) AS avg_active_seconds,
            round((countIf(is_idle = 1) * 15) / uniqExact(toDate(beat_timestamp, 'America/New_York')), 2) AS avg_idle_seconds,
            round(sum(keyboard_count) / uniqExact(toDate(beat_timestamp, 'America/New_York')), 2) AS avg_keyboard_inputs,
            round(sum(mouse_clicks) / uniqExact(toDate(beat_timestamp, 'America/New_York')), 2) AS avg_mouse_clicks
          FROM contractor_activity_15s
          WHERE contractor_id = '${contractorId}'
            AND ${dateFilter}
            AND toHour(beat_timestamp) >= ${startHour}
            AND toHour(beat_timestamp) < ${endHour}
          GROUP BY hour
          ORDER BY hour ASC
        `;

        const results = (await this.clickHouseService.query(query)) as Array<{
          hour: number;
          days_with_data: number;
          total_beat_count: number;
          avg_beat_count: number;
          avg_duration_seconds: number;
          avg_active_seconds: number;
          avg_idle_seconds: number;
          avg_keyboard_inputs: number;
          avg_mouse_clicks: number;
        }>;

        // Crear array con todas las horas de la jornada (rellenando con 0 las que no tienen datos)
        const hourlyData: Array<{
          hour: number;
          hour_label: string;
          days_with_data: number;
          total_beat_count: number;
          avg_beat_count: number;
          avg_duration_seconds: number;
          avg_active_seconds: number;
          avg_idle_seconds: number;
          avg_keyboard_inputs: number;
          avg_mouse_clicks: number;
        }> = [];

        for (let h = startHour; h < endHour; h++) {
          const existing = results.find((r) => Number(r.hour) === h);
          hourlyData.push({
            hour: h,
            hour_label: `${String(h).padStart(2, '0')}:00`,
            days_with_data: existing ? Number(existing.days_with_data) : 0,
            total_beat_count: existing ? Number(existing.total_beat_count) : 0,
            avg_beat_count: existing ? Number(existing.avg_beat_count) : 0,
            avg_duration_seconds: existing
              ? Number(existing.avg_duration_seconds)
              : 0,
            avg_active_seconds: existing
              ? Number(existing.avg_active_seconds)
              : 0,
            avg_idle_seconds: existing ? Number(existing.avg_idle_seconds) : 0,
            avg_keyboard_inputs: existing
              ? Number(existing.avg_keyboard_inputs)
              : 0,
            avg_mouse_clicks: existing ? Number(existing.avg_mouse_clicks) : 0,
          });
        }

        return hourlyData;
      },
      envs.redis.ttl,
    );
  }

  /**
   * Obtiene el % de productividad PROMEDIO por hora para un contractor.
   * Calcula la productividad por hora usando la misma fórmula del ETL,
   * incluyendo ponderación por apps y browser (opción B).
   * Si se pasa agentId, filtra por ese agente; si no, devuelve datos consolidados (todos los agentes).
   *
   * @param contractorId ID del contractor
   * @param from Fecha de inicio (opcional)
   * @param to Fecha de fin (opcional)
   * @param days Días hacia atrás (default: 30)
   * @param startHour Hora de inicio de jornada (default: 8)
   * @param endHour Hora de fin de jornada (default: 17)
   * @param agentId ID del agente (opcional). Si se indica, solo se consideran beats y eventos de ese agente.
   * @returns Array de objetos con % de productividad promedio por hora
   */
  async getHourlyProductivity(
    contractorId: string,
    from?: string,
    to?: string,
    days: number = 30,
    startHour: number = 8,
    endHour: number = 17,
    agentId?: string,
  ): Promise<
    Array<{
      hour: number;
      hour_label: string;
      days_with_data: number;
      avg_productivity_score: number;
      avg_active_percentage: number;
      avg_keyboard_mouse_score: number;
      avg_app_score: number;
      avg_browser_score: number;
    }>
  > {
    const effectiveAgentId =
      agentId && agentId !== 'consolidated' ? agentId : undefined;
    const cacheKey =
      RedisKeys.hourlyProductivityByContractor(contractorId, from, to, days) +
      `:hours:${startHour}-${endHour}:avg` +
      (effectiveAgentId ? `:agent:${effectiveAgentId}` : '');

    return this.redisService.getOrSet(
      cacheKey,
      async () => {
        // Construir filtro de fecha
        let dateFilterBeats: string;
        let dateFilterEvents: string;
        if (from && to) {
          const fromDate = from.split('T')[0];
          const toDate = to.split('T')[0];
          dateFilterBeats = `toDate(beat_timestamp, 'America/New_York') >= '${fromDate}' AND toDate(beat_timestamp, 'America/New_York') <= '${toDate}'`;
          dateFilterEvents = `toDate(timestamp, 'America/New_York') >= '${fromDate}' AND toDate(timestamp, 'America/New_York') <= '${toDate}'`;
        } else {
          dateFilterBeats = `toDate(beat_timestamp, 'America/New_York') >= today() - ${days}`;
          dateFilterEvents = `toDate(timestamp, 'America/New_York') >= today() - ${days}`;
        }

        const agentFilterBeats = effectiveAgentId
          ? ` AND agent_id = '${effectiveAgentId}'`
          : '';
        const agentFilterEvents = effectiveAgentId
          ? ` AND agent_id = '${effectiveAgentId}'`
          : '';

        const activePercentExpr =
          '100.0 * b.active_beats / nullIf(b.total_beats, 0)';
        const keyboardMouseExpr =
          'least(100.0, 15.0 * ln(1 + (((b.total_keyboard_inputs + b.total_mouse_clicks) / nullIf(b.total_beats * 15 / 60, 0)) / 2.0)))';
        const appScoreExpr =
          'ifNull(100.0 * greatest(0.0, least(1.0, ((app.weighted_seconds / nullIf(app.total_seconds, 0)) - 0.2) / 0.8)), 50.0)';
        const browserScoreExpr =
          'ifNull(100.0 * greatest(0.0, least(1.0, ((web.weighted_seconds / nullIf(web.total_seconds, 0)) - 0.2) / 0.8)), 50.0)';

        const query = `
          SELECT
            hour,
            uniqExact(workday) AS days_with_data,
            round(avg(productivity_score), 2) AS avg_productivity_score,
            round(avg(active_percentage), 2) AS avg_active_percentage,
            round(avg(keyboard_mouse_score), 2) AS avg_keyboard_mouse_score,
            round(avg(app_score), 2) AS avg_app_score,
            round(avg(browser_score), 2) AS avg_browser_score
          FROM (
            SELECT
              b.workday AS workday,
              b.hour AS hour,
              ${activePercentExpr} AS active_percentage,
              ${keyboardMouseExpr} AS keyboard_mouse_score,
              ${appScoreExpr} AS app_score,
              ${browserScoreExpr} AS browser_score,
              least(100.0, greatest(0.0,
                0.35 * (${activePercentExpr}) +
                0.20 * (${keyboardMouseExpr}) +
                0.30 * (${appScoreExpr}) +
                0.15 * (${browserScoreExpr})
              )) AS productivity_score
            FROM (
              SELECT
                contractor_id,
                toDate(beat_timestamp, 'America/New_York') AS workday,
                toHour(beat_timestamp) AS hour,
                count() AS total_beats,
                countIf(is_idle = 0) AS active_beats,
                sum(keyboard_count) AS total_keyboard_inputs,
                sum(mouse_clicks) AS total_mouse_clicks
              FROM contractor_activity_15s
              WHERE contractor_id = '${contractorId}'
                AND ${dateFilterBeats}
                AND toHour(beat_timestamp) >= ${startHour}
                AND toHour(beat_timestamp) < ${endHour}${agentFilterBeats}
              GROUP BY contractor_id, workday, hour
            ) b
            LEFT JOIN (
              SELECT
                contractor_id,
                toDate(timestamp, 'America/New_York') AS workday,
                toHour(timestamp) AS hour,
                sum(JSONExtractFloat(payload, 'AppUsage', app) * ifNull(d.weight, 0.5)) AS weighted_seconds,
                sum(JSONExtractFloat(payload, 'AppUsage', app)) AS total_seconds
              FROM events_raw
              ARRAY JOIN JSONExtractKeys(payload, 'AppUsage') AS app
              LEFT JOIN apps_dimension d ON d.name = app
              WHERE contractor_id = '${contractorId}'
                AND ${dateFilterEvents}
                AND toHour(timestamp) >= ${startHour}
                AND toHour(timestamp) < ${endHour}${agentFilterEvents}
                AND JSONHas(payload, 'AppUsage')
              GROUP BY contractor_id, workday, hour
            ) app ON app.contractor_id = b.contractor_id
              AND app.workday = b.workday
              AND app.hour = b.hour
            LEFT JOIN (
              SELECT
                contractor_id,
                toDate(timestamp, 'America/New_York') AS workday,
                toHour(timestamp) AS hour,
                sum(
                  JSONExtractFloat(payload, 'browser', dc) *
                  ifNull(d.weight, 1)
                ) AS weighted_seconds,
                sum(JSONExtractFloat(payload, 'browser', dc)) AS total_seconds
              FROM events_raw
              ARRAY JOIN JSONExtractKeys(payload, 'browser') AS dc
              LEFT JOIN domains_dimension d ON d.domain = dc
              WHERE contractor_id = '${contractorId}'
                AND ${dateFilterEvents}
                AND toHour(timestamp) >= ${startHour}
                AND toHour(timestamp) < ${endHour}${agentFilterEvents}
                AND JSONHas(payload, 'browser')
              GROUP BY contractor_id, workday, hour
            ) web ON web.contractor_id = b.contractor_id
              AND web.workday = b.workday
              AND web.hour = b.hour
          )
          GROUP BY hour
          ORDER BY hour ASC
        `;

        const results = (await this.clickHouseService.query(query)) as Array<{
          hour: number;
          days_with_data: number;
          avg_productivity_score: number;
          avg_active_percentage: number;
          avg_keyboard_mouse_score: number;
          avg_app_score: number;
          avg_browser_score: number;
        }>;

        const hourlyData: Array<{
          hour: number;
          hour_label: string;
          days_with_data: number;
          avg_productivity_score: number;
          avg_active_percentage: number;
          avg_keyboard_mouse_score: number;
          avg_app_score: number;
          avg_browser_score: number;
        }> = [];

        for (let h = startHour; h < endHour; h++) {
          const existing = results.find((r) => Number(r.hour) === h);
          hourlyData.push({
            hour: h,
            hour_label: `${String(h).padStart(2, '0')}:00`,
            days_with_data: existing ? Number(existing.days_with_data) : 0,
            avg_productivity_score: existing
              ? Number(existing.avg_productivity_score)
              : 0,
            avg_active_percentage: existing
              ? Number(existing.avg_active_percentage)
              : 0,
            avg_keyboard_mouse_score: existing
              ? Number(existing.avg_keyboard_mouse_score)
              : 0,
            avg_app_score: existing ? Number(existing.avg_app_score) : 0,
            avg_browser_score: existing
              ? Number(existing.avg_browser_score)
              : 0,
          });
        }

        return hourlyData;
      },
      envs.redis.ttl,
    );
  }

  /**
   * Obtiene la duración promedio de sesiones agrupada dinámicamente según los filtros.
   *
   * - Sin cliente: Agrupa por cliente (avg de todos los contratistas por cliente)
   * - Con cliente: Agrupa por equipo (avg de contratistas del cliente por equipo)
   * - Con cliente + equipo: Agrupa por contratista individual
   * - Con cliente + equipo + job: Igual que anterior, filtrado por job position
   *
   * @param from Fecha de inicio
   * @param to Fecha de fin
   * @param clientId ID del cliente (opcional)
   * @param teamId ID del equipo (opcional)
   * @param jobPosition Job position (opcional)
   * @param days Días hacia atrás si no hay from/to (default: 30)
   * @returns Array de objetos con duración promedio agrupada
   */
  async getGroupedAvgSessionDuration(
    from?: string,
    to?: string,
    clientId?: string,
    teamId?: string,
    jobPosition?: string,
    country?: string,
    days: number = 30,
  ): Promise<GroupedAvgDuration[]> {
    // Determinar nivel de agrupación
    let groupBy: GroupByLevel;
    if (!clientId) {
      groupBy = 'client';
    } else if (!teamId) {
      groupBy = 'team';
    } else {
      groupBy = 'contractor';
    }

    const cacheKey = `grouped-avg-duration:${groupBy}:${clientId || 'all'}:${teamId || 'all'}:${jobPosition || 'all'}:${country || 'all'}:${from || 'none'}:${to || 'none'}:${days}`;

    return this.redisService.getOrSet(
      cacheKey,
      async () => {
        const dbName = envs.clickhouse.database;

        // Construir filtro de fecha
        let dateFilter: string;
        if (from && to) {
          const fromDate = from.split('T')[0];
          const toDate = to.split('T')[0];
          dateFilter = `toDate(ss.session_start) >= '${fromDate}' AND toDate(ss.session_start) <= '${toDate}'`;
        } else {
          dateFilter = `toDate(ss.session_start) >= today() - ${days}`;
        }

        // Construir filtros adicionales basados en los parámetros
        const filters: string[] = [dateFilter];

        if (clientId) {
          filters.push(`ci.client_id = '${clientId}'`);
        }
        if (teamId) {
          filters.push(`ci.team_id = '${teamId}'`);
        }
        if (jobPosition) {
          filters.push(`ci.job_position = '${jobPosition}'`);
        }
        if (country) {
          filters.push(`ci.country = '${country}'`);
        }

        const whereClause = filters.join(' AND ');

        // Determinar campos de agrupación según el nivel
        // IMPORTANTE: Usar 'ca.' porque es el alias del CTE, no 'ci.' que solo existe dentro del CTE
        let groupByFields: string;
        let selectFields: string;

        switch (groupBy) {
          case 'client':
            groupByFields = 'ca.client_id, cl.client_name';
            selectFields = `
              ca.client_id AS group_id,
              COALESCE(cl.client_name, 'Unknown') AS group_name
            `;
            break;
          case 'team':
            groupByFields = 'ca.team_id, t.team_name';
            selectFields = `
              ca.team_id AS group_id,
              COALESCE(t.team_name, 'Unknown') AS group_name
            `;
            break;
          case 'contractor':
            groupByFields = 'ca.contractor_id, ca.name';
            selectFields = `
              ca.contractor_id AS group_id,
              COALESCE(ca.name, 'Unknown') AS group_name
            `;
            break;
        }

        // Query principal: Calcular avg duration por contratista y luego agrupar
        // IMPORTANTE: Usar alias explícitos en el CTE para que ClickHouse los reconozca
        const query = `
          WITH contractor_avg AS (
            SELECT 
              ci.contractor_id AS contractor_id,
              ci.client_id AS client_id,
              ci.team_id AS team_id,
              ci.job_position AS job_position,
              ci.name AS name,
              avg(ss.total_seconds) AS avg_session_seconds
            FROM ${dbName}.session_summary ss
            INNER JOIN ${dbName}.contractor_info_raw ci FINAL
              ON ss.contractor_id = ci.contractor_id
            WHERE ${whereClause}
            GROUP BY ci.contractor_id, ci.client_id, ci.team_id, ci.job_position, ci.name
          )
          SELECT 
            ${selectFields},
            count(DISTINCT ca.contractor_id) AS contractor_count,
            round(avg(ca.avg_session_seconds) / 3600, 2) AS avg_duration_hours
          FROM contractor_avg ca
          LEFT JOIN ${dbName}.clients_dimension cl FINAL ON ca.client_id = cl.client_id
          LEFT JOIN ${dbName}.teams_dimension t FINAL ON ca.team_id = t.team_id
          WHERE ${groupBy === 'client' ? 'ca.client_id IS NOT NULL' : groupBy === 'team' ? 'ca.team_id IS NOT NULL' : '1=1'}
          GROUP BY ${groupByFields}
          ORDER BY avg_duration_hours DESC
        `;

        this.logger.debug(
          `Executing grouped avg duration query with groupBy=${groupBy}`,
        );

        const results = (await this.clickHouseService.query(query)) as Array<{
          group_id: string;
          group_name: string;
          contractor_count: number;
          avg_duration_hours: number;
        }>;

        return results.map((r) => ({
          group_id: r.group_id || 'unknown',
          group_name: r.group_name || 'Unknown',
          contractor_count: Number(r.contractor_count) || 0,
          avg_duration_hours: Number(r.avg_duration_hours) || 0,
        }));
      },
      envs.redis.ttl,
    );
  }
}
