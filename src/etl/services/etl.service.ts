import { Injectable, Logger } from '@nestjs/common';

import { ClickHouseService } from '../../clickhouse/clickhouse.service';
import { ContractorDailyMetricsDto } from '../dto/contractor-daily-metrics.dto';
import { SessionSummaryDto } from '../dto/session-summary.dto';
import {
  AppUsageData,
  BrowserUsageData,
} from '../transformers/activity-to-daily-metrics.transformer';
import { ActivityToDailyMetricsTransformer } from '../transformers/activity-to-daily-metrics.transformer';
import { ActivityToSessionSummaryTransformer } from '../transformers/activity-to-session-summary.transformer';
import { EventsToActivityTransformer } from '../transformers/events-to-activity.transformer';

/**
 * Servicio ETL que orquesta las transformaciones RAW → ADT.
 * Lee datos desde ClickHouse, aplica transformaciones y guarda resultados.
 */
@Injectable()
export class EtlService {
  private readonly logger = new Logger(EtlService.name);

  constructor(
    private readonly clickHouseService: ClickHouseService,
    private readonly eventsToActivityTransformer: EventsToActivityTransformer,
    private readonly activityToDailyMetricsTransformer: ActivityToDailyMetricsTransformer,
    private readonly activityToSessionSummaryTransformer: ActivityToSessionSummaryTransformer,
  ) {}

  /**
   * Procesa eventos RAW y genera contractor_activity_15s.
   * Lee desde events_raw y guarda en contractor_activity_15s.
   * @param contractorId - Si se pasa, procesa solo ese contratista (para flujo trigger al cerrar sesión)
   */
  async processEventsToActivity(
    fromDate?: Date,
    toDate?: Date,
    contractorId?: string,
  ): Promise<number> {
    try {
      let totalInserted = 0;
      const { from, to } = this.normalizeDateRange(
        fromDate,
        toDate,
        2 * 60 * 60 * 1000,
      );

      // 1) Procesar por día, solo si el día NO existe en destino (idempotencia sin DELETE)
      await this.iterateDays(from, to, async (day) => {
        const dayStr = `${day.getUTCFullYear()}-${String(
          day.getUTCMonth() + 1,
        ).padStart(2, '0')}-${String(day.getUTCDate()).padStart(2, '0')}`;

        // Verificar existencia en destino (por contratista si se especifica)
        const contractorFilter = contractorId
          ? ` AND contractor_id = '${contractorId}'`
          : '';
        const exists = await this.clickHouseService.query<{ cnt: number }>(`
          SELECT count() AS cnt 
          FROM contractor_activity_15s
          WHERE workday = toDate('${dayStr}')
          ${contractorFilter}
        `);
        if ((exists[0]?.cnt || 0) > 0) {
          this.logger.log(
            `⏭️ Skipping contractor_activity_15s for ${dayStr}${
              contractorId ? ` contractor=${contractorId}` : ''
            } (already populated)`,
          );
          return;
        }

        // Insertar solo ese día desde events_raw (filtro por contratista si se especifica)
        const insertQueryPerDay = this.buildInsertActivityQuery(
          day,
          contractorId,
        );
        await this.clickHouseService.command(insertQueryPerDay);

        const eventsFilter = contractorId
          ? ` AND contractor_id = '${contractorId}'`
          : '';

        // Contar insertados estimando por events_raw del día
        const insertedRes = await this.clickHouseService.query<{
          cnt: number;
        }>(`
          SELECT count() AS cnt
          FROM events_raw
          WHERE toDate(timestamp) = toDate('${dayStr}')
          ${eventsFilter}
        `);
        const estimatedInserted = Number(insertedRes[0]?.cnt || 0);

        // Contar filas reales en destino para ese día
        const destCountRes = await this.clickHouseService.query<{
          cnt: number;
        }>(`
          SELECT count() AS cnt
          FROM contractor_activity_15s
          WHERE workday = toDate('${dayStr}')
          ${contractorFilter}
        `);
        const actualCount = Number(destCountRes[0]?.cnt || 0);

        totalInserted += estimatedInserted;
        this.logger.log(
          `✅ Processed contractor_activity_15s for ${dayStr}. ` +
            `Estimated inserted: ${
              estimatedInserted === 0
                ? '0'
                : estimatedInserted.toLocaleString('en-US')
            }, ` +
            `Actual rows in destination: ${
              actualCount === 0 ? '0' : actualCount.toLocaleString('en-US')
            }`,
        );
      });

      this.logger.log(
        `✅ Total processed events to activity beats (days without existing data): ${totalInserted === 0 ? '0' : totalInserted.toLocaleString('en-US')}`,
      );
      return totalInserted;
    } catch (error) {
      this.logger.error(
        `Error processing events to activity: ${error.message}`,
      );
      throw error;
    }
  }

  /**
   * Normaliza un rango de fechas. Si no se pasa nada, usa un rango corto hacia atrás
   * para evitar procesar demasiado por defecto.
   */
  private normalizeDateRange(
    fromDate: Date | undefined,
    toDate: Date | undefined,
    defaultWindowMs: number,
    floorToDay: boolean = true,
  ): { from: Date; to: Date } {
    if (!fromDate && !toDate) {
      const now = new Date();
      const from = new Date(now.getTime() - defaultWindowMs);
      this.logger.warn(
        'normalizeDateRange called without range. Defaulting to a short window for safety.',
      );
      return { from, to: now };
    }

    if (fromDate && !toDate) {
      const d = new Date(fromDate);
      if (floorToDay) {
        d.setUTCHours(0, 0, 0, 0);
      }
      return { from: d, to: d };
    }

    if (!fromDate && toDate) {
      const d = new Date(toDate);
      if (floorToDay) {
        d.setUTCHours(0, 0, 0, 0);
      }
      return { from: d, to: d };
    }

    const from = new Date(fromDate as Date);
    const to = new Date(toDate as Date);

    if (floorToDay) {
      from.setUTCHours(0, 0, 0, 0);
      to.setUTCHours(0, 0, 0, 0);
    }

    return { from, to };
  }

  /**
   * Itera día por día (UTC) desde `from` hasta `to`, inclusive.
   */
  private async iterateDays(
    from: Date,
    to: Date,
    fn: (day: Date) => Promise<void>,
  ): Promise<void> {
    const start = new Date(from.getTime());
    const end = new Date(to.getTime());

    start.setUTCHours(0, 0, 0, 0);
    end.setUTCHours(0, 0, 0, 0);

    for (
      let d = new Date(start.getTime());
      d.getTime() <= end.getTime();
      d = new Date(d.getTime() + 24 * 60 * 60 * 1000)
    ) {
      // Pasamos una copia para evitar mutaciones externas
      await fn(new Date(d.getTime()));
    }
  }

  /**
   * Construye la query de INSERT SELECT para un día concreto de contractor_activity_15s.
   */
  private buildInsertActivityQuery(day: Date, contractorId?: string): string {
    const dayStr = `${day.getUTCFullYear()}-${String(
      day.getUTCMonth() + 1,
    ).padStart(2, '0')}-${String(day.getUTCDate()).padStart(2, '0')}`;

    const eventsFilter = contractorId
      ? ` AND contractor_id = '${contractorId}'`
      : '';

    return `
      INSERT INTO contractor_activity_15s
      SELECT
        contractor_id,
        agent_id,
        session_id,
        agent_session_id,
        timestamp AS beat_timestamp,
        if(
          (toUInt32OrZero(JSON_VALUE(payload, '$.Keyboard.InputsCount'))
           + toUInt32OrZero(JSON_VALUE(payload, '$.Mouse.ClicksCount'))) = 0,
          1, 0
        ) AS is_idle,
        toUInt32OrZero(JSON_VALUE(payload, '$.Keyboard.InputsCount')) AS keyboard_count,
        toUInt32OrZero(JSON_VALUE(payload, '$.Mouse.ClicksCount')) AS mouse_clicks,
        toDate(timestamp) AS workday,
        now() AS created_at
      FROM events_raw
      WHERE toDate(timestamp) = toDate('${dayStr}')
      ${eventsFilter}
    `;
  }

  /**
   * Fuerza el reprocesamiento de eventos RAW → contractor_activity_15s.
   * Borra los datos existentes del rango y vuelve a insertar (DELETE + INSERT SELECT).
   * @param contractorId - Si se pasa, procesa solo ese contratista
   */
  async processEventsToActivityForce(
    fromDate?: Date,
    toDate?: Date,
    contractorId?: string,
  ): Promise<number> {
    try {
      // Rango por defecto: últimas 2 horas (evitar borrar demasiado por accidente)
      const { from, to } = this.normalizeDateRange(
        fromDate,
        toDate,
        2 * 60 * 60 * 1000,
        false,
      );

      const fromStr = from ? this.formatDate(from) : null;
      const toStr = to ? this.formatDate(to) : null;
      const fromDay = fromStr ? fromStr.split(' ')[0] : null;
      const toDay = toStr ? toStr.split(' ')[0] : null;

      const contractorFilter = contractorId
        ? ` AND contractor_id = '${contractorId}'`
        : '';

      // 1) Borrar por partición (workday) en el rango (y contratista si aplica)
      await this.clickHouseService.command(`
        ALTER TABLE contractor_activity_15s DELETE
        WHERE 1=1
          ${fromDay ? `AND workday >= toDate('${fromDay}')` : ''}
          ${toDay ? `AND workday <= toDate('${toDay}')` : ''}
          ${contractorFilter}
      `);

      // 2) Insertar con INSERT SELECT usando filtros de timestamp (y contratista si aplica)
      const filters =
        (fromStr ? ` AND timestamp >= '${fromStr}'` : '') +
        (toStr ? ` AND timestamp <= '${toStr}'` : '') +
        contractorFilter;

      const insertQuery = `
        INSERT INTO contractor_activity_15s
        SELECT
          contractor_id,
          agent_id,
          session_id,
          agent_session_id,
          timestamp AS beat_timestamp,
          if(
            (toUInt32OrZero(JSON_VALUE(payload, '$.Keyboard.InputsCount'))
             + toUInt32OrZero(JSON_VALUE(payload, '$.Mouse.ClicksCount'))) = 0,
            1, 0
          ) AS is_idle,
          toUInt32OrZero(JSON_VALUE(payload, '$.Keyboard.InputsCount')) AS keyboard_count,
          toUInt32OrZero(JSON_VALUE(payload, '$.Mouse.ClicksCount')) AS mouse_clicks,
          toDate(timestamp) AS workday,
          now() AS created_at
        FROM events_raw
        WHERE 1=1
        ${filters}
        SETTINGS max_partitions_per_insert_block=1000
      `;

      await this.clickHouseService.command(insertQuery);

      // 3) Retornar cantidad insertada en el rango
      const countRes = await this.clickHouseService.query<{ cnt: number }>(`
        SELECT count() AS cnt FROM contractor_activity_15s
        WHERE 1=1
        ${fromStr ? ` AND beat_timestamp >= '${fromStr}'` : ''}
        ${toStr ? ` AND beat_timestamp <= '${toStr}'` : ''}
        ${contractorFilter}
      `);

      const count = Number(countRes[0]?.cnt || 0);
      this.logger.log(
        `✅ Force processed events to activity beats. Inserted: ${count === 0 ? '0' : count.toLocaleString('en-US')}`,
      );
      return count;
    } catch (error) {
      this.logger.error(
        `Error processing events to activity (force): ${error.message}`,
      );
      throw error;
    }
  }

  /**
   * Genera métricas diarias desde contractor_activity_15s.
   * Agrupa por contractor_id y workday, calcula productividad con fórmula multi-factor.
   *
   * @param workday - Día específico a procesar (opcional, por defecto: día anterior)
   * @param fromDate - Fecha de inicio del rango (opcional, para procesar múltiples días)
   * @param toDate - Fecha de fin del rango (opcional, para procesar múltiples días)
   * @param contractorIds - Si se pasa, procesa solo esos contratistas (para flujo trigger al cerrar sesión)
   */
  async processActivityToDailyMetrics(
    workday?: Date,
    fromDate?: Date,
    toDate?: Date,
    contractorIds?: string[],
  ): Promise<ContractorDailyMetricsDto[]> {
    try {
      // Asegurar que las columnas app_usage y browser_usage existan (migración)
      try {
        await this.clickHouseService.command(`
          ALTER TABLE contractor_daily_metrics 
          ADD COLUMN IF NOT EXISTS app_usage Map(String, UInt64) DEFAULT map()
        `);
        await this.clickHouseService.command(`
          ALTER TABLE contractor_daily_metrics 
          ADD COLUMN IF NOT EXISTS browser_usage Map(String, UInt64) DEFAULT map()
        `);
        this.logger.log('✅ Columns app_usage and browser_usage verified');
      } catch {
        this.logger.debug('Migration skipped or columns already exist');
      }

      // Construir lista de días a procesar
      const days: string[] = [];
      if (fromDate || toDate) {
        const { from, to } = this.normalizeDateRange(fromDate, toDate, 0, true);
        await this.iterateDays(from, to, async (d) => {
          const dayStr = `${d.getUTCFullYear()}-${String(
            d.getUTCMonth() + 1,
          ).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
          days.push(dayStr);
        });
      } else if (workday) {
        const d = new Date(workday);
        d.setHours(0, 0, 0, 0);
        const dayStr = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
        days.push(dayStr);
      } else {
        const d = new Date();
        d.setHours(0, 0, 0, 0);
        const dayStr = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
        days.push(dayStr);
      }

      const contractorFilter = contractorIds?.length
        ? ` AND contractor_id IN (${contractorIds.map((c) => `'${c}'`).join(',')})`
        : '';

      const allMetrics: ContractorDailyMetricsDto[] = [];
      for (const dayStr of days) {
        // Si ya existen métricas para el día, no recalcular (salvo cuando contractorIds: borrar y reinsertar solo esos)
        if (!contractorIds?.length) {
          const exists = await this.clickHouseService.query<{ cnt: number }>(`
            SELECT count() AS cnt FROM contractor_daily_metrics WHERE workday = toDate('${dayStr}')
          `);
          if ((exists[0]?.cnt || 0) > 0) {
            this.logger.log(
              `⏭️ Skipping contractor_daily_metrics for ${dayStr} (already populated)`,
            );
            const existing = await this.clickHouseService
              .query<ContractorDailyMetricsDto>(`
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
              FROM contractor_daily_metrics
              WHERE workday = toDate('${dayStr}')
              ORDER BY contractor_id
            `);
            allMetrics.push(...existing);
            continue;
          }
        } else {
          // Por contratista: borrar filas existentes de esos contractors para ese día antes de reinsertar
          await this.clickHouseService.command(`
            ALTER TABLE contractor_daily_metrics DELETE
            WHERE workday = toDate('${dayStr}')
            ${contractorFilter}
          `);
        }

        // ✅ CONSOLIDACIÓN MULTI-AGENTE: Consolidar beats por timestamp antes de calcular métricas
        // Esto evita que agentes idle en segundo plano penalicen la productividad
        // cuando otro agente está activo en el mismo intervalo de 15s
        const insertQuery = `
        INSERT INTO contractor_daily_metrics (
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
        )
        SELECT
          ca.contractor_id,
          ca.workday,
          count() AS total_beats,
          sum(if(ca.is_idle_contractor = 0, 1, 0)) AS active_beats,
          sum(if(ca.is_idle_contractor = 1, 1, 0)) AS idle_beats,
          100.0 * sum(if(ca.is_idle_contractor = 0, 1, 0)) / nullIf(count(), 0) AS active_percentage,
          sum(ca.keyboard_count_contractor) AS total_keyboard_inputs,
          sum(ca.mouse_clicks_contractor) AS total_mouse_clicks,
          round(sum(ca.keyboard_count_contractor) / nullIf(count() / 4.0, 0), 2) AS avg_keyboard_per_min,
          round(sum(ca.mouse_clicks_contractor) / nullIf(count() / 4.0, 0), 2) AS avg_mouse_per_min,
          count() * 15 AS total_session_time_seconds,
          sum(if(ca.is_idle_contractor = 0, 1, 0)) * 15 AS effective_work_seconds,
          least(100.0, greatest(0.0,
            0.35 * (100.0 * sum(if(ca.is_idle_contractor = 0, 1, 0)) / nullIf(count(), 0)) +
            0.20 * least(100.0, 15.0 * ln(1 + (((sum(ca.keyboard_count_contractor) + sum(ca.mouse_clicks_contractor)) / nullIf(count() * 15 / 60, 0)) / 2.0))) +
            0.30 * ifNull(
              100.0 * greatest(0.0, least(1.0, ((any(app.weighted_seconds) / nullIf(any(app.total_seconds), 0)) - 0.2) / 0.8)),
              50.0
            ) +
            0.15 * ifNull(
              100.0 * greatest(0.0, least(1.0, ((any(web.weighted_seconds) / nullIf(any(web.total_seconds), 0)) - 0.2) / 0.8)),
              50.0
            )
          )) AS productivity_score,
          ifNull(any(app_map.app_usage), map()) AS app_usage,
          ifNull(any(browser_map.browser_usage), map()) AS browser_usage,
          now() AS created_at
        FROM (
          SELECT
            contractor_id,
            workday,
            beat_timestamp,
            MIN(is_idle) AS is_idle_contractor,
            SUM(keyboard_count) AS keyboard_count_contractor,
            SUM(mouse_clicks) AS mouse_clicks_contractor
          FROM contractor_activity_15s
          WHERE workday = toDate('${dayStr}')
          ${contractorFilter}
          GROUP BY contractor_id, workday, beat_timestamp
        ) ca
        -- JOIN para productivity_score: totales ponderados por día
        LEFT JOIN (
          SELECT 
            contractor_id,
            toDate(timestamp) AS workday,
            sum(JSONExtractFloat(payload, 'AppUsage', app) * ifNull(d.weight, 0.5)) AS weighted_seconds,
            sum(JSONExtractFloat(payload, 'AppUsage', app)) AS total_seconds
          FROM events_raw
          ARRAY JOIN JSONExtractKeys(payload, 'AppUsage') AS app
          LEFT JOIN apps_dimension d ON d.name = app
          WHERE toDate(timestamp) = toDate('${dayStr}')
          ${contractorFilter}
          GROUP BY contractor_id, workday
        ) app ON app.contractor_id = ca.contractor_id AND app.workday = ca.workday
        -- JOIN para app_usage Map: segundos por app por (contractor_id, workday)
        LEFT JOIN (
          SELECT
            contractor_id,
            workday,
            mapFromArrays(groupArray(app), groupArray(toUInt64(round(sec)))) AS app_usage
          FROM (
            SELECT
              contractor_id,
              toDate(timestamp) AS workday,
              app,
              sum(JSONExtractFloat(payload, 'AppUsage', app)) AS sec
            FROM events_raw
            ARRAY JOIN JSONExtractKeys(payload, 'AppUsage') AS app
            WHERE toDate(timestamp) = toDate('${dayStr}')
            ${contractorFilter}
            GROUP BY contractor_id, workday, app
          )
          GROUP BY contractor_id, workday
        ) app_map ON app_map.contractor_id = ca.contractor_id AND app_map.workday = ca.workday
        -- JOIN para productivity_score: totales ponderados browser por día (peso por dominio desde domains_dimension cuando existe)
        LEFT JOIN (
          SELECT 
            contractor_id,
            toDate(timestamp) AS workday,
            sum(
              JSONExtractFloat(payload, 'browser', dc) *
              ifNull(d.weight, 1)
            ) AS weighted_seconds,
            sum(JSONExtractFloat(payload, 'browser', dc)) AS total_seconds
          FROM events_raw
          ARRAY JOIN JSONExtractKeys(payload, 'browser') AS dc
          LEFT JOIN domains_dimension d ON d.domain = dc
          WHERE toDate(timestamp) = toDate('${dayStr}')
          ${contractorFilter}
          GROUP BY contractor_id, workday
        ) web ON web.contractor_id = ca.contractor_id AND web.workday = ca.workday
        -- JOIN para browser_usage Map: segundos por dominio por (contractor_id, workday)
        LEFT JOIN (
          SELECT
            contractor_id,
            workday,
            mapFromArrays(groupArray(dc), groupArray(toUInt64(round(sec)))) AS browser_usage
          FROM (
            SELECT
              contractor_id,
              toDate(timestamp) AS workday,
              dc,
              sum(JSONExtractFloat(payload, 'browser', dc)) AS sec
            FROM events_raw
            ARRAY JOIN JSONExtractKeys(payload, 'browser') AS dc
            WHERE toDate(timestamp) = toDate('${dayStr}')
            ${contractorFilter}
            GROUP BY contractor_id, workday, dc
          )
          GROUP BY contractor_id, workday
        ) browser_map ON browser_map.contractor_id = ca.contractor_id AND browser_map.workday = ca.workday
        GROUP BY ca.contractor_id, ca.workday
      `;

        await this.clickHouseService.command(insertQuery);

        const metrics = await this.clickHouseService
          .query<ContractorDailyMetricsDto>(`
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
          FROM contractor_daily_metrics
          WHERE workday = toDate('${dayStr}')
          ${contractorFilter}
          ORDER BY contractor_id
        `);
        this.logger.log(
          `✅ Generated ${metrics.length} daily metrics for ${dayStr}` +
            (contractorIds?.length
              ? ` (${contractorIds.length} contractors)`
              : ''),
        );
        allMetrics.push(...metrics);
      }

      return allMetrics;
    } catch (error) {
      this.logger.error(
        `Error processing activity to daily metrics: ${error.message}`,
      );
      throw error;
    }
  }

  /**
   * Genera resúmenes de sesión desde contractor_activity_15s.
   * Una fila por (session_id, agent_id). Agrupa por ambos y calcula productividad con fórmula multi-factor.
   *
   * Modos de uso:
   * - sessionId: recalcula solo esa sesión (DELETE + INSERT de ese session_id).
   * - contractorId + workday: recalcula todas las sesiones de ese contractor en ese día (DELETE + INSERT de ese contractor/día).
   * - sin parámetros: inserta solo las sesiones que aún no existen en session_summary (modo "all pending", idempotente).
   */
  async processActivityToSessionSummary(
    sessionId?: string,
    contractorId?: string,
    workday?: Date,
  ): Promise<SessionSummaryDto[]> {
    try {
      let workdayStr: string | undefined;

      // Normalizar workday a yyyy-MM-dd si viene informado
      if (workday) {
        const d = new Date(workday);
        d.setHours(0, 0, 0, 0);
        workdayStr = `${d.getUTCFullYear()}-${String(
          d.getUTCMonth() + 1,
        ).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
      }

      // 1) Borrado previo según el modo
      if (sessionId) {
        // Recalcular completamente una sesión específica
        await this.clickHouseService.command(`
          ALTER TABLE session_summary DELETE
          WHERE session_id = '${sessionId}'
        `);
      } else if (contractorId && workdayStr) {
        // Recalcular todas las sesiones de un contractor en un día concreto
        await this.clickHouseService.command(`
          ALTER TABLE session_summary DELETE
          WHERE contractor_id = '${contractorId}'
            AND toDate(session_start) = toDate('${workdayStr}')
        `);
      }

      // 2) Construir filtro principal y cláusula de idempotencia
      // Idempotencia por defecto: excluir pares (session_id, agent_id) que ya están en session_summary.
      // Para sessionId específico o contractor+workday ya borramos antes, así que NO aplicamos NOT IN en esos casos.
      const shouldApplyNotIn = !sessionId && !(contractorId && workdayStr);

      const notInClause = shouldApplyNotIn
        ? `AND (a.session_id, coalesce(a.agent_id, '')) NOT IN (
        SELECT session_id, coalesce(agent_id, '') FROM session_summary
      )`
        : '';

      let sessionFilter: string;
      if (sessionId) {
        sessionFilter = `WHERE a.session_id = '${sessionId}'`;
      } else if (contractorId && workdayStr) {
        sessionFilter = `
          WHERE a.contractor_id = '${contractorId}'
            AND toDate(a.beat_timestamp) = toDate('${workdayStr}')
        `;
      } else {
        sessionFilter = `
          WHERE a.session_id IN (
             SELECT DISTINCT session_id FROM contractor_activity_15s WHERE session_id IS NOT NULL
           ) ${notInClause}
        `;
      }

      const insertQuery = `
        INSERT INTO session_summary (session_id, contractor_id, agent_id, session_start, session_end, total_seconds, active_seconds, idle_seconds, productivity_score, created_at)
        SELECT
          a.session_id,
          any(a.contractor_id) AS contractor_id,
          any(a.agent_id) AS agent_id,
          min(a.beat_timestamp) AS session_start,
          max(a.beat_timestamp) AS session_end,
          count() * 15 AS total_seconds,
          sum(if(a.is_idle = 0, 15, 0)) AS active_seconds,
          sum(if(a.is_idle = 1, 15, 0)) AS idle_seconds,
          least(100.0, greatest(0.0,
            0.35 * (100.0 * sum(if(a.is_idle = 0, 1, 0)) / nullIf(count(), 0)) +
            0.20 * least(100.0, 15.0 * ln(1 + (((sum(a.keyboard_count) + sum(a.mouse_clicks)) / nullIf(count() * 15 / 60, 0)) / 2.0))) +
            0.30 * ifNull(
              100.0 * greatest(0.0, least(1.0, ((any(app.weighted_seconds) / nullIf(any(app.app_total_seconds), 0)) - 0.2) / 0.8)),
              50.0
            ) +
            0.15 * ifNull(
              100.0 * greatest(0.0, least(1.0, ((any(web.weighted_seconds) / nullIf(any(web.web_total_seconds), 0)) - 0.2) / 0.8)),
              50.0
            )
          )) AS productivity_score,
          now() AS created_at
        FROM contractor_activity_15s a
        LEFT JOIN (
          SELECT 
            e.session_id,
            e.agent_id,
            sum(JSONExtractFloat(e.payload, 'AppUsage', app) * ifNull(d.weight, 0.5)) AS weighted_seconds,
            sum(JSONExtractFloat(e.payload, 'AppUsage', app)) AS app_total_seconds
          FROM events_raw e
          ARRAY JOIN JSONExtractKeys(e.payload, 'AppUsage') AS app
          LEFT JOIN apps_dimension d ON d.name = app
          GROUP BY e.session_id, e.agent_id
        ) app ON app.session_id = a.session_id AND coalesce(app.agent_id, '') = coalesce(a.agent_id, '')
        LEFT JOIN (
          SELECT 
            e.session_id,
            e.agent_id,
            sum(
              JSONExtractFloat(e.payload, 'browser', dc) *
              ifNull(d.weight, 1)
            ) AS weighted_seconds,
            sum(JSONExtractFloat(e.payload, 'browser', dc)) AS web_total_seconds
          FROM events_raw e
          ARRAY JOIN JSONExtractKeys(e.payload, 'browser') AS dc
          LEFT JOIN domains_dimension d ON d.domain = dc
          GROUP BY e.session_id, e.agent_id
        ) web ON web.session_id = a.session_id AND coalesce(web.agent_id, '') = coalesce(a.agent_id, '')
        ${sessionFilter}
        GROUP BY a.session_id, a.agent_id
        SETTINGS max_partitions_per_insert_block=1000
      `;

      await this.clickHouseService.command(insertQuery);

      let selectWhere: string;
      if (sessionId) {
        selectWhere = `WHERE session_id = '${sessionId}'`;
      } else if (contractorId && workdayStr) {
        selectWhere = `
          WHERE contractor_id = '${contractorId}'
            AND toDate(session_start) = toDate('${workdayStr}')
        `;
      } else {
        selectWhere = `WHERE session_start >= today() - 7`;
      }

      const selectQuery = `
        SELECT 
          session_id,
          contractor_id,
          agent_id,
          session_start,
          session_end,
          total_seconds,
          active_seconds,
          idle_seconds,
          productivity_score,
          created_at
        FROM session_summary
        ${selectWhere}
        ORDER BY session_start DESC
        LIMIT 1000
      `;

      const summaries =
        await this.clickHouseService.query<SessionSummaryDto>(selectQuery);

      this.logger.log(
        `✅ Generated ${summaries.length} session summaries with ClickHouse SQL`,
      );

      return summaries;
    } catch (error) {
      this.logger.error(
        `Error processing activity to session summary: ${error.message}`,
      );
      throw error;
    }
  }

  /**
   * Reprocesa TODOS los resúmenes de sesión (`session_summary`) para un rango de fechas,
   * para TODOS los contractors. Útil para corregir errores en la fórmula de productividad.
   *
   * Pasos:
   * - DELETE de filas en `session_summary` cuyo session_start esté entre from/to.
   * - INSERT SELECT desde `contractor_activity_15s` (mismas joins de apps y browser) sin cláusula NOT IN.
   */
  async reprocessSessionSummariesForDateRange(
    fromDate: Date,
    toDate: Date,
  ): Promise<number> {
    try {
      const fromStr = fromDate.toISOString().split('T')[0];
      const toStr = toDate.toISOString().split('T')[0];

      this.logger.log(
        `🔄 Reprocessing session_summary for date range ${fromStr} to ${toStr} (ALL contractors)`,
      );

      // 1) Borrar todas las filas en el rango de fechas
      await this.clickHouseService.command(`
        ALTER TABLE session_summary DELETE
        WHERE toDate(session_start) >= toDate('${fromStr}')
          AND toDate(session_start) <= toDate('${toStr}')
      `);

      // 2) Insertar nuevamente todas las sesiones del rango desde contractor_activity_15s
      const insertQuery = `
        INSERT INTO session_summary (
          session_id,
          contractor_id,
          agent_id,
          session_start,
          session_end,
          total_seconds,
          active_seconds,
          idle_seconds,
          productivity_score,
          created_at
        )
        SELECT
          a.session_id,
          any(a.contractor_id) AS contractor_id,
          any(a.agent_id) AS agent_id,
          min(a.beat_timestamp) AS session_start,
          max(a.beat_timestamp) AS session_end,
          count() * 15 AS total_seconds,
          sum(if(a.is_idle = 0, 15, 0)) AS active_seconds,
          sum(if(a.is_idle = 1, 15, 0)) AS idle_seconds,
          least(100.0, greatest(0.0,
            0.35 * (100.0 * sum(if(a.is_idle = 0, 1, 0)) / nullIf(count(), 0)) +
            0.20 * least(100.0, 15.0 * ln(1 + (((sum(a.keyboard_count) + sum(a.mouse_clicks)) / nullIf(count() * 15 / 60, 0)) / 2.0))) +
            0.30 * ifNull(
              100.0 * greatest(0.0, least(1.0, ((any(app.weighted_seconds) / nullIf(any(app.app_total_seconds), 0)) - 0.2) / 0.8)),
              50.0
            ) +
            0.15 * ifNull(
              100.0 * greatest(0.0, least(1.0, ((any(web.weighted_seconds) / nullIf(any(web.web_total_seconds), 0)) - 0.2) / 0.8)),
              50.0
            )
          )) AS productivity_score,
          now() AS created_at
        FROM contractor_activity_15s a
        LEFT JOIN (
          SELECT 
            e.session_id,
            e.agent_id,
            sum(JSONExtractFloat(e.payload, 'AppUsage', app) * ifNull(d.weight, 0.5)) AS weighted_seconds,
            sum(JSONExtractFloat(e.payload, 'AppUsage', app)) AS app_total_seconds
          FROM events_raw e
          ARRAY JOIN JSONExtractKeys(e.payload, 'AppUsage') AS app
          LEFT JOIN apps_dimension d ON d.name = app
          GROUP BY e.session_id, e.agent_id
        ) app ON app.session_id = a.session_id AND coalesce(app.agent_id, '') = coalesce(a.agent_id, '')
        LEFT JOIN (
          SELECT 
            e.session_id,
            e.agent_id,
            sum(
              JSONExtractFloat(e.payload, 'browser', dc) *
              ifNull(d.weight, 1)
            ) AS weighted_seconds,
            sum(JSONExtractFloat(e.payload, 'browser', dc)) AS web_total_seconds
          FROM events_raw e
          ARRAY JOIN JSONExtractKeys(e.payload, 'browser') AS dc
          LEFT JOIN domains_dimension d ON d.domain = dc
          GROUP BY e.session_id, e.agent_id
        ) web ON web.session_id = a.session_id AND coalesce(web.agent_id, '') = coalesce(a.agent_id, '')
        WHERE a.session_id IS NOT NULL
          AND toDate(a.beat_timestamp) >= toDate('${fromStr}')
          AND toDate(a.beat_timestamp) <= toDate('${toStr}')
        GROUP BY a.session_id, a.agent_id
        SETTINGS max_partitions_per_insert_block=1000
      `;

      await this.clickHouseService.command(insertQuery);

      // 3) Contar cuántos resúmenes quedaron en el rango
      const countRes = await this.clickHouseService.query<{ cnt: number }>(`
        SELECT count() AS cnt
        FROM session_summary
        WHERE toDate(session_start) >= toDate('${fromStr}')
          AND toDate(session_start) <= toDate('${toStr}')
      `);

      const count = Number(countRes[0]?.cnt || 0);
      this.logger.log(
        `✅ Reprocessed ${count.toLocaleString(
          'en-US',
        )} session summaries for date range ${fromStr} to ${toStr}`,
      );

      return count;
    } catch (error) {
      this.logger.error(
        `Error reprocessing session summaries for range: ${error.message}`,
      );
      throw error;
    }
  }

  /**
   * Orquesta los 3 ETL en orden para un contratista al cerrar sesión.
   * Usa siempre el día de hoy (TODAY) como rango, no el default de 2h.
   *
   * @param contractorId - ID del contratista
   * @param sessionId - ID de la sesión cerrada
   */
  async runFullEtlForContractorOnSessionClose(
    contractorId: string,
    sessionId: string,
  ): Promise<void> {
    const now = new Date();
    const todayStart = new Date(now);
    todayStart.setHours(0, 0, 0, 0);
    const todayEnd = new Date(now);

    this.logger.log(
      `🔄 [Orchestrator] Starting full ETL for contractor=${contractorId} session=${sessionId} (today: ${todayStart.toISOString().slice(0, 10)})`,
    );

    // 1) process-events para hoy (recalcular rango del día para este contractor)
    await this.processEventsToActivityForce(todayStart, todayEnd, contractorId);

    // 2) process-daily-metrics para hoy, solo este contratista
    await this.processActivityToDailyMetrics(todayStart, undefined, undefined, [
      contractorId,
    ]);

    // 3) process-session-summaries para este contractor y día (recalcular todas las sesiones del día)
    await this.processActivityToSessionSummary(
      undefined,
      contractorId,
      todayStart,
    );

    this.logger.log(
      `✅ [Orchestrator] Full ETL completed for contractor=${contractorId} session=${sessionId}`,
    );
  }

  /**
   * Obtiene datos de AppUsage para un contractor y día específico.
   */
  private async getAppUsageForDay(
    contractorId: string,
    workday: Date,
  ): Promise<AppUsageData[]> {
    try {
      const workdayStr = this.formatDate(workday).split(' ')[0];
      const query = `
        SELECT 
          app_name,
          sum(JSONExtractFloat(payload, 'AppUsage', app_name)) as seconds
        FROM events_raw
        ARRAY JOIN JSONExtractKeys(payload, 'AppUsage') as app_name
        WHERE contractor_id = '${contractorId}'
          AND toDate(timestamp) = '${workdayStr}'
          AND JSONHas(payload, 'AppUsage')
        GROUP BY app_name
        HAVING seconds > 0
      `;

      const results = await this.clickHouseService.query<{
        app_name: string;
        seconds: number;
      }>(query);

      return results.map((r) => ({
        appName: r.app_name,
        seconds: Number(r.seconds) || 0,
      }));
    } catch (error) {
      this.logger.warn(
        `Error getting AppUsage for day: ${error.message}. Returning empty array.`,
      );
      return [];
    }
  }

  /**
   * Obtiene datos de Browser para un contractor y día específico.
   */
  private async getBrowserUsageForDay(
    contractorId: string,
    workday: Date,
  ): Promise<BrowserUsageData[]> {
    try {
      const workdayStr = this.formatDate(workday).split(' ')[0];
      const query = `
        SELECT 
          domain,
          sum(JSONExtractFloat(payload, 'browser', domain)) as seconds
        FROM events_raw
        ARRAY JOIN JSONExtractKeys(payload, 'browser') as domain
        WHERE contractor_id = '${contractorId}'
          AND toDate(timestamp) = '${workdayStr}'
          AND JSONHas(payload, 'browser')
        GROUP BY domain
        HAVING seconds > 0
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
        `Error getting Browser usage for day: ${error.message}. Returning empty array.`,
      );
      return [];
    }
  }

  /**
   * Obtiene datos de AppUsage para una sesión (rango de fechas).
   */
  private async getAppUsageForSession(
    contractorId: string,
    fromDate: Date,
    toDate: Date,
  ): Promise<AppUsageData[]> {
    try {
      const fromStr = this.formatDate(fromDate);
      const toStr = this.formatDate(toDate);
      const query = `
        SELECT 
          app_name,
          sum(JSONExtractFloat(payload, 'AppUsage', app_name)) as seconds
        FROM events_raw
        ARRAY JOIN JSONExtractKeys(payload, 'AppUsage') as app_name
        WHERE contractor_id = '${contractorId}'
          AND timestamp >= '${fromStr}'
          AND timestamp <= '${toStr}'
          AND JSONHas(payload, 'AppUsage')
        GROUP BY app_name
        HAVING seconds > 0
      `;

      const results = await this.clickHouseService.query<{
        app_name: string;
        seconds: number;
      }>(query);

      return results.map((r) => ({
        appName: r.app_name,
        seconds: Number(r.seconds) || 0,
      }));
    } catch (error) {
      this.logger.warn(
        `Error getting AppUsage for session: ${error.message}. Returning empty array.`,
      );
      return [];
    }
  }

  /**
   * Obtiene datos de Browser para una sesión (rango de fechas).
   */
  private async getBrowserUsageForSession(
    contractorId: string,
    fromDate: Date,
    toDate: Date,
  ): Promise<BrowserUsageData[]> {
    try {
      const fromStr = this.formatDate(fromDate);
      const toStr = this.formatDate(toDate);
      const query = `
        SELECT 
          domain,
          sum(JSONExtractFloat(payload, 'browser', domain)) as seconds
        FROM events_raw
        ARRAY JOIN JSONExtractKeys(payload, 'browser') as domain
        WHERE contractor_id = '${contractorId}'
          AND timestamp >= '${fromStr}'
          AND timestamp <= '${toStr}'
          AND JSONHas(payload, 'browser')
        GROUP BY domain
        HAVING seconds > 0
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
        `Error getting Browser usage for session: ${error.message}. Returning empty array.`,
      );
      return [];
    }
  }

  /**
   * Formatea una fecha al formato DateTime de ClickHouse.
   */
  private formatDate(date: Date): string {
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, '0');
    const day = String(date.getUTCDate()).padStart(2, '0');
    const hours = String(date.getUTCHours()).padStart(2, '0');
    const minutes = String(date.getUTCMinutes()).padStart(2, '0');
    const seconds = String(date.getUTCSeconds()).padStart(2, '0');
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
  }
}
