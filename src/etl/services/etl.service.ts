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
import { EventsToAppUsageTransformer } from '../transformers/events-to-app-usage.transformer';

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
    private readonly eventsToAppUsageTransformer: EventsToAppUsageTransformer,
    private readonly activityToDailyMetricsTransformer: ActivityToDailyMetricsTransformer,
    private readonly activityToSessionSummaryTransformer: ActivityToSessionSummaryTransformer,
  ) {}

  /**
   * Procesa eventos RAW y genera contractor_activity_15s.
   * Lee desde events_raw y guarda en contractor_activity_15s.
   */
  async processEventsToActivity(
    fromDate?: Date,
    toDate?: Date,
  ): Promise<number> {
    try {
      // Normalizar rango por defecto para evitar procesar demasiado si no se especifica
      if (!fromDate && !toDate) {
        // Procesar solo las últimas 2 horas por defecto
        const now = new Date();
        const twoHoursAgo = new Date(now.getTime() - 2 * 60 * 60 * 1000);
        fromDate = twoHoursAgo;
        toDate = now;
        this.logger.warn(
          'processEventsToActivity called without range. Defaulting to last 2 hours to ensure idempotency.',
        );
      }

      // 1) Procesar por día, solo si el día NO existe en destino (idempotencia sin DELETE)
      const start = new Date(fromDate as Date);
      const end = new Date(toDate as Date);
      // Normalizar a 00:00 UTC para iteración por días
      start.setUTCHours(0, 0, 0, 0);
      end.setUTCHours(0, 0, 0, 0);

      let totalInserted = 0;
      for (
        let d = new Date(start.getTime());
        d.getTime() <= end.getTime();
        d = new Date(d.getTime() + 24 * 60 * 60 * 1000)
      ) {
        const dayStr = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;

        // Verificar existencia en destino
        const exists = await this.clickHouseService.query<{ cnt: number }>(`
          SELECT count() AS cnt 
          FROM contractor_activity_15s
          WHERE workday = toDate('${dayStr}')
        `);
        if ((exists[0]?.cnt || 0) > 0) {
          this.logger.log(
            `⏭️ Skipping contractor_activity_15s for ${dayStr} (already populated)`,
          );
          continue;
        }

        // Insertar solo ese día desde events_raw
        const insertQueryPerDay = `
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
        `;
        await this.clickHouseService.command(insertQueryPerDay);

        // Contar insertados estimando por events_raw del día
        const insertedRes = await this.clickHouseService.query<{
          cnt: number;
        }>(`
          SELECT count() AS cnt
          FROM events_raw
          WHERE toDate(timestamp) = toDate('${dayStr}')
        `);
        const estimatedInserted = Number(insertedRes[0]?.cnt || 0);

        // Contar filas reales en destino para ese día
        const destCountRes = await this.clickHouseService.query<{
          cnt: number;
        }>(`
          SELECT count() AS cnt
          FROM contractor_activity_15s
          WHERE workday = toDate('${dayStr}')
        `);
        const actualCount = Number(destCountRes[0]?.cnt || 0);

        totalInserted += estimatedInserted;
        this.logger.log(
          `✅ Processed contractor_activity_15s for ${dayStr}. ` +
            `Estimated inserted: ${estimatedInserted === 0 ? '0' : estimatedInserted.toLocaleString('en-US')}, ` +
            `Actual rows in destination: ${actualCount === 0 ? '0' : actualCount.toLocaleString('en-US')}`,
        );
      }

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
   * Procesa eventos RAW y genera app_usage_summary.
   * Lee desde events_raw y guarda en app_usage_summary.
   */
  async processEventsToAppUsage(
    fromDate?: Date,
    toDate?: Date,
  ): Promise<number> {
    try {
      // Si no se proporciona rango, por defecto procesar el día anterior completo (idempotente y estable)
      if (!fromDate && !toDate) {
        const now = new Date();
        const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);
        // Inicio y fin del día de ayer en UTC
        const startOfYesterday = new Date(
          Date.UTC(
            yesterday.getUTCFullYear(),
            yesterday.getUTCMonth(),
            yesterday.getUTCDate(),
            0,
            0,
            0,
          ),
        );
        const endOfYesterday = new Date(
          Date.UTC(
            yesterday.getUTCFullYear(),
            yesterday.getUTCMonth(),
            yesterday.getUTCDate(),
            23,
            59,
            59,
          ),
        );
        fromDate = startOfYesterday;
        toDate = endOfYesterday;
        this.logger.warn(
          'processEventsToAppUsage called without range. Defaulting to YESTERDAY (UTC) to ensure idempotency and stable counts.',
        );
      }

      // 1) Procesar por día únicamente si no existen registros para ese día
      const start = new Date(fromDate as Date);
      const end = new Date(toDate as Date);
      start.setUTCHours(0, 0, 0, 0);
      end.setUTCHours(0, 0, 0, 0);

      let totalRows = 0;
      for (
        let d = new Date(start.getTime());
        d.getTime() <= end.getTime();
        d = new Date(d.getTime() + 24 * 60 * 60 * 1000)
      ) {
        const dayStr = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;

        const expectedRowsRes = await this.clickHouseService.query<{
          cnt: number;
        }>(`
          SELECT count() AS cnt
          FROM (
            SELECT contractor_id, toDate(timestamp) AS workday, app_name
            FROM events_raw
            ARRAY JOIN JSONExtractKeys(payload, 'AppUsage') AS app_name
            WHERE JSONHas(payload, 'AppUsage') AND toDate(timestamp) = toDate('${dayStr}')
            GROUP BY contractor_id, workday, app_name
          )
        `);
        const expectedRows = Number(expectedRowsRes[0]?.cnt || 0);

        const insertQueryPerDay = `
          INSERT INTO app_usage_summary
          SELECT
            contractor_id,
            app_name,
            toDate(timestamp) AS workday,
            toUInt32(
              greatest(
                0,
                round(
                  sum(
                    JSONExtractFloat(payload, 'AppUsage', app_name)
                    + toFloat64OrZero(JSONExtractString(payload, 'AppUsage', app_name))
                  ) / 15.0
                )
              )
            ) AS active_beats,
            now() AS created_at
          FROM events_raw
          ARRAY JOIN JSONExtractKeys(payload, 'AppUsage') AS app_name
          WHERE JSONHas(payload, 'AppUsage') AND toDate(timestamp) = toDate('${dayStr}')
          GROUP BY contractor_id, workday, app_name
          HAVING (contractor_id, workday, app_name) NOT IN (
            SELECT contractor_id, workday, app_name
            FROM app_usage_summary
            WHERE workday = toDate('${dayStr}')
          )
        `;
        await this.clickHouseService.command(insertQueryPerDay);
        totalRows += expectedRows;
        this.logger.log(
          `✅ Processed app_usage_summary for ${dayStr}. Rows: ${expectedRows === 0 ? '0' : expectedRows.toLocaleString('en-US')}`,
        );
      }

      return Number(totalRows || 0);
    } catch (error) {
      this.logger.error(
        `Error processing events to app usage: ${error.message}`,
      );
      throw error;
    }
  }

  /**
   * Fuerza el reprocesamiento de AppUsage (RAW → app_usage_summary).
   * Borra los datos existentes del rango y vuelve a insertar (DELETE + INSERT).
   */
  async processEventsToAppUsageForce(
    fromDate?: Date,
    toDate?: Date,
  ): Promise<number> {
    try {
      // Default: día actual si no se provee rango
      if (!fromDate && !toDate) {
        const d = new Date();
        d.setUTCHours(0, 0, 0, 0);
        fromDate = new Date(d);
        toDate = new Date(d);
        this.logger.warn(
          'processEventsToAppUsageForce called without range. Defaulting to TODAY (UTC).',
        );
      }

      const start = new Date(fromDate as Date);
      const end = new Date(toDate as Date);
      start.setUTCHours(0, 0, 0, 0);
      end.setUTCHours(0, 0, 0, 0);

      let totalRows = 0;
      for (
        let d = new Date(start.getTime());
        d.getTime() <= end.getTime();
        d = new Date(d.getTime() + 24 * 60 * 60 * 1000)
      ) {
        const dayStr = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;

        // 1) Borrar por partición/día
        await this.clickHouseService.command(`
          ALTER TABLE app_usage_summary DELETE
          WHERE workday = toDate('${dayStr}')
        `);

        // 2) Insertar agregados del día
        const expectedRowsRes = await this.clickHouseService.query<{
          cnt: number;
        }>(`
          SELECT count() AS cnt
          FROM (
            SELECT contractor_id, toDate(timestamp) AS workday, app_name
            FROM events_raw
            ARRAY JOIN JSONExtractKeys(payload, 'AppUsage') AS app_name
            WHERE JSONHas(payload, 'AppUsage') AND toDate(timestamp) = toDate('${dayStr}')
            GROUP BY contractor_id, workday, app_name
          )
        `);
        const expectedRows = Number(expectedRowsRes[0]?.cnt || 0);

        const insertQueryPerDay = `
          INSERT INTO app_usage_summary
          SELECT
            contractor_id,
            app_name,
            toDate(timestamp) AS workday,
            toUInt32(
              greatest(
                0,
                round(
                  sum(
                    JSONExtractFloat(payload, 'AppUsage', app_name)
                    + toFloat64OrZero(JSONExtractString(payload, 'AppUsage', app_name))
                  ) / 15.0
                )
              )
            ) AS active_beats,
            now() AS created_at
          FROM events_raw
          ARRAY JOIN JSONExtractKeys(payload, 'AppUsage') AS app_name
          WHERE JSONHas(payload, 'AppUsage') AND toDate(timestamp) = toDate('${dayStr}')
          GROUP BY contractor_id, workday, app_name
        `;
        await this.clickHouseService.command(insertQueryPerDay);

        totalRows += expectedRows;
        this.logger.log(
          `✅ FORCE processed app_usage_summary for ${dayStr}. Rows: ${expectedRows === 0 ? '0' : expectedRows.toLocaleString('en-US')}`,
        );
      }

      this.logger.log(
        `✅ Total FORCE processed rows into app_usage_summary: ${totalRows === 0 ? '0' : totalRows.toLocaleString('en-US')}`,
      );
      return Number(totalRows || 0);
    } catch (error) {
      this.logger.error(`Error processing app usage (force): ${error.message}`);
      throw error;
    }
  }

  /**
   * Fuerza el reprocesamiento de eventos RAW → contractor_activity_15s.
   * Borra los datos existentes del rango y vuelve a insertar (DELETE + INSERT SELECT).
   */
  async processEventsToActivityForce(
    fromDate?: Date,
    toDate?: Date,
  ): Promise<number> {
    try {
      // Rango por defecto: últimas 2 horas (evitar borrar demasiado por accidente)
      if (!fromDate && !toDate) {
        const now = new Date();
        const twoHoursAgo = new Date(now.getTime() - 2 * 60 * 60 * 1000);
        fromDate = twoHoursAgo;
        toDate = now;
        this.logger.warn(
          'processEventsToActivityForce called without range. Defaulting to last 2 hours.',
        );
      }

      const fromStr = fromDate ? this.formatDate(fromDate) : null;
      const toStr = toDate ? this.formatDate(toDate) : null;
      const fromDay = fromDate ? fromStr!.split(' ')[0] : null;
      const toDay = toDate ? toStr!.split(' ')[0] : null;

      // 1) Borrar por partición (workday) en el rango
      await this.clickHouseService.command(`
        ALTER TABLE contractor_activity_15s DELETE
        WHERE 1=1
          ${fromDay ? `AND workday >= toDate('${fromDay}')` : ''}
          ${toDay ? `AND workday <= toDate('${toDay}')` : ''}
      `);

      // 2) Insertar con INSERT SELECT usando filtros de timestamp
      const filters =
        (fromStr ? ` AND timestamp >= '${fromStr}'` : '') +
        (toStr ? ` AND timestamp <= '${toStr}'` : '');

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
      `;

      await this.clickHouseService.command(insertQuery);

      // 3) Retornar cantidad insertada en el rango
      const countRes = await this.clickHouseService.query<{ cnt: number }>(`
        SELECT count() AS cnt FROM contractor_activity_15s
        WHERE 1=1
        ${fromStr ? ` AND beat_timestamp >= '${fromStr}'` : ''}
        ${toStr ? ` AND beat_timestamp <= '${toStr}'` : ''}
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
   */
  async processActivityToDailyMetrics(
    workday?: Date,
    fromDate?: Date,
    toDate?: Date,
  ): Promise<ContractorDailyMetricsDto[]> {
    try {
      // Construir lista de días a procesar
      const days: string[] = [];
      if (fromDate || toDate) {
        const start = new Date((fromDate || toDate) as Date);
        const end = new Date((toDate || fromDate) as Date);
        start.setUTCHours(0, 0, 0, 0);
        end.setUTCHours(0, 0, 0, 0);
        for (
          let d = new Date(start.getTime());
          d.getTime() <= end.getTime();
          d = new Date(d.getTime() + 24 * 60 * 60 * 1000)
        ) {
          const dayStr = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
          days.push(dayStr);
        }
      } else if (workday) {
        const d = new Date(workday);
        d.setUTCHours(0, 0, 0, 0);
        const dayStr = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
        days.push(dayStr);
      } else {
        const d = new Date();
        d.setUTCHours(0, 0, 0, 0);
        const dayStr = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
        days.push(dayStr);
      }

      const allMetrics: ContractorDailyMetricsDto[] = [];
      for (const dayStr of days) {
        // Si ya existen métricas para el día, no recalcular
        const exists = await this.clickHouseService.query<{ cnt: number }>(`
          SELECT count() AS cnt FROM contractor_daily_metrics WHERE workday = toDate('${dayStr}')
        `);
        if ((exists[0]?.cnt || 0) > 0) {
          this.logger.log(
            `⏭️ Skipping contractor_daily_metrics for ${dayStr} (already populated)`,
          );
          // Cargar y acumular métricas existentes para retorno
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
              created_at
            FROM contractor_daily_metrics
            WHERE workday = toDate('${dayStr}')
            ORDER BY contractor_id
          `);
          allMetrics.push(...existing);
          continue;
        }

        // Insertar métricas diarias calculadas en SQL para ese día
        const insertQuery = `
        INSERT INTO contractor_daily_metrics
        SELECT
          a.contractor_id,
          a.workday,
          count() AS total_beats,
          sum(if(a.is_idle = 0, 1, 0)) AS active_beats,
          sum(if(a.is_idle = 1, 1, 0)) AS idle_beats,
          100.0 * sum(if(a.is_idle = 0, 1, 0)) / nullIf(count(), 0) AS active_percentage,
          sum(a.keyboard_count) AS total_keyboard_inputs,
          sum(a.mouse_clicks) AS total_mouse_clicks,
          round(sum(a.keyboard_count) / nullIf(count() / 4.0, 0), 2) AS avg_keyboard_per_min,
          round(sum(a.mouse_clicks) / nullIf(count() / 4.0, 0), 2) AS avg_mouse_per_min,
          count() * 15 AS total_session_time_seconds,
          sum(if(a.is_idle = 0, 1, 0)) * 15 AS effective_work_seconds,
          least(100.0, greatest(0.0,
            0.35 * (100.0 * sum(if(a.is_idle = 0, 1, 0)) / nullIf(count(), 0)) +
            0.20 * least(100.0, 15.0 * ln(1 + (((sum(a.keyboard_count) + sum(a.mouse_clicks)) / nullIf(count() * 15 / 60, 0)) / 2.0))) +
            0.30 * ifNull(
              100.0 * greatest(0.0, least(1.0, ((any(app.weighted_seconds) / nullIf(any(app.total_seconds), 0)) - 0.2) / 0.8)),
              50.0
            ) +
            0.15 * ifNull(
              100.0 * greatest(0.0, least(1.0, ((any(web.weighted_seconds) / nullIf(any(web.total_seconds), 0)) - 0.2) / 0.8)),
              50.0
            )
          )) AS productivity_score,
          now() AS created_at
        FROM contractor_activity_15s a
        LEFT JOIN (
          SELECT 
            contractor_id,
            toDate(timestamp) AS workday,
            sum(JSONExtractFloat(payload, 'AppUsage', app) * ifNull(d.weight, 0.5)) AS weighted_seconds,
            sum(JSONExtractFloat(payload, 'AppUsage', app)) AS total_seconds
          FROM events_raw
          ARRAY JOIN JSONExtractKeys(payload, 'AppUsage') AS app
          LEFT JOIN apps_dimension d ON d.app_name = app
          WHERE toDate(timestamp) = toDate('${dayStr}')
          GROUP BY contractor_id, workday
        ) app ON app.contractor_id = a.contractor_id AND app.workday = a.workday
        LEFT JOIN (
          SELECT 
            contractor_id,
            toDate(timestamp) AS workday,
            sum(
              JSONExtractFloat(payload, 'browser', dc) *
              ifNull(
                if(
                  arrayFirstIndex(x -> x = dc, de_exact_domains) > 0,
                  de_exact_weights[arrayFirstIndex(x -> x = dc, de_exact_domains)],
                  if(
                    arrayFirstIndex(p -> startsWith(dc, p), dp_prefix_domains) > 0,
                    dp_prefix_weights[arrayFirstIndex(p -> startsWith(dc, p), dp_prefix_domains)],
                    0.5
                  )
                ),
                0.5
              )
            ) AS weighted_seconds,
            sum(JSONExtractFloat(payload, 'browser', dc)) AS total_seconds
          FROM events_raw
          CROSS JOIN (
            SELECT 
              groupArray(domain) AS de_exact_domains,
              groupArray(weight) AS de_exact_weights
            FROM domains_dimension
            WHERE right(domain, 1) != '.'
          ) de
          CROSS JOIN (
            SELECT 
              groupArray(domain) AS dp_prefix_domains,
              groupArray(weight) AS dp_prefix_weights
            FROM domains_dimension
            WHERE right(domain, 1) = '.'
          ) dp
          ARRAY JOIN JSONExtractKeys(payload, 'browser') AS dc
          WHERE toDate(timestamp) = toDate('${dayStr}')
          GROUP BY contractor_id, workday
        ) web ON web.contractor_id = a.contractor_id AND web.workday = a.workday
        WHERE a.workday = toDate('${dayStr}')
        GROUP BY a.contractor_id, a.workday
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
            created_at
          FROM contractor_daily_metrics
          WHERE workday = toDate('${dayStr}')
          ORDER BY contractor_id
        `);
        this.logger.log(
          `✅ Generated ${metrics.length} daily metrics for ${dayStr}`,
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
   * Agrupa por session_id, calcula productividad con fórmula multi-factor.
   */
  async processActivityToSessionSummary(
    sessionId?: string,
  ): Promise<SessionSummaryDto[]> {
    try {
      // 1) Insertar resúmenes solo para sesiones que no existen aún (idempotencia sin DELETE)
      //    Calculamos S_active, S_inputs, S_apps y S_browser en SQL y luego el productivity_score
      const sessionFilter = sessionId
        ? `WHERE a.session_id = '${sessionId}' AND a.session_id NOT IN (SELECT DISTINCT session_id FROM session_summary)`
        : `WHERE a.session_id IN (
             SELECT DISTINCT session_id 
             FROM contractor_activity_15s 
             WHERE session_id IS NOT NULL
           ) 
           AND a.session_id NOT IN (SELECT DISTINCT session_id FROM session_summary)`;

      const insertQuery = `
        INSERT INTO session_summary
        SELECT
          a.session_id,
          any(a.contractor_id) AS contractor_id,
          min(a.beat_timestamp) AS session_start,
          max(a.beat_timestamp) AS session_end,
          count() * 15 AS total_seconds,
          sum(if(a.is_idle = 0, 15, 0)) AS active_seconds,
          sum(if(a.is_idle = 1, 15, 0)) AS idle_seconds,
          -- Score final (sin exponer sub-scores)
          least(100.0, greatest(0.0,
            0.35 * (100.0 * sum(if(a.is_idle = 0, 1, 0)) / nullIf(count(), 0)) +
            0.20 * least(100.0, 15.0 * ln(1 + (((sum(a.keyboard_count) + sum(a.mouse_clicks)) / nullIf(count() * 15 / 60, 0)) / 2.0))) +
            0.30 * ifNull(
              100.0 * greatest(0.0, least(1.0, ((any(app.weighted_seconds) / nullIf(any(app.total_seconds), 0)) - 0.2) / 0.8)),
              50.0
            ) +
            0.15 * ifNull(
              100.0 * greatest(0.0, least(1.0, ((any(web.weighted_seconds) / nullIf(any(web.total_seconds), 0)) - 0.2) / 0.8)),
              50.0
            )
          )) AS productivity_score,
          now() AS created_at
        FROM contractor_activity_15s a
        LEFT JOIN (
          SELECT 
            e.session_id,
            sum(JSONExtractFloat(e.payload, 'AppUsage', app) * ifNull(d.weight, 0.5)) AS weighted_seconds,
            sum(JSONExtractFloat(e.payload, 'AppUsage', app)) AS total_seconds
          FROM events_raw e
          ARRAY JOIN JSONExtractKeys(e.payload, 'AppUsage') AS app
          LEFT JOIN apps_dimension d ON d.app_name = app
          GROUP BY e.session_id
        ) app ON app.session_id = a.session_id
        LEFT JOIN (
          SELECT 
            e.session_id,
            sum(
              JSONExtractFloat(e.payload, 'browser', dc) *
              ifNull(
                if(
                  arrayFirstIndex(x -> x = dc, de_exact_domains) > 0,
                  de_exact_weights[arrayFirstIndex(x -> x = dc, de_exact_domains)],
                  if(
                    arrayFirstIndex(p -> startsWith(dc, p), dp_prefix_domains) > 0,
                    dp_prefix_weights[arrayFirstIndex(p -> startsWith(dc, p), dp_prefix_domains)],
                    0.5
                  )
                ),
                0.5
              )
            ) AS weighted_seconds,
            sum(JSONExtractFloat(e.payload, 'browser', dc)) AS total_seconds
          FROM events_raw e
          CROSS JOIN (
            SELECT 
              groupArray(domain) AS de_exact_domains,
              groupArray(weight) AS de_exact_weights
            FROM domains_dimension
            WHERE right(domain, 1) != '.'
          ) de
          CROSS JOIN (
            SELECT 
              groupArray(domain) AS dp_prefix_domains,
              groupArray(weight) AS dp_prefix_weights
            FROM domains_dimension
            WHERE right(domain, 1) = '.'
          ) dp
          ARRAY JOIN JSONExtractKeys(e.payload, 'browser') AS dc
          GROUP BY e.session_id
        ) web ON web.session_id = a.session_id
        ${sessionFilter}
        GROUP BY a.session_id
      `;

      await this.clickHouseService.command(insertQuery);

      // 3) Devolver filas insertadas
      const selectQuery = `
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
        ${
          sessionId
            ? `WHERE session_id = '${sessionId}'`
            : `WHERE session_start >= today() - 7`
        }
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
