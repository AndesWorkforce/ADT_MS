import { DateTime } from 'luxon';
import { Injectable, Logger } from '@nestjs/common';

import {
  coerceToOperationalDayStart,
  formatDateInTZ,
  OPERATIONAL_TIMEZONE,
  toDateTZ,
  wallTimeToUtcInOperationalZone,
} from 'config';
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
import {
  appUsageMapSql,
  domainUsageMapSql,
  payloadCteSql,
} from './payload-sql.util';
import { ProductivityScoreService } from './productivity-score.service';

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
    private readonly productivityScoreService: ProductivityScoreService,
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
        const dayStr = formatDateInTZ(day);

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
          WHERE toDate(timestamp, '${OPERATIONAL_TIMEZONE}') = toDate('${dayStr}')
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

    const copy = (d: Date) => new Date(d.getTime());

    if (fromDate && !toDate) {
      const d = copy(fromDate);
      if (floorToDay) {
        return {
          from: DateTime.fromJSDate(d)
            .setZone(OPERATIONAL_TIMEZONE)
            .startOf('day')
            .toJSDate(),
          to: DateTime.fromJSDate(d)
            .setZone(OPERATIONAL_TIMEZONE)
            .startOf('day')
            .toJSDate(),
        };
      }
      return { from: d, to: d };
    }

    if (!fromDate && toDate) {
      const d = copy(toDate);
      if (floorToDay) {
        const day = DateTime.fromJSDate(d)
          .setZone(OPERATIONAL_TIMEZONE)
          .startOf('day')
          .toJSDate();
        return { from: day, to: day };
      }
      return { from: d, to: d };
    }

    const from = copy(fromDate as Date);
    const to = copy(toDate as Date);

    if (floorToDay) {
      return {
        from: DateTime.fromJSDate(from)
          .setZone(OPERATIONAL_TIMEZONE)
          .startOf('day')
          .toJSDate(),
        to: DateTime.fromJSDate(to)
          .setZone(OPERATIONAL_TIMEZONE)
          .startOf('day')
          .toJSDate(),
      };
    }

    return { from, to };
  }

  /**
   * Itera día por día (calendario en OPERATIONAL_TIMEZONE) desde `from` hasta `to`, inclusive.
   */
  private async iterateDays(
    from: Date,
    to: Date,
    fn: (day: Date) => Promise<void>,
  ): Promise<void> {
    let cursor = DateTime.fromJSDate(from)
      .setZone(OPERATIONAL_TIMEZONE)
      .startOf('day');
    const end = DateTime.fromJSDate(to)
      .setZone(OPERATIONAL_TIMEZONE)
      .startOf('day');

    if (cursor > end) {
      return;
    }

    while (cursor <= end) {
      await fn(cursor.toJSDate());
      cursor = cursor.plus({ days: 1 });
    }
  }

  /**
   * Umbral de idle a nivel beat (micro-idle). Debe coincidir con
   * IDLE_THRESHOLD_SECONDS de EventsToActivityTransformer para mantener UNA
   * sola definición de is_idle en todo el sistema.
   */
  private static readonly IDLE_THRESHOLD_SECONDS = 10;

  /**
   * Expresiones SELECT compartidas por buildInsertActivityQuery y el force-reprocess.
   * Extrae del payload RAW todas las columnas de contractor_activity_15s, incluyendo
   * los campos del agente v2 (beat_duration, power_state, browser_source, señales de
   * presencia). Es backward-compatible con payloads v1 gracias a los defaults:
   *   - BeatDuration ausente → 15s
   *   - PowerState ausente   → 'active'
   *   - PresenceSignals/DomainUsage ausentes → 0 / payload_version = 1
   *
   * is_idle (micro-idle) UNIFICADO: sin teclado ni mouse Y IdleTime >= umbral.
   */
  /**
   * Expresiones SELECT compartidas por buildInsertActivityQuery y el force-reprocess.
   * Extrae del payload RAW todas las columnas de contractor_activity_15s, incluyendo
   * los campos del agente v2 (beat_duration, power_state, browser_source, señales de
   * presencia). Es backward-compatible con payloads v1 gracias a los defaults:
   *   - BeatDuration ausente → 15s
   *   - PowerState ausente   → 'active'
   *   - PresenceSignals/DomainUsage ausentes → 0 / payload_version = 1
   *
   * is_idle (micro-idle) UNIFICADO: sin teclado ni mouse Y IdleTime >= umbral.
   */
  /**
   * Expresiones SELECT compartidas por buildInsertActivityQuery y el force-reprocess.
   * Extrae del payload RAW todas las columnas de contractor_activity_15s.
   *
   * Requiere que la query arranque con payloadCteSql(): todo sale del alias `p`,
   * que es el payload parseado UNA sola vez.
   *
   * Compatibilidad de versiones (`p.v` vale 3 solo en payloads v3):
   *   - BeatDuration/beat_duration ausente → 15s
   *   - PowerState/power_state ausente     → 'active'
   *   - PresenceSignals/*_active ausentes  → 0
   *
   * is_idle (micro-idle) UNIFICADO: sin teclado ni mouse Y idle >= umbral.
   */
  private buildActivitySelectColumns(): string {
    const idleThreshold = EtlService.IDLE_THRESHOLD_SECONDS;

    // OJO: ClickHouse 23.8 NO resuelve el acceso con punto sobre un alias de
    // Tuple venido de un WITH (`p.v` lo toma como nombre de columna y falla con
    // UNKNOWN_IDENTIFIER). Hay que usar tupleElement() por nombre.
    const f = (field: string) => `tupleElement(p, '${field}')`;
    const nested = (outer: string, inner: string) =>
      `tupleElement(tupleElement(p, '${outer}'), '${inner}')`;

    // Un payload v3 trae "v": 3; los anteriores no traen la clave y cae en 0.
    const isV3 = `(${f('v')} >= 3)`;

    const keyboard = `if(${isV3}, ${f('keyboard_count')}, ${nested('Keyboard', 'InputsCount')})`;
    const mouse = `if(${isV3}, ${f('mouse_clicks')}, ${nested('Mouse', 'ClicksCount')})`;
    const idle = `if(${isV3}, ${f('idle_time')}, ${f('IdleTime')})`;
    const rawBeat = `if(${isV3}, ${f('beat_duration')}, ${f('BeatDuration')})`;
    const powerState = `if(${isV3}, ${f('power_state')}, ${f('PowerState')})`;
    const browserSource = `if(${isV3}, ${f('browser_source')}, ${f('BrowserSource')})`;
    const mic = `if(${isV3}, ${f('mic_active')}, ${nested('PresenceSignals', 'microphone_active')})`;
    const cam = `if(${isV3}, ${f('cam_active')}, ${nested('PresenceSignals', 'camera_active')})`;
    const call = `if(${isV3}, ${f('call_app_active')}, ${nested('PresenceSignals', 'call_app_active')})`;
    const legacyBeat = f('BeatDuration');

    return `
        contractor_id,
        agent_id,
        session_id,
        agent_session_id,
        timestamp AS beat_timestamp,
        if((${keyboard}) + (${mouse}) = 0 AND (${idle}) >= ${idleThreshold}, 1, 0) AS is_idle,
        toUInt32(${keyboard}) AS keyboard_count,
        toUInt32(${mouse}) AS mouse_clicks,
        if((${rawBeat}) > 0, ${rawBeat}, 15) AS beat_duration,
        if((${powerState}) != '', ${powerState}, 'active') AS power_state,
        ${browserSource} AS browser_source,
        toUInt8(${mic}) AS mic_active,
        toUInt8(${cam}) AS cam_active,
        toUInt8(${call}) AS call_app_active,
        -- v3 se autodeclara; v2 se reconoce por traer BeatDuration (se agregó
        -- junto con DomainUsage, así que alcanza con mirar uno de los dos).
        if(${isV3}, 3, if(${legacyBeat} > 0, 2, 1)) AS payload_version,
        toDate(timestamp, '${OPERATIONAL_TIMEZONE}') AS workday,
        now() AS created_at
    `;
  }

  /** Lista de columnas destino (orden explícito) para INSERT en contractor_activity_15s. */
  private static readonly ACTIVITY_INSERT_COLUMNS = `
    contractor_id, agent_id, session_id, agent_session_id, beat_timestamp,
    is_idle, keyboard_count, mouse_clicks, beat_duration, power_state,
    browser_source, mic_active, cam_active, call_app_active, payload_version,
    workday, created_at
  `;

  /**
   * Construye la query de INSERT SELECT para un día concreto de contractor_activity_15s.
   */
  private buildInsertActivityQuery(day: Date, contractorId?: string): string {
    const dayStr = formatDateInTZ(day);

    const eventsFilter = contractorId
      ? ` AND contractor_id = '${contractorId}'`
      : '';

    return `
      INSERT INTO contractor_activity_15s (${EtlService.ACTIVITY_INSERT_COLUMNS})
      ${payloadCteSql()}
      SELECT
        ${this.buildActivitySelectColumns()}
      FROM events_raw
      WHERE toDate(timestamp, '${OPERATIONAL_TIMEZONE}') = toDate('${dayStr}')
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

      // Los límites de partición se derivan en la ZONA OPERATIVA, no en UTC.
      // `formatDate()` es `toISOString()`, así que el fin de un día operativo cae
      // en el día UTC siguiente: reprocesar el día D calculaba `toDay = D+1` y el
      // DELETE se llevaba puesta la partición del día siguiente sin reponerla.
      const fromDay = from ? formatDateInTZ(from) : null;
      const toDay = to ? formatDateInTZ(to) : null;

      const contractorFilter = contractorId
        ? ` AND contractor_id = '${contractorId}'`
        : '';

      // 1) Borrar EXACTAMENTE lo que se va a reinsertar.
      //
      // Los límites de `workday` solo podan particiones (barato); la precisión
      // real la da el filtro por `beat_timestamp`, que es el mismo rango que usa
      // el INSERT de abajo. Antes se borraba el workday completo mientras el
      // INSERT reponía solo el rango pedido: como el rango por defecto es de 2
      // horas, una llamada sin argumentos borraba el día entero y devolvía 2
      // horas de datos.
      //
      // `mutations_sync = 1` espera a que la mutación termine: sin eso el INSERT
      // corre contra un borrado a medias y quedan filas duplicadas.
      await this.clickHouseService.command(`
        ALTER TABLE contractor_activity_15s DELETE
        WHERE 1=1
          ${fromDay ? `AND workday >= toDate('${fromDay}')` : ''}
          ${toDay ? `AND workday <= toDate('${toDay}')` : ''}
          ${fromStr ? `AND beat_timestamp >= '${fromStr}'` : ''}
          ${toStr ? `AND beat_timestamp <= '${toStr}'` : ''}
          ${contractorFilter}
        SETTINGS mutations_sync = 1
      `);

      // 2) Insertar con INSERT SELECT usando filtros de timestamp (y contratista si aplica)
      const filters =
        (fromStr ? ` AND timestamp >= '${fromStr}'` : '') +
        (toStr ? ` AND timestamp <= '${toStr}'` : '') +
        contractorFilter;

      const insertQuery = `
        INSERT INTO contractor_activity_15s (${EtlService.ACTIVITY_INSERT_COLUMNS})
        ${payloadCteSql()}
        SELECT
          ${this.buildActivitySelectColumns()}
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
   * @param force - Si true, borra y recalcula los días indicados aunque ya estén poblados.
   *                Necesario para backfills tras un cambio de fórmula o de pesos; sin este
   *                flag los días ya existentes se saltean y el reproceso es un no-op.
   */
  async processActivityToDailyMetrics(
    workday?: Date,
    fromDate?: Date,
    toDate?: Date,
    contractorIds?: string[],
    force = false,
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
          days.push(formatDateInTZ(d));
        });
      } else if (workday) {
        days.push(formatDateInTZ(coerceToOperationalDayStart(workday)));
      } else {
        days.push(formatDateInTZ(new Date()));
      }

      const contractorFilter = contractorIds?.length
        ? ` AND contractor_id IN (${contractorIds.map((c) => `'${c}'`).join(',')})`
        : '';

      const allMetrics: ContractorDailyMetricsDto[] = [];
      /** Días pedidos con force pero sin beats fuente (TTL vencido): no se tocaron. */
      const skippedNoSource: string[] = [];
      for (const dayStr of days) {
        // Precedencia:
        //  1. force        → borrar y recalcular SIEMPRE (backfill por cambio de pesos).
        //  2. contractorIds → borrar solo esos contractors y reinsertarlos.
        //  3. default      → idempotente: si el día ya está poblado, devolver lo existente.
        if (force) {
          // GUARDA ANTI-BORRADO: contractor_activity_15s tiene TTL de 365 días y
          // contractor_daily_metrics de 730. Para días viejos los beats fuente ya no
          // existen: borrar y recalcular dejaría el día VACÍO en vez de recalculado.
          // Si no hay fuente, se preserva lo que haya y se avisa.
          const src = await this.clickHouseService.query<{ cnt: number }>(`
            SELECT count() AS cnt
            FROM contractor_activity_15s
            WHERE workday = toDate('${dayStr}')
              AND power_state = 'active'
            ${contractorFilter}
          `);
          if ((src[0]?.cnt || 0) === 0) {
            this.logger.warn(
              `⚠️ Skipping force recompute for ${dayStr}: no source beats in ` +
                `contractor_activity_15s (fuera del TTL de 365 días o día sin actividad). ` +
                `Se preservan las métricas existentes.`,
            );
            skippedNoSource.push(dayStr);
            continue;
          }

          // mutations_sync = 1: esperar a que la mutación termine antes de reinsertar.
          // Sin esto el INSERT puede correr contra el DELETE todavía en vuelo y dejar
          // filas duplicadas hasta el próximo merge del ReplacingMergeTree.
          await this.clickHouseService.command(`
            ALTER TABLE contractor_daily_metrics DELETE
            WHERE workday = toDate('${dayStr}')
            ${contractorFilter}
            SETTINGS mutations_sync = 1
          `);
          this.logger.log(
            `♻️ Force recompute contractor_daily_metrics for ${dayStr}` +
              `${contractorIds?.length ? ` (${contractorIds.length} contractors)` : ' (all contractors)'}`,
          );
        } else if (!contractorIds?.length) {
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

        // ✅ CONSOLIDACIÓN MULTI-AGENTE + GRACE PERIOD (real_idle) + score multiplicativo
        // - Consolida beats por timestamp: contratista activo si CUALQUIER agente
        //   está activo, o si hay señal de presencia (mic/cam/llamada).
        // - is_real_active: ventana deslizante de 8 beats (~2 min). Un beat cuenta
        //   como activo si hubo actividad/presencia en los últimos 2 min (grace).
        // - Tiempos usan beat_duration (segundos reales), no count()*15.
        // - Score generado por ProductivityScoreService (única fuente de verdad).
        const scoreSql = this.productivityScoreService.buildScoreSql({
          activeBeats: 'sum(ca.is_real_active)',
          totalBeats: 'count()',
          weightedQualitySeconds:
            '(ifNull(any(app.weighted_seconds), 0) + ifNull(any(web.weighted_seconds), 0))',
          totalQualitySeconds:
            '(ifNull(any(app.total_seconds), 0) + ifNull(any(web.total_seconds), 0))',
        });

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
          metrics_version,
          created_at
        )
        SELECT
          ca.contractor_id,
          ca.workday,
          count() AS total_beats,
          sum(ca.is_real_active) AS active_beats,
          count() - sum(ca.is_real_active) AS idle_beats,
          100.0 * sum(ca.is_real_active) / nullIf(count(), 0) AS active_percentage,
          sum(ca.keyboard_count_contractor) AS total_keyboard_inputs,
          sum(ca.mouse_clicks_contractor) AS total_mouse_clicks,
          round(sum(ca.keyboard_count_contractor) / nullIf(sum(ca.beat_duration) / 60.0, 0), 2) AS avg_keyboard_per_min,
          round(sum(ca.mouse_clicks_contractor) / nullIf(sum(ca.beat_duration) / 60.0, 0), 2) AS avg_mouse_per_min,
          toUInt64(round(sum(ca.beat_duration))) AS total_session_time_seconds,
          toUInt64(round(sum(if(ca.is_real_active = 1, ca.beat_duration, 0)))) AS effective_work_seconds,
          ${scoreSql} AS productivity_score,
          ifNull(any(app_map.app_usage), map()) AS app_usage,
          ifNull(any(browser_map.browser_usage), map()) AS browser_usage,
          max(ca.payload_version) AS metrics_version,
          now() AS created_at
        FROM (
          SELECT
            contractor_id,
            workday,
            beat_timestamp,
            is_idle_contractor,
            keyboard_count_contractor,
            mouse_clicks_contractor,
            beat_duration,
            payload_version,
            -- Grace period: activo si hubo actividad/presencia en los últimos 8 beats (~2 min)
            max(is_active_micro) OVER (
              PARTITION BY contractor_id
              ORDER BY beat_timestamp
              ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
            ) AS is_real_active
          FROM (
            SELECT
              contractor_id,
              workday,
              beat_timestamp,
              MIN(is_idle) AS is_idle_contractor,
              SUM(keyboard_count) AS keyboard_count_contractor,
              SUM(mouse_clicks) AS mouse_clicks_contractor,
              MAX(beat_duration) AS beat_duration,
              MAX(payload_version) AS payload_version,
              -- micro-activo: algún agente activo, o presencia (mic/cam/llamada)
              toUInt8(
                MIN(is_idle) = 0
                OR MAX(mic_active) = 1
                OR MAX(cam_active) = 1
                OR MAX(call_app_active) = 1
              ) AS is_active_micro
            FROM contractor_activity_15s
            WHERE workday = toDate('${dayStr}')
              AND power_state = 'active'
              ${contractorFilter}
            GROUP BY contractor_id, workday, beat_timestamp
          )
        ) ca
        -- JOIN calidad apps: segundos ponderados por día (weight desde apps_dimension)
        LEFT JOIN (
          SELECT
            contractor_id,
            toDate(timestamp, '${OPERATIONAL_TIMEZONE}') AS workday,
            sum(tupleElement(app_kv, 2) * if(d.weight > 0, d.weight, ${ProductivityScoreService.W_UNKNOWN})) AS weighted_seconds,
            sum(tupleElement(app_kv, 2)) AS total_seconds
          FROM events_raw
          ARRAY JOIN ${appUsageMapSql()} AS app_kv
          LEFT JOIN apps_dimension d ON d.name = tupleElement(app_kv, 1)
          WHERE toDate(timestamp, '${OPERATIONAL_TIMEZONE}') = toDate('${dayStr}')
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
              toDate(timestamp, '${OPERATIONAL_TIMEZONE}') AS workday,
              tupleElement(app_kv, 1) AS app,
              sum(tupleElement(app_kv, 2)) AS sec
            FROM events_raw
            ARRAY JOIN ${appUsageMapSql()} AS app_kv
            WHERE toDate(timestamp, '${OPERATIONAL_TIMEZONE}') = toDate('${dayStr}')
            ${contractorFilter}
            GROUP BY contractor_id, workday, app
          )
          GROUP BY contractor_id, workday
        ) app_map ON app_map.contractor_id = ca.contractor_id AND app_map.workday = ca.workday
        -- JOIN calidad dominios: segundos ponderados por día (weight desde domains_dimension)
        LEFT JOIN (
          SELECT
            contractor_id,
            toDate(timestamp, '${OPERATIONAL_TIMEZONE}') AS workday,
            sum(
              tupleElement(dom_kv, 2) *
              if(d.weight > 0, d.weight, ${ProductivityScoreService.W_UNKNOWN})
            ) AS weighted_seconds,
            sum(tupleElement(dom_kv, 2)) AS total_seconds
          FROM events_raw
          ARRAY JOIN ${domainUsageMapSql()} AS dom_kv
          LEFT JOIN domains_dimension d ON d.domain = tupleElement(dom_kv, 1)
          WHERE toDate(timestamp, '${OPERATIONAL_TIMEZONE}') = toDate('${dayStr}')
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
              toDate(timestamp, '${OPERATIONAL_TIMEZONE}') AS workday,
              tupleElement(dom_kv, 1) AS dc,
              sum(tupleElement(dom_kv, 2)) AS sec
            FROM events_raw
            ARRAY JOIN ${domainUsageMapSql()} AS dom_kv
            WHERE toDate(timestamp, '${OPERATIONAL_TIMEZONE}') = toDate('${dayStr}')
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

      if (skippedNoSource.length) {
        this.logger.warn(
          `⚠️ Force recompute omitió ${skippedNoSource.length}/${days.length} día(s) sin beats fuente ` +
            `(TTL de contractor_activity_15s: 365 días). Esos días conservan su valor anterior: ` +
            `${skippedNoSource.slice(0, 10).join(', ')}` +
            `${skippedNoSource.length > 10 ? `, … (+${skippedNoSource.length - 10})` : ''}`,
        );
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
   * Construye el INSERT ... SELECT de session_summary.
   * Una fila por (session_id, agent_id) con score multiplicativo, real_idle (grace)
   * y tiempos basados en beat_duration. Fuente única de la query de sesión —
   * usado por processActivityToSessionSummary y reprocessSessionSummariesForDateRange.
   *
   * @param innerScope WHERE extra aplicado en el scan interno de contractor_activity_15s
   *                   (ej: `AND session_id = '...'`). Siempre incluye power_state='active'.
   * @param outerFilter WHERE aplicado sobre la subquery ventaneada `a`
   *                    (ej: cláusula NOT IN de idempotencia, o filtro por sesión).
   */
  private buildSessionSummaryInsertQuery(
    innerScope: string,
    outerFilter: string,
  ): string {
    const scoreSql = this.productivityScoreService.buildScoreSql({
      activeBeats: 'sum(a.is_real_active)',
      totalBeats: 'count()',
      weightedQualitySeconds:
        '(ifNull(any(app.weighted_seconds), 0) + ifNull(any(web.weighted_seconds), 0))',
      totalQualitySeconds:
        '(ifNull(any(app.app_total_seconds), 0) + ifNull(any(web.web_total_seconds), 0))',
    });

    return `
      INSERT INTO session_summary (session_id, contractor_id, agent_id, session_start, session_end, total_seconds, active_seconds, idle_seconds, productivity_score, metrics_version, created_at)
      SELECT
        a.session_id,
        any(a.contractor_id) AS contractor_id,
        any(a.agent_id) AS agent_id,
        min(a.beat_timestamp) AS session_start,
        max(a.beat_timestamp) AS session_end,
        toUInt32(round(sum(a.beat_duration))) AS total_seconds,
        toUInt32(round(sum(if(a.is_real_active = 1, a.beat_duration, 0)))) AS active_seconds,
        toUInt32(round(sum(if(a.is_real_active = 0, a.beat_duration, 0)))) AS idle_seconds,
        ${scoreSql} AS productivity_score,
        max(a.payload_version) AS metrics_version,
        now() AS created_at
      FROM (
        SELECT
          session_id,
          agent_id,
          contractor_id,
          beat_timestamp,
          keyboard_count,
          mouse_clicks,
          beat_duration,
          payload_version,
          is_active_micro,
          -- Grace period por (session_id, agent_id): activo si hubo actividad/presencia
          -- en los últimos 8 beats (~2 min).
          max(is_active_micro) OVER (
            PARTITION BY session_id, agent_id
            ORDER BY beat_timestamp
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
          ) AS is_real_active
        FROM (
          SELECT
            session_id,
            agent_id,
            contractor_id,
            beat_timestamp,
            keyboard_count,
            mouse_clicks,
            beat_duration,
            payload_version,
            toUInt8(
              is_idle = 0 OR mic_active = 1 OR cam_active = 1 OR call_app_active = 1
            ) AS is_active_micro
          FROM contractor_activity_15s
          WHERE power_state = 'active'
          ${innerScope}
        )
      ) a
      LEFT JOIN (
        SELECT
          e.session_id,
          e.agent_id,
          sum(tupleElement(app_kv, 2) * if(d.weight > 0, d.weight, ${ProductivityScoreService.W_UNKNOWN})) AS weighted_seconds,
          sum(tupleElement(app_kv, 2)) AS app_total_seconds
        FROM events_raw e
        ARRAY JOIN ${appUsageMapSql()} AS app_kv
        LEFT JOIN apps_dimension d ON d.name = tupleElement(app_kv, 1)
        GROUP BY e.session_id, e.agent_id
      ) app ON app.session_id = a.session_id AND coalesce(app.agent_id, '') = coalesce(a.agent_id, '')
      LEFT JOIN (
        SELECT
          e.session_id,
          e.agent_id,
          sum(tupleElement(dom_kv, 2) * if(d.weight > 0, d.weight, ${ProductivityScoreService.W_UNKNOWN})) AS weighted_seconds,
          sum(tupleElement(dom_kv, 2)) AS web_total_seconds
        FROM events_raw e
        ARRAY JOIN ${domainUsageMapSql()} AS dom_kv
        LEFT JOIN domains_dimension d ON d.domain = tupleElement(dom_kv, 1)
        GROUP BY e.session_id, e.agent_id
      ) web ON web.session_id = a.session_id AND coalesce(web.agent_id, '') = coalesce(a.agent_id, '')
      ${outerFilter}
      GROUP BY a.session_id, a.agent_id
      SETTINGS max_partitions_per_insert_block=1000
    `;
  }

  /**
   * Genera resúmenes de sesión desde contractor_activity_15s.
   * Una fila por (session_id, agent_id). Score multiplicativo + real_idle + beat_duration.
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
        workdayStr = formatDateInTZ(coerceToOperationalDayStart(workday));
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
            AND ${toDateTZ('session_start')} = toDate('${workdayStr}')
        `);
      }

      // 2) Construir scope interno + filtro de idempotencia según el modo.
      // - Idempotencia (modo "all pending"): excluir pares (session_id, agent_id)
      //   ya presentes en session_summary.
      // - sessionId o contractor+workday: ya borramos antes, no aplicamos NOT IN.
      let innerScope: string;
      let outerFilter: string;
      if (sessionId) {
        innerScope = `AND session_id = '${sessionId}'`;
        outerFilter = '';
      } else if (contractorId && workdayStr) {
        innerScope = `AND contractor_id = '${contractorId}' AND toDate(beat_timestamp, '${OPERATIONAL_TIMEZONE}') = toDate('${workdayStr}')`;
        outerFilter = '';
      } else {
        innerScope = `AND session_id IS NOT NULL`;
        outerFilter = `WHERE (a.session_id, coalesce(a.agent_id, '')) NOT IN (
          SELECT session_id, coalesce(agent_id, '') FROM session_summary
        )`;
      }

      const insertQuery = this.buildSessionSummaryInsertQuery(
        innerScope,
        outerFilter,
      );

      await this.clickHouseService.command(insertQuery);

      let selectWhere: string;
      if (sessionId) {
        selectWhere = `WHERE session_id = '${sessionId}'`;
      } else if (contractorId && workdayStr) {
        selectWhere = `
          WHERE contractor_id = '${contractorId}'
            AND ${toDateTZ('session_start')} = toDate('${workdayStr}')
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

  private static readonly YMD_RE = /^\d{4}-\d{2}-\d{2}$/;

  /**
   * Reprocesa TODOS los resúmenes de sesión (`session_summary`) para un rango de fechas,
   * para TODOS los contractors. Útil para corregir errores en la fórmula de productividad.
   *
   * @param fromDay - Inicio inclusive, calendario en TZ operativa (`YYYY-MM-DD`), alineado con la UI / GET sessions.
   * @param toDay - Fin inclusive, mismo formato.
   *
   * Pasos:
   * - DELETE de filas en `session_summary` cuyo session_start esté entre from/to.
   * - INSERT SELECT desde `contractor_activity_15s` (mismas joins de apps y browser) sin cláusula NOT IN.
   */
  async reprocessSessionSummariesForDateRange(
    fromDay: string,
    toDay: string,
  ): Promise<number> {
    try {
      const fromStr = fromDay.trim();
      const toStr = toDay.trim();
      if (!EtlService.YMD_RE.test(fromStr) || !EtlService.YMD_RE.test(toStr)) {
        throw new Error(
          `Invalid session summary range: expected YYYY-MM-DD, got '${fromDay}' / '${toDay}'`,
        );
      }

      this.logger.log(
        `🔄 Reprocessing session_summary for date range ${fromStr} to ${toStr} (ALL contractors)`,
      );

      // 1) Borrar todas las filas en el rango de fechas
      await this.clickHouseService.command(`
        ALTER TABLE session_summary DELETE
        WHERE ${toDateTZ('session_start')} >= toDate('${fromStr}')
          AND ${toDateTZ('session_start')} <= toDate('${toStr}')
      `);

      // 2) Insertar nuevamente todas las sesiones del rango desde contractor_activity_15s.
      // Reusa el mismo builder (score multiplicativo + real_idle + beat_duration), scopeando
      // el rango de fechas en el scan interno; sin cláusula NOT IN (ya borramos el rango).
      const innerScope = `AND session_id IS NOT NULL
        AND toDate(beat_timestamp, '${OPERATIONAL_TIMEZONE}') >= toDate('${fromStr}')
        AND toDate(beat_timestamp, '${OPERATIONAL_TIMEZONE}') <= toDate('${toStr}')`;

      const insertQuery = this.buildSessionSummaryInsertQuery(innerScope, '');

      await this.clickHouseService.command(insertQuery);

      // 3) Contar cuántos resúmenes quedaron en el rango
      const countRes = await this.clickHouseService.query<{ cnt: number }>(`
        SELECT count() AS cnt
        FROM session_summary
        WHERE ${toDateTZ('session_start')} >= toDate('${fromStr}')
          AND ${toDateTZ('session_start')} <= toDate('${toStr}')
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
    const todayStr = formatDateInTZ(now);
    const todayStart = wallTimeToUtcInOperationalZone(todayStr, 0, 0, 0);
    const todayEnd = now;

    this.logger.log(
      `🔄 [Orchestrator] Starting full ETL for contractor=${contractorId} session=${sessionId} (today: ${todayStr})`,
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
      const workdayStr = formatDateInTZ(workday);
      const query = `
        SELECT 
          tupleElement(app_kv, 1) AS app_name,
          sum(tupleElement(app_kv, 2)) as seconds
        FROM events_raw
        ARRAY JOIN ${appUsageMapSql()} AS app_kv
        WHERE contractor_id = '${contractorId}'
          AND toDate(timestamp, '${OPERATIONAL_TIMEZONE}') = '${workdayStr}'
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
      const workdayStr = formatDateInTZ(workday);
      const query = `
        SELECT 
          tupleElement(dom_kv, 1) AS domain,
          sum(tupleElement(dom_kv, 2)) as seconds
        FROM events_raw
        ARRAY JOIN ${domainUsageMapSql()} AS dom_kv
        WHERE contractor_id = '${contractorId}'
          AND toDate(timestamp, '${OPERATIONAL_TIMEZONE}') = '${workdayStr}'
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
          tupleElement(app_kv, 1) AS app_name,
          sum(tupleElement(app_kv, 2)) as seconds
        FROM events_raw
        ARRAY JOIN ${appUsageMapSql()} AS app_kv
        WHERE contractor_id = '${contractorId}'
          AND timestamp >= '${fromStr}'
          AND timestamp <= '${toStr}'
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
          tupleElement(dom_kv, 1) AS domain,
          sum(tupleElement(dom_kv, 2)) as seconds
        FROM events_raw
        ARRAY JOIN ${domainUsageMapSql()} AS dom_kv
        WHERE contractor_id = '${contractorId}'
          AND timestamp >= '${fromStr}'
          AND timestamp <= '${toStr}'
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
    return date.toISOString().replace('T', ' ').slice(0, 19);
  }
}
