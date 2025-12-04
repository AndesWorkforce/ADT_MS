import { Injectable, Logger } from '@nestjs/common';

import { ContractorActivity15sDto } from '../dto/contractor-activity-15s.dto';
import { SessionSummaryDto } from '../dto/session-summary.dto';
import { DimensionsService } from '../services/dimensions.service';
import {
  AppUsageData,
  BrowserUsageData,
} from './activity-to-daily-metrics.transformer';

/**
 * Agrega beats de 15s (contractor_activity_15s) a un resumen por sesión (session_summary).
 * Asume que todos los beats corresponden a una misma sesión.
 * Implementa la fórmula multi-factor de productividad según PRODUCTIVITY_SCORE.md.
 */
@Injectable()
export class ActivityToSessionSummaryTransformer {
  private readonly logger = new Logger(
    ActivityToSessionSummaryTransformer.name,
  );
  constructor(private readonly dimensionsService: DimensionsService) {}

  aggregate(
    sessionId: string,
    contractorId: string,
    beats: ContractorActivity15sDto[],
    appUsage?: AppUsageData[], // Opcional: datos de AppUsage de la sesión
    browserUsage?: BrowserUsageData[], // Opcional: datos de Browser de la sesión
  ): SessionSummaryDto | null {
    if (beats.length === 0) {
      return null;
    }

    const sorted = [...beats].sort(
      (a, b) => a.beat_timestamp.getTime() - b.beat_timestamp.getTime(),
    );

    const dto = new SessionSummaryDto();
    dto.session_id = sessionId;
    dto.contractor_id = contractorId;

    dto.session_start = sorted[0].beat_timestamp;
    dto.session_end = sorted[sorted.length - 1].beat_timestamp;

    const totalBeats = sorted.length;
    const activeBeats = sorted.filter((b) => !b.is_idle).length;
    const idleBeats = totalBeats - activeBeats;

    dto.total_seconds = totalBeats * 15;
    dto.active_seconds = activeBeats * 15;
    dto.idle_seconds = idleBeats * 15;

    // Calcular inputs totales y minutos
    const totalKeyboard = sorted.reduce(
      (acc, b) => acc + (b.keyboard_count || 0),
      0,
    );
    const totalMouse = sorted.reduce(
      (acc, b) => acc + (b.mouse_clicks || 0),
      0,
    );
    const minutes = totalBeats > 0 ? (totalBeats * 15) / 60 : 0;

    // Calcular productivity_score usando fórmula multi-factor
    dto.productivity_score = this.calculateProductivityScore(
      activeBeats,
      totalBeats,
      totalKeyboard,
      totalMouse,
      minutes,
      appUsage || [],
      browserUsage || [],
    );

    if (process.env.ETL_DEBUG_LOGS === '1') {
      const sActive = totalBeats > 0 ? 100 * (activeBeats / totalBeats) : 0;
      const inputsPerMinDbg =
        minutes > 0 ? (totalKeyboard + totalMouse) / minutes : 0;
      const sInputs = Math.min(100, 15 * Math.log(1 + inputsPerMinDbg / 2));
      const sApps = this.calculateAppsScore(appUsage || []);
      const sBrowser = this.calculateBrowserScore(browserUsage || []);
      this.logger.debug(
        `SessionSummary agg ${dto.session_id} ` +
          `S_active=${sActive.toFixed(2)} S_inputs=${sInputs.toFixed(2)} ` +
          `S_apps=${sApps.toFixed(2)} S_browser=${sBrowser.toFixed(2)} ` +
          `score=${dto.productivity_score.toFixed(2)}`,
      );
    }

    return dto;
  }

  /**
   * Calcula el productivity_score usando la fórmula multi-factor.
   * Ver PRODUCTIVITY_SCORE.md para detalles.
   */
  private calculateProductivityScore(
    activeBeats: number,
    totalBeats: number,
    totalKeyboard: number,
    totalMouse: number,
    minutes: number,
    appUsage: AppUsageData[],
    browserUsage: BrowserUsageData[],
  ): number {
    // 1. S_active: Tiempo activo vs idle (35%)
    const sActive = totalBeats > 0 ? 100 * (activeBeats / totalBeats) : 0;

    // 2. S_inputs: Intensidad de inputs (20%)
    const inputsPerMin =
      minutes > 0 ? (totalKeyboard + totalMouse) / minutes : 0;
    const sInputs = Math.min(100, 20 * Math.log(1 + inputsPerMin));

    // 3. S_apps: Apps productivas (30%)
    const sApps = this.calculateAppsScore(appUsage);

    // 4. S_browser: Web productiva (15%)
    const sBrowser = this.calculateBrowserScore(browserUsage);

    // Pesos (configurables)
    const w1 = 0.35; // S_active
    const w2 = 0.2; // S_inputs
    const w3 = 0.3; // S_apps
    const w4 = 0.15; // S_browser

    // Score final ponderado
    const score = w1 * sActive + w2 * sInputs + w3 * sApps + w4 * sBrowser;

    // Normalizar a 0-100
    return Math.min(100, Math.max(0, score));
  }

  /**
   * Calcula S_apps: score basado en apps productivas.
   */
  private calculateAppsScore(appUsage: AppUsageData[]): number {
    if (appUsage.length === 0) {
      return 50; // Default si no hay datos
    }

    let weightedSeconds = 0;
    let totalSeconds = 0;

    for (const usage of appUsage) {
      const weight = this.dimensionsService.getAppWeight(usage.appName);
      weightedSeconds += usage.seconds * weight;
      totalSeconds += usage.seconds;
    }

    if (totalSeconds === 0) {
      return 50;
    }

    return 100 * (weightedSeconds / totalSeconds);
  }

  /**
   * Calcula S_browser: score basado en dominios productivos.
   */
  private calculateBrowserScore(browserUsage: BrowserUsageData[]): number {
    if (browserUsage.length === 0) {
      return 50; // Default si no hay datos
    }

    let weightedSeconds = 0;
    let totalSeconds = 0;

    for (const usage of browserUsage) {
      const weight = this.dimensionsService.getDomainWeight(usage.domain);
      weightedSeconds += usage.seconds * weight;
      totalSeconds += usage.seconds;
    }

    if (totalSeconds === 0) {
      return 50;
    }

    return 100 * (weightedSeconds / totalSeconds);
  }
}
