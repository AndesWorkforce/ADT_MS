import { Injectable, Logger } from '@nestjs/common';

import { ContractorActivity15sDto } from '../dto/contractor-activity-15s.dto';
import { ContractorDailyMetricsDto } from '../dto/contractor-daily-metrics.dto';
import { DimensionsService } from '../services/dimensions.service';

/**
 * Interfaces auxiliares para pasar datos de AppUsage y Browser
 */
export interface AppUsageData {
  appName: string;
  seconds: number;
  type?: string; // Tipo de aplicación desde apps_dimension
}

export interface BrowserUsageData {
  domain: string;
  seconds: number;
}

/**
 * Agrega beats de 15s (contractor_activity_15s) a métricas diarias por contractor.
 * Implementa la fórmula multi-factor de productividad según PRODUCTIVITY_SCORE.md:
 * - S_active: tiempo activo vs idle (35%)
 * - S_inputs: intensidad de inputs (20%)
 * - S_apps: apps productivas (30%)
 * - S_browser: web productiva (15%)
 */
@Injectable()
export class ActivityToDailyMetricsTransformer {
  private readonly logger = new Logger(ActivityToDailyMetricsTransformer.name);
  constructor(private readonly dimensionsService: DimensionsService) {}

  aggregate(
    contractorId: string,
    workday: Date,
    beats: ContractorActivity15sDto[],
    appUsage?: AppUsageData[], // Opcional: datos de AppUsage del día
    browserUsage?: BrowserUsageData[], // Opcional: datos de Browser del día
  ): ContractorDailyMetricsDto {
    const dto = new ContractorDailyMetricsDto();
    dto.contractor_id = contractorId;
    dto.workday = workday;

    const totalBeats = beats.length;
    const activeBeats = beats.filter((b) => !b.is_idle).length;
    const idleBeats = totalBeats - activeBeats;

    dto.total_beats = totalBeats;
    dto.active_beats = activeBeats;
    dto.idle_beats = idleBeats;
    dto.active_percentage =
      totalBeats > 0 ? (activeBeats / totalBeats) * 100 : 0;

    const totalKeyboard = beats.reduce(
      (acc, b) => acc + (b.keyboard_count || 0),
      0,
    );
    const totalMouse = beats.reduce((acc, b) => acc + (b.mouse_clicks || 0), 0);

    dto.total_keyboard_inputs = totalKeyboard;
    dto.total_mouse_clicks = totalMouse;

    // Cada beat son 15 segundos ⇒ 4 beats por minuto
    const minutes = totalBeats > 0 ? (totalBeats * 15) / 60 : 0; // total_seconds / 60

    dto.avg_keyboard_per_min = minutes > 0 ? totalKeyboard / minutes : 0;
    dto.avg_mouse_per_min = minutes > 0 ? totalMouse / minutes : 0;

    dto.total_session_time_seconds = totalBeats * 15;
    dto.effective_work_seconds = activeBeats * 15;

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

    // Debug opcional de sub-scores
    if (process.env.ETL_DEBUG_LOGS === '1') {
      const sActive = totalBeats > 0 ? 100 * (activeBeats / totalBeats) : 0;
      const inputsPerMinDbg =
        minutes > 0 ? (totalKeyboard + totalMouse) / minutes : 0;
      const sInputs = Math.min(100, 15 * Math.log(1 + inputsPerMinDbg / 2));
      const sApps = this.calculateAppsScore(appUsage || []);
      const sBrowser = this.calculateBrowserScore(browserUsage || []);
      this.logger.debug(
        `DailyMetrics agg ${dto.contractor_id} ${dto.workday.toISOString().split('T')[0]} ` +
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
   * Expuesto como público para ser reutilizado por RealtimeMetricsService en el flujo bulk.
   */
  calculateProductivityScore(
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
   * Normalizado a 0-100.
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

    // Normalizar a 0-100 (los pesos pueden ser > 1.0, así que el resultado puede exceder 100)
    const score = 100 * (weightedSeconds / totalSeconds);
    return Math.min(100, Math.max(0, score));
  }

  /**
   * Calcula S_browser: score basado en dominios productivos.
   * Normalizado a 0-100.
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

    // Normalizar a 0-100 (los pesos pueden ser > 1.0, así que el resultado puede exceder 100)
    const score = 100 * (weightedSeconds / totalSeconds);
    return Math.min(100, Math.max(0, score));
  }
}
