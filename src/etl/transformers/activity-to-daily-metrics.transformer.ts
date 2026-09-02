import { Injectable, Logger } from '@nestjs/common';

import { formatDateInTZ } from 'config';
import { ContractorActivity15sDto } from '../dto/contractor-activity-15s.dto';
import { ContractorDailyMetricsDto } from '../dto/contractor-daily-metrics.dto';
import { DimensionsService } from '../services/dimensions.service';
import { ProductivityScoreService } from '../services/productivity-score.service';

/**
 * Interfaces auxiliares para pasar datos de AppUsage y Browser
 */
export type AppCategory = 'productive' | 'neutral' | 'non_productive';

const VALID_APP_CATEGORIES: readonly AppCategory[] = [
  'productive',
  'neutral',
  'non_productive',
];

export function normalizeAppCategory(
  category?: string | null,
): AppCategory | null {
  if (!category) {
    return null;
  }
  return VALID_APP_CATEGORIES.includes(category as AppCategory)
    ? (category as AppCategory)
    : null;
}

export interface AppUsageData {
  appName: string;
  seconds: number;
  type?: string; // Tipo de aplicación desde apps_dimension
  category?: AppCategory | null;
}

export interface BrowserUsageData {
  domain: string;
  seconds: number;
}

/**
 * Agrega beats de 15s (contractor_activity_15s) a métricas diarias por contractor.
 * Usa la fórmula multiplicativa unificada (ProductivityScoreService):
 *   score = S_active * S_quality / 100
 * - S_active: tiempo activo vs idle
 * - S_quality: calidad de apps + dominios ponderados
 *
 * NOTA: este camino (realtime) usa is_idle por beat sin el grace period de 2 min
 * que sí aplica el ETL batch (contractor_daily_metrics). Es una aproximación
 * "en vivo"; la métrica oficial persistida usa real_idle.
 */
@Injectable()
export class ActivityToDailyMetricsTransformer {
  private readonly logger = new Logger(ActivityToDailyMetricsTransformer.name);
  constructor(
    private readonly dimensionsService: DimensionsService,
    private readonly productivityScoreService: ProductivityScoreService,
  ) {}

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

    // Debug opcional
    if (process.env.ETL_DEBUG_LOGS === '1') {
      const sActive = totalBeats > 0 ? 100 * (activeBeats / totalBeats) : 0;
      const { weighted, total } = this.computeQuality(
        appUsage || [],
        browserUsage || [],
      );
      const sQuality = total > 0 ? 100 * (weighted / total) : 0;
      this.logger.debug(
        `DailyMetrics agg ${dto.contractor_id} ${formatDateInTZ(dto.workday)} ` +
          `S_active=${sActive.toFixed(2)} S_quality=${sQuality.toFixed(2)} ` +
          `score=${dto.productivity_score.toFixed(2)}`,
      );
    }

    return dto;
  }

  /**
   * Calcula el productivity_score (fórmula multiplicativa unificada).
   * Expuesto como público para ser reutilizado por RealtimeMetricsService.
   *
   * Los parámetros totalKeyboard/totalMouse/minutes se mantienen en la firma por
   * compatibilidad con los callers existentes, pero ya NO influyen en el score
   * (S_inputs fue eliminado: los inputs se muestran como valor crudo en la UI).
   */
  calculateProductivityScore(
    activeBeats: number,
    totalBeats: number,
    _totalKeyboard: number,
    _totalMouse: number,
    _minutes: number,
    appUsage: AppUsageData[],
    browserUsage: BrowserUsageData[],
  ): number {
    const { weighted, total } = this.computeQuality(appUsage, browserUsage);
    return this.productivityScoreService.calculate({
      activeBeats,
      totalBeats,
      weightedQualitySeconds: weighted,
      totalQualitySeconds: total,
    });
  }

  /**
   * Combina apps + dominios en una sola "bolsa de calidad":
   * weighted = Σ(seconds * weight), total = Σ(seconds).
   * El weight de apps viene de DimensionsService.getAppWeight y el de dominios
   * de getDomainWeight. Sin datos → total = 0 (el score usa solo S_active).
   */
  private computeQuality(
    appUsage: AppUsageData[],
    browserUsage: BrowserUsageData[],
  ): { weighted: number; total: number } {
    let weighted = 0;
    let total = 0;

    for (const usage of appUsage) {
      const weight = this.dimensionsService.getAppWeight(usage.appName);
      weighted += usage.seconds * weight;
      total += usage.seconds;
    }
    for (const usage of browserUsage) {
      const weight = this.dimensionsService.getDomainWeight(usage.domain);
      weighted += usage.seconds * weight;
      total += usage.seconds;
    }

    return { weighted, total };
  }
}
