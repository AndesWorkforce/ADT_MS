import { Injectable, Logger } from '@nestjs/common';

import { ContractorActivity15sDto } from '../dto/contractor-activity-15s.dto';
import { SessionSummaryDto } from '../dto/session-summary.dto';
import { DimensionsService } from '../services/dimensions.service';
import { ProductivityScoreService } from '../services/productivity-score.service';
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
  constructor(
    private readonly dimensionsService: DimensionsService,
    private readonly productivityScoreService: ProductivityScoreService,
  ) {}

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
      const { weighted, total } = this.computeQuality(
        appUsage || [],
        browserUsage || [],
      );
      const sQuality = total > 0 ? 100 * (weighted / total) : 0;
      this.logger.debug(
        `SessionSummary agg ${dto.session_id} ` +
          `S_active=${sActive.toFixed(2)} S_quality=${sQuality.toFixed(2)} ` +
          `score=${dto.productivity_score.toFixed(2)}`,
      );
    }

    return dto;
  }

  /**
   * Calcula el productivity_score (fórmula multiplicativa unificada).
   * totalKeyboard/totalMouse/minutes se mantienen por compatibilidad de firma
   * pero ya no influyen en el score.
   */
  private calculateProductivityScore(
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

  /** apps + dominios en una sola bolsa de calidad (Σ seconds*weight, Σ seconds). */
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
