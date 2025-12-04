import { Injectable } from '@nestjs/common';

import { EventRawDto } from '../../raw/dto/event-raw.dto';
import { AppUsageSummaryDto } from '../dto/app-usage-summary.dto';

/**
 * Agrega events_raw a métricas diarias de uso de aplicaciones (app_usage_summary).
 * Usa el campo AppUsage del payload:
 *  AppUsage: { [appName: string]: number /* segundos en ese app en el heartbeat */

@Injectable()
export class EventsToAppUsageTransformer {
  aggregate(events: EventRawDto[]): AppUsageSummaryDto[] {
    const byKey = new Map<string, AppUsageSummaryDto>();

    for (const event of events) {
      const parsed =
        typeof event.payload === 'string'
          ? JSON.parse(event.payload || '{}')
          : event.payload || {};

      const appUsage = parsed.AppUsage || {};
      const workday = this.getWorkday(event.timestamp);

      for (const appName of Object.keys(appUsage)) {
        const seconds = Number(appUsage[appName]) || 0;
        if (seconds <= 0) continue;

        const key = `${event.contractor_id}__${workday.toISOString()}__${appName}`;
        let row = byKey.get(key);
        if (!row) {
          row = {
            contractor_id: event.contractor_id,
            app_name: appName,
            workday,
            active_beats: 0,
          };
          byKey.set(key, row);
        }

        // Aproximar beats activos desde duración / 15s
        row.active_beats += Math.round(seconds / 15);
      }
    }

    return Array.from(byKey.values());
  }

  private getWorkday(timestamp: Date): Date {
    const d = new Date(timestamp);
    return new Date(d.getFullYear(), d.getMonth(), d.getDate());
  }
}
