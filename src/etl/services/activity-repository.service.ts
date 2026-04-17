import { Injectable } from '@nestjs/common';

import { OPERATIONAL_TIMEZONE } from 'config';
import { ClickHouseService } from '../../clickhouse/clickhouse.service';
import { ContractorActivity15sDto } from '../dto/contractor-activity-15s.dto';

@Injectable()
export class ActivityRepository {
  constructor(private readonly clickHouseService: ClickHouseService) {}

  /**
   * Devuelve beats de un contractor opcionalmente acotados por rango DateTime.
   * Acepta strings ya formateadas para ClickHouse o Date que se formatean aquí.
   */
  async getBeatsForContractor(
    contractorId: string,
    from?: string | Date,
    to?: string | Date,
    limit: number = 1000,
  ): Promise<ContractorActivity15sDto[]> {
    let query = `
      SELECT 
        contractor_id,
        agent_id,
        session_id,
        agent_session_id,
        formatDateTime(
          toTimeZone(beat_timestamp, '${OPERATIONAL_TIMEZONE}'),
          '%Y-%m-%d %H:%i:%s'
        ) AS beat_timestamp,
        is_idle,
        keyboard_count,
        mouse_clicks,
        workday
      FROM contractor_activity_15s
      WHERE contractor_id = '${contractorId}'
    `;

    const fromStr = from
      ? this.ensureClickHouseDateTimeString(from)
      : undefined;
    const toStr = to ? this.ensureClickHouseDateTimeString(to) : undefined;

    if (fromStr) {
      query += ` AND beat_timestamp >= '${fromStr}'`;
    }

    if (toStr) {
      query += ` AND beat_timestamp <= '${toStr}'`;
    }

    query += ` ORDER BY beat_timestamp DESC LIMIT ${limit}`;

    return this.clickHouseService.query<ContractorActivity15sDto>(query);
  }

  /**
   * Devuelve todos los beats de un contractor para un workday (YYYY-MM-DD).
   */
  async getBeatsForWorkday(
    contractorId: string,
    workday: string,
  ): Promise<ContractorActivity15sDto[]> {
    const query = `
      SELECT 
        contractor_id,
        agent_id,
        session_id,
        agent_session_id,
        formatDateTime(
          toTimeZone(beat_timestamp, '${OPERATIONAL_TIMEZONE}'),
          '%Y-%m-%d %H:%i:%s'
        ) AS beat_timestamp,
        is_idle,
        keyboard_count,
        mouse_clicks,
        workday
      FROM contractor_activity_15s
      WHERE contractor_id = '${contractorId}'
        AND toDate(beat_timestamp, '${OPERATIONAL_TIMEZONE}') = '${workday}'
      ORDER BY beat_timestamp
    `;

    return this.clickHouseService.query<ContractorActivity15sDto>(query);
  }

  /**
   * Normaliza un string o Date a 'YYYY-MM-DD HH:MM:SS' para ClickHouse.
   * Si ya viene en formato con espacio (YYYY-MM-DD HH:MM:SS), se respeta tal cual.
   */
  private ensureClickHouseDateTimeString(input: string | Date): string {
    if (typeof input === 'string') {
      if (input.includes(' ')) {
        return input;
      }

      if (input.includes('T')) {
        const dateObj = new Date(input);
        return dateObj.toISOString().replace('T', ' ').slice(0, 19);
      }

      return `${input} 00:00:00`;
    }

    return input.toISOString().replace('T', ' ').slice(0, 19);
  }
}
