import { Injectable } from '@nestjs/common';

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
        beat_timestamp,
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
        beat_timestamp,
        is_idle,
        keyboard_count,
        mouse_clicks,
        workday
      FROM contractor_activity_15s
      WHERE contractor_id = '${contractorId}'
        AND toDate(beat_timestamp) = '${workday}'
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
      // Si ya parece un DateTime con espacio, lo usamos tal cual
      if (input.includes(' ')) {
        return input;
      }

      // Si viene en ISO con 'T', lo parseamos y formateamos
      if (input.includes('T')) {
        const dateObj = new Date(input);
        return this.formatDateTime(dateObj);
      }

      // Si viene como 'YYYY-MM-DD', dejamos que el caller decida si quiere
      // 00:00 o 23:59; aquí lo tratamos como 00:00 por defecto.
      return `${input} 00:00:00`;
    }

    return this.formatDateTime(input);
  }

  private formatDateTime(date: Date): string {
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, '0');
    const day = String(date.getUTCDate()).padStart(2, '0');
    const hours = String(date.getUTCHours()).padStart(2, '0');
    const minutes = String(date.getUTCMinutes()).padStart(2, '0');
    const seconds = String(date.getUTCSeconds()).padStart(2, '0');

    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
  }
}
