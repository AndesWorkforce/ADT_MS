import { Injectable, Logger } from '@nestjs/common';

import { logError, formatDateInTZ } from 'config';

import { ClickHouseService } from '../clickhouse/clickhouse.service';
import { AgentSessionRawDto } from './dto/agent-session-raw.dto';
import { ContractorRawDto } from './dto/contractor-raw.dto';
import { EventRawDto } from './dto/event-raw.dto';
import { SessionRawDto } from './dto/session-raw.dto';

@Injectable()
export class RawService {
  private readonly logger = new Logger(RawService.name);

  constructor(private readonly clickHouseService: ClickHouseService) {}

  /**
   * Helper genérico para insertar en ClickHouse con logs y manejo de errores consistentes.
   */
  private async safeInsert(
    table: string,
    payload: Record<string, unknown>,
    logContext: string,
  ): Promise<void> {
    try {
      await this.clickHouseService.insert(table, payload);
      this.logger.debug(logContext);
    } catch (error) {
      logError(this.logger, `Failed to insert into ${table}`, error);
      throw error;
    }
  }

  /**
   * Guardar evento en la tabla events_raw
   */
  async saveEvent(event: EventRawDto): Promise<void> {
    try {
      // Logs de prueba para MVs: contar antes
      const enableMvLogs = process.env.ETL_DEBUG_LOGS === '1';
      const contractorId = event.contractor_id;
      const workdayStr = formatDateInTZ(
        event.timestamp ? new Date(event.timestamp) : new Date(),
      );

      let beforeBeats = 0;

      if (enableMvLogs && contractorId) {
        try {
          const b1 = await this.clickHouseService.query<{ cnt: number }>(`
            SELECT count() AS cnt
            FROM contractor_activity_15s
            WHERE contractor_id = '${contractorId}' AND workday = toDate('${workdayStr}')
          `);
          beforeBeats = Number(b1[0]?.cnt || 0);
        } catch {}
      }

      await this.clickHouseService.insert('events_raw', {
        event_id: event.event_id,
        contractor_id: event.contractor_id,
        agent_id: event.agent_id || null,
        session_id: event.session_id || null,
        agent_session_id: event.agent_session_id || null,
        timestamp: event.timestamp,
        payload:
          typeof event.payload === 'string'
            ? event.payload
            : JSON.stringify(event.payload),
        created_at: event.created_at,
      });

      // Solo log en debug para reducir ruido
      this.logger.debug(
        `✅ RawService: Event saved - Event ID: ${event.event_id}, Contractor: ${event.contractor_id}`,
      );

      // Logs de verificación de MVs (post-insert)
      if (enableMvLogs && contractorId) {
        try {
          // pequeña espera para permitir que las MVs disparen
          await new Promise((r) => setTimeout(r, 200));

          const a1 = await this.clickHouseService.query<{ cnt: number }>(`
            SELECT count() AS cnt
            FROM contractor_activity_15s
            WHERE contractor_id = '${contractorId}' AND workday = toDate('${workdayStr}')
          `);
          const afterBeats = Number(a1[0]?.cnt || 0);

          const dBeats = afterBeats - beforeBeats;

          this.logger.log(
            `🧪 MV check for contractor ${contractorId} on ${workdayStr} → ` +
              `activity_15s: ${beforeBeats.toLocaleString('en-US')} → ${afterBeats.toLocaleString('en-US')} (Δ ${dBeats})`,
          );
        } catch (err) {
          this.logger.debug(`MV check skipped: ${(err as Error).message}`);
        }
      }
    } catch (error) {
      this.logger.error(
        `❌ RawService: Error saving event - Event ID: ${event.event_id}, Error: ${error.message}`,
      );
      logError(this.logger, 'Failed to save event to RAW', error);
      throw error;
    }
  }

  /**
   * Guardar sesión en la tabla sessions_raw
   */
  async saveSession(session: SessionRawDto): Promise<void> {
    await this.safeInsert(
      'sessions_raw',
      {
        session_id: session.session_id,
        contractor_id: session.contractor_id,
        session_start: session.session_start,
        session_end: session.session_end || null,
        total_duration: session.total_duration || null,
        created_at: session.created_at,
        updated_at: session.updated_at,
      },
      `Session saved to sessions_raw: ${session.session_id} for contractor ${session.contractor_id}`,
    );
  }

  /**
   * Guardar sesión de agente en la tabla agent_sessions_raw
   */
  async saveAgentSession(agentSession: AgentSessionRawDto): Promise<void> {
    await this.safeInsert(
      'agent_sessions_raw',
      {
        agent_session_id: agentSession.agent_session_id,
        contractor_id: agentSession.contractor_id,
        agent_id: agentSession.agent_id,
        session_id: agentSession.session_id || null,
        session_start: agentSession.session_start,
        session_end: agentSession.session_end || null,
        total_duration: agentSession.total_duration || null,
        created_at: agentSession.created_at,
        updated_at: agentSession.updated_at,
      },
      `Agent session saved to agent_sessions_raw: ${agentSession.agent_session_id} for contractor ${agentSession.contractor_id}`,
    );
  }

  /**
   * Guardar o actualizar contractor en la tabla contractor_info_raw
   * Usa el motor ReplacingMergeTree, por lo que las actualizaciones son manejadas por ClickHouse
   */
  async saveContractor(contractor: ContractorRawDto): Promise<void> {
    await this.safeInsert(
      'contractor_info_raw',
      {
        contractor_id: contractor.contractor_id,
        name: contractor.name,
        email: contractor.email || null,
        job_position: contractor.job_position,
        work_schedule_start: contractor.work_schedule_start || null,
        work_schedule_end: contractor.work_schedule_end || null,
        country: contractor.country || null,
        client_id: contractor.client_id,
        team_id: contractor.team_id || null,
        isActive: contractor.isActive ? 1 : 0,
        created_at: contractor.created_at,
        updated_at: contractor.updated_at,
      },
      `Contractor saved to contractor_info_raw: ${contractor.contractor_id}`,
    );
  }

  /**
   * Guardar o actualizar team en la tabla teams_dimension
   * Usa el motor ReplacingMergeTree, por lo que las actualizaciones son manejadas por ClickHouse
   */
  async saveTeam(teamId: string, teamName: string): Promise<void> {
    const now = new Date();
    await this.safeInsert(
      'teams_dimension',
      {
        team_id: teamId,
        team_name: teamName,
        created_at: now,
        updated_at: now,
      },
      `Team saved to teams_dimension: ${teamId} - ${teamName}`,
    );
  }

  /**
   * Guardar o actualizar client en la tabla clients_dimension
   * Usa el motor ReplacingMergeTree, por lo que las actualizaciones son manejadas por ClickHouse
   */
  async saveClient(
    clientId: string,
    clientName: string,
    isActive: boolean = true,
  ): Promise<void> {
    const now = new Date();
    await this.safeInsert(
      'clients_dimension',
      {
        client_id: clientId,
        client_name: clientName,
        isActive: isActive ? 1 : 0,
        created_at: now,
        updated_at: now,
      },
      `Client saved to clients_dimension: ${clientId} - ${clientName}`,
    );
  }
}
