import { Injectable, Logger } from '@nestjs/common';

import { logError } from 'config';

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
   * Guardar evento en la tabla events_raw
   */
  async saveEvent(event: EventRawDto): Promise<void> {
    try {
      await this.clickHouseService.insert('events_raw', {
        event_id: event.event_id,
        contractor_id: event.contractor_id,
        agent_id: event.agent_id || null,
        session_id: event.session_id || null,
        agent_session_id: event.agent_session_id || null,
        timestamp: event.timestamp,
        payload: typeof event.payload === 'string' 
          ? event.payload 
          : JSON.stringify(event.payload),
        created_at: event.created_at,
      });

      // Solo log en debug para reducir ruido
      this.logger.debug(
        `✅ RawService: Event saved - Event ID: ${event.event_id}, Contractor: ${event.contractor_id}`,
      );
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
    try {
      await this.clickHouseService.insert('sessions_raw', {
        session_id: session.session_id,
        contractor_id: session.contractor_id,
        session_start: session.session_start,
        session_end: session.session_end || null,
        total_duration: session.total_duration || null,
        created_at: session.created_at,
      });

      this.logger.debug(
        `Session saved to sessions_raw: ${session.session_id} for contractor ${session.contractor_id}`,
      );
    } catch (error) {
      logError(this.logger, 'Failed to save session to RAW', error);
      throw error;
    }
  }

  /**
   * Guardar sesión de agente en la tabla agent_sessions_raw
   */
  async saveAgentSession(agentSession: AgentSessionRawDto): Promise<void> {
    try {
      await this.clickHouseService.insert('agent_sessions_raw', {
        agent_session_id: agentSession.agent_session_id,
        contractor_id: agentSession.contractor_id,
        agent_id: agentSession.agent_id,
        session_id: agentSession.session_id || null,
        session_start: agentSession.session_start,
        session_end: agentSession.session_end || null,
        total_duration: agentSession.total_duration || null,
        created_at: agentSession.created_at,
      });

      this.logger.debug(
        `Agent session saved to agent_sessions_raw: ${agentSession.agent_session_id} for contractor ${agentSession.contractor_id}`,
      );
    } catch (error) {
      logError(this.logger, 'Failed to save agent session to RAW', error);
      throw error;
    }
  }

  /**
   * Guardar o actualizar contractor en la tabla contractor_info_raw
   * Usa el motor ReplacingMergeTree, por lo que las actualizaciones son manejadas por ClickHouse
   */
  async saveContractor(contractor: ContractorRawDto): Promise<void> {
    try {
      await this.clickHouseService.insert('contractor_info_raw', {
        contractor_id: contractor.contractor_id,
        name: contractor.name,
        email: contractor.email || null,
        job_position: contractor.job_position,
        work_schedule_start: contractor.work_schedule_start || null,
        work_schedule_end: contractor.work_schedule_end || null,
        country: contractor.country || null,
        client_id: contractor.client_id,
        team_id: contractor.team_id || null,
        created_at: contractor.created_at,
        updated_at: contractor.updated_at,
      });

      this.logger.debug(
        `Contractor saved to contractor_info_raw: ${contractor.contractor_id}`,
      );
    } catch (error) {
      logError(this.logger, 'Failed to save contractor to RAW', error);
      throw error;
    }
  }
}

