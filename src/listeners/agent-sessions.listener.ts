import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';

import { getMessagePattern, logError } from 'config';

import { RawService } from '../raw/raw.service';
import { AgentSessionRawDto } from '../raw/dto/agent-session-raw.dto';
import { AgentSessionPayload } from 'src/listeners/listener.interfaces';

@Controller()
export class AgentSessionsListener {
  private readonly logger = new Logger(AgentSessionsListener.name);

  constructor(private readonly rawService: RawService) {}

  private toAgentSessionRaw(
    agentSession: AgentSessionPayload,
  ): AgentSessionRawDto {
    return {
      agent_session_id: agentSession.id,
      contractor_id: agentSession.contractor_id,
      agent_id: agentSession.agent_id,
      session_id: agentSession.session_id || null,
      session_start: new Date(agentSession.session_start),
      session_end: agentSession.session_end
        ? new Date(agentSession.session_end)
        : null,
      total_duration: agentSession.total_duration || null,
      created_at: agentSession.created_at
        ? new Date(agentSession.created_at)
        : new Date(),
      updated_at: agentSession.updated_at
        ? new Date(agentSession.updated_at)
        : new Date(),
    };
  }

  /**
   * Escuchar evento agentSession.created de USER_MS
   */
  @EventPattern(getMessagePattern('agentSession.created'))
  async handleAgentSessionCreated(
    @Payload() agentSession: AgentSessionPayload,
  ): Promise<void> {
    try {
      this.logger.debug(
        `Received agentSession.created: ${agentSession.id} for agent ${agentSession.agent_id}`,
      );

      const agentSessionRaw: AgentSessionRawDto =
        this.toAgentSessionRaw(agentSession);
      await this.rawService.saveAgentSession(agentSessionRaw);
      this.logger.debug(
        `✅ Agent session saved to RAW: ${agentSessionRaw.agent_session_id}`,
      );
    } catch (error) {
      logError(this.logger, 'Error processing agentSession.created', error);
    }
  }

  /**
   * Escuchar evento agentSession.updated de USER_MS
   */
  @EventPattern(getMessagePattern('agentSession.updated'))
  async handleAgentSessionUpdated(
    @Payload() agentSession: AgentSessionPayload,
  ): Promise<void> {
    try {
      this.logger.debug(
        `Received agentSession.updated: ${agentSession.id} for agent ${agentSession.agent_id}`,
      );

      const agentSessionRaw: AgentSessionRawDto =
        this.toAgentSessionRaw(agentSession);
      await this.rawService.saveAgentSession(agentSessionRaw);
      this.logger.debug(
        `✅ Agent session updated in RAW: ${agentSessionRaw.agent_session_id}`,
      );
    } catch (error) {
      logError(this.logger, 'Error processing agentSession.updated', error);
    }
  }
}
