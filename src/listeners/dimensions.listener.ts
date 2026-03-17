import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';

import { getMessagePattern, logError } from 'config';

import { RawService } from '../raw/raw.service';
import { TeamPayload, ClientPayload } from 'src/listeners/listener.interfaces';

@Controller()
export class DimensionsListener {
  private readonly logger = new Logger(DimensionsListener.name);

  constructor(private readonly rawService: RawService) {}

  /**
   * Escuchar evento team.created de USER_MS
   */
  @EventPattern(getMessagePattern('team.created'))
  async handleTeamCreated(@Payload() team: TeamPayload): Promise<void> {
    try {
      this.logger.debug(`Received team.created: ${team.id}`);

      if (!team.id || !team.name) {
        this.logger.warn(
          `⚠️ DimensionsListener: Team received without valid ID or name: ${JSON.stringify(team)}`,
        );
        return;
      }

      await this.rawService.saveTeam(team.id, team.name);
      this.logger.debug(
        `✅ Team saved to dimensions: ${team.id} - ${team.name}`,
      );
    } catch (error) {
      logError(this.logger, 'Error processing team.created', error);
    }
  }

  /**
   * Escuchar evento team.updated de USER_MS
   */
  @EventPattern(getMessagePattern('team.updated'))
  async handleTeamUpdated(@Payload() team: TeamPayload): Promise<void> {
    try {
      this.logger.debug(`Received team.updated: ${team.id}`);

      if (!team.id || !team.name) {
        this.logger.warn(
          `⚠️ DimensionsListener: Team received without valid ID or name: ${JSON.stringify(team)}`,
        );
        return;
      }

      await this.rawService.saveTeam(team.id, team.name);
      this.logger.debug(
        `✅ Team updated in dimensions: ${team.id} - ${team.name}`,
      );
    } catch (error) {
      logError(this.logger, 'Error processing team.updated', error);
    }
  }

  /**
   * Escuchar evento client.created de USER_MS
   */
  @EventPattern(getMessagePattern('client.created'))
  async handleClientCreated(@Payload() client: ClientPayload): Promise<void> {
    try {
      this.logger.debug(`Received client.created: ${client.id}`);

      if (!client.id || !client.name) {
        this.logger.warn(
          `⚠️ DimensionsListener: Client received without valid ID or name: ${JSON.stringify(client)}`,
        );
        return;
      }

      await this.rawService.saveClient(
        client.id,
        client.name,
        client.isActive !== undefined ? client.isActive : true,
      );
      this.logger.debug(
        `✅ Client saved to dimensions: ${client.id} - ${client.name}`,
      );
    } catch (error) {
      logError(this.logger, 'Error processing client.created', error);
    }
  }

  /**
   * Escuchar evento client.updated de USER_MS
   */
  @EventPattern(getMessagePattern('client.updated'))
  async handleClientUpdated(@Payload() client: ClientPayload): Promise<void> {
    try {
      this.logger.debug(`Received client.updated: ${client.id}`);

      if (!client.id || !client.name) {
        this.logger.warn(
          `⚠️ DimensionsListener: Client received without valid ID or name: ${JSON.stringify(client)}`,
        );
        return;
      }

      await this.rawService.saveClient(client.id, client.name);
      this.logger.debug(
        `✅ Client updated in dimensions: ${client.id} - ${client.name}`,
      );
    } catch (error) {
      logError(this.logger, 'Error processing client.updated', error);
    }
  }
}
