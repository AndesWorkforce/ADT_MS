import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';

import { getMessagePattern, logError } from 'config';

import { RawService } from '../raw/raw.service';
import { SessionRawDto } from '../raw/dto/session-raw.dto';

@Controller()
export class SessionsListener {
  private readonly logger = new Logger(SessionsListener.name);

  constructor(private readonly rawService: RawService) {}

  /**
   * Escuchar evento session.created de USER_MS
   */
  @EventPattern(getMessagePattern('session.created'))
  async handleSessionCreated(@Payload() session: any): Promise<void> {
    try {
      this.logger.debug(`Received session.created: ${session.id}`);

      const sessionRaw: SessionRawDto = {
        session_id: session.id,
        contractor_id: session.contractor_id,
        session_start: new Date(session.session_start),
        session_end: session.session_end ? new Date(session.session_end) : null,
        total_duration: session.total_duration || null,
        created_at: session.created_at ? new Date(session.created_at) : new Date(),
      };

      await this.rawService.saveSession(sessionRaw);
      this.logger.debug(`✅ Session saved to RAW: ${sessionRaw.session_id}`);
    } catch (error) {
      logError(this.logger, 'Error processing session.created', error);
    }
  }

  /**
   * Escuchar evento session.updated de USER_MS
   */
  @EventPattern(getMessagePattern('session.updated'))
  async handleSessionUpdated(@Payload() session: any): Promise<void> {
    try {
      this.logger.debug(`Received session.updated: ${session.id}`);

      const sessionRaw: SessionRawDto = {
        session_id: session.id,
        contractor_id: session.contractor_id,
        session_start: new Date(session.session_start),
        session_end: session.session_end ? new Date(session.session_end) : null,
        total_duration: session.total_duration || null,
        created_at: session.created_at ? new Date(session.created_at) : new Date(),
      };

      // Para actualizaciones, insertamos de nuevo (ClickHouse manejará la deduplicación si es necesario)
      // O podrías implementar una lógica UPDATE si tu tabla lo soporta
      await this.rawService.saveSession(sessionRaw);
      this.logger.debug(`✅ Session updated in RAW: ${sessionRaw.session_id}`);
    } catch (error) {
      logError(this.logger, 'Error processing session.updated', error);
    }
  }
}

