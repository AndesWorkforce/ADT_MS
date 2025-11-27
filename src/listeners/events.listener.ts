import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';

import { getMessagePattern, logError } from 'config';

import { RawService } from '../raw/raw.service';
import { EventRawDto } from '../raw/dto/event-raw.dto';

@Controller()
export class EventsListener {
  private readonly logger = new Logger(EventsListener.name);

  constructor(private readonly rawService: RawService) {}

  /**
   * Escuchar event.created de EVENTS_MS
   * Este evento se emite después de que un evento se crea exitosamente en la base de datos
   * e incluye el ID real del evento
   */
  @EventPattern(getMessagePattern('event.created'))
  async handleEventCreated(@Payload() event: any): Promise<void> {
    try {
      if (!event || !event.id) {
        this.logger.warn(
          `⚠️ EventsListener: Event received without valid ID: ${JSON.stringify(event)}`,
        );
        return;
      }

      // Transformar a EventRawDto usando el ID real del evento
      const eventRaw: EventRawDto = {
        event_id: event.id,
        contractor_id: event.contractor_id,
        agent_id: event.agent_id || null,
        session_id: event.session_id || null,
        agent_session_id: event.agent_session_id || null,
        timestamp: event.timestamp ? new Date(event.timestamp) : new Date(),
        payload: typeof event.payload === 'string' 
          ? event.payload 
          : JSON.stringify(event.payload || {}),
        created_at: event.created_at ? new Date(event.created_at) : new Date(),
      };

      await this.rawService.saveEvent(eventRaw);
      
      // Solo log en debug para reducir ruido
      this.logger.debug(
        `✅ EventsListener: Event processed - Event ID: ${eventRaw.event_id}, Contractor: ${eventRaw.contractor_id}`,
      );
    } catch (error) {
      logError(this.logger, 'Error processing event.created', error);
      // No lanzar - no queremos romper el flujo de eventos
      // Registrar el error y continuar
    }
  }
}

