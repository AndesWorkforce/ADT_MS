import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';

import { getMessagePattern, logError, envs } from 'config';

import { RawService } from '../raw/raw.service';
import { EventRawDto } from '../raw/dto/event-raw.dto';
import { EventQueueService } from '../queues/services';

@Controller()
export class EventsListener {
  private readonly logger = new Logger(EventsListener.name);

  constructor(
    private readonly rawService: RawService,
    private readonly eventQueueService: EventQueueService,
  ) {}

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
        payload:
          typeof event.payload === 'string'
            ? event.payload
            : JSON.stringify(event.payload || {}),
        created_at: event.created_at ? new Date(event.created_at) : new Date(),
      };

      if (envs.queues.useEventQueue) {
        await this.eventQueueService.addEvent({
          event_id: eventRaw.event_id,
          contractor_id: eventRaw.contractor_id,
          agent_id: eventRaw.agent_id,
          session_id: eventRaw.session_id,
          agent_session_id: eventRaw.agent_session_id,
          timestamp: eventRaw.timestamp,
          payload: eventRaw.payload,
          created_at: eventRaw.created_at,
        });

        this.logger.debug(
          `📬 Event queued - Event ID: ${eventRaw.event_id}, Contractor: ${eventRaw.contractor_id}`,
        );
      } else {
        await this.rawService.saveEvent(eventRaw);

        this.logger.debug(
          `✅ Event processed (direct) - Event ID: ${eventRaw.event_id}, Contractor: ${eventRaw.contractor_id}`,
        );
      }
    } catch (error) {
      logError(this.logger, 'Error processing event.created', error);
    }
  }
}
