import { Controller, Logger, Inject } from '@nestjs/common';
import { EventPattern, Payload, ClientProxy } from '@nestjs/microservices';

import { getMessagePattern, logError, envs } from 'config';

import { RawService } from '../raw/raw.service';
import { EventRawDto } from '../raw/dto/event-raw.dto';
import { EventQueueService } from '../queues/services';
import { RedisService } from '../redis/redis.service';
import { EventCreatedPayload } from 'src/listeners/listener.interfaces';

@Controller()
export class EventsListener {
  private readonly logger = new Logger(EventsListener.name);

  constructor(
    private readonly rawService: RawService,
    private readonly eventQueueService: EventQueueService,
    private readonly redisService: RedisService,
    @Inject('NATS_SERVICE') private readonly natsClient: ClientProxy,
  ) {}

  @EventPattern(getMessagePattern('event.created'))
  async handleEventCreated(
    @Payload() event: EventCreatedPayload,
  ): Promise<void> {
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

      // Inactivity tracking (si está habilitado)
      if (envs.queues.useInactivityAlerts && eventRaw.agent_session_id) {
        await this.trackActivityForInactivityAlerts(
          eventRaw.agent_session_id,
          eventRaw.contractor_id,
          eventRaw.timestamp,
        );
      }
    } catch (error) {
      logError(this.logger, 'Error processing event.created', error);
    }
  }

  /**
   * Rastrea la actividad del agente para el sistema de alertas de inactividad
   */
  private async trackActivityForInactivityAlerts(
    agentSessionId: string,
    contractorId: string,
    timestamp: Date,
  ): Promise<void> {
    try {
      // Actualizar última actividad
      await this.redisService.setLastActivity(agentSessionId, timestamp);

      // Si no existe session_start, es una nueva sesión - guardarla
      const existingSessionStart =
        await this.redisService.getSessionStart(agentSessionId);
      if (!existingSessionStart) {
        await this.redisService.setSessionStart(agentSessionId, timestamp);
        this.logger.debug(
          `🆕 New session start tracked: ${agentSessionId} at ${timestamp.toISOString()}`,
        );
      }

      // Verificar si hay una alerta activa para resolver
      const activeAlertId =
        await this.redisService.getAlertActive(agentSessionId);
      if (activeAlertId) {
        this.logger.log(
          `✅ Resolving inactivity alert ${activeAlertId} for agent_session ${agentSessionId}`,
        );

        // Llamar a EVENTS_MS para resolver la alerta
        try {
          await this.natsClient
            .send(getMessagePattern('resolveInactivityAlert'), {
              agent_session_id: agentSessionId,
            })
            .toPromise();

          // Publicar evento de alerta resuelta
          this.natsClient.emit(getMessagePattern('inactivity.alert.resolved'), {
            alert_id: activeAlertId,
            agent_session_id: agentSessionId,
            contractor_id: contractorId,
            resolved_at: new Date().toISOString(),
          });

          // Limpiar flag de alerta activa
          await this.redisService.clearAlertActive(agentSessionId);

          this.logger.log(`✅ Inactivity alert resolved: ${activeAlertId}`);
        } catch (error) {
          this.logger.error(
            `❌ Error resolving inactivity alert ${activeAlertId}:`,
            error,
          );
        }
      }
    } catch (error) {
      this.logger.error(
        `❌ Error tracking activity for inactivity alerts:`,
        error,
      );
    }
  }
}
