import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger, Inject } from '@nestjs/common';
import { ClientProxy } from '@nestjs/microservices';
import { lastValueFrom } from 'rxjs';

import { QUEUE_NAMES, QUEUE_CONCURRENCY } from 'config/bullmq.config';
import { envs, getMessagePattern } from 'config';

import { RedisService } from '../../redis/redis.service';

interface AgentSession {
  id: string;
  contractor_id: string;
  agent_id: string;
  session_id?: string;
  session_start: string;
  session_end?: string;
}

interface WorkSchedule {
  contractor_id: string;
  work_schedule_start?: string; // Format: "HH:mm"
  work_schedule_end?: string; // Format: "HH:mm"
}

@Processor(QUEUE_NAMES.INACTIVITY_SCAN, {
  concurrency: QUEUE_CONCURRENCY.INACTIVITY_SCAN,
})
export class InactivityScanProcessor extends WorkerHost {
  private readonly logger = new Logger(InactivityScanProcessor.name);

  constructor(
    private readonly redisService: RedisService,
    @Inject('NATS_SERVICE') private readonly natsClient: ClientProxy,
  ) {
    super();
  }

  async process(): Promise<any> {
    const startTime = Date.now();
    this.logger.log(
      `🔍 Starting inactivity scan at ${new Date().toISOString()}`,
    );

    try {
      // 1. Obtener todas las AgentSessions activas
      const activeSessions = await this.getActiveAgentSessions();
      this.logger.log(
        `📊 Found ${activeSessions.length} active agent sessions`,
      );

      if (activeSessions.length === 0) {
        this.logger.log('✅ No active sessions to scan');
        return { scanned: 0, alerts: 0 };
      }

      // 2. Procesar sesiones en batches de 20
      const candidates = await this.findInactivityCandidates(activeSessions);
      this.logger.log(`⚠️ Found ${candidates.length} inactivity candidates`);

      if (candidates.length === 0) {
        this.logger.log('✅ No inactivity detected');
        return { scanned: activeSessions.length, alerts: 0 };
      }

      // 3. Verificar horarios laborales
      const validAlerts = await this.filterByWorkSchedule(candidates);
      this.logger.log(
        `✅ ${validAlerts.length} alerts pass work schedule validation`,
      );

      if (validAlerts.length === 0) {
        this.logger.log('✅ No alerts within work hours');
        return { scanned: activeSessions.length, alerts: 0 };
      }

      // 4. Crear alertas y notificar
      let alertsCreated = 0;
      for (const alert of validAlerts) {
        const created = await this.createAndNotifyAlert(alert);
        if (created) alertsCreated++;
      }

      const duration = Date.now() - startTime;
      this.logger.log(
        `✅ Inactivity scan completed: ${alertsCreated} alerts created in ${duration}ms`,
      );

      return {
        scanned: activeSessions.length,
        candidates: candidates.length,
        alerts: alertsCreated,
        duration,
      };
    } catch (error) {
      this.logger.error('❌ Error in inactivity scan:', error);
      throw error;
    }
  }

  /**
   * Obtiene todas las AgentSessions activas desde EVENTS_MS
   */
  private async getActiveAgentSessions(): Promise<AgentSession[]> {
    try {
      const response = await lastValueFrom(
        this.natsClient.send<AgentSession[]>(
          getMessagePattern('getActiveAgentSessions'),
          {},
        ),
      );
      return response || [];
    } catch (error) {
      this.logger.error('❌ Error fetching active agent sessions:', error);
      return [];
    }
  }

  /**
   * Encuentra candidatos para alerta de inactividad
   */
  private async findInactivityCandidates(
    sessions: AgentSession[],
  ): Promise<
    Array<AgentSession & { inactiveMinutes: number; lastActivity: Date }>
  > {
    const candidates: Array<
      AgentSession & { inactiveMinutes: number; lastActivity: Date }
    > = [];
    const threshold = envs.queues.inactivityThresholdMinutes;
    const now = new Date();

    for (const session of sessions) {
      try {
        // Verificar si la sesión ya pasó el umbral de tiempo desde inicio
        const sessionStart = new Date(session.session_start);
        const minutesSinceStart =
          (now.getTime() - sessionStart.getTime()) / 60000;

        if (minutesSinceStart < threshold) {
          // Sesión muy reciente, aún no aplica el threshold
          continue;
        }

        // Obtener última actividad desde Redis
        const lastActivity = await this.redisService.getLastActivity(
          session.id,
        );
        if (!lastActivity) {
          // No hay registro de actividad en Redis, skip
          continue;
        }

        // Calcular minutos de inactividad
        const inactiveMinutes =
          (now.getTime() - lastActivity.getTime()) / 60000;

        if (inactiveMinutes >= threshold) {
          candidates.push({
            ...session,
            inactiveMinutes: Math.floor(inactiveMinutes),
            lastActivity,
          });
        }
      } catch (error) {
        this.logger.error(`❌ Error processing session ${session.id}:`, error);
      }
    }

    return candidates;
  }

  /**
   * Filtra candidatos que están dentro de su horario laboral
   */
  private async filterByWorkSchedule(
    candidates: Array<
      AgentSession & { inactiveMinutes: number; lastActivity: Date }
    >,
  ): Promise<
    Array<AgentSession & { inactiveMinutes: number; lastActivity: Date }>
  > {
    if (candidates.length === 0) return [];

    try {
      // Obtener work schedules de todos los contractors
      const contractorIds = [
        ...new Set(candidates.map((c) => c.contractor_id)),
      ];
      const workSchedules = await lastValueFrom(
        this.natsClient.send<WorkSchedule[]>(
          getMessagePattern('getContractorsWorkSchedules'),
          { contractor_ids: contractorIds },
        ),
      );

      const scheduleMap = new Map<string, WorkSchedule>();
      workSchedules.forEach((schedule) => {
        scheduleMap.set(schedule.contractor_id, schedule);
      });

      // Filtrar por horario laboral
      const now = new Date();
      const currentTime = `${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}`;

      return candidates.filter((candidate) => {
        const schedule = scheduleMap.get(candidate.contractor_id);
        if (
          !schedule ||
          !schedule.work_schedule_start ||
          !schedule.work_schedule_end
        ) {
          // Sin horario configurado, no alertar
          return false;
        }

        // Verificar si la hora actual está dentro del horario laboral
        const isWithinWorkHours =
          currentTime >= schedule.work_schedule_start &&
          currentTime <= schedule.work_schedule_end;

        return isWithinWorkHours;
      });
    } catch (error) {
      this.logger.error('❌ Error fetching work schedules:', error);
      return [];
    }
  }

  /**
   * Crea la alerta y notifica via NATS
   */
  private async createAndNotifyAlert(
    alert: AgentSession & { inactiveMinutes: number; lastActivity: Date },
  ): Promise<boolean> {
    try {
      // Verificar si ya existe una alerta activa (evitar duplicados)
      const existingAlert = await this.redisService.getAlertActive(alert.id);
      if (existingAlert) {
        this.logger.debug(
          `⚠️ Alert already active for agent_session ${alert.id}, skipping`,
        );
        return false;
      }

      // Crear alerta en EVENTS_MS
      const createdAlert = await lastValueFrom(
        this.natsClient.send<{ id: string }>(
          getMessagePattern('createInactivityAlert'),
          {
            contractor_id: alert.contractor_id,
            agent_session_id: alert.id,
            session_id: alert.session_id || null,
            inactivity_start: alert.lastActivity.toISOString(),
            inactivity_duration_minutes: alert.inactiveMinutes,
          },
        ),
      );

      if (!createdAlert || !createdAlert.id) {
        this.logger.error('❌ Failed to create alert (no ID returned)');
        return false;
      }

      // Publicar evento NATS
      this.natsClient.emit(getMessagePattern('inactivity.alert.triggered'), {
        alert_id: createdAlert.id,
        contractor_id: alert.contractor_id,
        agent_session_id: alert.id,
        session_id: alert.session_id || null,
        inactivity_start: alert.lastActivity.toISOString(),
        inactivity_duration_minutes: alert.inactiveMinutes,
        detected_at: new Date().toISOString(),
      });

      // Marcar alerta como activa en Redis
      await this.redisService.setAlertActive(alert.id, createdAlert.id);

      this.logger.log(
        `🚨 Inactivity alert created: ${createdAlert.id} for contractor ${alert.contractor_id} (inactive ${alert.inactiveMinutes} min)`,
      );

      return true;
    } catch (error) {
      this.logger.error(
        `❌ Error creating alert for session ${alert.id}:`,
        error,
      );
      return false;
    }
  }
}
