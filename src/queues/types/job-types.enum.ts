/**
 * Tipos de trabajos que pueden ser procesados por las colas de BullMQ
 */
export enum JobType {
  // Eventos de agentes (alta frecuencia: ~400/min con 100 agentes)
  SAVE_EVENT = 'save-event',

  // Sesiones
  SAVE_SESSION = 'save-session',
  SAVE_AGENT_SESSION = 'save-agent-session',

  // Contractors
  SAVE_CONTRACTOR = 'save-contractor',

  // ETLs pesados
  EVENTS_TO_ACTIVITY = 'events-to-activity',
  EVENTS_TO_ACTIVITY_FORCE = 'events-to-activity-force',
  DAILY_METRICS = 'daily-metrics',
  SESSION_SUMMARIES = 'session-summaries',
  FULL_ETL_ON_SESSION_CLOSE = 'full-etl-on-session-close',
  APP_USAGE = 'app-usage',
}
