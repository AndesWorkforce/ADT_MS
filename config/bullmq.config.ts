export const QUEUE_NAMES = {
  EVENTS: 'adt-events',
  SESSIONS: 'adt-sessions',
  CONTRACTORS: 'adt-contractors',
  ETL_EVENTS_TO_ACTIVITY: 'adt-etl-events-to-activity',
  ETL_DAILY_METRICS: 'adt-etl-daily-metrics',
  ETL_SESSION_SUMMARIES: 'adt-etl-session-summaries',
  ETL_APP_USAGE: 'adt-etl-app-usage',
} as const;

export const DEFAULT_JOB_OPTIONS = {
  attempts: 3,
  backoff: {
    type: 'exponential' as const,
    delay: 5000,
  },
  removeOnComplete: {
    age: 86400,
    count: 1000,
  },
  removeOnFail: {
    age: 604800,
  },
};

export const QUEUE_CONCURRENCY = {
  EVENTS: 5, // 5 workers procesando eventos simultáneamente
  SESSIONS: 3,
  CONTRACTORS: 2,
  ETL_EVENTS_TO_ACTIVITY: 1, // ETLs pesados: 1 a la vez
  ETL_DAILY_METRICS: 1,
  ETL_SESSION_SUMMARIES: 1,
  ETL_APP_USAGE: 1,
} as const;
