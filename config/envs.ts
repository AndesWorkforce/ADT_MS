import 'dotenv/config';

import * as Joi from 'joi';

interface EnvVars {
  PORT: number;
  NATS_HOST: string;
  NATS_PORT: number;
  NATS_USERNAME: string;
  NATS_PASSWORD: string;
  DEV_LOGS: boolean;
  ENVIRONMENT: string;
  CLICKHOUSE_HOST: string;
  CLICKHOUSE_PORT: number;
  CLICKHOUSE_USERNAME: string;
  CLICKHOUSE_PASSWORD: string;
  CLICKHOUSE_DATABASE: string;
  REDIS_HOST: string;
  REDIS_PORT: number;
  REDIS_PASSWORD: string;
  REDIS_DB: number;
  REDIS_TTL: number;
  REDIS_MAX_RETRIES: number;
  REDIS_RETRY_DELAY: number;
  // Variables para BullMQ (colas)
  REDIS_QUEUE_DB: number;
  USE_EVENT_QUEUE: boolean;
  USE_SESSION_QUEUE: boolean;
  USE_CONTRACTOR_QUEUE: boolean;
  USE_ETL_QUEUE: boolean;
  // Variables para sistema de alertas de inactividad
  USE_INACTIVITY_ALERTS: boolean;
  INACTIVITY_THRESHOLD_MINUTES: number;
  INACTIVITY_SCAN_INTERVAL_MINUTES: number;
}

export const envSchema = Joi.object({
  PORT: Joi.number().required(),
  NATS_HOST: Joi.string().required(),
  NATS_PORT: Joi.number().required(),
  NATS_USERNAME: Joi.string().required(),
  NATS_PASSWORD: Joi.string().required(),
  DEV_LOGS: Joi.boolean()
    .truthy('true')
    .truthy('1')
    .truthy('yes')
    .falsy('false')
    .falsy('0')
    .falsy('no')
    .default(false),
  ENVIRONMENT: Joi.string()
    .valid('development', 'production', 'staging')
    .default('development'),
  CLICKHOUSE_HOST: Joi.string().required(),
  CLICKHOUSE_PORT: Joi.number().required(),
  CLICKHOUSE_USERNAME: Joi.string().required(),
  CLICKHOUSE_PASSWORD: Joi.string().allow('').default(''),
  CLICKHOUSE_DATABASE: Joi.string().required(),
  REDIS_HOST: Joi.string().default('localhost'),
  REDIS_PORT: Joi.number().default(6379),
  REDIS_PASSWORD: Joi.string().allow('').default(''),
  REDIS_DB: Joi.number().default(0),
  REDIS_TTL: Joi.number().default(3600),
  REDIS_MAX_RETRIES: Joi.number().default(3),
  REDIS_RETRY_DELAY: Joi.number().default(1000),
  // Variables para BullMQ (colas) - DB separada para evitar conflictos con caché
  REDIS_QUEUE_DB: Joi.number().default(1),
  // Feature flags para activar/desactivar colas de forma controlada
  USE_EVENT_QUEUE: Joi.boolean()
    .truthy('true')
    .truthy('1')
    .falsy('false')
    .falsy('0')
    .default(false),
  USE_SESSION_QUEUE: Joi.boolean()
    .truthy('true')
    .truthy('1')
    .falsy('false')
    .falsy('0')
    .default(false),
  USE_CONTRACTOR_QUEUE: Joi.boolean()
    .truthy('true')
    .truthy('1')
    .falsy('false')
    .falsy('0')
    .default(false),
  USE_ETL_QUEUE: Joi.boolean()
    .truthy('true')
    .truthy('1')
    .falsy('false')
    .falsy('0')
    .default(false),
  // Sistema de alertas de inactividad
  USE_INACTIVITY_ALERTS: Joi.boolean()
    .truthy('true')
    .truthy('1')
    .falsy('false')
    .falsy('0')
    .default(false),
  INACTIVITY_THRESHOLD_MINUTES: Joi.number().default(60),
  INACTIVITY_SCAN_INTERVAL_MINUTES: Joi.number().default(10),
}).unknown(true);

const { error, value } = envSchema.validate(process.env);

if (error) {
  throw new Error(`Invalid environment variables: ${error.message}`);
}

const envVars: EnvVars = value;

/**
 * Zona horaria operativa para interpretar workdays, agrupar métricas por día
 * y generar strings YYYY-MM-DD coherentes con el calendario del cliente.
 */
export const OPERATIONAL_TIMEZONE =
  process.env.EVENTS_TIMEZONE || 'America/New_York';

export const envs = {
  port: envVars.PORT,
  natsHost: envVars.NATS_HOST,
  natsPort: envVars.NATS_PORT,
  natsUsername: envVars.NATS_USERNAME,
  natsPassword: envVars.NATS_PASSWORD,
  devLogsEnabled: envVars.DEV_LOGS,
  environment: envVars.ENVIRONMENT,
  clickhouse: {
    host: envVars.CLICKHOUSE_HOST,
    port: envVars.CLICKHOUSE_PORT,
    username: envVars.CLICKHOUSE_USERNAME,
    password: envVars.CLICKHOUSE_PASSWORD,
    database: envVars.CLICKHOUSE_DATABASE,
  },
  redis: {
    host: envVars.REDIS_HOST,
    port: envVars.REDIS_PORT,
    password: envVars.REDIS_PASSWORD,
    db: envVars.REDIS_DB,
    ttl: envVars.REDIS_TTL,
    maxRetries: envVars.REDIS_MAX_RETRIES,
    retryDelay: envVars.REDIS_RETRY_DELAY,
  },
  // Configuración de colas con BullMQ
  queues: {
    redisDb: envVars.REDIS_QUEUE_DB,
    useEventQueue: envVars.USE_EVENT_QUEUE,
    useSessionQueue: envVars.USE_SESSION_QUEUE,
    useContractorQueue: envVars.USE_CONTRACTOR_QUEUE,
    useEtlQueue: envVars.USE_ETL_QUEUE,
    useInactivityAlerts: envVars.USE_INACTIVITY_ALERTS,
    inactivityThresholdMinutes: envVars.INACTIVITY_THRESHOLD_MINUTES,
    inactivityScanIntervalMinutes: envVars.INACTIVITY_SCAN_INTERVAL_MINUTES,
  },
};

/**
 * Genera un MessagePattern con prefijo según el entorno
 * @param pattern - El nombre del pattern sin prefijo
 * @returns El pattern con el prefijo del entorno (dev, prod, staging)
 *
 * @example
 * getMessagePattern('findUser') // 'dev.findUser' en desarrollo
 * getMessagePattern('findUser') // 'prod.findUser' en producción
 */
export function getMessagePattern(pattern: string): string {
  const prefix =
    envs.environment === 'production'
      ? 'prod'
      : envs.environment === 'staging'
        ? 'staging'
        : 'dev';
  return `${prefix}.${pattern}`;
}
