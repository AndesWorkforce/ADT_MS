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
}).unknown(true);

const { error, value } = envSchema.validate(process.env);

if (error) {
  throw new Error(`Invalid environment variables: ${error.message}`);
}

const envVars: EnvVars = value;

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

