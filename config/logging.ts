import { LogLevel, Logger } from '@nestjs/common';

import { envs } from './envs';

const DEV_LOG_LEVELS: LogLevel[] = ['error', 'warn', 'log', 'debug', 'verbose'];
const PROD_LOG_LEVELS: LogLevel[] = ['error', 'warn'];

export const resolveLogLevels = (): LogLevel[] =>
  envs.devLogsEnabled ? DEV_LOG_LEVELS : PROD_LOG_LEVELS;

export const getLogModeMessage = (): string =>
  envs.devLogsEnabled
    ? 'DEV_LOGS → Dev Logs Enabled.'
    : 'DEV_LOGS → Dev Logs Disabled.';

// Shared logging helpers
export const formatErrorForLog = (error: unknown): string => {
  if (error instanceof Error) {
    return error.stack ?? error.message;
  }
  try {
    return JSON.stringify(error);
  } catch {
    return String(error);
  }
};

export const logError = (
  logger: Logger,
  message: string,
  error: unknown,
): void => {
  logger.error(message, formatErrorForLog(error));
};

