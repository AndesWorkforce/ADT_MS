import { OPERATIONAL_TIMEZONE } from './envs';

const dateFormatter = new Intl.DateTimeFormat('en-CA', {
  timeZone: OPERATIONAL_TIMEZONE,
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
});

/**
 * Devuelve un string YYYY-MM-DD interpretando el instante en OPERATIONAL_TIMEZONE.
 * Ejemplo: un Date que es 2026-04-16T03:00:00Z se formatea como '2026-04-15'
 * si la TZ operativa es America/New_York (UTC-4 en EDT).
 */
export function formatDateInTZ(date: Date): string {
  return dateFormatter.format(date);
}

/**
 * Fragmento SQL para ClickHouse: `toDate(column, 'America/New_York')`.
 * Centraliza el uso de la timezone en queries para evitar hardcodeos.
 */
export function toDateTZ(column: string): string {
  return `toDate(${column}, '${OPERATIONAL_TIMEZONE}')`;
}
