import { DateTime } from 'luxon';

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

/**
 * Instante UTC correspondiente a un reloj de pared (día + hora) en OPERATIONAL_TIMEZONE.
 * Evita `new Date('YYYY-MM-DDTHH:mm:ss')` que en Node usa la TZ del host.
 */
export function wallTimeToUtcInOperationalZone(
  dayYmd: string,
  hour: number,
  minute = 0,
  second = 0,
): Date {
  return DateTime.fromObject(
    {
      year: Number(dayYmd.slice(0, 4)),
      month: Number(dayYmd.slice(5, 7)),
      day: Number(dayYmd.slice(8, 10)),
      hour,
      minute,
      second,
    },
    { zone: OPERATIONAL_TIMEZONE },
  ).toJSDate();
}

/**
 * Interpreta strings devueltos por ClickHouse (p. ej. formatDateTime + toTimeZone)
 * como hora local en OPERATIONAL_TIMEZONE, alineado con toHour(toTimeZone(...)).
 */
export function parseDateTimeInOperationalZone(raw: string): Date {
  const s = String(raw).trim();
  const withoutFraction = s.replace(/\.\d+$/, '');
  const isoLike = withoutFraction.replace(' ', 'T');
  let dt = DateTime.fromISO(isoLike, { zone: OPERATIONAL_TIMEZONE });
  if (!dt.isValid) {
    dt = DateTime.fromFormat(withoutFraction, 'yyyy-MM-dd HH:mm:ss', {
      zone: OPERATIONAL_TIMEZONE,
    });
  }
  if (!dt.isValid) {
    throw new Error(`Invalid datetime for ${OPERATIONAL_TIMEZONE}: ${raw}`);
  }
  return dt.toJSDate();
}

/**
 * Día calendario YYYY-MM-DD en OPERATIONAL_TIMEZONE → inicio del día (instante JS en UTC).
 */
export function parseCalendarDayStart(input: string): Date {
  const s = String(input).trim().split('T')[0];
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) {
    throw new Error(`Invalid calendar day (YYYY-MM-DD): ${input}`);
  }
  return DateTime.fromISO(s, { zone: OPERATIONAL_TIMEZONE })
    .startOf('day')
    .toJSDate();
}

/**
 * Día calendario YYYY-MM-DD en OPERATIONAL_TIMEZONE → fin del día (instante JS en UTC).
 */
export function parseCalendarDayEnd(input: string): Date {
  const s = String(input).trim().split('T')[0];
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) {
    throw new Error(`Invalid calendar day (YYYY-MM-DD): ${input}`);
  }
  return DateTime.fromISO(s, { zone: OPERATIONAL_TIMEZONE })
    .endOf('day')
    .toJSDate();
}

export function parseOptionalCalendarDayStart(
  input?: string | null,
): Date | undefined {
  if (input == null || String(input).trim() === '') return undefined;
  return parseCalendarDayStart(String(input));
}

export function toStartOfOperationalDay(d: Date): Date {
  return DateTime.fromJSDate(d)
    .setZone(OPERATIONAL_TIMEZONE)
    .startOf('day')
    .toJSDate();
}

export function toEndOfOperationalDay(d: Date): Date {
  return DateTime.fromJSDate(d)
    .setZone(OPERATIONAL_TIMEZONE)
    .endOf('day')
    .toJSDate();
}

/** Instant UTC → literal `YYYY-MM-DD HH:MM:SS` para columnas DateTime en ClickHouse. */
export function instantToClickHouseUtcDateTime(d: Date): string {
  return d.toISOString().replace('T', ' ').slice(0, 19);
}

/**
 * Normaliza `workday` serializado por BullMQ/API (ISO o YYYY-MM-DD) a YYYY-MM-DD en la zona operativa.
 */
export function jobWorkdayToYmd(
  workday: string | Date | null | undefined,
): string {
  if (workday == null) return 'today';
  if (
    typeof workday === 'string' &&
    /^\d{4}-\d{2}-\d{2}$/.test(workday.trim())
  ) {
    return workday.trim();
  }
  return formatDateInTZ(new Date(workday));
}

/**
 * Evita `new Date('YYYY-MM-DD')` (UTC midnight): mismo día de negocio en la zona operativa.
 */
export function coerceToOperationalDayStart(workday: Date | string): Date {
  if (
    typeof workday === 'string' &&
    /^\d{4}-\d{2}-\d{2}$/.test(workday.trim())
  ) {
    return parseCalendarDayStart(workday);
  }
  return toStartOfOperationalDay(new Date(workday));
}

/**
 * Rango [inicio, fin] en zona operativa para dos fechas ancla (p. ej. desde API).
 */
export function getOperationalDayRangeFromDates(
  fromDate: Date,
  toDate: Date,
): { from: Date; to: Date; fromStr: string; toStr: string } {
  const from = DateTime.fromJSDate(fromDate)
    .setZone(OPERATIONAL_TIMEZONE)
    .startOf('day')
    .toJSDate();
  const to = DateTime.fromJSDate(toDate)
    .setZone(OPERATIONAL_TIMEZONE)
    .endOf('day')
    .toJSDate();
  return {
    from,
    to,
    fromStr: formatDateInTZ(from),
    toStr: formatDateInTZ(to),
  };
}
