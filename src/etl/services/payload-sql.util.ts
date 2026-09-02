/**
 * 🎯 FUENTE ÚNICA del contrato SQL para leer `events_raw.payload`.
 *
 * El agente emitió tres formatos a lo largo del tiempo y el ETL tiene que
 * leerlos todos:
 *
 *   v1  anidado, sin BeatDuration ni DomainUsage
 *   v2  anidado: { Keyboard: {InputsCount}, Mouse: {ClicksCount}, IdleTime,
 *                  BeatDuration, PowerState, BrowserSource, PresenceSignals,
 *                  AppUsage, DomainUsage }
 *   v3  plano:   { v: 3, keyboard_count, mouse_clicks, idle_time,
 *                  beat_duration, power_state, browser_source, mic_active,
 *                  cam_active, call_app_active, apps, domains }
 *
 * REGLA DE ORO: cada llamada a JSONExtract / JSON_VALUE reparsea el documento
 * entero. Por eso acá todo se resuelve con UN parse por fila: un Tuple tipado
 * que declara los campos de ambos formatos. El formato que no vino se resuelve
 * a los defaults (0 / ''), y el coalesce elige el que exista.
 *
 * Medido sobre 359k eventos reales:
 *   15 llamadas sueltas (versión anterior) ... 0.135 s
 *   1 JSONExtract a Tuple .................... 0.028 s   (~4.8x)
 *   piso de I/O sin parsear .................. 0.009 s
 *
 * Nota para quien agregue un campo: sumarlo al Tuple, NO agregar una llamada
 * JSONExtract suelta. Una sola llamada extra cuesta más que todo el Tuple.
 */

/** Campos del Tuple: primero los de v3 (planos), después los de v1/v2 (anidados). */
const PAYLOAD_TUPLE_FIELDS = [
  // --- v3 plano ---
  'v UInt8',
  'keyboard_count UInt32',
  'mouse_clicks UInt32',
  'idle_time Float64',
  'beat_duration Float64',
  'power_state String',
  'browser_source String',
  'mic_active UInt8',
  'cam_active UInt8',
  'call_app_active UInt8',
  // --- v1 / v2 anidado ---
  'IdleTime Float64',
  'BeatDuration Float64',
  'PowerState String',
  'BrowserSource String',
  'Keyboard Tuple(InputsCount UInt32)',
  'Mouse Tuple(ClicksCount UInt32)',
  'PresenceSignals Tuple(microphone_active UInt8, camera_active UInt8, call_app_active UInt8)',
];

export const PAYLOAD_TUPLE_TYPE = `Tuple(${PAYLOAD_TUPLE_FIELDS.join(', ')})`;

/**
 * Cláusula WITH que deja el payload parseado UNA vez en el alias `p`.
 * Tiene que preceder al SELECT que use las expresiones de abajo.
 */
export function payloadCteSql(alias = 'p'): string {
  return `WITH JSONExtract(payload, '${PAYLOAD_TUPLE_TYPE}') AS ${alias}`;
}

/**
 * Map(String, Float64) con el uso por app o por dominio, sea cual sea la
 * versión del payload.
 *
 * Un solo JSONExtract a un Tuple con los dos Maps posibles; el ausente queda
 * vacío y mapConcat se queda con el otro. Reemplaza al patrón
 * `JSONExtractKeys(...)` + un `JSONExtractFloat` por clave, que costaba
 * 1 + N parses por fila (0.055 s → 0.025 s sobre 359k filas).
 */
function usageMapSql(legacyKey: string, flatKey: string): string {
  const tupleType = `Tuple(${legacyKey} Map(String, Float64), ${flatKey} Map(String, Float64))`;
  const extracted = `JSONExtract(payload, '${tupleType}')`;
  const merged = `mapConcat(tupleElement(${extracted}, 1), tupleElement(${extracted}, 2))`;

  // mapFilter descarta segundos <= 0 EN EL ORIGEN, para que ningun consumidor
  // tenga que acordarse de hacerlo.
  //
  // Por que hace falta: el agente pre-Fase 1 emitia tiempos NEGATIVOS para
  // navegadores. En el historico local hay 223.503 beats afectados (29% del
  // total), con valores de hasta -8s; agregado, Chrome suma -161.390s y Edge
  // -742.990s. Sin este filtro, un backfill del historico produce
  // S_quality basura: el denominador sum(seconds) se achica y el numerador
  // puede irse a negativo.
  //
  // Filtrar (en vez de clampear a 0) tambien evita que los Map de app_usage /
  // browser_usage queden con entradas en 0 segundos, que serian ruido en la UI.
  return `mapFilter((k, v) -> v > 0, ${merged})`;
}

/** Uso por app: v1/v2 'AppUsage' o v3 'apps'. */
export function appUsageMapSql(): string {
  return usageMapSql('AppUsage', 'apps');
}

/** Uso por dominio: v1/v2 'DomainUsage' o v3 'domains'. */
export function domainUsageMapSql(): string {
  return usageMapSql('DomainUsage', 'domains');
}

/**
 * ARRAY JOIN que expande un Map de uso en dos columnas paralelas
 * (clave y segundos), preservando los alias que espera el SQL de alrededor.
 */
export function usageArrayJoinSql(
  mapSql: string,
  keyAlias: string,
  valueAlias: string,
): string {
  return `ARRAY JOIN mapKeys(${mapSql}) AS ${keyAlias}, mapValues(${mapSql}) AS ${valueAlias}`;
}
