export class RedisKeys {
  private static readonly NAMESPACE = 'adt';

  static allRealTimeMetricsByDateRange(
    fromDate: string,
    toDate: string,
    filters?: {
      name?: string;
      job_position?: string;
      country?: string;
      client_id?: string;
      team_id?: string;
    },
  ): string {
    const baseKey = `${this.NAMESPACE}:metrics:realtime:range:${fromDate}:${toDate}`;

    if (!filters || Object.keys(filters).length === 0) {
      return baseKey;
    }
    // Filtros para que las claves sean consistentes
    const filterEntries = Object.entries(filters)
      .filter(
        ([, value]) => value !== undefined && value !== null && value !== '',
      )
      .sort(([keyA], [keyB]) => keyA.localeCompare(keyB))
      .map(([key, value]) => `${key}=${value}`)
      .join('&');

    return filterEntries ? `${baseKey}:${filterEntries}` : baseKey;
  }
}
