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

    if (!filters) {
      return baseKey;
    }

    const keys = Object.keys(filters).filter((key) => {
      const value = filters[key as keyof typeof filters];
      return value !== undefined && value !== null && value !== '';
    });

    if (keys.length === 0) {
      return baseKey;
    }

    keys.sort();

    let filterStr = '';
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
      if (i > 0) filterStr += '&';
      filterStr += `${key}=${filters[key as keyof typeof filters]}`;
    }

    return `${baseKey}:${filterStr}`;
  }

  static allRealTimeMetricsByWorkday(
    workday: string,
    filters?: {
      name?: string;
      job_position?: string;
      country?: string;
      client_id?: string;
      team_id?: string;
    },
  ): string {
    const baseKey = `${this.NAMESPACE}:metrics:realtime:workday:${workday}`;

    if (!filters) {
      return baseKey;
    }

    const keys = Object.keys(filters).filter((key) => {
      const value = filters[key as keyof typeof filters];
      return value !== undefined && value !== null && value !== '';
    });

    if (keys.length === 0) {
      return baseKey;
    }

    keys.sort();

    let filterStr = '';
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
      if (i > 0) filterStr += '&';
      filterStr += `${key}=${filters[key as keyof typeof filters]}`;
    }

    return `${baseKey}:${filterStr}`;
  }

  static realTimeMetricsByContractor(
    contractorId: string,
    workday: string,
  ): string {
    return `${this.NAMESPACE}:metrics:realtime:contractor:${contractorId}:${workday}`;
  }

  static realTimeMetricsByContractorRange(
    contractorId: string,
    fromDate: string,
    toDate: string,
  ): string {
    return `${this.NAMESPACE}:metrics:realtime:contractor:${contractorId}:range:${fromDate}:${toDate}`;
  }

  static topRanking(period: string, order: string): string {
    return `${this.NAMESPACE}:ranking:top:${period}:${order}`;
  }

  static activeTalentPercentage(period: string): string {
    return `${this.NAMESPACE}:active-talent:${period}`;
  }

  static productivityByAgent(contractorId: string, workday: string): string {
    return `${this.NAMESPACE}:productivity:by-agent:${contractorId}:${workday}`;
  }

  static productivityByAgentRange(
    contractorId: string,
    fromDate: string,
    toDate: string,
  ): string {
    return `${this.NAMESPACE}:productivity:by-agent:${contractorId}:range:${fromDate}:${toDate}`;
  }

  static dailyMetricsByContractor(contractorId: string, days: number): string {
    return `${this.NAMESPACE}:daily-metrics:contractor:${contractorId}:days:${days}`;
  }

  static sessionSummariesByContractor(
    contractorId: string,
    from?: string,
    to?: string,
    days?: number,
  ): string {
    if (from && to) {
      const fromDate = from.split('T')[0];
      const toDate = to.split('T')[0];
      return `${this.NAMESPACE}:sessions:contractor:${contractorId}:range:${fromDate}:${toDate}`;
    }
    return `${this.NAMESPACE}:sessions:contractor:${contractorId}:days:${days || 30}`;
  }

  static activityByContractor(
    contractorId: string,
    from?: string,
    to?: string,
    limit?: number,
  ): string {
    let key = `${this.NAMESPACE}:activity:contractor:${contractorId}`;
    if (from && to) {
      const fromDate = from.split('T')[0];
      const toDate = to.split('T')[0];
      key += `:range:${fromDate}:${toDate}`;
    }
    if (limit) {
      key += `:limit:${limit}`;
    }
    return key;
  }

  static appUsageByContractor(
    contractorId: string,
    from?: string,
    to?: string,
    days?: number,
  ): string {
    if (from && to) {
      const fromDate = from.split('T')[0];
      const toDate = to.split('T')[0];
      return `${this.NAMESPACE}:app-usage:contractor:${contractorId}:range:${fromDate}:${toDate}`;
    }
    return `${this.NAMESPACE}:app-usage:contractor:${contractorId}:days:${days || 30}`;
  }

  static ranking(workday?: string, limit?: number): string {
    const workdayStr = workday ? workday.split('T')[0] : 'yesterday';
    const limitStr = limit || 10;
    return `${this.NAMESPACE}:ranking:${workdayStr}:limit:${limitStr}`;
  }

  static hourlyActivityByContractor(
    contractorId: string,
    from?: string,
    to?: string,
    days?: number,
  ): string {
    if (from && to) {
      const fromDate = from.split('T')[0];
      const toDate = to.split('T')[0];
      return `${this.NAMESPACE}:hourly-activity:contractor:${contractorId}:range:${fromDate}:${toDate}`;
    }
    return `${this.NAMESPACE}:hourly-activity:contractor:${contractorId}:days:${days || 30}`;
  }

  static hourlyProductivityByContractor(
    contractorId: string,
    from?: string,
    to?: string,
    days?: number,
  ): string {
    if (from && to) {
      const fromDate = from.split('T')[0];
      const toDate = to.split('T')[0];
      return `${this.NAMESPACE}:hourly-productivity:contractor:${contractorId}:range:${fromDate}:${toDate}`;
    }
    return `${this.NAMESPACE}:hourly-productivity:contractor:${contractorId}:days:${days || 30}`;
  }

  // Inactivity Alerts System Keys
  static lastActivity(agentSessionId: string): string {
    return `${this.NAMESPACE}:last_activity:${agentSessionId}`;
  }

  static sessionStart(agentSessionId: string): string {
    return `${this.NAMESPACE}:session_start:${agentSessionId}`;
  }

  static alertActive(agentSessionId: string): string {
    return `${this.NAMESPACE}:alert_active:${agentSessionId}`;
  }
}
