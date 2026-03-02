import { Injectable, Inject, Logger } from '@nestjs/common';
import { CACHE_MANAGER } from '@nestjs/cache-manager';
import { Cache } from 'cache-manager';
import { envs } from '../../config';

@Injectable()
export class RedisService {
  private readonly logger = new Logger(RedisService.name);

  constructor(@Inject(CACHE_MANAGER) private cacheManager: Cache) {}

  async get<T>(key: string): Promise<T | null> {
    try {
      const value = await this.cacheManager.get<T>(key);
      return value || null;
    } catch (error) {
      this.logger.error(`Error getting key ${key}:`, error);
      return null;
    }
  }

  async set<T>(key: string, value: T, ttl?: number): Promise<void> {
    try {
      const timeToLive = ttl || envs.redis.ttl;
      await this.cacheManager.set(key, value, timeToLive * 1000);
      this.logger.debug(`Cache set: ${key} (TTL: ${timeToLive}s)`);
    } catch (error) {
      this.logger.error(`Error setting key ${key}:`, error);
    }
  }

  async delete(key: string): Promise<void> {
    try {
      await this.cacheManager.del(key);
      this.logger.debug(`Cache deleted: ${key}`);
    } catch (error) {
      this.logger.error(`Error deleting key ${key}:`, error);
    }
  }

  async getOrSet<T>(
    key: string,
    factory: () => Promise<T>,
    ttl?: number,
  ): Promise<T> {
    const cached = await this.get<T>(key);
    if (cached !== null) {
      this.logger.debug(`Cache hit: ${key}`);
      return cached;
    }

    this.logger.debug(`Cache miss: ${key}`);
    const value = await factory();
    await this.set(key, value, ttl);
    return value;
  }

  // Inactivity Alerts Tracking Methods
  async setLastActivity(
    agentSessionId: string,
    timestamp: Date,
  ): Promise<void> {
    const key = `last_activity:${agentSessionId}`;
    await this.set(key, timestamp.toISOString(), 7200); // TTL 2 horas
  }

  async getLastActivity(agentSessionId: string): Promise<Date | null> {
    const key = `last_activity:${agentSessionId}`;
    const value = await this.get<string>(key);
    return value ? new Date(value) : null;
  }

  async setSessionStart(
    agentSessionId: string,
    timestamp: Date,
  ): Promise<void> {
    const key = `session_start:${agentSessionId}`;
    await this.set(key, timestamp.toISOString(), 86400); // TTL 24 horas
  }

  async getSessionStart(agentSessionId: string): Promise<Date | null> {
    const key = `session_start:${agentSessionId}`;
    const value = await this.get<string>(key);
    return value ? new Date(value) : null;
  }

  async setAlertActive(agentSessionId: string, alertId: string): Promise<void> {
    const key = `alert_active:${agentSessionId}`;
    await this.set(key, alertId, 86400); // TTL 24 horas
  }

  async getAlertActive(agentSessionId: string): Promise<string | null> {
    const key = `alert_active:${agentSessionId}`;
    return await this.get<string>(key);
  }

  async clearAlertActive(agentSessionId: string): Promise<void> {
    const key = `alert_active:${agentSessionId}`;
    await this.delete(key);
  }
}
