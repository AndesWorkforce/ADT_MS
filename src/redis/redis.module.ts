import { Module, Global, Logger, OnModuleInit } from '@nestjs/common';
import { CacheModule } from '@nestjs/cache-manager';
import Keyv from 'keyv';
import KeyvRedis from '@keyv/redis';
import { RedisService } from './redis.service';
import { envs } from '../../config';

@Global()
@Module({
  imports: [
    CacheModule.registerAsync({
      isGlobal: true,
      useFactory: async () => {
        const redisUrl = envs.redis.password
          ? `redis://:${envs.redis.password}@${envs.redis.host}:${envs.redis.port}/${envs.redis.db}`
          : `redis://${envs.redis.host}:${envs.redis.port}/${envs.redis.db}`;

        const keyvRedis = new KeyvRedis(redisUrl);

        const keyv = new Keyv({
          store: keyvRedis,
          ttl: envs.redis.ttl * 1000,
        });

        keyv.on('error', (err) => {
          console.error('Keyv connection error:', err);
        });

        return {
          store: keyv,
          ttl: envs.redis.ttl * 1000,
        };
      },
    }),
  ],
  providers: [RedisService],
  exports: [RedisService, CacheModule],
})
export class RedisModule implements OnModuleInit {
  private readonly logger = new Logger(RedisModule.name);

  constructor(private readonly redisService: RedisService) {}

  async onModuleInit() {
    await new Promise((resolve) => setTimeout(resolve, 1000));

    try {
      const testKey = 'redis:connection:test';
      const testValue = { test: true, timestamp: Date.now() };

      await this.redisService.set(testKey, testValue, 5);
      const retrieved = await this.redisService.get(testKey);

      if (
        retrieved &&
        JSON.stringify(retrieved) === JSON.stringify(testValue)
      ) {
        this.logger.log(
          `✅ Redis connected: ${envs.redis.host}:${envs.redis.port} (DB: ${envs.redis.db})`,
        );
        await this.redisService.delete(testKey);
      } else {
        this.logger.warn(
          '⚠️ Redis connection test: Value mismatch (connection may be working but test failed)',
        );
      }
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `❌ Redis connection failed: ${errorMessage}. ` +
          'Redis cache will not be available. Check your Redis configuration.',
      );
      this.logger.error(
        '💡 Run "pnpm run test:redis" to diagnose the connection issue.',
      );
    }
  }
}
