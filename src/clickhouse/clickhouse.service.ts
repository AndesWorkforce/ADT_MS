import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { createClient, ClickHouseClient } from '@clickhouse/client';

import { envs } from 'config';

@Injectable()
export class ClickHouseService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(ClickHouseService.name);
  private client: ClickHouseClient;
  private readonly verifiedTables = new Set<string>();

  async onModuleInit() {
    try {
      // Primero, crear un cliente temporal sin base de datos para asegurar que la base de datos existe
      const tempClient = createClient({
        host: `http://${envs.clickhouse.host}:${envs.clickhouse.port}`,
        username: envs.clickhouse.username,
        password: envs.clickhouse.password,
        // No se especificó base de datos - la crearemos primero
      });

      // Probar conexión
      await tempClient.ping();
      this.logger.log(
        `✅ ClickHouse connected: ${envs.clickhouse.host}:${envs.clickhouse.port}`,
      );

      // Asegurar que la base de datos existe (usando cliente temporal sin base de datos)
      await this.ensureDatabase(tempClient);

      // Ahora crear el cliente principal con la base de datos
      // Configurar timeouts más largos para queries complejas
      this.client = createClient({
        host: `http://${envs.clickhouse.host}:${envs.clickhouse.port}`,
        username: envs.clickhouse.username,
        password: envs.clickhouse.password,
        database: envs.clickhouse.database,
        request_timeout: 300000, // 5 minutos para queries complejas
        max_open_connections: 10, // Limitar conexiones simultáneas
      });

      // Probar conexión con la base de datos
      await this.client.ping();
      this.logger.log(
        `✅ ClickHouse connected to database: ${envs.clickhouse.database}`,
      );

      // Asegurar que las tablas RAW existen
      await this.ensureRawTables();

      // Asegurar que las tablas de dimensiones existen
      await this.ensureDimensionsTables();

      // Asegurar que las tablas ADT existen
      await this.ensureAdtTables();

      // Pre-cachear las tablas RAW que acabamos de crear/verificar
      this.verifiedTables.add('events_raw');
      this.verifiedTables.add('sessions_raw');
      this.verifiedTables.add('agent_sessions_raw');
      this.verifiedTables.add('contractor_info_raw');
      this.verifiedTables.add('apps_dimension');
      this.verifiedTables.add('domains_dimension');
      this.verifiedTables.add('contractor_activity_15s');
      this.verifiedTables.add('contractor_daily_metrics');
      this.verifiedTables.add('session_summary');

      // Cerrar cliente temporal
      await tempClient.close();
    } catch (error) {
      this.logger.error('Failed to connect to ClickHouse', error);
      throw error;
    }
  }

  async onModuleDestroy() {
    if (this.client) {
      await this.client.close();
      this.logger.log('ClickHouse connection closed');
    }
  }

  /**
   * Obtener la instancia del cliente ClickHouse
   */
  getClient(): ClickHouseClient {
    if (!this.client) {
      throw new Error('ClickHouse client is not initialized');
    }
    return this.client;
  }

  /**
   * Ejecutar una consulta y retornar resultados
   * Con timeout y mejor manejo de errores
   */
  async query<T = unknown>(
    query: string,
    timeoutMs: number = 300000,
  ): Promise<T[]> {
    const startTime = Date.now();
    try {
      // Crear un timeout manual para queries muy largas
      const timeoutPromise = new Promise<never>((_, reject) => {
        setTimeout(() => {
          reject(new Error(`Query timeout after ${timeoutMs}ms`));
        }, timeoutMs);
      });

      const queryPromise = this.client
        .query({
          query,
          format: 'JSONEachRow',
        })
        .then(async (result) => {
          const data = await result.json<T[]>();
          return data;
        });

      const data = await Promise.race([queryPromise, timeoutPromise]);
      const duration = Date.now() - startTime;

      if (duration > 5000) {
        // Log queries que tardan más de 5 segundos
        this.logger.warn(
          `Slow query detected (${duration}ms): ${query.substring(0, 200)}...`,
        );
      }

      return data;
    } catch (error) {
      const duration = Date.now() - startTime;
      const errorMessage =
        error instanceof Error ? error.message : String(error);

      // Log más detallado para errores de conexión
      if (
        errorMessage.includes('socket hang up') ||
        errorMessage.includes('ECONNRESET') ||
        errorMessage.includes('timeout')
      ) {
        this.logger.error(
          `Connection error after ${duration}ms. Query: ${query.substring(0, 200)}...`,
        );
        this.logger.error(`Error: ${errorMessage}`);
      } else {
        this.logger.error(
          `Query failed after ${duration}ms: ${query.substring(0, 200)}...`,
          error,
        );
      }

      throw error;
    }
  }

  /**
   * Ejecutar un comando (DDL, DML sin retorno)
   */
  async command(command: string): Promise<void> {
    try {
      await this.client.command({
        query: command,
      });
    } catch (error) {
      const errorMessage =
        error instanceof Error
          ? error.message
          : error instanceof AggregateError
            ? error.errors?.map((e: Error) => e.message).join('; ') ||
              String(error)
            : String(error);
      this.logger.error(`Command failed: ${command}`);
      this.logger.error(`Error details: ${errorMessage}`);
      if (error instanceof AggregateError && error.errors) {
        error.errors.forEach((err: Error, index: number) => {
          this.logger.error(`  Error ${index + 1}: ${err.message}`);
        });
      }
      throw error;
    }
  }

  /**
   * Convertir objetos Date al formato DateTime de ClickHouse (YYYY-MM-DD HH:MM:SS)
   */
  private formatDateForClickHouse(
    date: Date | string | null | undefined,
  ): string | null {
    if (!date) return null;
    if (typeof date === 'string') {
      // Si ya es un string, intentar parsearlo y formatearlo
      const parsed = new Date(date);
      if (isNaN(parsed.getTime())) return null;
      date = parsed;
    }
    if (!(date instanceof Date) || isNaN(date.getTime())) return null;

    // Formato: YYYY-MM-DD HH:MM:SS (formato DateTime de ClickHouse)
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, '0');
    const day = String(date.getUTCDate()).padStart(2, '0');
    const hours = String(date.getUTCHours()).padStart(2, '0');
    const minutes = String(date.getUTCMinutes()).padStart(2, '0');
    const seconds = String(date.getUTCSeconds()).padStart(2, '0');

    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
  }

  /**
   * Formatear fecha como solo fecha (sin hora) para campos Date en ClickHouse
   */
  private formatDateOnlyForClickHouse(
    date: Date | string | null | undefined,
  ): string | null {
    if (!date) return null;
    if (typeof date === 'string') {
      const parsed = new Date(date);
      if (isNaN(parsed.getTime())) return null;
      date = parsed;
    }
    if (!(date instanceof Date) || isNaN(date.getTime())) return null;

    // Formato: YYYY-MM-DD (solo fecha, sin hora)
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, '0');
    const day = String(date.getUTCDate()).padStart(2, '0');

    return `${year}-${month}-${day}`;
  }

  /**
   * Convertir recursivamente objetos Date en los datos al formato de ClickHouse
   * Campos específicos como 'workday' se formatean como solo fecha (Date),
   * otros campos Date se formatean como DateTime
   */
  private prepareDataForClickHouse<T extends Record<string, unknown>>(
    data: T,
  ): Record<string, unknown> {
    const prepared: Record<string, unknown> = { ...data };

    // Campos que son solo fecha (Date) en ClickHouse, no DateTime
    const dateOnlyFields = ['workday'];

    for (const key in prepared) {
      if (prepared[key] instanceof Date) {
        // Si es un campo de solo fecha, usar formato YYYY-MM-DD
        if (dateOnlyFields.includes(key)) {
          prepared[key] = this.formatDateOnlyForClickHouse(
            prepared[key] as Date,
          );
        } else {
          // Otros campos Date se formatean como DateTime
          prepared[key] = this.formatDateForClickHouse(prepared[key] as Date);
        }
      } else if (prepared[key] === null || prepared[key] === undefined) {
        // Mantener null/undefined como está
        continue;
      }
    }
    return prepared;
  }

  /**
   * Insertar datos en una tabla
   */
  async insert<T extends Record<string, unknown>>(
    table: string,
    data: T | T[],
  ): Promise<void> {
    try {
      const dataArray = Array.isArray(data) ? data : [data];

      // Verificar que la tabla existe (usando cache)
      if (!this.verifiedTables.has(table)) {
        const tableExists = await this.tableExists(table);
        if (!tableExists) {
          this.logger.warn(
            `⚠️ ClickHouseService: Table ${table} does not exist. Attempting to create RAW tables...`,
          );
          // Intentar crear las tablas RAW si no existen
          await this.ensureRawTables();

          // Verificar nuevamente si la tabla existe
          const tableExistsAfter = await this.tableExists(table);
          if (!tableExistsAfter) {
            this.logger.error(
              `❌ ClickHouseService: Table ${table} does not exist in database ${envs.clickhouse.database} and could not be created automatically`,
            );
            throw new Error(
              `Table ${table} does not exist in database ${envs.clickhouse.database}`,
            );
          }
          this.logger.log(
            `✅ ClickHouseService: Table ${table} was created successfully`,
          );
        }
        // Agregar al cache después de verificar
        this.verifiedTables.add(table);
      }

      // Convertir objetos Date al formato DateTime de ClickHouse
      const preparedData = dataArray.map((item) =>
        this.prepareDataForClickHouse(item),
      );

      await this.client.insert({
        table,
        values: preparedData,
        format: 'JSONEachRow',
      });

      // Solo log en debug para reducir ruido
      this.logger.debug(
        `✅ ClickHouseService: ${dataArray.length} record(s) inserted into table: ${table}`,
      );
    } catch (error) {
      this.logger.error(
        `❌ ClickHouseService: Error inserting into table ${table} - ${error.message}`,
      );
      // Si hay error, invalidar el cache de esta tabla por si fue eliminada
      this.verifiedTables.delete(table);
      throw error;
    }
  }

  /**
   * Insertar múltiples registros de forma masiva (batch insert)
   * Alias para `insert` que acepta arrays - más eficiente para múltiples registros
   *
   * @param table - Nombre de la tabla
   * @param dataArray - Array de objetos a insertar
   */
  async insertBatch(table: string, dataArray: any[]): Promise<void> {
    // insertBatch es simplemente un alias de insert cuando recibe un array
    // El método insert ya maneja arrays de forma eficiente
    return this.insert(table, dataArray);
  }

  /**
   * Verificar si una tabla existe
   */
  async tableExists(table: string): Promise<boolean> {
    try {
      // Validar nombre de tabla (prevenir inyección SQL)
      if (!/^[a-zA-Z0-9_]+$/.test(table)) {
        this.logger.warn(`⚠️ ClickHouseService: Invalid table name: ${table}`);
        return false;
      }

      const dbName = envs.clickhouse.database;
      this.logger.debug(
        `🔍 ClickHouseService: Checking if table ${table} exists in database ${dbName}`,
      );
      const result = await this.query<{ name: string }>(
        `SELECT name FROM system.tables WHERE database = '${dbName}' AND name = '${table}'`,
      );
      const exists = result.length > 0;
      if (exists) {
        this.logger.debug(
          `✅ ClickHouseService: Table ${table} exists in database ${dbName}`,
        );
      } else {
        this.logger.warn(
          `⚠️ ClickHouseService: Table ${table} does NOT exist in database ${dbName}`,
        );
      }
      return exists;
    } catch (error) {
      this.logger.error(
        `❌ ClickHouseService: Error checking if table ${table} exists - ${error.message}`,
      );
      return false;
    }
  }

  /**
   * Crear base de datos si no existe
   * Nota: El script de inicialización debería crear la base de datos, pero la aseguramos como respaldo
   * @param client - Cliente opcional a usar (sin base de datos). Si no se proporciona, usa this.client
   */
  async ensureDatabase(client?: ClickHouseClient): Promise<void> {
    const dbName = envs.clickhouse.database;
    const clientToUse = client || this.client;

    if (!clientToUse) {
      this.logger.warn('No client available to ensure database');
      return;
    }

    // Validar nombre de base de datos (prevenir inyección SQL)
    if (!/^[a-zA-Z0-9_]+$/.test(dbName)) {
      this.logger.error(
        `Invalid database name: ${dbName}. Only alphanumeric and underscore allowed.`,
      );
      return;
    }

    try {
      // Intentar crear base de datos (IF NOT EXISTS no fallará si ya existe)
      // Esto es más seguro que verificar primero, ya que verificar system.databases puede requerir permisos
      await clientToUse.command({
        query: `CREATE DATABASE IF NOT EXISTS ${dbName}`,
      });
      this.logger.log(
        `✅ Database ${dbName} ensured (created or already exists)`,
      );
    } catch (error) {
      // Si la creación falla, probablemente es porque:
      // 1. La base de datos ya existe (pero IF NOT EXISTS debería manejar esto)
      // 2. Problemas de permisos
      // 3. La base de datos fue creada por el script de inicialización
      // En cualquier caso, continuamos - el script de inicialización debería manejar la creación de la base de datos
      const errorMessage =
        error instanceof Error
          ? error.message
          : error instanceof AggregateError
            ? error.errors?.map((e: Error) => e.message).join('; ') ||
              String(error)
            : String(error);
      this.logger.warn(
        `⚠️ Could not ensure database ${dbName}: ${errorMessage}. ` +
          `This is usually fine - the init script should have created it. Continuing...`,
      );
      if (error instanceof AggregateError && error.errors) {
        error.errors.forEach((err: Error, index: number) => {
          this.logger.debug(`  Database error ${index + 1}: ${err.message}`);
        });
      }
      // No lanzar - dejar que continúe, el script de inicialización debería manejarlo
    }
  }

  /**
   * Eliminar tablas RAW (solo en desarrollo)
   * Esto permite resetear las tablas y recrearlas con la estructura correcta
   */
  async dropRawTables(): Promise<void> {
    if (envs.environment === 'production') {
      this.logger.warn('⚠️ Cannot drop tables in production environment');
      return;
    }

    const dbName = envs.clickhouse.database;

    try {
      const tables = [
        'events_raw',
        'sessions_raw',
        'agent_sessions_raw',
        'contractor_info_raw',
      ];

      for (const table of tables) {
        try {
          await this.command(`DROP TABLE IF EXISTS ${dbName}.${table}`);
          this.logger.log(`✅ Dropped table ${table}`);
        } catch (error) {
          // Ignorar errores si la tabla no existe
          this.logger.debug(
            `Table ${table} does not exist or could not be dropped: ${error.message}`,
          );
        }
      }

      this.logger.log('✅ All RAW tables dropped');
    } catch (error) {
      this.logger.warn(`⚠️ Error dropping RAW tables: ${error.message}`);
    }
  }

  /**
   * Crear tablas RAW si no existen
   * Estas tablas son usadas por ADT_MS para almacenar datos raw de eventos
   */
  async ensureRawTables(): Promise<void> {
    const dbName = envs.clickhouse.database;

    try {
      // ClickHouse no usa el comando USE - la base de datos se especifica en el nombre de la tabla
      // El cliente ya está configurado con la base de datos, así que solo necesitamos crear las tablas

      // Crear tabla events_raw
      await this.command(`
        CREATE TABLE IF NOT EXISTS ${dbName}.events_raw (
          event_id String,
          contractor_id String,
          agent_id Nullable(String),
          session_id Nullable(String),
          agent_session_id Nullable(String),
          timestamp DateTime,
          payload String,
          created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toDate(timestamp)
        ORDER BY (contractor_id, timestamp, event_id)
        TTL timestamp + INTERVAL 365 DAY
      `);
      this.logger.log('✅ Table events_raw verified/created');

      // Crear tabla sessions_raw
      // Usa ReplacingMergeTree para manejar actualizaciones (cuando se cierra una sesión)
      await this.command(`
        CREATE TABLE IF NOT EXISTS ${dbName}.sessions_raw (
          session_id String,
          contractor_id String,
          session_start DateTime,
          session_end Nullable(DateTime),
          total_duration Nullable(UInt32),
          created_at DateTime DEFAULT now(),
          updated_at DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated_at)
        PARTITION BY toDate(session_start)
        ORDER BY (session_id, contractor_id, session_start)
        TTL session_start + INTERVAL 365 DAY
      `);
      this.logger.log('✅ Table sessions_raw verified/created');

      // Crear tabla agent_sessions_raw
      // Usa ReplacingMergeTree para manejar actualizaciones (cuando se cierra una sesión)
      await this.command(`
        CREATE TABLE IF NOT EXISTS ${dbName}.agent_sessions_raw (
          agent_session_id String,
          contractor_id String,
          agent_id String,
          session_id Nullable(String),
          session_start DateTime,
          session_end Nullable(DateTime),
          total_duration Nullable(UInt32),
          created_at DateTime DEFAULT now(),
          updated_at DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated_at)
        PARTITION BY toDate(session_start)
        ORDER BY (agent_session_id, contractor_id, agent_id, session_start)
        TTL session_start + INTERVAL 365 DAY
      `);
      this.logger.log('✅ Table agent_sessions_raw verified/created');

      // Crear tabla contractor_info_raw
      await this.command(`
        CREATE TABLE IF NOT EXISTS ${dbName}.contractor_info_raw (
          contractor_id String,
          name String,
          email Nullable(String),
          job_position String,
          work_schedule_start Nullable(String),
          work_schedule_end Nullable(String),
          country Nullable(String),
          client_id String,
          team_id Nullable(String),
          isActive UInt8 DEFAULT 1,
          created_at DateTime,
          updated_at DateTime
        ) ENGINE = ReplacingMergeTree(updated_at)
        PARTITION BY toYYYYMM(created_at)
        ORDER BY (contractor_id, created_at)
        TTL created_at + INTERVAL 730 DAY
      `);
      this.logger.log('✅ Table contractor_info_raw verified/created');

      this.logger.log('✅ All RAW tables are ready');
    } catch (error) {
      const errorMessage =
        error instanceof Error
          ? error.message
          : error instanceof AggregateError
            ? error.errors?.map((e: Error) => e.message).join('; ') ||
              String(error)
            : String(error);
      this.logger.error(
        `❌ Error creating/verifying RAW tables: ${errorMessage}`,
      );
      if (error instanceof AggregateError && error.errors) {
        error.errors.forEach((err: Error, index: number) => {
          this.logger.error(`  Detailed error ${index + 1}: ${err.message}`);
        });
      }
      // No lanzar - intentaremos crear las tablas bajo demanda si es necesario
      this.logger.warn(
        '⚠️ RAW tables could not be created automatically. They will be created when needed.',
      );
    }
  }

  /**
   * Crear tablas de dimensiones si no existen
   * Estas tablas definen los pesos de productividad para apps y dominios
   */
  async ensureDimensionsTables(): Promise<void> {
    const dbName = envs.clickhouse.database;

    try {
      // Crear tabla apps_dimension
      await this.command(`
        CREATE TABLE IF NOT EXISTS ${dbName}.apps_dimension (
          app_name String,
          category String,
          weight Float64,
          created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY app_name
      `);
      this.logger.log('✅ Table apps_dimension verified/created');

      // Crear tabla domains_dimension
      await this.command(`
        CREATE TABLE IF NOT EXISTS ${dbName}.domains_dimension (
          domain String,
          category String,
          weight Float64,
          created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY domain
      `);
      this.logger.log('✅ Table domains_dimension verified/created');

      // Crear tabla teams_dimension
      await this.command(`
        CREATE TABLE IF NOT EXISTS ${dbName}.teams_dimension (
          team_id String,
          team_name String,
          created_at DateTime DEFAULT now(),
          updated_at DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY team_id
      `);
      this.logger.log('✅ Table teams_dimension verified/created');

      // Crear tabla clients_dimension
      await this.command(`
        CREATE TABLE IF NOT EXISTS ${dbName}.clients_dimension (
          client_id String,
          client_name String,
          isActive UInt8 DEFAULT 1,
          created_at DateTime DEFAULT now(),
          updated_at DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY client_id
      `);
      this.logger.log('✅ Table clients_dimension verified/created');

      this.logger.log('✅ All dimensions tables are ready');
    } catch (error) {
      const errorMessage =
        error instanceof Error
          ? error.message
          : error instanceof AggregateError
            ? error.errors?.map((e: Error) => e.message).join('; ') ||
              String(error)
            : String(error);
      this.logger.error(
        `❌ Error creating/verifying dimensions tables: ${errorMessage}`,
      );
      if (error instanceof AggregateError && error.errors) {
        error.errors.forEach((err: Error, index: number) => {
          this.logger.error(`  Detailed error ${index + 1}: ${err.message}`);
        });
      }
      this.logger.warn(
        '⚠️ Dimensions tables could not be created automatically.',
      );
    }
  }

  /**
   * Crear tablas ADT (Analytical Data Tables) si no existen.
   * Estas tablas almacenan las métricas agregadas y procesadas para análisis.
   */
  async ensureAdtTables(): Promise<void> {
    const dbName = envs.clickhouse.database;

    try {
      // Crear tabla contractor_activity_15s
      // Cada fila representa un heartbeat de 15 segundos
      await this.command(`
        CREATE TABLE IF NOT EXISTS ${dbName}.contractor_activity_15s (
          contractor_id String,
          agent_id Nullable(String),
          session_id Nullable(String),
          agent_session_id Nullable(String),
          beat_timestamp DateTime,
          is_idle UInt8,
          keyboard_count UInt32,
          mouse_clicks UInt32,
          workday Date DEFAULT toDate(beat_timestamp),
          created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY workday
        ORDER BY (contractor_id, beat_timestamp)
        TTL beat_timestamp + INTERVAL 365 DAY
      `);
      this.logger.log('✅ Table contractor_activity_15s verified/created');

      // Crear tabla contractor_daily_metrics
      // Agregación diaria por contractor con productivity_score
      // Usa ReplacingMergeTree para evitar que se sumen active_percentage y productivity_score
      await this.command(`
        CREATE TABLE IF NOT EXISTS ${dbName}.contractor_daily_metrics (
          contractor_id String,
          workday Date,
          total_beats UInt32,
          active_beats UInt32,
          idle_beats UInt32,
          active_percentage Float64,
          total_keyboard_inputs UInt64,
          total_mouse_clicks UInt64,
          avg_keyboard_per_min Float64,
          avg_mouse_per_min Float64,
          total_session_time_seconds UInt64,
          effective_work_seconds UInt64,
          productivity_score Float64,
          app_usage Map(String, UInt64) DEFAULT map(),
          browser_usage Map(String, UInt64) DEFAULT map(),
          created_at DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(created_at)
        PARTITION BY workday
        ORDER BY (contractor_id, workday)
        TTL workday + INTERVAL 730 DAY
      `);
      this.logger.log('✅ Table contractor_daily_metrics verified/created');

      // Migración: agregar columnas app_usage y browser_usage si no existen (para tablas existentes)
      try {
        await this.command(`
          ALTER TABLE ${dbName}.contractor_daily_metrics 
          ADD COLUMN IF NOT EXISTS app_usage Map(String, UInt64) DEFAULT map()
        `);
        await this.command(`
          ALTER TABLE ${dbName}.contractor_daily_metrics 
          ADD COLUMN IF NOT EXISTS browser_usage Map(String, UInt64) DEFAULT map()
        `);
        this.logger.log(
          '✅ Columns app_usage and browser_usage verified/added',
        );
      } catch {
        // Ignorar si las columnas ya existen
        this.logger.debug(
          'Columns app_usage/browser_usage already exist or migration skipped',
        );
      }

      // Crear tabla session_summary
      // Resumen por sesión con productivity_score
      await this.command(`
        CREATE TABLE IF NOT EXISTS ${dbName}.session_summary (
          session_id String,
          contractor_id String,
          session_start DateTime,
          session_end DateTime,
          total_seconds UInt32,
          active_seconds UInt32,
          idle_seconds UInt32,
          productivity_score Float64,
          created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toDate(session_start)
        ORDER BY (contractor_id, session_start, session_id)
        TTL session_start + INTERVAL 365 DAY
      `);
      this.logger.log('✅ Table session_summary verified/created');

      // ✅ OPTIMIZACIÓN: Crear índices secundarios para queries frecuentes
      await this.createPerformanceIndexes(dbName);

      this.logger.log('✅ All ADT tables are ready');
    } catch (error) {
      const errorMessage =
        error instanceof Error
          ? error.message
          : error instanceof AggregateError
            ? error.errors?.map((e: Error) => e.message).join('; ') ||
              String(error)
            : String(error);
      this.logger.error(
        `❌ Error creating/verifying ADT tables: ${errorMessage}`,
      );
      if (error instanceof AggregateError && error.errors) {
        error.errors.forEach((err: Error, index: number) => {
          this.logger.error(`  Detailed error ${index + 1}: ${err.message}`);
        });
      }
      this.logger.warn('⚠️ ADT tables could not be created automatically.');
    }
  }

  /**
   * Crea índices secundarios para optimizar queries frecuentes.
   * Los skip indexes en ClickHouse reducen la cantidad de datos escaneados.
   */
  private async createPerformanceIndexes(dbName: string): Promise<void> {
    try {
      // Índice para events_raw: filtra por contractor_id y timestamp
      // minmax: bueno para rangos de fechas
      try {
        await this.command(`
          ALTER TABLE ${dbName}.events_raw
          ADD INDEX IF NOT EXISTS idx_contractor_timestamp (contractor_id, timestamp)
          TYPE minmax GRANULARITY 4
        `);
        this.logger.debug(
          '✅ Index idx_contractor_timestamp on events_raw verified/created',
        );
      } catch {
        this.logger.debug('Index idx_contractor_timestamp may already exist');
      }

      // Índice bloom_filter para búsquedas exactas en contractor_id
      try {
        await this.command(`
          ALTER TABLE ${dbName}.events_raw
          ADD INDEX IF NOT EXISTS idx_contractor_bf (contractor_id)
          TYPE bloom_filter(0.01) GRANULARITY 1
        `);
        this.logger.debug(
          '✅ Index idx_contractor_bf on events_raw verified/created',
        );
      } catch {
        this.logger.debug('Index idx_contractor_bf may already exist');
      }

      // Índice para contractor_activity_15s
      try {
        await this.command(`
          ALTER TABLE ${dbName}.contractor_activity_15s
          ADD INDEX IF NOT EXISTS idx_activity_contractor (contractor_id)
          TYPE bloom_filter(0.01) GRANULARITY 1
        `);
        this.logger.debug(
          '✅ Index idx_activity_contractor on contractor_activity_15s verified/created',
        );
      } catch {
        this.logger.debug('Index idx_activity_contractor may already exist');
      }

      // Índice para contractor_daily_metrics
      try {
        await this.command(`
          ALTER TABLE ${dbName}.contractor_daily_metrics
          ADD INDEX IF NOT EXISTS idx_daily_contractor (contractor_id)
          TYPE bloom_filter(0.01) GRANULARITY 1
        `);
        this.logger.debug(
          '✅ Index idx_daily_contractor on contractor_daily_metrics verified/created',
        );
      } catch {
        this.logger.debug('Index idx_daily_contractor may already exist');
      }

      // Índice para workday en contractor_daily_metrics (útil para rangos)
      try {
        await this.command(`
          ALTER TABLE ${dbName}.contractor_daily_metrics
          ADD INDEX IF NOT EXISTS idx_daily_workday (workday)
          TYPE minmax GRANULARITY 1
        `);
        this.logger.debug(
          '✅ Index idx_daily_workday on contractor_daily_metrics verified/created',
        );
      } catch {
        this.logger.debug('Index idx_daily_workday may already exist');
      }

      this.logger.log('✅ Performance indexes verified/created');
    } catch (error) {
      this.logger.warn(
        `⚠️ Could not create some performance indexes: ${error}`,
      );
      // No lanzar error, los índices son opcionales
    }
  }
}
