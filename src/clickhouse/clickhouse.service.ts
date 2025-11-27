import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
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
      this.client = createClient({
        host: `http://${envs.clickhouse.host}:${envs.clickhouse.port}`,
        username: envs.clickhouse.username,
        password: envs.clickhouse.password,
        database: envs.clickhouse.database,
      });

      // Probar conexión con la base de datos
      await this.client.ping();
      this.logger.log(
        `✅ ClickHouse connected to database: ${envs.clickhouse.database}`,
      );

      // Asegurar que las tablas RAW existen
      await this.ensureRawTables();

      // Pre-cachear las tablas RAW que acabamos de crear/verificar
      this.verifiedTables.add('events_raw');
      this.verifiedTables.add('sessions_raw');
      this.verifiedTables.add('agent_sessions_raw');
      this.verifiedTables.add('contractor_info_raw');

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
   */
  async query<T = unknown>(query: string): Promise<T[]> {
    try {
      const result = await this.client.query({
        query,
        format: 'JSONEachRow',
      });

      const data = await result.json<T[]>();
      return data;
    } catch (error) {
      this.logger.error(`Query failed: ${query}`, error);
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
      const errorMessage = error instanceof Error 
        ? error.message 
        : error instanceof AggregateError
          ? error.errors?.map((e: Error) => e.message).join('; ') || String(error)
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
  private formatDateForClickHouse(date: Date | string | null | undefined): string | null {
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
   * Convertir recursivamente objetos Date en los datos al formato de ClickHouse
   */
  private prepareDataForClickHouse<T extends Record<string, unknown>>(data: T): Record<string, unknown> {
    const prepared: Record<string, unknown> = { ...data };
    for (const key in prepared) {
      if (prepared[key] instanceof Date) {
        prepared[key] = this.formatDateForClickHouse(prepared[key] as Date);
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
          this.logger.log(`✅ ClickHouseService: Table ${table} was created successfully`);
        }
        // Agregar al cache después de verificar
        this.verifiedTables.add(table);
      }

      // Convertir objetos Date al formato DateTime de ClickHouse
      const preparedData = dataArray.map(item => this.prepareDataForClickHouse(item));

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
      this.logger.log(`✅ Database ${dbName} ensured (created or already exists)`);
    } catch (error) {
      // Si la creación falla, probablemente es porque:
      // 1. La base de datos ya existe (pero IF NOT EXISTS debería manejar esto)
      // 2. Problemas de permisos
      // 3. La base de datos fue creada por el script de inicialización
      // En cualquier caso, continuamos - el script de inicialización debería manejar la creación de la base de datos
      const errorMessage = error instanceof Error 
        ? error.message 
        : error instanceof AggregateError
          ? error.errors?.map((e: Error) => e.message).join('; ') || String(error)
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
      await this.command(`
        CREATE TABLE IF NOT EXISTS ${dbName}.sessions_raw (
          session_id String,
          contractor_id String,
          session_start DateTime,
          session_end Nullable(DateTime),
          total_duration Nullable(UInt32),
          created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toDate(session_start)
        ORDER BY (contractor_id, session_start, session_id)
        TTL session_start + INTERVAL 365 DAY
      `);
      this.logger.log('✅ Table sessions_raw verified/created');

      // Crear tabla agent_sessions_raw
      await this.command(`
        CREATE TABLE IF NOT EXISTS ${dbName}.agent_sessions_raw (
          agent_session_id String,
          contractor_id String,
          agent_id String,
          session_id Nullable(String),
          session_start DateTime,
          session_end Nullable(DateTime),
          total_duration Nullable(UInt32),
          created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toDate(session_start)
        ORDER BY (contractor_id, agent_id, session_start, agent_session_id)
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
      const errorMessage = error instanceof Error 
        ? error.message 
        : error instanceof AggregateError
          ? error.errors?.map((e: Error) => e.message).join('; ') || String(error)
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
}

