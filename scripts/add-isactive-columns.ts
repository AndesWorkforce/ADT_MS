import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';

import { AppModule } from '../src/app.module';
import { ClickHouseService } from '../src/clickhouse/clickhouse.service';
import { envs } from '../config/envs';

/**
 * Script para agregar la columna isActive a las tablas contractor_info_raw y clients_dimension
 * Ejecutar con: pnpm ts-node scripts/add-isactive-columns.ts
 */
async function addIsActiveColumns() {
  const logger = new Logger('AddIsActiveColumns');
  const app = await NestFactory.createApplicationContext(AppModule);
  const clickHouseService = app.get(ClickHouseService);

  const dbName = envs.clickhouse.database;

  try {
    logger.log('🔄 Starting migration to add isActive columns...');
    logger.warn(
      '⚠️ This will DROP and RECREATE the tables. All data will be lost!',
    );

    // Eliminar y recrear contractor_info_raw con isActive
    logger.log('Dropping and recreating contractor_info_raw...');
    await clickHouseService.command(
      `DROP TABLE IF EXISTS ${dbName}.contractor_info_raw`,
    );

    await clickHouseService.command(`
      CREATE TABLE ${dbName}.contractor_info_raw (
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
    logger.log('✅ Recreated contractor_info_raw with isActive column');

    // Eliminar y recrear clients_dimension con isActive
    logger.log('Dropping and recreating clients_dimension...');
    await clickHouseService.command(
      `DROP TABLE IF EXISTS ${dbName}.clients_dimension`,
    );

    await clickHouseService.command(`
      CREATE TABLE ${dbName}.clients_dimension (
        client_id String,
        client_name String,
        isActive UInt8 DEFAULT 1,
        created_at DateTime DEFAULT now(),
        updated_at DateTime DEFAULT now()
      ) ENGINE = ReplacingMergeTree(updated_at)
      ORDER BY client_id
    `);
    logger.log('✅ Recreated clients_dimension with isActive column');

    logger.log('✅ Migration completed successfully!');
  } catch (error) {
    logger.error(`❌ Migration failed: ${error.message}`);
    throw error;
  } finally {
    await app.close();
  }
}

// Ejecutar el script
addIsActiveColumns()
  .then(() => {
    console.log('✅ Script completed');
    process.exit(0);
  })
  .catch((error) => {
    console.error('❌ Script failed:', error);
    process.exit(1);
  });
