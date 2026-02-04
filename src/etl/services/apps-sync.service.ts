import { Injectable, Logger } from '@nestjs/common';

import { ClickHouseService } from '../../clickhouse/clickhouse.service';

/**
 * Servicio para sincronizar la tabla apps de Prisma (USER_MS) con apps_dimension en ClickHouse.
 * Mantiene ambas tablas idénticas cuando se crea, actualiza o elimina una aplicación.
 */
@Injectable()
export class AppsSyncService {
  private readonly logger = new Logger(AppsSyncService.name);

  constructor(private readonly clickHouseService: ClickHouseService) {}

  /**
   * Sincroniza una aplicación desde Prisma a ClickHouse.
   * Si existe, la actualiza; si no existe, la crea.
   *
   * @param app Datos de la aplicación desde Prisma
   */
  async syncApp(app: {
    id: string;
    name: string;
    category?: string | null;
    type?: string | null;
    weight?: number | null;
    created_at: Date;
    updated_at: Date;
  }): Promise<void> {
    try {
      // Verificar si la app ya existe en ClickHouse
      const existing = await this.clickHouseService.query<{ count: number }>(`
        SELECT count() as count 
        FROM apps_dimension 
        WHERE id = '${app.id}'
      `);

      const createdAt = app.created_at
        .toISOString()
        .replace('T', ' ')
        .slice(0, 19);
      const updatedAt = app.updated_at
        .toISOString()
        .replace('T', ' ')
        .slice(0, 19);

      if (existing[0]?.count > 0) {
        // Actualizar app existente: DELETE + INSERT (ClickHouse no soporta UPDATE directo)
        await this.clickHouseService.command(`
          ALTER TABLE apps_dimension
          DELETE WHERE id = '${app.id}'
        `);
      }

      // Insertar (nuevo o actualizado)
      await this.clickHouseService.insert('apps_dimension', [
        {
          id: app.id,
          name: app.name,
          category: app.category || null,
          type: app.type || null,
          weight:
            app.weight !== null && app.weight !== undefined ? app.weight : 0.5,
          created_at: createdAt,
          updated_at: updatedAt,
        },
      ]);

      if (existing[0]?.count > 0) {
        this.logger.log(
          `✅ App actualizada en ClickHouse: ${app.name} (${app.id})`,
        );
      } else {
        this.logger.log(`✅ App creada en ClickHouse: ${app.name} (${app.id})`);
      }
    } catch (error) {
      this.logger.error(
        `❌ Error sincronizando app ${app.name} (${app.id}): ${error.message}`,
      );
      throw error;
    }
  }

  /**
   * Elimina una aplicación de ClickHouse cuando se elimina en Prisma.
   *
   * @param appId ID de la aplicación a eliminar
   */
  async deleteApp(appId: string): Promise<void> {
    try {
      await this.clickHouseService.command(`
        ALTER TABLE apps_dimension
        DELETE WHERE id = '${appId}'
      `);
      this.logger.log(`✅ App eliminada de ClickHouse: ${appId}`);
    } catch (error) {
      this.logger.error(
        `❌ Error eliminando app ${appId} de ClickHouse: ${error.message}`,
      );
      throw error;
    }
  }

  /**
   * Sincroniza todas las apps desde Prisma a ClickHouse.
   * Útil para sincronización inicial o resincronización completa.
   *
   * @param apps Array de apps desde Prisma
   */
  async syncAllApps(
    apps: Array<{
      id: string;
      name: string;
      category?: string | null;
      type?: string | null;
      weight?: number | null;
      created_at: Date;
      updated_at: Date;
    }>,
  ): Promise<void> {
    this.logger.log(`🔄 Sincronizando ${apps.length} apps a ClickHouse...`);

    for (const app of apps) {
      await this.syncApp(app);
    }

    this.logger.log(
      `✅ Sincronización completa: ${apps.length} apps procesadas`,
    );
  }
}
