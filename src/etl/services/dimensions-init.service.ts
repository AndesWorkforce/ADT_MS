import { Injectable, Logger, OnModuleInit } from '@nestjs/common';

import { ClickHouseService } from '../../clickhouse/clickhouse.service';
import { AppDimensionDto } from '../dto/app-dimension.dto';
import { DomainDimensionDto } from '../dto/domain-dimension.dto';

/**
 * Servicio para inicializar/poblar las tablas de dimensiones con valores por defecto.
 * Se ejecuta al iniciar el módulo si las tablas están vacías.
 */
@Injectable()
export class DimensionsInitService implements OnModuleInit {
  private readonly logger = new Logger(DimensionsInitService.name);

  constructor(private readonly clickHouseService: ClickHouseService) {}

  async onModuleInit() {
    // Esperar un poco para que ClickHouse esté listo
    await new Promise((resolve) => setTimeout(resolve, 2000));

    try {
      this.logger.log('🔍 Checking dimensions tables...');
      await this.ensureDimensionsPopulated();
      this.logger.log('✅ Dimensions tables check completed');
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `❌ Could not populate dimensions tables: ${errorMessage}. ` +
          'You may need to run the script manually: pnpm run populate:dimensions',
      );
      this.logger.error(
        'Stack trace:',
        error instanceof Error ? error.stack : '',
      );
    }
  }

  /**
   * Asegura que las tablas de dimensiones estén pobladas.
   * Si están vacías, las pobla con valores por defecto.
   */
  async ensureDimensionsPopulated(): Promise<void> {
    // Verificar si apps_dimension tiene datos
    const appsCount = await this.clickHouseService.query<{ count: number }>(
      'SELECT count() as count FROM apps_dimension',
    );

    if (appsCount[0]?.count === 0) {
      this.logger.log('Populating apps_dimension with default values...');
      await this.populateAppsDimension();
    }

    // Verificar si domains_dimension tiene datos
    const domainsCount = await this.clickHouseService.query<{ count: number }>(
      'SELECT count() as count FROM domains_dimension',
    );

    if (domainsCount[0]?.count === 0) {
      this.logger.log('Populating domains_dimension with default values...');
      await this.populateDomainsDimension();
    }
  }

  /**
   * Pobla apps_dimension con valores por defecto.
   */
  private async populateAppsDimension(): Promise<void> {
    const apps: AppDimensionDto[] = [
      // Productivas
      { app_name: 'Code', category: 'productive', weight: 1.2 },
      { app_name: 'Visual Studio Code', category: 'productive', weight: 1.2 },
      { app_name: 'IntelliJ', category: 'productive', weight: 1.2 },
      { app_name: 'Word', category: 'productive', weight: 1.0 },
      { app_name: 'Excel', category: 'productive', weight: 1.0 },
      { app_name: 'PowerPoint', category: 'productive', weight: 1.0 },
      { app_name: 'Notion', category: 'productive', weight: 1.0 },
      // Neutras
      { app_name: 'Slack', category: 'neutral', weight: 0.8 },
      { app_name: 'Teams', category: 'neutral', weight: 0.8 },
      { app_name: 'Chrome', category: 'neutral', weight: 0.6 },
      { app_name: 'Edge', category: 'neutral', weight: 0.6 },
      { app_name: 'Firefox', category: 'neutral', weight: 0.6 },
      // No productivas
      { app_name: 'YouTube', category: 'non_productive', weight: 0.2 },
      { app_name: 'Spotify', category: 'non_productive', weight: 0.3 },
      { app_name: 'Discord', category: 'non_productive', weight: 0.4 },
      { app_name: 'Games', category: 'non_productive', weight: 0.1 },
    ];

    try {
      await this.clickHouseService.insert(
        'apps_dimension',
        apps as unknown as Record<string, unknown>[],
      );
      this.logger.log(`✅ Populated apps_dimension with ${apps.length} apps`);
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(`❌ Error populating apps_dimension: ${errorMessage}`);
      throw error;
    }
  }

  /**
   * Pobla domains_dimension con valores por defecto.
   */
  private async populateDomainsDimension(): Promise<void> {
    const domains: DomainDimensionDto[] = [
      // Productivos
      { domain: 'github.com', category: 'productive', weight: 1.3 },
      { domain: 'stackoverflow.com', category: 'productive', weight: 1.2 },
      { domain: 'atlassian.net', category: 'productive', weight: 1.1 },
      {
        domain: 'teamandes.atlassian.net',
        category: 'productive',
        weight: 1.1,
      },
      { domain: 'jira.', category: 'productive', weight: 1.1 }, // Prefijo para match
      { domain: 'confluence.', category: 'productive', weight: 1.1 }, // Prefijo para match
      { domain: 'docs.google.com', category: 'productive', weight: 1.0 },
      { domain: 'notion.so', category: 'productive', weight: 1.0 },
      // Neutros
      { domain: 'google.com', category: 'neutral', weight: 0.7 },
      { domain: 'bing.com', category: 'neutral', weight: 0.7 },
      { domain: 'www.bing.com', category: 'neutral', weight: 0.7 },
      { domain: 'extensions', category: 'neutral', weight: 0.5 },
      // No productivos
      { domain: 'youtube.com', category: 'non_productive', weight: 0.2 },
      { domain: 'www.youtube.com', category: 'non_productive', weight: 0.2 },
      { domain: 'facebook.com', category: 'non_productive', weight: 0.1 },
      { domain: 'www.facebook.com', category: 'non_productive', weight: 0.1 },
      { domain: 'twitter.com', category: 'non_productive', weight: 0.2 },
      { domain: 'instagram.com', category: 'non_productive', weight: 0.1 },
      { domain: 'www.instagram.com', category: 'non_productive', weight: 0.1 },
      { domain: 'reddit.com', category: 'non_productive', weight: 0.3 },
      { domain: 'www.reddit.com', category: 'non_productive', weight: 0.3 },
    ];

    try {
      await this.clickHouseService.insert(
        'domains_dimension',
        domains as unknown as Record<string, unknown>[],
      );
      this.logger.log(
        `✅ Populated domains_dimension with ${domains.length} domains`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `❌ Error populating domains_dimension: ${errorMessage}`,
      );
      throw error;
    }
  }
}
