import { Injectable, Logger, OnModuleInit } from '@nestjs/common';

import { ClickHouseService } from '../../clickhouse/clickhouse.service';
import { AppDimensionDto } from '../dto/app-dimension.dto';
import { DomainDimensionDto } from '../dto/domain-dimension.dto';

/**
 * Servicio que provee los pesos de productividad para apps y dominios.
 * Lee desde las tablas apps_dimension y domains_dimension en ClickHouse.
 * Si ClickHouse no está disponible o las tablas no existen, usa valores por defecto.
 */
@Injectable()
export class DimensionsService implements OnModuleInit {
  private readonly logger = new Logger(DimensionsService.name);
  private appsDimension: Map<string, AppDimensionDto>;
  private domainsDimension: Map<string, DomainDimensionDto>;
  private loadedFromClickHouse = false;

  constructor(private readonly clickHouseService: ClickHouseService) {
    // Inicializar con valores por defecto (fallback)
    this.appsDimension = this.initializeAppsDimensionDefault();
    this.domainsDimension = this.initializeDomainsDimensionDefault();
  }

  async onModuleInit() {
    // Intentar cargar desde ClickHouse al iniciar
    await this.loadFromClickHouse();
  }

  /**
   * Carga las dimensiones desde ClickHouse.
   * Si falla, mantiene los valores por defecto.
   */
  async loadFromClickHouse(): Promise<void> {
    try {
      // Verificar si las tablas existen
      const appsTableExists =
        await this.clickHouseService.tableExists('apps_dimension');
      const domainsTableExists =
        await this.clickHouseService.tableExists('domains_dimension');

      if (!appsTableExists || !domainsTableExists) {
        this.logger.warn(
          '⚠️ DimensionsService: Tablas de dimensiones no existen en ClickHouse. ' +
            'Usando valores por defecto. Ejecuta los scripts SQL para crearlas.',
        );
        return;
      }

      // Cargar apps
      const apps = await this.clickHouseService.query<AppDimensionDto>(
        'SELECT app_name, category, weight, created_at FROM apps_dimension',
      );
      if (apps.length > 0) {
        const appsMap = new Map<string, AppDimensionDto>();
        for (const app of apps) {
          appsMap.set(app.app_name, app);
        }
        this.appsDimension = appsMap;
        this.logger.log(
          `✅ DimensionsService: Cargadas ${apps.length} apps desde ClickHouse`,
        );
      }

      // Cargar dominios
      const domains = await this.clickHouseService.query<DomainDimensionDto>(
        'SELECT domain, category, weight, created_at FROM domains_dimension',
      );
      if (domains.length > 0) {
        const domainsMap = new Map<string, DomainDimensionDto>();
        for (const domain of domains) {
          domainsMap.set(domain.domain, domain);
        }
        this.domainsDimension = domainsMap;
        this.logger.log(
          `✅ DimensionsService: Cargados ${domains.length} dominios desde ClickHouse`,
        );
      }

      this.loadedFromClickHouse = true;
    } catch (error) {
      this.logger.warn(
        `⚠️ DimensionsService: Error cargando dimensiones desde ClickHouse: ${error.message}. ` +
          'Usando valores por defecto.',
      );
    }
  }

  /**
   * Recarga las dimensiones desde ClickHouse (útil para actualizar sin reiniciar).
   */
  async reload(): Promise<void> {
    await this.loadFromClickHouse();
  }

  /**
   * Obtiene el peso de una app. Si no existe, retorna el peso default (0.5).
   */
  getAppWeight(appName: string): number {
    const app = this.appsDimension.get(appName);
    if (app) {
      return app.weight;
    }
    // Default para apps desconocidas
    return 0.5;
  }

  /**
   * Obtiene el peso de un dominio. Si no existe, retorna el peso default (0.5).
   */
  getDomainWeight(domain: string): number {
    // Intentar match exacto primero
    const exact = this.domainsDimension.get(domain);
    if (exact) {
      return exact.weight;
    }

    // Intentar match por prefijo (ej: "jira." para "jira.company.com")
    for (const [key, value] of this.domainsDimension.entries()) {
      if (key.endsWith('.') && domain.startsWith(key)) {
        return value.weight;
      }
    }

    // Default para dominios desconocidos
    return 0.5;
  }

  /**
   * Obtiene todas las apps dimensionadas (útil para debugging/admin).
   */
  getAllApps(): AppDimensionDto[] {
    return Array.from(this.appsDimension.values());
  }

  /**
   * Obtiene todos los dominios dimensionados (útil para debugging/admin).
   */
  getAllDomains(): DomainDimensionDto[] {
    return Array.from(this.domainsDimension.values());
  }

  /**
   * Indica si las dimensiones fueron cargadas desde ClickHouse.
   */
  isLoadedFromClickHouse(): boolean {
    return this.loadedFromClickHouse;
  }

  private initializeAppsDimensionDefault(): Map<string, AppDimensionDto> {
    const apps: AppDimensionDto[] = [
      // Productivas
      { app_name: 'Code', category: 'productive', weight: 1.2 },
      { app_name: 'Visual Studio Code', category: 'productive', weight: 1.2 },
      { app_name: 'IntelliJ', category: 'productive', weight: 1.2 },
      { app_name: 'Word', category: 'productive', weight: 1.0 },
      { app_name: 'Excel', category: 'productive', weight: 1.0 },
      { app_name: 'PowerPoint', category: 'productive', weight: 1.0 },
      // Neutras
      { app_name: 'Slack', category: 'neutral', weight: 0.8 },
      { app_name: 'Teams', category: 'neutral', weight: 0.8 },
      { app_name: 'Chrome', category: 'neutral', weight: 0.6 },
      { app_name: 'Edge', category: 'neutral', weight: 0.6 },
      // No productivas
      { app_name: 'YouTube', category: 'non_productive', weight: 0.2 },
      { app_name: 'Spotify', category: 'non_productive', weight: 0.3 },
      { app_name: 'Discord', category: 'non_productive', weight: 0.4 },
      { app_name: 'Games', category: 'non_productive', weight: 0.1 },
    ];

    const map = new Map<string, AppDimensionDto>();
    for (const app of apps) {
      map.set(app.app_name, app);
    }
    return map;
  }

  private initializeDomainsDimensionDefault(): Map<string, DomainDimensionDto> {
    const domains: DomainDimensionDto[] = [
      // Productivos
      { domain: 'github.com', category: 'productive', weight: 1.3 },
      { domain: 'stackoverflow.com', category: 'productive', weight: 1.2 },
      { domain: 'atlassian.net', category: 'productive', weight: 1.1 },
      { domain: 'jira.', category: 'productive', weight: 1.1 }, // Prefijo para match
      { domain: 'confluence.', category: 'productive', weight: 1.1 },
      { domain: 'docs.google.com', category: 'productive', weight: 1.0 },
      { domain: 'notion.so', category: 'productive', weight: 1.0 },
      // Neutros
      { domain: 'google.com', category: 'neutral', weight: 0.7 },
      { domain: 'bing.com', category: 'neutral', weight: 0.7 },
      { domain: 'extensions', category: 'neutral', weight: 0.5 },
      // No productivos
      { domain: 'youtube.com', category: 'non_productive', weight: 0.2 },
      { domain: 'facebook.com', category: 'non_productive', weight: 0.1 },
      { domain: 'twitter.com', category: 'non_productive', weight: 0.2 },
      { domain: 'instagram.com', category: 'non_productive', weight: 0.1 },
      { domain: 'reddit.com', category: 'non_productive', weight: 0.3 },
    ];

    const map = new Map<string, DomainDimensionDto>();
    for (const domain of domains) {
      map.set(domain.domain, domain);
    }
    return map;
  }
}
