import { Injectable, Logger, OnModuleInit } from '@nestjs/common';

import { ClickHouseService } from '../../clickhouse/clickhouse.service';
import { AppDimensionDto, AppType } from '../dto/app-dimension.dto';
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
   * Mapea una app legacy (sin id o con estructura antigua) a AppDimensionDto
   */
  private mapLegacyApp(app: {
    name: string;
    category?: string | null;
    type?: string | null;
    weight?: number | null;
    created_at?: Date;
  }): AppDimensionDto {
    // Validar que category sea uno de los valores permitidos
    const validCategories = [
      'productive',
      'neutral',
      'non_productive',
    ] as const;
    const category =
      app.category && validCategories.includes(app.category as any)
        ? (app.category as 'productive' | 'neutral' | 'non_productive')
        : null;

    // Validar que type sea uno de los valores permitidos
    const validTypes: AppType[] = [
      'Code',
      'Web',
      'Design',
      'Chat',
      'Office',
      'Productivity',
      'Development',
      'Database',
      'Cloud',
      'Entertainment',
      'System',
    ];
    const type =
      app.type && validTypes.includes(app.type as AppType)
        ? (app.type as AppType)
        : null;

    return {
      id: `legacy-${app.name.toLowerCase().replace(/\s+/g, '-')}`,
      name: app.name,
      category,
      type,
      weight: app.weight || 0.5,
      created_at: app.created_at || new Date(),
      updated_at: new Date(),
    };
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

      // Cargar apps (manejar casos de estructura antigua)
      let apps: AppDimensionDto[];
      try {
        // Intentar con estructura nueva (id, name)
        apps = await this.clickHouseService.query<AppDimensionDto>(
          'SELECT id, name, category, type, weight, created_at, updated_at FROM apps_dimension',
        );
      } catch {
        // Si falla, intentar con estructura antigua (sin id o con app_name)
        this.logger.warn(
          '⚠️ Error loading apps with new structure, trying legacy structure...',
        );
        try {
          // Intentar con name (sin id)
          const appsWithoutId = await this.clickHouseService.query<{
            name: string;
            category?: string | null;
            type?: string | null;
            weight?: number | null;
            created_at?: Date;
          }>(
            'SELECT name, category, type, weight, created_at FROM apps_dimension',
          );
          apps = appsWithoutId.map(
            (app): AppDimensionDto => this.mapLegacyApp(app),
          );
        } catch {
          // Si también falla, intentar con app_name (estructura muy antigua)
          this.logger.warn(
            '⚠️ Error loading apps with name column, trying app_name...',
          );
          const appsWithAppName = await this.clickHouseService.query<{
            app_name: string;
            category?: string | null;
            type?: string | null;
            weight?: number | null;
            created_at?: Date;
          }>(
            'SELECT app_name, category, type, weight, created_at FROM apps_dimension',
          );
          apps = appsWithAppName.map(
            (app): AppDimensionDto =>
              this.mapLegacyApp({
                name: app.app_name,
                category: app.category,
                type: app.type,
                weight: app.weight,
                created_at: app.created_at,
              }),
          );
        }
      }
      if (apps.length > 0) {
        const appsMap = new Map<string, AppDimensionDto>();
        for (const app of apps) {
          appsMap.set(app.name, app);
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
    if (app && app.weight !== null && app.weight !== undefined) {
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
      {
        id: 'default-code',
        name: 'Code',
        category: 'productive',
        type: 'Code',
        weight: 1.2,
      },
      {
        id: 'default-vscode',
        name: 'Visual Studio Code',
        category: 'productive',
        type: 'Code',
        weight: 1.2,
      },
      {
        id: 'default-intellij',
        name: 'IntelliJ',
        category: 'productive',
        type: 'Code',
        weight: 1.2,
      },
      {
        id: 'default-word',
        name: 'Word',
        category: 'productive',
        type: 'Office',
        weight: 1.0,
      },
      {
        id: 'default-excel',
        name: 'Excel',
        category: 'productive',
        type: 'Office',
        weight: 1.0,
      },
      {
        id: 'default-powerpoint',
        name: 'PowerPoint',
        category: 'productive',
        type: 'Office',
        weight: 1.0,
      },
      // Neutras
      {
        id: 'default-slack',
        name: 'Slack',
        category: 'neutral',
        type: 'Chat',
        weight: 0.8,
      },
      {
        id: 'default-teams',
        name: 'Teams',
        category: 'neutral',
        type: 'Chat',
        weight: 0.8,
      },
      {
        id: 'default-chrome',
        name: 'Chrome',
        category: 'neutral',
        type: 'Web',
        weight: 0.6,
      },
      {
        id: 'default-edge',
        name: 'Edge',
        category: 'neutral',
        type: 'Web',
        weight: 0.6,
      },
      // No productivas
      {
        id: 'default-youtube',
        name: 'YouTube',
        category: 'non_productive',
        type: 'Entertainment',
        weight: 0.2,
      },
      {
        id: 'default-spotify',
        name: 'Spotify',
        category: 'non_productive',
        type: 'Entertainment',
        weight: 0.3,
      },
      {
        id: 'default-discord',
        name: 'Discord',
        category: 'non_productive',
        type: 'Chat',
        weight: 0.4,
      },
      {
        id: 'default-games',
        name: 'Games',
        category: 'non_productive',
        type: 'Entertainment',
        weight: 0.1,
      },
    ];

    const map = new Map<string, AppDimensionDto>();
    for (const app of apps) {
      map.set(app.name, app);
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
