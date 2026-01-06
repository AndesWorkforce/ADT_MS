/**
 * Script para poblar ClickHouse con datos de prueba
 *
 * Genera:
 * - 30 contratistas
 * - 2 meses de datos (desde 2 meses atrás hasta hoy)
 * - ~8 horas de trabajo por día por contratista
 * - Eventos distribuidos en sesiones a lo largo del día
 * - 10 muy productivos (80-90%)
 * - 10 medianamente productivos (60-75%)
 * - 10 poco productivos (30-50%)
 */

import 'dotenv/config';
import { createClient } from '@clickhouse/client';
import { Client as PgClient } from 'pg';
import * as crypto from 'crypto';

// Configuración desde variables de entorno
const CLICKHOUSE_HOST = process.env.CLICKHOUSE_HOST || 'localhost';
const CLICKHOUSE_PORT = parseInt(process.env.CLICKHOUSE_PORT || '8123');
const CLICKHOUSE_USERNAME = process.env.CLICKHOUSE_USERNAME || 'default';
const CLICKHOUSE_PASSWORD = process.env.CLICKHOUSE_PASSWORD || '';
const CLICKHOUSE_DATABASE = process.env.CLICKHOUSE_DATABASE || 'metrics_db';

// Configuración de datos de prueba
const WORK_HOURS_MIN = 6; // Horas mínimas de trabajo por día
const WORK_HOURS_MAX = 8; // Horas máximas de trabajo por día
const BEAT_INTERVAL_SECONDS = 15; // Cada heartbeat es de 15 segundos

// Fechas de inicio y fin para generar datos (10 de noviembre de 2025 a 10 de enero de 2026)
const START_DATE = new Date(2025, 10, 10); // 10 de noviembre de 2025 (mes 10 = noviembre, 0-indexed)
const END_DATE = new Date(2026, 0, 10); // 10 de enero de 2026 (mes 0 = enero)

const PRODUCTIVE_APPS = ['Code', 'Notion'];
const NEUTRAL_APPS = ['Chrome', 'Edge', 'Slack', 'Teams'];

const PRODUCTIVE_DOMAINS = [
  'github.com',
  'stackoverflow.com',
  'teamandes.atlassian.net',
  'docs.google.com',
];
const NON_PRODUCTIVE_DOMAINS = [
  'www.youtube.com',
  'www.reddit.com',
  'www.facebook.com',
  'www.instagram.com',
];

// Tipos de productividad
type ProductivityLevel = 'high' | 'medium' | 'low';

interface ContractorConfig {
  contractor_id: string;
  name: string;
  email: string;
  productivity: ProductivityLevel;
  targetProductivity: { min: number; max: number };
}

interface Session {
  session_id: string;
  agent_session_id: string;
  start_time: Date;
  end_time: Date;
  beats: number;
}

// Generar IDs únicos
function generateId(prefix: string): string {
  return `${prefix}-${crypto.randomUUID()}`;
}

// Generar nombre aleatorio (sin caracteres especiales para evitar problemas de encoding)
function generateName(index: number): string {
  const firstNames = [
    'Juan',
    'Maria',
    'Carlos',
    'Ana',
    'Luis',
    'Laura',
    'Pedro',
    'Sofia',
    'Diego',
    'Valentina',
    'Andres',
    'Camila',
    'Fernando',
    'Isabella',
    'Roberto',
    'Gabriela',
    'Miguel',
    'Daniela',
    'Javier',
    'Natalia',
  ];
  const lastNames = [
    'Garcia',
    'Rodriguez',
    'Lopez',
    'Martinez',
    'Gonzalez',
    'Perez',
    'Sanchez',
    'Ramirez',
    'Torres',
    'Flores',
    'Rivera',
    'Cruz',
    'Morales',
    'Ortiz',
    'Gutierrez',
    'Chavez',
    'Ramos',
    'Mendoza',
    'Herrera',
    'Jimenez',
  ];
  return `${firstNames[index % firstNames.length]} ${lastNames[Math.floor(index / firstNames.length) % lastNames.length]}`;
}

// Generar email
function generateEmail(name: string, index: number): string {
  const normalized = name
    .toLowerCase()
    .replace(/\s+/g, '.')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '');
  return `${normalized}${index}@test.com`;
}

// Formatear fecha para ClickHouse (formato: 'YYYY-MM-DD HH:mm:ss' en UTC)
function formatDateForClickHouse(date: Date): string {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  const hours = String(date.getUTCHours()).padStart(2, '0');
  const minutes = String(date.getUTCMinutes()).padStart(2, '0');
  const seconds = String(date.getUTCSeconds()).padStart(2, '0');
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

// Calcular beats necesarios para un número de horas
function calculateBeatsForWorkDay(workHours: number): number {
  const totalSeconds = workHours * 3600;
  return Math.floor(totalSeconds / BEAT_INTERVAL_SECONDS);
}

// Generar horas de trabajo aleatorias entre min y max
function generateWorkHours(): number {
  return WORK_HOURS_MIN + Math.random() * (WORK_HOURS_MAX - WORK_HOURS_MIN);
}

// Generar factor de variación diaria de productividad
// Devuelve un factor entre 0.65 y 1.35 que varía la productividad del día
// Esto permite que un contratista tenga días mejores y peores
function generateDailyProductivityFactor(
  baseProductivity: ProductivityLevel,
): number {
  // Factor base según el nivel de productividad
  // High: 0.85-1.15 (días buenos más frecuentes)
  // Medium: 0.75-1.25 (más variación)
  // Low: 0.65-1.35 (más variación, días muy malos posibles)
  let minFactor: number;
  let maxFactor: number;

  switch (baseProductivity) {
    case 'high':
      minFactor = 0.85;
      maxFactor = 1.15;
      break;
    case 'medium':
      minFactor = 0.75;
      maxFactor = 1.25;
      break;
    case 'low':
      minFactor = 0.65;
      maxFactor = 1.35;
      break;
  }

  // Aplicar distribución normal aproximada (usando suma de 3 random para aproximar curva normal)
  const randomSum = Math.random() + Math.random() + Math.random();
  const normalized = randomSum / 3; // 0-1
  const factor = minFactor + normalized * (maxFactor - minFactor);

  return Math.max(0.5, Math.min(1.5, factor)); // Limitar entre 0.5 y 1.5 para evitar extremos
}

// Generar sesiones distribuidas a lo largo del día
function generateSessionsForDay(
  day: Date,
  contractorId: string,
  agentId: string,
  totalBeats: number,
): Session[] {
  const sessions: Session[] = [];

  // Horario de trabajo típico: 9:00 AM - 6:00 PM (con pausas)
  const workStartHour = 9;

  // Crear 2-4 sesiones por día (simulando pausas, almuerzo, etc.)
  const numSessions = 2 + Math.floor(Math.random() * 3); // 2-4 sesiones
  const beatsPerSession = Math.floor(totalBeats / numSessions);
  const remainingBeats = totalBeats - beatsPerSession * numSessions;

  let currentHour = workStartHour;
  let currentMinute = 0;

  for (let i = 0; i < numSessions; i++) {
    const sessionStart = new Date(day);
    sessionStart.setHours(currentHour, currentMinute, 0, 0);

    // Duración de la sesión (en beats)
    const sessionBeats = beatsPerSession + (i === 0 ? remainingBeats : 0);
    const sessionDurationMinutes = (sessionBeats * BEAT_INTERVAL_SECONDS) / 60;

    const sessionEnd = new Date(sessionStart);
    sessionEnd.setMinutes(sessionEnd.getMinutes() + sessionDurationMinutes);

    // Agregar pausa entre sesiones (30-90 minutos)
    if (i < numSessions - 1) {
      const pauseMinutes = 30 + Math.floor(Math.random() * 60);
      currentHour = sessionEnd.getHours();
      currentMinute = sessionEnd.getMinutes() + pauseMinutes;
      if (currentMinute >= 60) {
        currentHour += Math.floor(currentMinute / 60);
        currentMinute = currentMinute % 60;
      }
    }

    sessions.push({
      session_id: generateId('session'),
      agent_session_id: generateId('agent-session'),
      start_time: sessionStart,
      end_time: sessionEnd,
      beats: sessionBeats,
    });
  }

  return sessions;
}

// Generar payload según nivel de productividad
function generatePayload(
  productivity: ProductivityLevel,
  beatIndex: number,
  sessionBeats: number,
  dailyFactor: number = 1.0, // Factor de variación diaria (default 1.0 = sin variación)
): string {
  // Progreso dentro de la sesión (0..1) y factor suave para variar la intensidad a lo largo de la sesión
  const progress = sessionBeats > 0 ? beatIndex / sessionBeats : 0;
  const phaseFactor = 0.9 + 0.2 * progress; // 0.9 → 1.1

  // Ajustar probabilidad de beat idle según el factor diario
  // Si dailyFactor < 1.0 (día malo), aumenta la probabilidad de beats idle
  // Si dailyFactor > 1.0 (día bueno), disminuye la probabilidad de beats idle
  const baseIdleProb =
    productivity === 'high' ? 0.12 : productivity === 'medium' ? 0.28 : 0.45;
  const adjustedIdleProb = Math.max(
    0.05,
    Math.min(0.7, baseIdleProb * (2 - dailyFactor)),
  ); // Invertir: factor alto = menos idle
  const isIdleBeat = Math.random() < adjustedIdleProb;

  let keyboardInputs: number;
  let mouseClicks: number;
  let idleTime: number;
  let appUsage: Record<string, number>;
  let browserUsage: Record<string, number>;

  if (isIdleBeat) {
    // Beat idle: sin inputs, descanso/observación
    keyboardInputs = 0;
    mouseClicks = 0;
    // IdleTime cercano a todo el beat
    idleTime = 12 + Math.random() * 3; // 12-15s
    // Sin uso de apps ni navegador relevante
    appUsage = {};
    browserUsage = {};
  } else {
    // Calcular valores según productividad (beat activo)
    // Aplicar el factor diario para variar la productividad del día
    switch (productivity) {
      case 'high': // 80-90%
        // Rango más realista por beat de 15s, ajustado por factor diario
        keyboardInputs = Math.round(
          (6 + Math.floor(Math.random() * 13)) * dailyFactor,
        ); // 6-18 inputs * factor
        mouseClicks = Math.round(
          (2 + Math.floor(Math.random() * 5)) * dailyFactor,
        ); // 2-6 clicks * factor
        idleTime = Math.max(0, (Math.random() * 0.6) / dailyFactor); // Menos idle si factor alto
        // Más tiempo en apps productivas (ajustado por factor diario)
        const highProductive = dailyFactor >= 0.95;
        const highNonProductive = dailyFactor < 0.85;
        appUsage = generateAppUsage(highProductive, highNonProductive);
        browserUsage = generateBrowserUsage(highProductive, highNonProductive);
        break;

      case 'medium': // 60-75%
        keyboardInputs = Math.round(
          (3 + Math.floor(Math.random() * 8)) * dailyFactor,
        ); // 3-10 inputs * factor
        mouseClicks = Math.round(
          (1 + Math.floor(Math.random() * 4)) * dailyFactor,
        ); // 1-4 clicks * factor
        idleTime = Math.max(0, (1.0 + Math.random() * 3.0) / dailyFactor); // Menos idle si factor alto
        // Mix de apps (ajustado por factor diario)
        const medProductive = dailyFactor >= 1.0;
        const medNonProductive = dailyFactor < 0.85;
        appUsage = generateAppUsage(medProductive, medNonProductive);
        browserUsage = generateBrowserUsage(medProductive, medNonProductive);
        break;

      case 'low': // 30-50%
        // En días malos (factor bajo), más probabilidad de 0 inputs
        const lowInputProb = dailyFactor < 0.8 ? 0.7 : 0.6;
        keyboardInputs = Math.round(
          (Math.random() < lowInputProb
            ? 0
            : 1 + Math.floor(Math.random() * 3)) * dailyFactor,
        ); // 0-3 inputs * factor
        const lowClickProb = dailyFactor < 0.8 ? 0.8 : 0.7;
        mouseClicks = Math.round(
          (Math.random() < lowClickProb
            ? 0
            : 1 + Math.floor(Math.random() * 2)) * dailyFactor,
        ); // 0-2 clicks * factor
        idleTime = Math.max(0, (4 + Math.random() * 8) / dailyFactor); // Más idle si factor bajo
        // Más tiempo en apps no productivas (ajustado por factor diario)
        const lowProductive = dailyFactor >= 1.0;
        const lowNonProductive = dailyFactor < 0.9;
        appUsage = generateAppUsage(lowProductive, lowNonProductive);
        browserUsage = generateBrowserUsage(lowProductive, lowNonProductive);
        break;
    }
  }

  // Variación suave por fase de sesión (ya aplicado el factor diario)
  keyboardInputs = Math.max(0, Math.round(keyboardInputs * phaseFactor));
  mouseClicks = Math.max(0, Math.round(mouseClicks * phaseFactor));

  // Asegurar que la suma de AppUsage y browser no exceda 15 segundos
  const totalAppSeconds = Object.values(appUsage).reduce(
    (sum, val) => sum + val,
    0,
  );
  const totalBrowserSeconds = Object.values(browserUsage).reduce(
    (sum, val) => sum + val,
    0,
  );
  const totalSeconds = totalAppSeconds + totalBrowserSeconds;

  if (totalSeconds > 15) {
    // Normalizar para que sumen 15 segundos
    const scale = 15 / totalSeconds;
    Object.keys(appUsage).forEach((app) => {
      appUsage[app] = Math.round(appUsage[app] * scale * 10) / 10;
    });
    Object.keys(browserUsage).forEach((domain) => {
      browserUsage[domain] = Math.round(browserUsage[domain] * scale * 10) / 10;
    });
  }

  const payload = {
    Keyboard: {
      InactiveTime: Math.random() * 0.5,
      InputsCount: keyboardInputs,
    },
    Mouse: {
      InactiveTime: Math.random() * 0.1,
      ClicksCount: mouseClicks,
    },
    IdleTime: idleTime,
    AppUsage: appUsage,
    browser: browserUsage,
  };

  return JSON.stringify(payload);
}

// Generar uso de apps según productividad
function generateAppUsage(
  productive: boolean,
  nonProductive: boolean,
): Record<string, number> {
  const usage: Record<string, number> = {};
  const totalSeconds = 15;

  if (productive) {
    // 70% apps productivas, 30% neutrales
    const productiveSeconds = totalSeconds * 0.7;
    const neutralSeconds = totalSeconds * 0.3;

    // Distribuir entre apps productivas
    const productiveApps = [...PRODUCTIVE_APPS];
    const numProductive = 1 + Math.floor(Math.random() * 2);
    const selectedProductive = productiveApps.slice(0, numProductive);
    selectedProductive.forEach((app, i) => {
      usage[app] =
        i === selectedProductive.length - 1
          ? productiveSeconds -
            Object.values(usage).reduce((sum, val) => sum + val, 0)
          : productiveSeconds / numProductive;
    });

    // Distribuir entre apps neutrales
    const neutralApps = [...NEUTRAL_APPS];
    const numNeutral = 1 + Math.floor(Math.random() * 2);
    const selectedNeutral = neutralApps.slice(0, numNeutral);
    selectedNeutral.forEach((app, i) => {
      usage[app] =
        i === selectedNeutral.length - 1
          ? neutralSeconds -
            Object.values(usage).reduce((sum, val) => sum + val, 0)
          : neutralSeconds / numNeutral;
    });
  } else if (nonProductive) {
    // 40% apps productivas, 60% neutrales (más tiempo en navegador)
    const productiveSeconds = totalSeconds * 0.4;
    const neutralSeconds = totalSeconds * 0.6;

    const productiveApps = [...PRODUCTIVE_APPS];
    const numProductive = Math.random() > 0.5 ? 1 : 0;
    if (numProductive > 0) {
      usage[productiveApps[0]] = productiveSeconds;
    }

    const neutralApps = [...NEUTRAL_APPS];
    const numNeutral = 1 + Math.floor(Math.random() * 2);
    const selectedNeutral = neutralApps.slice(0, numNeutral);
    selectedNeutral.forEach((app, i) => {
      usage[app] =
        i === selectedNeutral.length - 1
          ? neutralSeconds -
            Object.values(usage).reduce((sum, val) => sum + val, 0)
          : neutralSeconds / numNeutral;
    });
  } else {
    // Mix balanceado
    const productiveSeconds = totalSeconds * 0.5;
    const neutralSeconds = totalSeconds * 0.5;

    const productiveApps = [...PRODUCTIVE_APPS];
    const numProductive = 1 + Math.floor(Math.random() * 2);
    const selectedProductive = productiveApps.slice(0, numProductive);
    selectedProductive.forEach((app, i) => {
      usage[app] =
        i === selectedProductive.length - 1
          ? productiveSeconds -
            Object.values(usage).reduce((sum, val) => sum + val, 0)
          : productiveSeconds / numProductive;
    });

    const neutralApps = [...NEUTRAL_APPS];
    const numNeutral = 1 + Math.floor(Math.random() * 2);
    const selectedNeutral = neutralApps.slice(0, numNeutral);
    selectedNeutral.forEach((app, i) => {
      usage[app] =
        i === selectedNeutral.length - 1
          ? neutralSeconds -
            Object.values(usage).reduce((sum, val) => sum + val, 0)
          : neutralSeconds / numNeutral;
    });
  }

  return usage;
}

// Generar uso de browser según productividad
function generateBrowserUsage(
  productive: boolean,
  nonProductive: boolean,
): Record<string, number> {
  const usage: Record<string, number> = {};
  const totalSeconds = 15;

  if (productive) {
    // 80% dominios productivos, 20% neutrales/no productivos
    const productiveSeconds = totalSeconds * 0.8;
    const otherSeconds = totalSeconds * 0.2;

    const productiveDomains = [...PRODUCTIVE_DOMAINS];
    const numProductive = 1 + Math.floor(Math.random() * 2);
    const selectedProductive = productiveDomains.slice(0, numProductive);
    selectedProductive.forEach((domain, i) => {
      usage[domain] =
        i === selectedProductive.length - 1
          ? productiveSeconds -
            Object.values(usage).reduce((sum, val) => sum + val, 0)
          : productiveSeconds / numProductive;
    });

    if (otherSeconds > 0 && Math.random() > 0.5) {
      const otherDomains = [...NON_PRODUCTIVE_DOMAINS];
      usage[otherDomains[Math.floor(Math.random() * otherDomains.length)]] =
        otherSeconds;
    }
  } else if (nonProductive) {
    // 30% dominios productivos, 70% no productivos
    const productiveSeconds = totalSeconds * 0.3;
    const nonProductiveSeconds = totalSeconds * 0.7;

    if (productiveSeconds > 0 && Math.random() > 0.3) {
      const productiveDomains = [...PRODUCTIVE_DOMAINS];
      usage[
        productiveDomains[Math.floor(Math.random() * productiveDomains.length)]
      ] = productiveSeconds;
    }

    const nonProductiveDomains = [...NON_PRODUCTIVE_DOMAINS];
    const numNonProductive = 1 + Math.floor(Math.random() * 2);
    const selectedNonProductive = nonProductiveDomains.slice(
      0,
      numNonProductive,
    );
    selectedNonProductive.forEach((domain, i) => {
      usage[domain] =
        i === selectedNonProductive.length - 1
          ? nonProductiveSeconds -
            Object.values(usage).reduce((sum, val) => sum + val, 0)
          : nonProductiveSeconds / numNonProductive;
    });
  } else {
    // Mix balanceado
    const productiveSeconds = totalSeconds * 0.5;
    const otherSeconds = totalSeconds * 0.5;

    const productiveDomains = [...PRODUCTIVE_DOMAINS];
    const numProductive = 1 + Math.floor(Math.random() * 2);
    const selectedProductive = productiveDomains.slice(0, numProductive);
    selectedProductive.forEach((domain, i) => {
      usage[domain] =
        i === selectedProductive.length - 1
          ? productiveSeconds -
            Object.values(usage).reduce((sum, val) => sum + val, 0)
          : productiveSeconds / numProductive;
    });

    const otherDomains = [...NON_PRODUCTIVE_DOMAINS];
    const numOther = Math.random() > 0.5 ? 1 : 0;
    if (numOther > 0) {
      usage[otherDomains[Math.floor(Math.random() * otherDomains.length)]] =
        otherSeconds;
    }
  }

  return usage;
}

// Generar contratistas (30 total)
function generateContractors(): ContractorConfig[] {
  const contractors: ContractorConfig[] = [];

  // 10 muy productivos (80-90%)
  for (let i = 0; i < 10; i++) {
    contractors.push({
      contractor_id: generateId('contractor'),
      name: generateName(i),
      email: generateEmail(generateName(i), i),
      productivity: 'high',
      targetProductivity: { min: 80, max: 90 },
    });
  }

  // 10 medianamente productivos (60-75%)
  for (let i = 10; i < 20; i++) {
    contractors.push({
      contractor_id: generateId('contractor'),
      name: generateName(i),
      email: generateEmail(generateName(i), i),
      productivity: 'medium',
      targetProductivity: { min: 60, max: 75 },
    });
  }

  // 10 poco productivos (30-50%)
  for (let i = 20; i < 30; i++) {
    contractors.push({
      contractor_id: generateId('contractor'),
      name: generateName(i),
      email: generateEmail(generateName(i), i),
      productivity: 'low',
      targetProductivity: { min: 30, max: 50 },
    });
  }

  return contractors;
}

// Insertar clients y contractors en PostgreSQL (USER_MS)
async function seedPostgres(
  contractors: ContractorConfig[],
  clientNames: string[],
) {
  const databaseUrl = process.env.DATABASE_URL;

  if (!databaseUrl) {
    console.warn(
      '⚠️  DATABASE_URL no está definido. Se omite seed en PostgreSQL.',
    );
    return;
  }

  const pg = new PgClient({ connectionString: databaseUrl });
  await pg.connect();

  try {
    console.log(
      '🐘 Conectando a PostgreSQL para seed de clients/contractors...',
    );

    // Asegurar que existan clients
    const existingClients = await pg.query<{ id: string; name: string }>(
      'SELECT id, name FROM clients',
    );

    const clientsForPostgres = existingClients.rows;

    if (clientsForPostgres.length === 0) {
      console.log(
        '📂 No se encontraron clients en PostgreSQL. Creando nuevos...',
      );
      const insertClientText =
        'INSERT INTO clients (name, created_at, updated_at) VALUES ($1, NOW(), NOW()) RETURNING id, name';

      for (const name of clientNames) {
        const result = await pg.query(insertClientText, [name]);
        clientsForPostgres.push(result.rows[0]);
      }
    } else {
      console.log(
        `📂 Se encontraron ${clientsForPostgres.length} clients existentes en PostgreSQL. Se reutilizarán.`,
      );
    }

    // Insertar contractors (si no existen por email)
    console.log('👥 Insertando contractors en PostgreSQL (si no existen)...');

    const insertContractorText = `
      INSERT INTO contractors (
        id,
        name,
        email,
        job_position,
        client_id,
        country,
        work_schedule_start,
        work_schedule_end,
        created_at,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW())
      ON CONFLICT (email) DO NOTHING
    `;

    for (const contractor of contractors) {
      const randomClient =
        clientsForPostgres[
          Math.floor(Math.random() * clientsForPostgres.length)
        ];

      await pg.query(insertContractorText, [
        contractor.contractor_id,
        contractor.name,
        contractor.email,
        'Software Developer',
        randomClient.id,
        'Argentina',
        '09:00',
        '18:00',
      ]);
    }

    console.log('✅ Seed de clients y contractors en PostgreSQL completado.\n');
  } catch (error) {
    console.error('❌ Error haciendo seed en PostgreSQL:', error);
  } finally {
    await pg.end();
  }
}

// Función principal
async function populateTestData() {
  console.log('🚀 Iniciando población de datos de prueba...\n');

  // Conectar a ClickHouse
  const client = createClient({
    host: `http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}`,
    username: CLICKHOUSE_USERNAME,
    password: CLICKHOUSE_PASSWORD,
    database: CLICKHOUSE_DATABASE,
  });

  try {
    await client.ping();
    console.log(
      `✅ Conectado a ClickHouse: ${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}`,
    );
    console.log(`📊 Base de datos: ${CLICKHOUSE_DATABASE}\n`);
  } catch (error) {
    console.error('❌ Error conectando a ClickHouse:', error);
    process.exit(1);
  }

  // Generar contratistas
  const contractors = generateContractors();
  console.log(
    `👥 Generando datos para ${contractors.length} contratistas...\n`,
  );

  // Generar fechas desde START_DATE hasta END_DATE (ambas incluidas)
  const dates: Date[] = [];
  const currentDate = new Date(START_DATE);
  currentDate.setUTCHours(0, 0, 0, 0);
  const endDate = new Date(END_DATE);
  endDate.setUTCHours(0, 0, 0, 0);

  while (currentDate <= endDate) {
    dates.push(new Date(currentDate));
    currentDate.setUTCDate(currentDate.getUTCDate() + 1);
  }

  console.log(
    `📅 Generando datos desde ${dates[0].toISOString().split('T')[0]} hasta ${dates[dates.length - 1].toISOString().split('T')[0]} (${dates.length} días)\n`,
  );
  console.log(
    `⏱️  Horas de trabajo por día: ${WORK_HOURS_MIN} - ${WORK_HOURS_MAX} horas (variable)\n`,
  );

  // Preparar datos para inserción
  const eventsRaw: any[] = [];
  const sessionsRaw: any[] = [];
  const agentSessionsRaw: any[] = [];
  const contractorInfoRaw: any[] = [];
  const teamsDimension: any[] = [];
  const clientsDimension: any[] = [];

  // Generar un conjunto fijo de teams y clients (compartidos entre contractors)
  const teams = [
    { id: generateId('team'), name: 'Development' },
    { id: generateId('team'), name: 'Support' },
    { id: generateId('team'), name: 'DevOps' },
    { id: generateId('team'), name: 'QA' },
  ];

  const clients = [
    { id: generateId('client'), name: 'Tech Corporation' },
    { id: generateId('client'), name: 'RTM Corporation' },
    { id: generateId('client'), name: 'IBM Connect' },
    { id: generateId('client'), name: 'Bamboo' },
    { id: generateId('client'), name: 'Qilla' },
  ];

  // Seed en PostgreSQL (USER_MS): clients + contractors (isActive por defecto true)
  await seedPostgres(
    contractors,
    clients.map((c) => c.name),
  );

  // Poblar tablas de dimensiones
  teams.forEach((team) => {
    teamsDimension.push({
      team_id: team.id,
      team_name: team.name,
      created_at: formatDateForClickHouse(new Date()),
      updated_at: formatDateForClickHouse(new Date()),
    });
  });

  clients.forEach((client) => {
    clientsDimension.push({
      client_id: client.id,
      client_name: client.name,
      isActive: 1,
      created_at: formatDateForClickHouse(new Date()),
      updated_at: formatDateForClickHouse(new Date()),
    });
  });

  // Primero, generar información de todos los contratistas (sin eventos aún)
  const contractorAgents = new Map<string, string>(); // contractor_id -> agent_id
  const contractorTeams = new Map<
    string,
    { teamId: string; clientId: string }
  >(); // contractor_id -> {teamId, clientId}

  for (const contractor of contractors) {
    const agentId = generateId('agent');
    contractorAgents.set(contractor.contractor_id, agentId);

    // Asignar aleatoriamente un team y client del conjunto fijo
    const team = teams[Math.floor(Math.random() * teams.length)];
    const client = clients[Math.floor(Math.random() * clients.length)];
    contractorTeams.set(contractor.contractor_id, {
      teamId: team.id,
      clientId: client.id,
    });

    // Información del contratista
    const contractorCreatedAt = new Date(dates[0]);
    contractorInfoRaw.push({
      contractor_id: contractor.contractor_id,
      name: contractor.name,
      email: contractor.email,
      job_position: 'Software Developer',
      work_schedule_start: '09:00',
      work_schedule_end: '18:00',
      country: 'Argentina',
      client_id: client.id,
      team_id: team.id,
      isActive: 1,
      created_at: formatDateForClickHouse(contractorCreatedAt),
      updated_at: formatDateForClickHouse(contractorCreatedAt),
    });
  }

  // Contador de eventos por contratista para logging
  const contractorEventsCount = new Map<string, number>();
  contractors.forEach((c) => contractorEventsCount.set(c.contractor_id, 0));

  // Generar datos para cada día
  for (const day of dates) {
    // Seleccionar qué contratistas estarán activos este día (70-90% estarán activos)
    const activePercentage = 0.7 + Math.random() * 0.2; // 70% - 90%
    const numActive = Math.max(
      1,
      Math.floor(contractors.length * activePercentage),
    );

    // Mezclar contratistas y seleccionar los que estarán activos
    const shuffled = [...contractors].sort(() => Math.random() - 0.5);
    const activeContractors = shuffled.slice(0, numActive);
    // inactiveContractors no se usa, solo para logging del slice
    const _inactiveContractors = shuffled.slice(numActive);
    void _inactiveContractors; // Evitar warning de variable no usada

    console.log(
      `📅 ${day.toISOString().split('T')[0]}: ${activeContractors.length}/${contractors.length} contratistas activos (${Math.round((activeContractors.length / contractors.length) * 100)}%)`,
    );

    // Generar eventos solo para contratistas activos
    for (const contractor of activeContractors) {
      const agentId = contractorAgents.get(contractor.contractor_id)!;
      // teamId y clientId están disponibles si se necesitan en el futuro
      const { teamId: _teamId, clientId: _clientId } = contractorTeams.get(
        contractor.contractor_id,
      )!;
      void _teamId;
      void _clientId;

      // Generar factor de variación diaria de productividad
      // Este factor varía cada día, haciendo que algunos días sean mejores o peores
      const dailyProductivityFactor = generateDailyProductivityFactor(
        contractor.productivity,
      );

      // Generar horas de trabajo aleatorias para este día (entre 6 y 8 horas)
      // En días con factor bajo, puede trabajar menos horas
      const baseWorkHours = generateWorkHours();
      const workHours = Math.max(
        WORK_HOURS_MIN,
        baseWorkHours * (0.8 + dailyProductivityFactor * 0.2),
      ); // Ajustar horas según factor diario
      const beatsPerDay = calculateBeatsForWorkDay(workHours);

      const sessions = generateSessionsForDay(
        day,
        contractor.contractor_id,
        agentId,
        beatsPerDay,
      );

      // Procesar cada sesión
      for (const session of sessions) {
        const sessionStart = session.start_time;
        let currentTimestamp = new Date(sessionStart);

        // Generar eventos para cada beat de la sesión
        for (let beatIndex = 0; beatIndex < session.beats; beatIndex++) {
          const eventId = generateId('event');
          const payload = generatePayload(
            contractor.productivity,
            beatIndex,
            session.beats,
            dailyProductivityFactor, // Pasar el factor diario
          );

          eventsRaw.push({
            event_id: eventId,
            contractor_id: contractor.contractor_id,
            agent_id: agentId,
            session_id: session.session_id,
            agent_session_id: session.agent_session_id,
            timestamp: formatDateForClickHouse(currentTimestamp),
            payload: payload,
            created_at: formatDateForClickHouse(currentTimestamp),
          });

          const currentCount =
            contractorEventsCount.get(contractor.contractor_id) || 0;
          contractorEventsCount.set(contractor.contractor_id, currentCount + 1);

          // Avanzar 15 segundos
          currentTimestamp = new Date(
            currentTimestamp.getTime() + BEAT_INTERVAL_SECONDS * 1000,
          );
        }

        // Sesión RAW
        const sessionEnd = new Date(currentTimestamp);
        const sessionDuration = Math.floor(
          (sessionEnd.getTime() - sessionStart.getTime()) / 1000,
        );

        sessionsRaw.push({
          session_id: session.session_id,
          contractor_id: contractor.contractor_id,
          session_start: formatDateForClickHouse(sessionStart),
          session_end: formatDateForClickHouse(sessionEnd),
          total_duration: sessionDuration,
          created_at: formatDateForClickHouse(sessionStart),
          updated_at: formatDateForClickHouse(sessionEnd),
        });

        // Agent Session RAW
        agentSessionsRaw.push({
          agent_session_id: session.agent_session_id,
          contractor_id: contractor.contractor_id,
          agent_id: agentId,
          session_id: session.session_id,
          session_start: formatDateForClickHouse(sessionStart),
          session_end: formatDateForClickHouse(sessionEnd),
          total_duration: sessionDuration,
          created_at: formatDateForClickHouse(sessionStart),
          updated_at: formatDateForClickHouse(sessionEnd),
        });
      }
    }
  }

  // Log de eventos generados por contratista
  for (const contractor of contractors) {
    const count = contractorEventsCount.get(contractor.contractor_id) || 0;
    console.log(
      `✅ ${contractor.name} (${contractor.productivity}): ${count} eventos generados`,
    );
  }

  console.log(`\n📦 Total de registros generados:`);
  console.log(`   - Events: ${eventsRaw.length}`);
  console.log(`   - Sessions: ${sessionsRaw.length}`);
  console.log(`   - Agent Sessions: ${agentSessionsRaw.length}`);
  console.log(`   - Contractors: ${contractorInfoRaw.length}`);
  console.log(`   - Teams: ${teamsDimension.length}`);
  console.log(`   - Clients: ${clientsDimension.length}\n`);

  // Insertar datos en ClickHouse
  console.log('💾 Insertando datos en ClickHouse...\n');

  try {
    // Insertar teams_dimension
    if (teamsDimension.length > 0) {
      await client.insert({
        table: 'teams_dimension',
        values: teamsDimension,
        format: 'JSONEachRow',
      });
      console.log(
        `✅ Insertados ${teamsDimension.length} registros en teams_dimension`,
      );
    }

    // Insertar clients_dimension
    if (clientsDimension.length > 0) {
      await client.insert({
        table: 'clients_dimension',
        values: clientsDimension,
        format: 'JSONEachRow',
      });
      console.log(
        `✅ Insertados ${clientsDimension.length} registros en clients_dimension`,
      );
    }

    // Insertar contractor_info_raw
    if (contractorInfoRaw.length > 0) {
      await client.insert({
        table: 'contractor_info_raw',
        values: contractorInfoRaw,
        format: 'JSONEachRow',
      });
      console.log(
        `✅ Insertados ${contractorInfoRaw.length} registros en contractor_info_raw`,
      );
    }

    // Insertar sessions_raw
    if (sessionsRaw.length > 0) {
      await client.insert({
        table: 'sessions_raw',
        values: sessionsRaw,
        format: 'JSONEachRow',
      });
      console.log(
        `✅ Insertados ${sessionsRaw.length} registros en sessions_raw`,
      );
    }

    // Insertar agent_sessions_raw
    if (agentSessionsRaw.length > 0) {
      await client.insert({
        table: 'agent_sessions_raw',
        values: agentSessionsRaw,
        format: 'JSONEachRow',
      });
      console.log(
        `✅ Insertados ${agentSessionsRaw.length} registros en agent_sessions_raw`,
      );
    }

    // Insertar events_raw (en lotes para evitar problemas de memoria)
    const BATCH_SIZE = 1000;
    let insertedEvents = 0;
    for (let i = 0; i < eventsRaw.length; i += BATCH_SIZE) {
      const batch = eventsRaw.slice(i, i + BATCH_SIZE);
      await client.insert({
        table: 'events_raw',
        values: batch,
        format: 'JSONEachRow',
      });
      insertedEvents += batch.length;
      process.stdout.write(
        `\r   Insertando events_raw: ${insertedEvents}/${eventsRaw.length}...`,
      );
    }
    console.log(`\n✅ Insertados ${eventsRaw.length} registros en events_raw`);

    console.log('\n🎉 ¡Datos de prueba insertados exitosamente!\n');
    console.log('📝 Próximos pasos:');
    console.log('   1. Ejecuta el ETL para generar las tablas ADT');
    console.log(
      '   2. Consulta los endpoints ADT para verificar los resultados',
    );
    console.log(
      `   3. Verifica la productividad de los contratistas (debería estar entre los rangos esperados)\n`,
    );
  } catch (error) {
    console.error('\n❌ Error insertando datos:', error);
    process.exit(1);
  } finally {
    await client.close();
  }
}

// Ejecutar script
populateTestData().catch((error) => {
  console.error('❌ Error fatal:', error);
  process.exit(1);
});
