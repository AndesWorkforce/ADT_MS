/**
 * Script para poblar ClickHouse con datos de prueba
 *
 * Genera:
 * - 10 contratistas
 * - 5 días de datos (anteriores a hoy)
 * - ~8 horas de trabajo por día por contratista
 * - Eventos distribuidos en sesiones a lo largo del día
 * - 3 muy productivos (80-90%)
 * - 3 medianamente productivos (60-75%)
 * - 4 poco productivos (30-50%)
 */

import 'dotenv/config';
import { createClient } from '@clickhouse/client';
import * as crypto from 'crypto';

// Configuración desde variables de entorno
const CLICKHOUSE_HOST = process.env.CLICKHOUSE_HOST || 'localhost';
const CLICKHOUSE_PORT = parseInt(process.env.CLICKHOUSE_PORT || '8123');
const CLICKHOUSE_USERNAME = process.env.CLICKHOUSE_USERNAME || 'default';
const CLICKHOUSE_PASSWORD = process.env.CLICKHOUSE_PASSWORD || '';
const CLICKHOUSE_DATABASE = process.env.CLICKHOUSE_DATABASE || 'metrics_db';

// Configuración de datos de prueba
const NUM_DAYS = 10;
const WORK_HOURS_PER_DAY = 8;
const BEAT_INTERVAL_SECONDS = 15; // Cada heartbeat es de 15 segundos

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

// Calcular beats necesarios para ~8 horas
function calculateBeatsForWorkDay(workHours: number): number {
  const totalSeconds = workHours * 3600;
  return Math.floor(totalSeconds / BEAT_INTERVAL_SECONDS);
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
): string {
  // Progreso dentro de la sesión (0..1) y factor suave para variar la intensidad a lo largo de la sesión
  const progress = sessionBeats > 0 ? beatIndex / sessionBeats : 0;
  const phaseFactor = 0.9 + 0.2 * progress; // 0.9 → 1.1

  // Probabilidad de beat idle (sin inputs) por nivel de productividad
  // Alta: micro pausas bajas; Media: pausas moderadas; Baja: pausas frecuentes
  const idleProb =
    productivity === 'high' ? 0.12 : productivity === 'medium' ? 0.28 : 0.45;
  const isIdleBeat = Math.random() < idleProb;

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
    switch (productivity) {
      case 'high': // 80-90%
        // Rango más realista por beat de 15s
        keyboardInputs = 6 + Math.floor(Math.random() * 13); // 6-18 inputs
        mouseClicks = 2 + Math.floor(Math.random() * 5); // 2-6 clicks
        idleTime = Math.random() * 0.6; // 0-0.6 segundos idle
        // Más tiempo en apps productivas
        appUsage = generateAppUsage(true, false);
        browserUsage = generateBrowserUsage(true, false);
        break;

      case 'medium': // 60-75%
        keyboardInputs = 3 + Math.floor(Math.random() * 8); // 3-10 inputs
        mouseClicks = 1 + Math.floor(Math.random() * 4); // 1-4 clicks
        idleTime = 1.0 + Math.random() * 3.0; // 1.0-4.0 segundos idle
        // Mix de apps
        appUsage = generateAppUsage(false, false);
        browserUsage = generateBrowserUsage(false, false);
        break;

      case 'low': // 30-50%
        keyboardInputs =
          Math.random() < 0.6 ? 0 : 1 + Math.floor(Math.random() * 3); // 0-3 inputs
        mouseClicks =
          Math.random() < 0.7 ? 0 : 1 + Math.floor(Math.random() * 2); // 0-2 clicks
        idleTime = 4 + Math.random() * 8; // 4-12 segundos idle
        // Más tiempo en apps no productivas
        appUsage = generateAppUsage(false, true);
        browserUsage = generateBrowserUsage(false, true);
        break;
    }
  }

  // Variación suave por fase de sesión
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

// Generar contratistas
function generateContractors(): ContractorConfig[] {
  const contractors: ContractorConfig[] = [];

  // 3 muy productivos (80-90%)
  for (let i = 0; i < 3; i++) {
    contractors.push({
      contractor_id: generateId('contractor'),
      name: generateName(i),
      email: generateEmail(generateName(i), i),
      productivity: 'high',
      targetProductivity: { min: 80, max: 90 },
    });
  }

  // 3 medianamente productivos (60-75%)
  for (let i = 3; i < 6; i++) {
    contractors.push({
      contractor_id: generateId('contractor'),
      name: generateName(i),
      email: generateEmail(generateName(i), i),
      productivity: 'medium',
      targetProductivity: { min: 60, max: 75 },
    });
  }

  // 4 poco productivos (30-50%)
  for (let i = 6; i < 10; i++) {
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

  // Calcular beats por día
  const beatsPerDay = calculateBeatsForWorkDay(WORK_HOURS_PER_DAY);
  console.log(
    `⏱️  Beats por día: ${beatsPerDay} (~${WORK_HOURS_PER_DAY} horas)\n`,
  );

  // Generar fechas hasta el 5 de diciembre de 2025 (incluyendo ese día)
  // Genera NUM_DAYS días hacia atrás desde esa fecha
  const targetDate = new Date('2025-12-05T00:00:00.000Z');
  targetDate.setUTCHours(0, 0, 0, 0);
  const dates: Date[] = [];
  for (let i = 0; i < NUM_DAYS; i++) {
    const date = new Date(targetDate);
    date.setUTCDate(date.getUTCDate() - i);
    dates.push(date);
  }

  console.log(
    `📅 Generando datos desde ${dates[dates.length - 1].toISOString().split('T')[0]} hasta ${dates[0].toISOString().split('T')[0]}\n`,
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
      created_at: formatDateForClickHouse(new Date()),
      updated_at: formatDateForClickHouse(new Date()),
    });
  });

  // Generar datos para cada contratista
  for (const contractor of contractors) {
    const agentId = generateId('agent');
    // Asignar aleatoriamente un team y client del conjunto fijo
    const team = teams[Math.floor(Math.random() * teams.length)];
    const client = clients[Math.floor(Math.random() * clients.length)];
    const clientId = client.id;
    const teamId = team.id;

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
      client_id: clientId,
      team_id: teamId,
      created_at: formatDateForClickHouse(contractorCreatedAt),
      updated_at: formatDateForClickHouse(contractorCreatedAt),
    });

    let contractorEventsCount = 0;

    // Generar datos para cada día
    for (const day of dates) {
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

          contractorEventsCount++;

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

    console.log(
      `✅ ${contractor.name} (${contractor.productivity}): ${contractorEventsCount} eventos generados`,
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
