import { JobType } from './job-types.enum';

/**
 * Estructura base de datos para todos los trabajos
 */
export interface BaseJobData {
  jobType: JobType;
  requestedAt: Date;
  requestedBy?: string;
}

/**
 * Datos para trabajo de guardar evento
 * Se procesa en batch para mejor rendimiento
 */
export interface SaveEventJobData extends BaseJobData {
  jobType: JobType.SAVE_EVENT;
  event_id: string;
  contractor_id: string;
  agent_id: string | null;
  session_id: string | null;
  agent_session_id: string | null;
  timestamp: Date;
  payload: string;
  created_at: Date;
}

/**
 * Datos para trabajo de guardar sesión
 */
export interface SaveSessionJobData extends BaseJobData {
  jobType: JobType.SAVE_SESSION;
  session_id: string;
  contractor_id: string;
  session_start: Date;
  session_end: Date | null;
  total_duration: number | null;
  created_at: Date;
  updated_at: Date;
}

/**
 * Datos para trabajo de guardar agent session
 */
export interface SaveAgentSessionJobData extends BaseJobData {
  jobType: JobType.SAVE_AGENT_SESSION;
  agent_session_id: string;
  contractor_id: string;
  agent_id: string;
  session_id: string | null;
  session_start: Date;
  session_end: Date | null;
  total_duration: number | null;
  created_at: Date;
  updated_at: Date;
}

/**
 * Datos para trabajo de guardar contractor
 */
export interface SaveContractorJobData extends BaseJobData {
  jobType: JobType.SAVE_CONTRACTOR;
  contractor_id: string;
  name: string;
  email: string | null;
  team_id: string | null;
  client_id: string | null;
  created_at: Date;
  updated_at: Date;
}

/**
 * Datos para trabajos de ETL
 */
export interface EtlJobData extends BaseJobData {
  jobType:
    | JobType.EVENTS_TO_ACTIVITY
    | JobType.EVENTS_TO_ACTIVITY_FORCE
    | JobType.DAILY_METRICS
    | JobType.SESSION_SUMMARIES
    | JobType.FULL_ETL_ON_SESSION_CLOSE
    | JobType.APP_USAGE;
  fromDate?: Date;
  toDate?: Date;
  workday?: Date;
  sessionId?: string;
  contractorId?: string;
  contractorIds?: string[];
  force?: boolean;
}

/**
 * Union type de todos los tipos de datos de trabajos
 */
export type JobData =
  | SaveEventJobData
  | SaveSessionJobData
  | SaveAgentSessionJobData
  | SaveContractorJobData
  | EtlJobData;
