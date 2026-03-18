export interface EventCreatedPayload {
  id: string;
  contractor_id: string;
  agent_id?: string | null;
  session_id?: string | null;
  agent_session_id?: string | null;
  timestamp?: string | Date;
  payload?: unknown;
  created_at?: string | Date;
}

export interface ContractorPayload {
  id: string;
  name: string;
  email?: string | null;
  job_position: string;
  work_schedule_start?: string | null;
  work_schedule_end?: string | null;
  country?: string | null;
  client_id: string;
  team_id?: string | null;
  isActive?: boolean;
  created_at?: string | Date;
  updated_at?: string | Date;
}

export interface SessionPayload {
  id: string;
  contractor_id: string;
  session_start: string | Date;
  session_end?: string | Date | null;
  total_duration?: number | null;
  created_at?: string | Date;
  updated_at?: string | Date;
}

export interface AgentSessionPayload {
  id: string;
  contractor_id: string;
  agent_id: string;
  session_id?: string | null;
  session_start: string | Date;
  session_end?: string | Date | null;
  total_duration?: number | null;
  created_at?: string | Date;
  updated_at?: string | Date;
}

export interface TeamPayload {
  id: string;
  name: string;
}

export interface ClientPayload {
  id: string;
  name: string;
  isActive?: boolean;
}
