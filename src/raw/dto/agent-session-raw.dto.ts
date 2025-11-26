export class AgentSessionRawDto {
  agent_session_id: string;
  contractor_id: string;
  agent_id: string;
  session_id: string | null;
  session_start: Date;
  session_end: Date | null;
  total_duration: number | null;
  created_at: Date;
}

