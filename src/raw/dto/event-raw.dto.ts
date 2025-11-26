export class EventRawDto {
  event_id: string;
  contractor_id: string;
  agent_id: string | null;
  session_id: string | null;
  agent_session_id: string | null;
  timestamp: Date;
  payload: string; // JSON string
  created_at: Date;
}

