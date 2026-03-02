export class SessionSummaryDto {
  session_id: string;
  contractor_id: string;
  /** Identificador del agente (dispositivo). Una fila = una sesión de un agente. */
  agent_id?: string | null;

  session_start: Date;
  session_end: Date;

  total_seconds: number;
  active_seconds: number;
  idle_seconds: number;

  productivity_score: number;
}
