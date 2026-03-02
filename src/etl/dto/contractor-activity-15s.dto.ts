export class ContractorActivity15sDto {
  contractor_id: string;
  agent_id: string | null;
  session_id: string | null;
  agent_session_id: string | null;

  // Heartbeat
  beat_timestamp: Date; // timestamp real del heartbeat (intervalo de 15s)

  // Métricas de actividad
  keyboard_count: number;
  mouse_clicks: number;
  is_idle: boolean;
}
