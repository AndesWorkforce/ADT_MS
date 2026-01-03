export class ContractorDailyMetricsDto {
  contractor_id: string;
  workday: Date;

  total_beats: number;
  active_beats: number;
  idle_beats: number;
  active_percentage: number;

  total_keyboard_inputs: number;
  total_mouse_clicks: number;
  avg_keyboard_per_min: number;
  avg_mouse_per_min: number;

  total_session_time_seconds: number;
  effective_work_seconds: number;

  productivity_score: number;

  // Uso de aplicaciones: { "VSCode": 3600, "Chrome": 1200 } (segundos)
  app_usage?: Record<string, number>;

  // Uso de navegador por dominio: { "github.com": 1800, "stackoverflow.com": 900 } (segundos)
  browser_usage?: Record<string, number>;
}
