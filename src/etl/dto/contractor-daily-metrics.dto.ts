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
}
