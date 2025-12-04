export class AppUsageSummaryDto {
  contractor_id: string;
  app_name: string;
  workday: Date;

  // beats activos aproximados para esta app en el día
  active_beats: number;
}
