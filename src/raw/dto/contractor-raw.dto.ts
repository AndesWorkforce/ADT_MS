export class ContractorRawDto {
  contractor_id: string;
  name: string;
  email: string | null;
  job_position: string;
  work_schedule_start: string | null;
  work_schedule_end: string | null;
  country: string | null;
  client_id: string;
  team_id: string | null;
  created_at: Date;
  updated_at: Date;
}

