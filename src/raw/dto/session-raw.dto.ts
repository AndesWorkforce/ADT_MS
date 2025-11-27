export class SessionRawDto {
  session_id: string;
  contractor_id: string;
  session_start: Date;
  session_end: Date | null;
  total_duration: number | null;
  created_at: Date;
}

