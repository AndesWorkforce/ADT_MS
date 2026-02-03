export type AppType =
  | 'Code'
  | 'Web'
  | 'Design'
  | 'Chat'
  | 'Office'
  | 'Productivity'
  | 'Development'
  | 'Database'
  | 'Cloud'
  | 'Entertainment'
  | 'System';

export class AppDimensionDto {
  id: string;
  name: string;
  category?: 'productive' | 'neutral' | 'non_productive' | null;
  type?: AppType | null;
  weight?: number | null; // 0.0 - 2.0 (1.0 = neutro, >1.0 = productivo, <1.0 = no productivo)
  created_at?: Date;
  updated_at?: Date;
}
