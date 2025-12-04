export class AppDimensionDto {
  app_name: string;
  category: 'productive' | 'neutral' | 'non_productive';
  weight: number; // 0.0 - 2.0 (1.0 = neutro, >1.0 = productivo, <1.0 = no productivo)
  created_at?: Date;
}
