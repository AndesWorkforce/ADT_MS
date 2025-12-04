export class DomainDimensionDto {
  domain: string;
  category: 'productive' | 'neutral' | 'non_productive';
  weight: number; // 0.0 - 2.0
  created_at?: Date;
}
