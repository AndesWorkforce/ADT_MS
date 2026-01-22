/**
 * Prioridades de trabajos en las colas
 *
 * Números más bajos = mayor prioridad (se procesan primero)
 */
export enum JobPriority {
  HIGH = 1, // Procesamiento en tiempo real o crítico
  NORMAL = 2, // Procesamiento normal (default)
  LOW = 3, // Backfills masivos, procesamiento en batch
}
