import { Injectable } from '@nestjs/common';

/**
 * Entradas para el cálculo del productivity_score (camino TS / realtime).
 */
export interface ProductivityScoreInputs {
  /** Beats considerados ACTIVOS (usando is_real_idle con grace period). */
  activeBeats: number;
  /** Total de beats del período. */
  totalBeats: number;
  /**
   * Segundos ponderados de "calidad": sum(seconds * weight) sobre apps + dominios
   * unificados en una sola bolsa. Si no hay datos de calidad, pasar 0 y
   * totalQualitySeconds = 0 → el score usa solo S_active.
   */
  weightedQualitySeconds: number;
  /** Segundos totales de "calidad" (denominador). */
  totalQualitySeconds: number;
}

/**
 * Expresiones SQL (ClickHouse) que representan cada término de la fórmula.
 * Cada campo es un fragmento SQL que evalúa a un número.
 */
export interface ProductivityScoreSqlExprs {
  /** Expresión que evalúa a active_beats (>= 0). */
  activeBeats: string;
  /** Expresión que evalúa a total_beats (>= 0). */
  totalBeats: string;
  /** Expresión que evalúa a weighted_quality_seconds. */
  weightedQualitySeconds: string;
  /** Expresión que evalúa a total_quality_seconds. */
  totalQualitySeconds: string;
}

/**
 * 🎯 FUENTE ÚNICA DE VERDAD del productivity_score.
 *
 * Fórmula multiplicativa (calidad escala la presencia):
 *
 *   productivity_score = S_active * S_quality / 100
 *
 *   S_active  = 100 * active_beats / total_beats
 *               (active_beats usa is_real_idle: grace period de 2 min +
 *                nunca idle durante llamadas — ver EtlService)
 *
 *   S_quality = 100 * sum(seconds * weight) / (W_MAX * sum(seconds))
 *               (apps + dominios unificados en una sola bolsa; el weight viene
 *                de las dimensiones — ver scripts/seed-dimensions.ts)
 *
 *               El divisor W_MAX es lo que mantiene S_quality en [0, 100].
 *               Los pesos son multiplicadores de productividad:
 *                   productiva      1.5
 *                   neutra          1.0
 *                   no productiva   0.5
 *               Sin normalizar, "todo neutro" ya daba S_quality = 100 y el
 *               score se clavaba en el techo: el tier productivo no premiaba
 *               nada porque el clamp se lo comia, y la unica palanca real era
 *               el castigo. Dividiendo por 1.5:
 *                   todo productivo    -> 100
 *                   todo neutro        ->  66.7
 *                   todo no productivo ->  33.3
 *
 *   Por qué no es aditivo (60/40): sumar presencia y calidad deja un piso
 *   ~73 si alguien está 100 % activo solo en apps no productivas. El producto
 *   interpreta "estuvo activo el Sa % del tiempo, y de ese tiempo la calidad
 *   fue Sq": YouTube con el mouse en movimiento ya no parece un 68.
 *
 *   Si NO hay datos de calidad (total_quality_seconds == 0):
 *       productivity_score = S_active   (sin defaults arbitrarios de 50)
 *
 *   Resultado acotado a [0, 100].
 *
 * IMPORTANTE: existen dos representaciones de esta fórmula que DEBEN mantenerse
 * en sync — `calculate()` (TS, para realtime/transformers) y `buildScoreSql()`
 * (genera el fragmento SQL para las agregaciones del ETL). Están adyacentes a
 * propósito: cualquier cambio de fórmula se hace acá, en un solo lugar.
 */
@Injectable()
export class ProductivityScoreService {
  // --- Escala de pesos de las dimensiones (apps_dimension / domains_dimension) ---
  // Son multiplicadores de productividad. Tienen que coincidir con los que
  // carga scripts/seed-dimensions.ts.
  /** Multiplica la productividad. */
  static readonly W_PRODUCTIVE = 1.5;
  /** No la altera. */
  static readonly W_NEUTRAL = 1.0;
  /** La reduce a la mitad. */
  static readonly W_NON_PRODUCTIVE = 0.5;

  /**
   * Divisor que normaliza S_quality a [0, 100]. Es el peso maximo posible.
   * Si se agrega un tier por encima de 1.5, hay que subirlo aca tambien o
   * S_quality vuelve a poder pasarse de 100.
   */
  static readonly W_MAX = ProductivityScoreService.W_PRODUCTIVE;

  /**
   * Peso para una app o dominio que NO esta en la dimension.
   *
   * Es NEUTRO a proposito: desconocido no es lo mismo que improductivo. Antes
   * el fallback era 0.5, que en la escala nueva significa "no productiva", asi
   * que cualquier app fuera del catalogo penalizaba el score como si fuera una
   * distraccion. Con 42 apps y 49 dominios cargados, muchas apps legitimas
   * siguen quedando afuera.
   */
  static readonly W_UNKNOWN = ProductivityScoreService.W_NEUTRAL;

  /**
   * Cálculo en TypeScript (camino realtime / transformers).
   */
  calculate(inputs: ProductivityScoreInputs): number {
    const {
      activeBeats,
      totalBeats,
      weightedQualitySeconds,
      totalQualitySeconds,
    } = inputs;

    const sActive = totalBeats > 0 ? 100 * (activeBeats / totalBeats) : 0;

    // Sin datos de calidad → score = S_active (sin regalar 50).
    if (totalQualitySeconds <= 0) {
      return this.clamp(sActive);
    }

    // Dividir por W_MAX acota S_quality a [0, 100]; ver docblock.
    const sQuality =
      100 *
      (weightedQualitySeconds /
        (ProductivityScoreService.W_MAX * totalQualitySeconds));

    return this.clamp((sActive * sQuality) / 100);
  }

  /**
   * Genera el fragmento SQL (ClickHouse) que calcula el productivity_score.
   * Usado por todas las agregaciones del ETL (daily metrics, session summary,
   * hourly productivity) para evitar copias divergentes de la fórmula.
   *
   * Devuelve una expresión que evalúa a Float64 en [0, 100].
   */
  buildScoreSql(exprs: ProductivityScoreSqlExprs): string {
    const sActive = `(100.0 * (${exprs.activeBeats}) / nullIf(${exprs.totalBeats}, 0))`;
    const wMax = ProductivityScoreService.W_MAX;
    // Idem calculate(): el divisor W_MAX acota S_quality a [0, 100].
    const sQuality = `(100.0 * (${exprs.weightedQualitySeconds}) / nullIf(${wMax} * (${exprs.totalQualitySeconds}), 0))`;

    // Si total_quality <= 0 → usar solo S_active; si no, S_active * S_quality / 100.
    return `
      least(100.0, greatest(0.0,
        if(
          (${exprs.totalQualitySeconds}) > 0,
          ifNull(${sActive}, 0.0) * ifNull(${sQuality}, 0.0) / 100.0,
          ifNull(${sActive}, 0.0)
        )
      ))
    `;
  }

  private clamp(value: number): number {
    if (!Number.isFinite(value)) {
      return 0;
    }
    return Math.min(100, Math.max(0, value));
  }
}
