-- =============================================================================
-- Reproceso de `session_summary` y depuración de filas duplicadas
-- =============================================================================
-- Alineado con: ADT_MS src/etl/services/etl.service.ts
--   EtlService.reprocessSessionSummariesForDateRange(fromDay, toDay)
--
-- Valores configurados (editá aquí si necesitás otro rango / zona):
--   FROM_YMD:    2026-04-27
--   TO_YMD:      2026-04-28  (inclusive)
--   OP_TIMEZONE: America/New_York (alinear con EVENTS_TIMEZONE en ADT_MS)
--
-- Si las tablas no están en `default`, anteponé la base: `mi_db.session_summary`, etc.
--
-- ClickHouse: ALTER DELETE es mutación asíncrona. Tras §3, revisá §5; cuando is_done=1,
-- ejecutá §4. Opcional al final: OPTIMIZE TABLE session_summary FINAL;
--
-- =============================================================================
-- §1 Diagnóstico: duplicados por (session_id, agent_id) en el rango
-- =============================================================================

SELECT
    session_id,
    coalesce(agent_id, '') AS agent_key,
    count() AS row_cnt,
    groupArray(created_at) AS created_at_values
FROM session_summary
WHERE toDate(session_start, 'America/New_York') >= toDate('2026-04-27')
  AND toDate(session_start, 'America/New_York') <= toDate('2026-04-28')
GROUP BY session_id, agent_key
HAVING row_cnt > 1
ORDER BY row_cnt DESC, session_id
LIMIT 500;

-- =============================================================================
-- §2 Conteo antes (opcional)
-- =============================================================================

SELECT count() AS rows_before
FROM session_summary
WHERE toDate(session_start, 'America/New_York') >= toDate('2026-04-27')
  AND toDate(session_start, 'America/New_York') <= toDate('2026-04-28');

-- =============================================================================
-- §3 Borrar resúmenes del rango (mutación asíncrona — no mezclar con §4 hasta §5 ok)
-- =============================================================================

ALTER TABLE session_summary
DELETE WHERE
    toDate(session_start, 'America/New_York') >= toDate('2026-04-27')
    AND toDate(session_start, 'America/New_York') <= toDate('2026-04-28');

-- =============================================================================
-- §4 Reinsertar desde contractor_activity_15s (misma lógica que el ETL en código)
-- Ejecutar cuando las mutaciones de §3 estén completas (ver §5).
-- =============================================================================

INSERT INTO session_summary (
    session_id,
    contractor_id,
    agent_id,
    session_start,
    session_end,
    total_seconds,
    active_seconds,
    idle_seconds,
    productivity_score,
    created_at
)
SELECT
    a.session_id,
    any(a.contractor_id) AS contractor_id,
    any(a.agent_id) AS agent_id,
    min(a.beat_timestamp) AS session_start,
    max(a.beat_timestamp) AS session_end,
    count() * 15 AS total_seconds,
    sum(if(a.is_idle = 0, 15, 0)) AS active_seconds,
    sum(if(a.is_idle = 1, 15, 0)) AS idle_seconds,
    least(
        100.0,
        greatest(
            0.0,
            0.35 * (
                100.0 * sum(if(a.is_idle = 0, 1, 0)) / nullIf(count(), 0)
            )
            + 0.20 * least(
                100.0,
                15.0 * ln(
                    1 + (
                        (
                            (sum(a.keyboard_count) + sum(a.mouse_clicks))
                            / nullIf(count() * 15 / 60, 0)
                        ) / 2.0
                    )
                )
            )
            + 0.30 * ifNull(
                100.0 * greatest(
                    0.0,
                    least(
                        1.0,
                        (
                            (any(app.weighted_seconds) / nullIf(any(app.app_total_seconds), 0))
                            - 0.2
                        ) / 0.8
                    )
                ),
                50.0
            )
            + 0.15 * ifNull(
                100.0 * greatest(
                    0.0,
                    least(
                        1.0,
                        (
                            (any(web.weighted_seconds) / nullIf(any(web.web_total_seconds), 0))
                            - 0.2
                        ) / 0.8
                    )
                ),
                50.0
            )
        )
    ) AS productivity_score,
    now() AS created_at
FROM contractor_activity_15s AS a
LEFT JOIN (
    SELECT
        e.session_id,
        e.agent_id,
        sum(JSONExtractFloat(e.payload, 'AppUsage', app) * ifNull(d.weight, 0.5)) AS weighted_seconds,
        sum(JSONExtractFloat(e.payload, 'AppUsage', app)) AS app_total_seconds
    FROM events_raw AS e
    ARRAY JOIN JSONExtractKeys(e.payload, 'AppUsage') AS app
    LEFT JOIN apps_dimension AS d ON d.name = app
    GROUP BY e.session_id, e.agent_id
) AS app
    ON app.session_id = a.session_id
    AND coalesce(app.agent_id, '') = coalesce(a.agent_id, '')
LEFT JOIN (
    SELECT
        e.session_id,
        e.agent_id,
        sum(
            JSONExtractFloat(e.payload, 'browser', dc) * ifNull(d.weight, 1)
        ) AS weighted_seconds,
        sum(JSONExtractFloat(e.payload, 'browser', dc)) AS web_total_seconds
    FROM events_raw AS e
    ARRAY JOIN JSONExtractKeys(e.payload, 'browser') AS dc
    LEFT JOIN domains_dimension AS d ON d.domain = dc
    GROUP BY e.session_id, e.agent_id
) AS web
    ON web.session_id = a.session_id
    AND coalesce(web.agent_id, '') = coalesce(a.agent_id, '')
WHERE a.session_id IS NOT NULL
  AND toDate(a.beat_timestamp, 'America/New_York') >= toDate('2026-04-27')
  AND toDate(a.beat_timestamp, 'America/New_York') <= toDate('2026-04-28')
GROUP BY a.session_id, a.agent_id
SETTINGS max_partitions_per_insert_block = 1000;

-- =============================================================================
-- §5 Estado de mutaciones sobre session_summary
-- =============================================================================

SELECT
    database,
    table,
    mutation_id,
    command,
    create_time,
    parts_to_do,
    is_done
FROM system.mutations
WHERE table = 'session_summary'
ORDER BY create_time DESC
LIMIT 20;

-- =============================================================================
-- §6 Conteo después y comprobación de duplicados
-- =============================================================================

SELECT count() AS rows_after
FROM session_summary
WHERE toDate(session_start, 'America/New_York') >= toDate('2026-04-27')
  AND toDate(session_start, 'America/New_York') <= toDate('2026-04-28');

SELECT
    session_id,
    coalesce(agent_id, '') AS agent_key,
    count() AS row_cnt
FROM session_summary
WHERE toDate(session_start, 'America/New_York') >= toDate('2026-04-27')
  AND toDate(session_start, 'America/New_York') <= toDate('2026-04-28')
GROUP BY session_id, agent_key
HAVING row_cnt > 1;

-- =============================================================================
-- §7 (Opcional) Forzar fusión — puede ser pesado
-- =============================================================================
-- OPTIMIZE TABLE session_summary FINAL;
