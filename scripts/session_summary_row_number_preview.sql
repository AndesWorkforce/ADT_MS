-- Preview: row_number() por (session_id, contractor_id, agent_id) en session_summary.
-- rnk = 1 => fila conservada; rnk > 1 => duplicados ETL típicos.

SELECT
  s.session_id,
  s.contractor_id,
  ifNull(s.agent_id, '') AS agent_key,
  s.created_at,
  s.total_seconds,
  row_number() OVER (
    PARTITION BY s.session_id, s.contractor_id, ifNull(s.agent_id, '')
    ORDER BY
      s.created_at DESC,
      s.total_seconds DESC,
      ifNull(toString(s.session_end), '') DESC,
      ifNull(toString(s.session_start), '') DESC,
      ifNull(toString(s.idle_seconds), '') DESC,
      ifNull(toString(s.productivity_score), '') DESC,
      sipHash128(assumeNotNull(concat(ifNull(toString(s.session_end), ''),
                                    ifNull(toString(s.created_at), '')))) ASC
  ) AS rnk
FROM pulse_analytics.session_summary AS s;
