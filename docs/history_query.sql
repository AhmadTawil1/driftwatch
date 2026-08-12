-- Answers the question the plan's acceptance criteria names directly:
-- "what has this model cost me per task, and how has its quality
-- moved, over the last ninety days." One model, one query, grouped by
-- day so it plots directly as a time series.
--
-- Replace :model_id with a real models.id (e.g. 1 for gpt-4o-mini).

SELECT
    date_trunc('day', r.created_at) AS day,
    COUNT(*) AS calls,
    AVG(r.score) AS avg_score,
    SUM(r.cost_usd) AS total_cost_usd,
    AVG(r.latency_ms) AS avg_latency_ms
FROM results r
WHERE r.model_id = :model_id
  AND r.created_at >= NOW() - INTERVAL '90 days'
GROUP BY date_trunc('day', r.created_at)
ORDER BY day;
