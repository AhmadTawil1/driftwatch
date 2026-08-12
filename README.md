# Model Drift & Cost Watchdog

*The full README (architecture, setup, real numbers, "what this is
not") gets written on day 7, once three unattended nights have run. This
section is written early because the plan calls for it specifically —
the index decision needs to be documented at the moment it's measured,
not reconstructed from memory later.*

## Index decisions

The 90-day cost-and-quality history query (`docs/history_query.sql`)
filters `results` by one `model_id` and a `created_at` range, grouped
by day. Measured with `EXPLAIN (ANALYZE, BUFFERS)` against a realistic
volume — 150 nights, ~72,500 rows — before adding any index
(`docs/explain-before.txt`), the planner chose a **sequential scan**:
it read every row in the table, 2,069 buffer hits, to filter down to
the roughly 10,800 rows that actually matched, in 18-53ms depending on
cache state. This wasn't a planner mistake — at that table size,
scanning everything really was cheaper than the alternative it didn't
have available. But it means the query's cost scales with the *total*
size of `results` forever, not with the size of the answer: the same
90-day window returns roughly the same number of rows every night, yet
would get measurably slower every night regardless, purely because the
table keeps growing underneath it.

Adding `results (model_id, created_at DESC)` gives the planner an
index whose shape matches the query's own access pattern — filter by
one model, then a range on time — rather than needing to inspect every
row to find out which ones qualify. Re-measured after
(`docs/explain-after.txt`), the plan switched to a **Bitmap Index
Scan**: read the matching row locations out of the index first (cost
153, a fraction of the table), then fetch only those specific table
pages (the accompanying Bitmap Heap Scan), instead of reading every
page unconditionally. Buffer reads dropped from 2,069 to 424 — about a
5x reduction — and execution time from ~18-53ms to ~8ms. The second
index, `results (run_id, model_id)`, doesn't appear in this particular
plan; it exists for the scoring stage's own query pattern (unscored
rows for one run, joined to `models` for grader attribution), not the
history query. Same underlying idea for both: an index doesn't make
Postgres faster in the abstract, it hands the planner a shortcut whose
shape matches how the application actually asks its questions —
guessed at first, then confirmed or corrected by looking at what
`EXPLAIN` actually says, not by assuming.
