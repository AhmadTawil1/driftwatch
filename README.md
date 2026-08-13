# Model Drift & Cost Watchdog

A scheduled pipeline that re-benchmarks LLM providers on a fixed task
suite, scores every response, and raises an alert when quality moves
beyond what recent history's own variance would explain — so a silent
model swap or a quality regression is something you're told about,
not something a user finds first.

> **Status:** built and verified locally against real providers (OpenAI,
> Anthropic) and a real Postgres/Airflow stack — not deployed to a
> server or running on an unattended schedule. See
> [What this is not](#what-this-is-not).

---

## The problem

A hosted model is not a fixed component. Providers update the weights
behind a stable name, change default sampling behavior, and revise
pricing — none of which produces an error. The API keeps returning
`200`, the text keeps looking fluent, and the quality of what comes
back has moved. Teams rarely notice, because evaluation is usually
something done once before shipping, not a system that runs whether or
not anyone is watching. This project treats evaluation as
infrastructure: it runs on a schedule, keeps every raw record, and
tells you when something changed.

## How it works

```
  Airflow scheduler
        │  (manual trigger today; nightly schedule is a deploy-time decision)
        ▼
  [1] load_registry ──► active models, active suite version, idempotency check
        │
        ▼  (dynamic fan-out — one task group per model)
  [2] execute ──► async provider calls, bounded concurrency,
        │          retries, spend accounting, cost-cap enforcement
        ▼
  [3] score ──► deterministic scorers first, then graded (pinned grader)
        │
        ▼
  [4] detect_drift ──► rolling 14-night baseline, z-score normalized
        │
        ▼
  [5] notify ──► Slack webhook, only for fired-and-unnotified checks
```

Nothing downstream of stage 2 ever re-calls a provider. Once a result
is written, scoring, re-scoring, and backfilling are pure functions
over stored data — a scoring bug costs a re-run of stage 3, not
another night's spend.

## Real numbers

These come from actually running the pipeline against OpenAI and
Anthropic, not from projecting what it should do.

| Measurement | Result |
|---|---|
| Full nightly-style run (4 models × 40 tasks × 3 repeats) | **480/480 results written, 0 failures**, run marked `completed` |
| Total cost for that run | **$0.1134** |
| Graded scoring (6 rubric-based tasks, 4 models) | **72 rows, $0.0305**, tracked separately from deterministic cost |
| Drift detector false-positive rate (100 simulated noise nights) | **3.23%** (target: under 5%) |
| Drift detector latency (injected 15% quality drop) | **fires within 2 nights**, correct direction |
| Cost cap, tested end to end | deliberately capped at $0.0005 → **aborted cleanly at 5/480 rows**, all rows intact |
| `results` query buffer reads, before/after indexing | **2,069 → 424** (~5x reduction), ~18-53ms → ~8ms |
| Test suite | **45 tests passing** (unit, mock-server integration, live-DB statistical validation) |
| Real provider version strings captured | e.g. `gpt-4o-mini-2024-07-18`, `claude-sonnet-4-5-20250929` — the exact field this project exists to watch |

## Tech stack

| Layer | Choice | Why |
|---|---|---|
| Orchestration | Apache Airflow 3.x, `LocalExecutor` | Dynamic task mapping, retries, dependency graphs — the named skill this project exists to demonstrate |
| Language | Python 3.12 | |
| Concurrency | `asyncio` + `httpx.AsyncClient` | Provider calls are IO-bound; a semaphore per provider caps concurrency |
| Database | PostgreSQL 16 | Results store and Airflow's own metadata, deliberately separate databases in one instance |
| Access | SQLAlchemy 2.x + Alembic | Typed models, versioned migrations |
| Packaging | uv | Lockfile committed, environment reproducible |
| Containers | Docker + Compose | Scheduler, dag-processor, api-server, Postgres, one command |
| Tests | pytest + pytest-asyncio | Including a real local mock provider server and live-database statistical tests |

## Database schema

Nine tables: `providers`, `models`, `model_prices` (versioned by
`effective_from`), `suites` and `tasks` (versioned, immutable
snapshots), `runs`, `results` (one row per model × task × repeat, with
full provenance — suite version, prompt hash, provider-returned model
version, pipeline git SHA), `drift_checks` (one row per comparison,
fired or not — recording the non-events is what makes a false-positive
rate measurable), and `alerts`.

Money is stored as `Numeric`, never `float`. No Postgres `ENUM` types —
plain `String` for things like `runs.status`, so a new status value is
a code change, not a migration. See [Index decisions](#index-decisions)
for the one schema decision that got a real before/after measurement
rather than a guess.

## Getting started

```bash
git clone <this repo>
cd driftwatch
cp .env.example .env   # fill in real values — see below

uv sync
docker compose up --build -d

uv run alembic upgrade head
uv run python seed.py
uv run python suite_loader.py suites/v3.yaml   # latest suite version

# Airflow UI: http://localhost:8080 (airflow / airflow, dev only)
# Trigger manually via the UI, or:
docker compose exec airflow-scheduler airflow dags trigger nightly_pipeline
```

`.env` needs real values for `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`,
`SLACK_WEBHOOK_URL` (optional — alerting no-ops without it),
`NIGHTLY_COST_CAP_USD`, and `WATCHDOG_DATABASE_URL` (host-side; the
containers get their own via `docker-compose.yml`, since "localhost"
means something different inside a container).

## Testing

Three layers:

1. **Unit** — scorers, cost math, backoff timing. No network, no
   database.
2. **Integration against a real local mock provider** — a genuine
   `http.server.ThreadingHTTPServer` on a random port (not a mocked
   transport object), proving retry and timeout handling against real
   TCP rather than a hand-built response.
3. **Statistical validation against a live database** — the drift
   detector's tests generate synthetic history (an injected step drop,
   100 nights of pure noise) directly in Postgres and measure the
   actual false-positive rate and detection latency, rather than
   asserting a number that was never checked.

```bash
uv run pytest tests/
```

## Index decisions

The 90-day cost-and-quality history query (`docs/history_query.sql`)
filters `results` by one `model_id` and a `created_at` range, grouped
by day. Measured with `EXPLAIN (ANALYZE, BUFFERS)` against a realistic
volume — 150 nights, ~72,500 rows, generated specifically for this
measurement since the real table alone was too small to show any
difference — before adding any index (`docs/explain-before.txt`), the
planner chose a **sequential scan**: it read every row in the table,
2,069 buffer hits, to filter down to the roughly 10,800 rows that
actually matched, in 18-53ms depending on cache state. This wasn't a
planner mistake — at that table size, scanning everything really was
cheaper than the alternative it didn't have available. But it means
the query's cost scales with the *total* size of `results` forever,
not with the size of the answer: the same 90-day window returns
roughly the same number of rows every night, yet would get measurably
slower every night regardless, purely because the table keeps growing
underneath it.

Adding `results (model_id, created_at DESC)` gives the planner an
index whose shape matches the query's own access pattern — filter by
one model, then a range on time — rather than needing to inspect every
row to find out which ones qualify. Re-measured after
(`docs/explain-after.txt`), the plan switched to a **Bitmap Index
Scan**: read the matching row locations out of the index first (cost
153, a fraction of the table), then fetch only those specific table
pages, instead of reading every page unconditionally. Buffer reads
dropped from 2,069 to 424 — about a 5x reduction — and execution time
from ~18-53ms to ~8ms. The second index, `results (run_id, model_id)`,
doesn't appear in this particular plan; it exists for the scoring
stage's own query pattern (unscored rows for one run, joined to
`models` for grader attribution). Same underlying idea for both: an
index doesn't make Postgres faster in the abstract, it hands the
planner a shortcut whose shape matches how the application actually
asks its questions — guessed at first, then confirmed by looking at
what `EXPLAIN` actually says, not by assuming.

## What this is not

- **Not deployed.** Everything above is real — real API calls, real
  Postgres, real measured numbers — but it runs on a development
  machine, triggered manually, not on a server on an unattended
  schedule. There is no "three consecutive nights" uptime claim here,
  because there's no scheduled deployment for those nights to happen
  on.
- **Not real-time.** The design is nightly batch, by intent — a
  regression introduced this morning is caught on the next run, not
  the moment it happens.
- **Not a benchmark.** The 40-task suite is hand-written for this
  project. It measures what it measures; agreement with any public
  benchmark is not claimed.
- **Graded scoring inherits the grader's own instability.** The
  grader is pinned and is itself evaluated by the suite like any other
  model, so a grader-wide shift is visible — but that's mitigation,
  not a guarantee of grader correctness.
- **Temperature 0 is not determinism.** It's an assumption, which is
  exactly why each task runs 3 times rather than once.
- **Costs are computed, not invoiced.** Prices are entered by hand
  from each provider's published rates; the numbers above approximate
  the real bill, they are not a reconciled invoice.

## Repository layout

```
src/watchdog/
  db/         SQLAlchemy models, session/engine setup
  providers/  Async HTTP client, retry/backoff, cost math
  scoring/    Deterministic scorers + graded (pinned-grader) scoring
  drift/      Rolling-window z-score detector
  notify/     Slack webhook alerting
  dags/       nightly_pipeline (5-stage) and backfill (separate DAG)
tests/        Unit, mock-server integration, live-DB statistical tests
suites/       Versioned YAML task suites (v1-v3, each a full snapshot)
docs/         The 90-day query + committed EXPLAIN before/after output
scripts/      One-off verification scripts (first real call, test alert,
              synthetic load generation for the index measurement)
```
