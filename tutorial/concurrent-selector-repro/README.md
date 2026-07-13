# Concurrent-selector staleness repro

Minimal dbt project that reproduces a downstream model going **permanently stale**
when two pipelines with different selectors run concurrently.

## The DAG

```text
source raw.raw_events ──▶ model_a ──▶ model_b
```

- `model_a` is fed by a source (`source('raw', 'raw_events')`).
- `model_b` depends only on `model_a`.
- **No `build_after` / freshness SLA anywhere.** This matters: the bug only affects
  nodes *without* an SLA. The SLA code path already performs the "upstream ran more
  recently than me" catch-up; the default (no-SLA) path did not.

Two dbt selectors in [`selectors.yml`](selectors.yml):

- **`selector_x`** — narrow pipeline: `model_a` only.
- **`selector_y`** — full pipeline: `model_a+` (A and B).

## The bug

Downstream `model_b` never picks up `model_a`'s newer output after a narrow pipeline
(`selector_x`) refreshes `model_a` on its own. `model_b` stays stale on every
subsequent `selector_y` run, because from `model_b`'s point of view `model_a` is
"clean" (its source has no new data) and, with no SLA, that used to mean "reuse".

Two independent code paths contribute; both are fixed:

1. **Freshness catch-up (`sao.py`).** A clean upstream now still forces a rebuild
   when it ran more recently than the current node (`upstream.last_updated >
   current.last_updated`). Previously this comparison only ran when the node had a
   `build_after` SLA, so no-SLA nodes never caught up. This is the fix that makes
   step 4 rebuild `model_b`.
2. **Concurrent state clobbering (`state.py` `save_updated_state`).** Each run now
   re-reads the latest stored state at save time and writes back only the nodes it
   actually ran, so the narrow pipeline can't revert the full pipeline's write to
   `model_b`. Without this, the two runs' last-writer-wins saves corrupt state in
   other ways even though it isn't the direct cause of this symptom.

Residual limitation: `save_updated_state` still has a small time-of-check/
time-of-use window between its re-read and its write (no conditional/atomic write).
Fully closing it would need ETag/`If-Match` writes (S3/GCS/Azure), a file lock
(local), or server-side key-level merge of just the delta (HTTP).

## Wiring two Orchestra pipelines

Point both pipelines at this project.

- **Pipeline Y** runs: `orc dbt build --selector selector_y`
- **Pipeline X** runs: `orc dbt build --selector selector_x`

Both share the same state backend (the Orchestra HTTP backend for the same
integration account, or the same `ORCHESTRA_STATE_FILE`).

### Reproduction sequence

1. **New source data lands** in `raw.raw_events`.
2. **Run Pipeline Y.** New source → `model_a` runs → `model_b` runs. Both advance.
   State now has `A.last_updated < B.last_updated` (normal: B finishes after A).
3. **While Y is still running, start Pipeline X.** New source → `model_a` runs
   again, alone. X finishes *after* Y. State now has
   **`A.last_updated > B.last_updated`** — A was advanced on its own.
4. **Later, run Pipeline Y again with the source unchanged since step 3.**
   - Expected: `model_a` is reused (source unchanged), **`model_b` rebuilds** to
     catch up to A's step-3 output.
   - Buggy behaviour (before the fix): `model_a` reused **and `model_b` also reused**
     — B stays stale forever.

The physical staleness is observable: `model_b` will be missing whatever `model_a`
produced in step 3 until something else forces B dirty (a checksum change, or an
SLA firing).

## Verifying locally without Orchestra

You do not need two live pipelines to prove the state logic — you can hand-craft the
step-3 state and run a single `selector_y` build. See the unit regression test
`test_clean_upstream_ran_more_recently_forces_catch_up_without_sla` in
[`tests/unit/test_sao.py`](../../tests/unit/test_sao.py), which encodes exactly the
step-4 state (`A.last_updated > B.last_updated`, A clean, B no SLA) and asserts B is
rebuilt.

### Local Postgres run (optional)

```bash
docker run -d --rm -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres \
  -e POSTGRES_DB=repro -p 5432:5432 --name repro-postgres postgres:18

cd tutorial/concurrent-selector-repro
export PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGPASSWORD=postgres \
  PGDATABASE=repro DBT_SCHEMA=csr DBT_PROFILES_DIR="$(pwd)"
mkdir -p .orchestra && echo '{"state":{}}' > .orchestra/dbt_state.json

orc dbt seed                              # land the source table
orc dbt build --selector selector_y       # step 2: A and B build
orc dbt build --selector selector_x       # step 3: A rebuilds alone
orc dbt build --selector selector_y       # step 4: expect ONLY B to rebuild
```

To force "new source data" between runs, update `event_at` values in
`seeds/raw_events.csv` (or the underlying table) so source freshness advances.
