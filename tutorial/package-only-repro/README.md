# package-only-repro

A dbt project whose **root project owns no models at all** -- every model, and
every source, belongs to the installed `analytics_pkg` package (vendored under
`vendor/` so `dbt deps` needs no network).

This is the shape that broke source-freshness scoping.

## Why it matters

`dbt ls --output path` reports each node's `original_file_path`, which is
relative to **the package that owns the node**:

```
models/staging/stg_orders.sql          <- what dbt ls reports
dbt_packages/analytics_pkg/models/staging/stg_orders.sql   <- where the file is
```

dbt's `PathSelectorMethod` resolves `path:` criteria by **globbing the real
filesystem** from the project root:

```python
paths = set(p.relative_to(root) for p in root.glob(selector))
```

So `--select +path:models/staging/stg_orders.sql` globs a file that does not
exist at the root, matches nothing, and selects **zero** nodes. dbt treats an
empty selection as "Nothing to do" -- a *warning*, suppressed by the `-q` that
`orchestra_dbt` passes -- and still writes a valid, empty `sources.json`
(`"results": []`, `"elapsed_time": 0.0`).

The visible symptom was `Collected 0 source(s) information.` with no error
anywhere, on a real project where all 438 models lived in packages.

Selecting by dotted fqn (`analytics_pkg.staging.stg_orders`) instead reads from
the manifest, is package-qualified, and resolves for root- and package-owned
nodes alike. That is what `dbt ls --output selector` emits, and what
`get_args_for_source_freshness` now builds.

See `tests/integration/test_package_only_selection.py`.
