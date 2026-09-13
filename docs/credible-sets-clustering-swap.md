# Runbook: re-clustering `credible_sets` on `data_type, resource, variant, pos`

Status: **prepared, not executed.** The schema files, loader and view in this repo already
describe the target state. This document is the procedure for making the deployed BigQuery
table match them. Follow it top to bottom; it assumes no prior knowledge of the analysis.

---

## 0. Read this first: do not run `scripts/setup_bigquery.sh` at all until §4.6 is done

> ### 🚨 `setup_bigquery.sh` IS BROKEN FOR AS LONG AS THIS CHANGE IS PREPARED-BUT-NOT-EXECUTED
>
> This is not about `--recreate`. **A plain, flagless `./scripts/setup_bigquery.sh` now
> fails**, from the moment the committed `schemas/credible_sets_v.sql` landed in the working
> tree until §4.6 completes.
>
> The committed view body says `SELECT * EXCEPT(variant, resource)`, but the deployed
> `credible_sets` table does not have those columns yet. Against the live table it errors:
>
> ```
> Error in query string: Column variant in SELECT * EXCEPT list does not exist at [18:12]
> ```
>
> The script runs its view pass unconditionally (`setup_bigquery.sh:146-155`) with no flag
> to skip it, and it is under `set -euo pipefail`. The loop is `for view_file in
> "${SCHEMA_DIR}"/*_v.sql`, i.e. **alphabetical**, so the pipeline aborts at
> `credible_sets_v` and the **eleven view files that sort after it are silently never
> applied**:
>
> `datasets_v`, `exome_variant_results_v`, `gene_annotations_v`, `gene_burden_results_v`,
> `hla_associations_v`, `mpra_v`, `open_chromatin_v`, `peak_to_gene_v`, `phenotypes_v`,
> `variant_annotation_v`, `variant_effect_v`.
>
> They are not dropped — they keep whatever body they already had — so this is a *silent
> partial apply*, not an outage. If someone edited one of those eleven view files and ran
> the script, they will believe their edit is deployed when it is not.
>
> **No data is lost.** A failed `CREATE OR REPLACE VIEW` leaves the old view intact, and
> `credible_sets_v` keeps working against the old table throughout.
>
> But this is precisely the "something looks off, I'll just re-run `setup_bigquery.sh`"
> moment. Do not. Until §4.6 is complete:
>
> - **do not run `./scripts/setup_bigquery.sh`, with or without flags**;
> - if you need a table or view created in this window, run that one `schemas/*.sql` by
>   hand with the same `sed`-and-`bq query` incantation the script uses;
> - README, `../genetics-results-suite/docs/project-spec.md:761` and
>   `scripts/load_genebass_gene.sh:34` all still instruct people to run the script. Warn
>   anyone who might follow those instructions during the window.
>
> After §4.6 the committed view SQL and the deployed table agree again and the script is
> safe (modulo `--recreate`, which is never safe — see below).

`setup_bigquery.sh` iterates every `schemas/*.sql` that is not `*_v.sql` and creates a
table from it. Its behaviour towards a table that **already exists** is the single most
important fact in this runbook, and it is dangerous in both directions:

| invocation | what happens to the existing 115M-row `credible_sets` |
|---|---|
| `./scripts/setup_bigquery.sh` | The DDL runs as `CREATE TABLE IF NOT EXISTS`. The table is **left exactly as it is**. Your edited schema file is **silently ignored** — no error, no warning, no diff. The swap does not happen and the script prints "Creating table credible_sets..." while creating nothing. |
| `./scripts/setup_bigquery.sh --recreate` | Every table in the dataset is `bq rm -f -t`'d first. **THIS DESTROYS ALL 115 MILLION ROWS OF `credible_sets`, AND EVERY OTHER TABLE IN THE DATASET TOO** — `colocalization`, `coloc_credsets`, `variant_annotation`, all of them. It is not scoped to the table whose schema you changed. Recovery beyond BigQuery's 7-day time-travel window is impossible; several of these tables take hours to reload from GCS. |

**Do not run `setup_bigquery.sh --recreate` as part of this swap. Ever.** There is no
flag combination of that script that performs this change safely. Clustering and
partitioning cannot be altered in place by any `ALTER TABLE` either — the only way to
change them is to write the rows into a new table. That is what this runbook does.

The script now prints a red-alert banner, lists the row counts it is about to destroy and
demands you type the dataset name before proceeding. Treat that prompt as a stop sign, not
a speed bump.

---

## 1. Why the change

`credible_sets_v` derived both `variant` and `resource` in the view, while the base table
clustered on `dataset, data_type, gene_most_severe, most_severe`. Every caller — the API,
the MCP tools, the `datasets.yaml` example queries — is told to filter on `resource` and
`variant`. Neither pruned anything, because a view-derived expression cannot be a
clustering key.

It was worse than merely useless. Measured on the pre-swap view, `WHERE resource =
'finngen'` scanned **more** than the same query with no filter at all (4,176,706,833 B vs
2,520,802,636 B), because evaluating the `CASE` forced an extra read of `dataset`.

Three candidate clusterings were built as experiment tables and benchmarked with real
execution against the 10 commonest logged query shapes. The chosen one, `data_type,
resource, variant, pos`, took the weighted total from **333.14 GB to 43.45 GB (−87.0%)**.
That aggregate is unreproducible and is contradicted by an independent re-measurement; see
§6.4 before relying on it.

Accepted, already-approved costs:

| filter | change | share of logged queries |
|---|---|---|
| `dataset =` | +267% | 3.5% |
| `gene_most_severe =` | +44% | 6.1% |
| `most_severe =` | +8% | 4.8% |
| logical storage | +13.5% | — |

---

## 2. What changes in this repo (already committed to the working tree)

- **`schemas/credible_sets.sql`** — `resource` and `variant` become real stored `NOT NULL`
  columns; `CLUSTER BY data_type, resource, variant, pos`; partitioning unchanged
  (`RANGE_BUCKET(chr, GENERATE_ARRAY(1, 23, 1))`).
- **`schemas/credible_sets_v.sql`** — stops deriving `variant` and `resource`. `maf` stays
  derived. The view's **output schema is unchanged** (see §6).
- **`scripts/load_data.py`** — new `DERIVED_COLUMNS` mechanism computes `variant` and
  `resource` on the existing staging-table projection, so **the source TSVs do not
  change**. The `resource` `CASE` is generated at load time from `datasets.yaml` via
  `scripts/generate_resource_sql.py`, so the mapping still has one source of truth.
- **`scripts/generate_resource_sql.py`** — `credible_sets_v` becomes
  `resource_derivation.mode: load_time` in `datasets.yaml`; lint now asserts the view has
  *no* `CASE`.
- **`scripts/setup_bigquery.sh`** — documentation and a confirmation gate on `--recreate`.

> ### 🚨 Consequence: `setup_bigquery.sh` fails until §4.6 completes
>
> Because `schemas/credible_sets_v.sql` is committed *ahead* of the table it describes, the
> script's view pass (`setup_bigquery.sh:146-155`, unconditional, no skip flag, under `set
> -euo pipefail`) aborts on `credible_sets_v` with
> `Column variant in SELECT * EXCEPT list does not exist`, and the eleven alphabetically
> later views — `datasets_v`, `exome_variant_results_v`, `gene_annotations_v`,
> `gene_burden_results_v`, `hla_associations_v`, `mpra_v`, `open_chromatin_v`,
> `peak_to_gene_v`, `phenotypes_v`, `variant_annotation_v`, `variant_effect_v` — are never
> applied. Nothing is destroyed; the failure is a silent partial apply.
>
> **Do not run `setup_bigquery.sh` at all between this change landing in the tree and §4.6
> completing.** Apply individual `schemas/*.sql` by hand if you need to. See §0.

### The one behavioural regression, and it is permanent

`resource` is now frozen into the rows at load time. **Editing
`dataset_to_resource_rules` in `datasets.yaml` no longer takes effect by re-running the
view.** Previously a rule change was live the moment `setup_bigquery.sh` recreated the
view. Now it requires either a full reload of `credible_sets`, or the backfill below.

Anyone changing a resource mapping rule must be told this. It is the price of `resource`
being a clustering key.

#### 🚨 The backfill rewrites a clustering key across the whole table — do not paste it blind

This is the most destructive statement in this document. The `UPDATE` below touches
**every row** (~115M) and `resource` is a **clustering key**, so BigQuery rewrites the
table's storage, not just the values. There is no dry run that can tell you the `CASE` is
right, no partial mode, and no undo: a wrong or truncated `CASE` silently corrupts
`resource` table-wide, and **the only recovery is time travel** (§7) inside the 7-day
window. Nothing in this runbook requires you to run it — it is only for a later
`dataset_to_resource_rules` change.

Generate the `CASE`, never retype it, and run this preflight first. It writes nothing and
shows you the blast radius:

```sql
SELECT
  COUNTIF(resource != (<new CASE, from: python scripts/generate_resource_sql.py generate credible_sets_v>)) AS would_change,
  COUNTIF((<the same generated CASE>) IS NULL)                                                              AS would_be_null,
  COUNT(*)                                                                                                  AS total
FROM `phewas-development.genetics_results.credible_sets`;
```

- `would_change` must match the number of rows you *expect* the rule change to affect. `0`
  means the `CASE` already agrees with what is stored and the `UPDATE` is unnecessary. A
  number anywhere near `total` almost certainly means a truncated or wrong `CASE` —
  **stop, and do not run the `UPDATE`**.
- `would_be_null` must be `0`. `resource` is `NOT NULL`, so any NULL the `CASE` produces
  makes the `UPDATE` fail outright — find that here rather than after a 115M-row scan.

Only then:

```sql
UPDATE `phewas-development.genetics_results.credible_sets`
SET resource = <the same generated CASE>
WHERE TRUE;
```

Record `SELECT COUNT(*)` before and after; an `UPDATE` must not change it. Note the time
you started, so a time-travel restore has a timestamp to aim at.

---

## 3. Preconditions

1. `configs/datasets.yaml` is present and in sync with the canonical copy in
   `genetics-results-suite` (`diff` them; run `sync-datasets.sh` if not). The loader reads
   it to build the `resource` `CASE`.
2. `python scripts/generate_resource_sql.py lint` exits 0. In a worktree without the
   sibling repo checked out, pass `DATASETS_YAML=configs/datasets.yaml`.
3. `python -m pytest tests -q` passes.
4. **Record the current row count, and write it down.** This is the *single* place the
   number is captured. §4.5, §6.1 and §7 all compare against **the count you record
   here** — not against any literal printed in this document:
   ```sql
   SELECT COUNT(*) FROM `phewas-development.genetics_results.credible_sets`;
   ```
   It was 115,076,906 at preparation time. If yours differs, that is expected — the table
   is loaded periodically — and **yours is the one that counts**. Do not compare later
   checks against the literal: doing so can hide a loader `DELETE` (if the literal happens
   to be lower than reality) or make you declare corruption after a perfectly legitimate
   load. Note the wall-clock time you took the count, too; §7's time-travel recovery needs
   a timestamp.
5. Confirm no loader run (`scripts/load_credsets_coloc.sh`) is in flight. The rebuild
   copies a snapshot; rows appended to the old table after the copy starts would be lost.
6. **Arrange that no `credible_sets` loader runs at any point between §4.2 and §4.6** —
   not just before the rebuild. That means `scripts/load_credsets_coloc.sh` and
   `scripts/load_pseudo.sh`, including any cron/CI/Airflow trigger for them. Announce the
   freeze to whoever owns those runs and, if they are scheduled, disable the schedule for
   the duration of the soak. **Running either one during the soak destroys the rollback**
   — see §4.5.

---

## 4. The swap

The rebuild is unavoidable and takes minutes. **Readers must not see it.** Every reader —
API, MCP tools, browser — goes through the view `credible_sets_v`, never the base table.
That indirection is what makes a zero-outage cutover possible: build the new table under a
new name, and flip the view in a single atomic DDL statement.

### 4.1 Create the new table (empty, with the target layout)

**Precondition: `credible_sets_new` must not already exist.** `schemas/credible_sets.sql`
is `CREATE TABLE IF NOT EXISTS`, so if an aborted earlier attempt left a `credible_sets_new`
behind — possibly with a *different* clustering or partitioning — this step silently
no-ops, reuses that table, and every check in §6.1–6.3 still passes. Check first; the only
acceptable answer is "Not found":

```bash
bq show --format=prettyjson phewas-development:genetics_results.credible_sets_new
# BigQuery error in show operation: Not found: Table ...credible_sets_new
```

If it does exist, work out where it came from before doing anything else. Do not assume it
is a good partial run: either drop it deliberately (it is not the rollback target — the
rollback target is the untouched live `credible_sets`) or pick a different name and use
that consistently for the rest of §4.

Then build the DDL from `schemas/credible_sets.sql` with the doc's usual `sed`, plus **one
extra expression that renames the target table** — this is the only step in the runbook
whose `sed` is not just the plain `genetics_results` substitution, so read the generated
file rather than reusing muscle memory from §4.6:

```bash
sed -e 's/genetics_results/phewas-development.genetics_results/g' \
    -e 's/`phewas-development\.genetics_results\.credible_sets`/`phewas-development.genetics_results.credible_sets_new`/' \
    schemas/credible_sets.sql > /tmp/create_new.sql
grep -n 'CREATE TABLE' /tmp/create_new.sql   # MUST end in credible_sets_new`
bq query --project_id=phewas-development --use_legacy_sql=false --nouse_cache < /tmp/create_new.sql
```

If that `grep` shows the live table name, **stop** — the second expression did not match.
Running it anyway is harmless (`IF NOT EXISTS` no-ops against the live table and §4.2 then
errors on a missing `credible_sets_new`), but fix the file rather than pressing on.

Do **not** use a `CREATE TABLE AS SELECT`: a CTAS flattens every column to `NULLABLE` and
drops the column descriptions and labels. `NOT NULL` on `variant`/`resource` is
load-bearing — `api/main.py` reports a view column's mode from the base table, so a
nullable column would flip `/schema` from `REQUIRED` to `NULLABLE` for those two columns.

### 4.2 Populate it

```sql
INSERT INTO `phewas-development.genetics_results.credible_sets_new`
  (dataset, resource, data_type, trait, trait_original, cell_type, chr, pos, ref, alt,
   variant, mlog10p, beta, se, pip, cs_id, cs_size, cs_min_r2, aaf, most_severe,
   gene_most_severe)
SELECT
  dataset,
  <paste the CASE from: python scripts/generate_resource_sql.py generate credible_sets_v>,
  data_type, trait, trait_original, cell_type, chr, pos, ref, alt,
  CONCAT(chr, ':', pos, ':', ref, ':', alt),
  mlog10p, beta, se, pip, cs_id, cs_size, cs_min_r2, aaf, most_severe, gene_most_severe
FROM `phewas-development.genetics_results.credible_sets`;
```

Generate the `CASE` rather than retyping it. It must reproduce byte-for-byte what the old
view produced; that equivalence was verified against 115,043,095 rows with zero mismatches
(§6).

Readers are entirely unaffected during this step — the old table and view are untouched.

> ### 🚨 Run this INSERT exactly once
>
> There is no idempotence here. `INSERT` appends; it does not replace. If the statement
> times out in your client but the BigQuery job actually completed, or if you re-paste it
> "to be sure", you get **~230M rows — every row duplicated** — and §6.2's mismatch checks
> still return `0`, because every duplicated row is individually correct.
>
> Before doing anything else after this step, check:
>
> ```sql
> SELECT COUNT(*) FROM `phewas-development.genetics_results.credible_sets_new`;
> -- must equal the count you recorded in §3, precondition 4. Roughly double it means the
> -- INSERT ran twice.
> ```
>
> If it did run twice, do **not** try to de-duplicate. Empty the table and re-run the
> `INSERT` once:
>
> ```sql
> TRUNCATE TABLE `phewas-development.genetics_results.credible_sets_new`;
> ```
>
> `TRUNCATE` keeps the table's schema, partitioning and clustering, so §4.1 does not need
> repeating. It is safe here and only here — `credible_sets_new` holds no data that does
> not come from the live table, and the live table is untouched.
>
> If your client times out, do not re-paste: find the job
> (`bq ls -j -n 20 --format=prettyjson phewas-development`) and read its state before
> deciding anything.

### 4.3 Verify the new table BEFORE cutting over

**First, assert the table actually has the target layout.** This is the whole point of the
change and the one thing §6 cannot detect: row counts, column values and the view's output
schema are all identical whether or not the clustering took effect, so a silently no-opped
§4.1 (see its precondition) passes §6.1–6.3 unchanged — and §5 then tells you to blame
disappointing scan numbers on the storage optimiser, which is exactly the explanation that
would hide it.

```bash
bq show --format=prettyjson phewas-development:genetics_results.credible_sets_new
```

In the output, all of the following must hold:

- `clustering.fields` is exactly `["data_type", "resource", "variant", "pos"]` — same
  members **and same order**; clustering order is not cosmetic;
- `rangePartitioning.field` is `chr`, with `range` `{start: 1, end: 23, interval: 1}`;
- `schema.fields` for `variant` and `resource` both have `"mode": "REQUIRED"`.

Or, scripted:

```python
from google.cloud import bigquery
t = bigquery.Client(project="phewas-development").get_table(
    "phewas-development.genetics_results.credible_sets_new")
assert t.clustering_fields == ["data_type", "resource", "variant", "pos"], t.clustering_fields
rp = t.range_partitioning
assert rp is not None and rp.field == "chr", rp
assert (rp.range_.start, rp.range_.end, rp.range_.interval) == (1, 23, 1), rp.range_
modes = {f.name: f.mode for f in t.schema}
assert modes["variant"] == "REQUIRED" and modes["resource"] == "REQUIRED", modes
```

If any assertion fails, you are about to point the view at a table with the wrong layout.
**Do not continue to §4.4.** Drop `credible_sets_new` and redo §4.1 and §4.2.

Then run every check in §6. Do not proceed on a single failure.

### 4.4 Cut over (atomic, no outage)

One statement. Readers switch between queries; none of them errors.

```sql
CREATE OR REPLACE VIEW `phewas-development.genetics_results.credible_sets_v` AS
SELECT
  * EXCEPT(variant, resource),
  variant,
  LEAST(aaf, 1 - aaf) AS maf,
  resource
FROM `phewas-development.genetics_results.credible_sets_new`;
```

Note this differs from the committed `schemas/credible_sets_v.sql` in one token: the base
table name. That is deliberate and temporary — see §4.6.

> **Why not just rename the tables?** Because the view body and the table layout must
> change together. The old view body (`SELECT *, CONCAT(...) AS variant`) *fails* against a
> table that already has a `variant` column — duplicate output name — and the new body
> (`* EXCEPT(variant, resource)`) fails against a table that lacks them. Renaming first
> breaks the view; replacing the view first breaks against the old table. Pointing the new
> view at the new table in one DDL sidesteps the window entirely.

### 4.5 Soak

Leave the old `credible_sets` in place, untouched, for at least 24 hours. It is the
rollback. Watch API error rates and query costs.

> ### 🚨 NO LOADER RUNS BETWEEN §4.2 AND §4.6
>
> `scripts/load_credsets_coloc.sh` and `scripts/load_pseudo.sh` must not run at any point
> from the start of §4.2 until §4.6 is complete. Neither is aware of the swap:
>
> - `load_credsets_coloc.sh:125` and `load_pseudo.sh:33` both `DELETE FROM
>   ...genetics_results.credible_sets` — the **old** table, which during the soak is the
>   rollback target — and *then* append.
> - The append will fail, because after §4.2 the loader projects 21 columns
>   (`variant` and `resource` are now stored) into the old 19-column table.
> - **But the `DELETE` runs first, and it succeeds.**
>
> So a loader run during the soak does not fail cleanly. It half-executes: rows are
> removed from the rollback target and none are put back. §7's "rollback is one statement
> and instant — the old table was never touched" silently stops being true, and rolling
> back would serve a partial-data table to every reader.
>
> After §4.6 the loaders are safe again: `credible_sets` is by then the new 21-column
> table, and they delete and append against it correctly. The dangerous window is §4.2 →
> §4.6, and only that window.
>
> **If a loader was attempted during the window, assume the rollback target is
> corrupt.** Check before relying on it:
>
> ```sql
> SELECT COUNT(*) FROM `phewas-development.genetics_results.credible_sets`;
> -- must equal THE COUNT YOU RECORDED IN §3, precondition 4 — not the literal in this
> -- document (which was 115,076,906 at preparation time).
> -- fewer rows, or zero, means a loader's DELETE landed:
> -- credible_sets (pre-§4.6) / credible_sets_pre_swap (post-§4.6) is NO LONGER a valid
> -- rollback target.
> ```
>
> Also check the BigQuery job history for the dataset for any `DELETE` against
> `credible_sets` in the window — a partial delete scoped to one `dataset` value can leave
> a plausible-looking but wrong count.
>
> If the rollback target is corrupt, do not roll back to it. Recover it by time travel
> (§7, `bq cp ...@<timestamp>`, picking a timestamp before the loader ran) or reload from
> GCS. The forward path — staying on the new table — is unaffected either way, since the
> loader never touched it.

### 4.6 Normalise the names (deferred; the one step that can break every reader)

Once you are confident, restore the canonical naming so the deployed view matches
`schemas/credible_sets_v.sql` exactly.

**BigQuery does not repoint a view when its base table is renamed.** The instant
`credible_sets_new` is renamed, `credible_sets_v` references a table name that no longer
exists and **every reader errors** — API, MCP tools, browser — until the view is replaced.
Do not issue the renames as one command and the view replacement as another: that window
is not "about a second", it is however long the operator takes to paste the next command,
and it is unbounded if that command fails, if a credential expires, or if they are
interrupted between the two.

Issue all three statements as **one multi-statement `bq query` script**, so BigQuery runs
them back to back inside a single job with no human in the loop. Run it during a quiet
period anyway:

```bash
{
  echo 'ALTER TABLE `phewas-development.genetics_results.credible_sets` RENAME TO credible_sets_pre_swap;'
  echo 'ALTER TABLE `phewas-development.genetics_results.credible_sets_new` RENAME TO credible_sets;'
  sed 's/genetics_results/phewas-development.genetics_results/g' schemas/credible_sets_v.sql
} | bq query --project_id=phewas-development --use_legacy_sql=false --nouse_cache
```

The third statement is the committed `schemas/credible_sets_v.sql`, unchanged — that is
the point of §4.6, and generating it from the file rather than retyping it is what
guarantees the deployed view matches the repo. `credible_sets_v.sql` already ends in a
semicolon, so the three statements concatenate into a valid script.

Build the script into a file first, read it, then run that exact file — so what you
reviewed is what executes:

```bash
{
  echo 'ALTER TABLE `phewas-development.genetics_results.credible_sets` RENAME TO credible_sets_pre_swap;'
  echo 'ALTER TABLE `phewas-development.genetics_results.credible_sets_new` RENAME TO credible_sets;'
  sed 's/genetics_results/phewas-development.genetics_results/g' schemas/credible_sets_v.sql
} > /tmp/normalise.sql
cat /tmp/normalise.sql   # read every line before the next command
bq query --project_id=phewas-development --use_legacy_sql=false --nouse_cache < /tmp/normalise.sql
```

**Residual risk, stated plainly:** BigQuery multi-statement scripts are *not* transactional
for DDL — `ALTER TABLE` and `CREATE VIEW` cannot be wrapped in `BEGIN TRANSACTION`. If the
script fails after the second `ALTER TABLE` but before the `CREATE OR REPLACE VIEW`, the
view is broken for **every** reader and stays broken until you fix it. Recovery, in order
of preference:

1. Re-run just the view statement (the two renames are already done, so it will now
   succeed):
   ```bash
   sed 's/genetics_results/phewas-development.genetics_results/g' schemas/credible_sets_v.sql \
     | bq query --project_id=phewas-development --use_legacy_sql=false --nouse_cache
   ```
2. If that also fails, rename back — `credible_sets` → `credible_sets_new`,
   `credible_sets_pre_swap` → `credible_sets` — and re-apply the §4.4 view body, which
   points at `credible_sets_new`.

Have both of those commands open in a second terminal before you run the script.

Do not skip §4.6 indefinitely: until it is done, the deployed view does not match the
committed schema file, and `setup_bigquery.sh` remains unusable — its view pass still aborts
on `credible_sets_v` and silently skips the eleven alphabetically later views, exactly as
described in §0. (It does *not* break the running view: the `CREATE OR REPLACE VIEW` fails
against the old-layout `credible_sets`, and a failed `CREATE OR REPLACE` leaves the existing
view intact, so readers keep working. The damage is the silent partial apply, not an
outage.) If you must leave the system in the §4.4 state overnight, say so loudly in the
handover.

Drop `credible_sets_pre_swap` only after a week.

---

## 5. `--dry_run` CANNOT VERIFY ANY OF THIS

Read this before you try to save time.

On a freshly written table, BigQuery's dry-run estimate **ignores clustering** until the
storage optimiser has finished reorganising the blocks. During the benchmark this was
observed as **4,492,232,401 B dry-run versus 517,406,337 B actual** — an 8.7× overestimate
on the very same query. A dry run immediately after §4.2 will make the swap look like it
achieved nothing, and could easily be misread as a reason to roll back a change that in
fact worked.

Every figure in §6 must come from a **real executed query** with the cache disabled, read
from the job's `total_bytes_processed`:

```python
from google.cloud import bigquery
c = bigquery.Client(project="phewas-development")
j = c.query(sql, job_config=bigquery.QueryJobConfig(use_query_cache=False))
j.result()
print(j.total_bytes_processed)
```

or `bq query --nouse_cache ...` followed by reading the job. Allow the storage optimiser
some time (minutes to a couple of hours on a table this size) before treating a
disappointing number as real; re-measure before concluding anything.

---

## 6. Verification

### 6.1 Row count

```sql
SELECT
  (SELECT COUNT(*) FROM `phewas-development.genetics_results.credible_sets_new`) AS new_rows,
  (SELECT COUNT(*) FROM `phewas-development.genetics_results.credible_sets`)     AS old_rows;
```

They must be equal, **and both must equal the count you recorded in §3, precondition 4**
(115,076,906 at preparation time — compare against your recorded number, not that literal).
`new_rows` roughly double means §4.2 ran twice; see the box there. `old_rows` below your
recorded count means a loader `DELETE` landed on the rollback target; see §4.5.

(The benchmark table `credible_sets_exp_drvp` holds 115,043,095 rows — it is an older
snapshot, so do not use it as the expected count either.)

### 6.2 The two new columns reproduce the old view exactly

This is the correctness check that matters. Both must be `0`:

```sql
SELECT
  COUNTIF(variant != CONCAT(chr, ':', pos, ':', ref, ':', alt)) AS variant_mismatches,
  COUNTIF(resource != <generated CASE>)                          AS resource_mismatches
FROM `phewas-development.genetics_results.credible_sets_new`;
```

Already run against `credible_sets_exp_drvp`: **0 and 0 across all 115,043,095 rows.**

### 6.3 The view's output schema is unchanged

Everything downstream reads the view, so its column names, order and types must be
byte-identical to the pre-swap ones. Compare against this, which is the recorded pre-swap
schema:

```
0  dataset STRING      6  pos INTEGER      12 pip FLOAT          18 gene_most_severe STRING
1  data_type STRING    7  ref STRING       13 cs_id STRING       19 variant STRING
2  trait STRING        8  alt STRING       14 cs_size INTEGER    20 maf FLOAT
3  trait_original STR  9  mlog10p FLOAT    15 cs_min_r2 FLOAT    21 resource STRING
4  cell_type STRING    10 beta FLOAT       16 aaf FLOAT
5  chr INTEGER         11 se FLOAT         17 most_severe STRING
```

```python
from google.cloud import bigquery
c = bigquery.Client(project="phewas-development")
print([(f.name, f.field_type) for f in
       c.get_table("phewas-development.genetics_results.credible_sets_v").schema])
```

The `* EXCEPT(variant, resource)` + explicit re-projection in the view is what preserves
this: the base table stores `resource` as its second column and `variant` as its eleventh
(`INFORMATION_SCHEMA` `ordinal_position` 2 and 11), so a bare `SELECT *` would silently
reorder the view's columns.

Also confirm `variant` and `resource` still report `mode = REQUIRED` from
`GET /schema` — that comes from the base table's `NOT NULL`.

### 6.4 Scan sizes

Measured with real execution, cache off, projection `SELECT dataset, trait, pip, mlog10p`,
comparing the pre-swap view against the benchmarked new layout. Reproduce with the same
projection or the absolute numbers will not line up.

| query shape | pre-swap view | new layout | change |
|---|---:|---:|---:|
| `WHERE resource = 'finngen'` | 5,035,212,695 B | 3,110,953,252 B | −38% |
| `WHERE variant = '12:111446804:T:C'` | 7,606,541,126 B | 253,707,488 B | **−97% (30×)** |
| `WHERE chr = 12 AND variant = '12:111446804:T:C'` | 367,715,852 B | 288,326,272 B | −22% |

The acceptance criterion for the whole change is the aggregate from the benchmark: the
weighted total over the 10 commonest logged shapes goes from **333.14 GB to 43.45 GB**.

**That figure has never been reproduced, and the independent re-measurement disagrees with
it.** Its inputs — which ten shapes, and their weights — were not recorded anywhere, so it
cannot be recomputed. `scripts/bench_credible_sets_layout.py` rebuilds the measurement from
the two inputs that do survive: BigQuery job history, and both layouts sitting side by side
in `genetics_results_dev`. Re-run it rather than trusting either number here:

```bash
./.venv/bin/python scripts/bench_credible_sets_layout.py shapes   # population, no scan cost
./.venv/bin/python scripts/bench_credible_sets_layout.py run      # the A/B
./.venv/bin/python scripts/bench_credible_sets_layout.py verify   # this table, re-measured
```

Over the 1,320 production-service-account queries logged since 2026-04-09, weighting the
top 10 shapes by observed frequency, that harness measures **409.37 GB → 312.88 GB
(−23.6%)**, not −87.0%.

The gap is in *shape selection*, not in measurement: `verify` reproduces every pre-swap
figure in the table above to the exact byte. What the −87.0% figure needs is a population
dominated by `WHERE variant = <literal>`, the one shape that gains 96%. In the logged
traffic that predicate appears in **2.5% of jobs and 1.9% of bytes**, and its commonest
shape ranks 39th of 1,145 — it cannot be in any traffic-derived top 10. Meanwhile
`most_severe`, an *approved regression*, carries 16.5% of logged bytes.

Two further facts that bear on the criterion, both from the same harness:

- The traffic barely has repeated shapes at all: 1,320 queries collapse to 1,145 shapes,
  97% of them singletons. **Any** top-10 weighting covers only 11% of jobs and 8% of bytes,
  so it is a narrow measure of this workload however the ten are chosen.
- The new layout is larger on disk (26.96 GB vs 23.75 GB) because `variant` and `resource`
  are materialised, so unfiltered scans and column-aggregate queries pay more. Shape 2 in
  the traffic-derived set regresses +52.9% for exactly this reason.

Whether −23.6% still justifies the rewrite and the two approved regressions is a decision,
not a measurement, and it is not settled here.

Expect these to regress, as approved: `WHERE dataset = ...` (+267%),
`WHERE gene_most_severe = ...` (+44%), `WHERE most_severe = ...` (+8%).

### 6.5 A real end-to-end read

Hit the deployed API's `/schema` and run one representative MCP query. A green BigQuery
check with a broken API is still an outage.

---

## 7. Rollback

**Before §4.6** (the normal case) rollback is one statement and instant — the old table was
never touched. Re-create the pre-swap view body against the pre-swap table:

```sql
CREATE OR REPLACE VIEW `phewas-development.genetics_results.credible_sets_v` AS
SELECT
  *,
  CONCAT(chr, ':', pos, ':', ref, ':', alt) AS variant,
  LEAST(aaf, 1 - aaf) AS maf,
  <the generated CASE> AS resource
FROM `phewas-development.genetics_results.credible_sets`;
```

and revert `schemas/credible_sets.sql`, `schemas/credible_sets_v.sql`,
`scripts/load_data.py` and `scripts/generate_resource_sql.py` in git. Drop
`credible_sets_new`.

**After §4.6**, the pre-swap table is `credible_sets_pre_swap`, and rolling back means
undoing the two renames and restoring the old view body. This is why §4.6 must not drop the
old table.

**Do not issue the renames and the view replacement as separate commands.** Between the two
renames, `credible_sets_v` points at a table name that does not exist and **every reader
errors** — the identical defect §4.6 exists to avoid, except this one runs under incident
pressure on a system already known to be broken. Use one multi-statement `bq query` script,
built into a file and read before it is executed, exactly as in §4.6:

```bash
{
  echo 'ALTER TABLE `phewas-development.genetics_results.credible_sets` RENAME TO credible_sets_new;'
  echo 'ALTER TABLE `phewas-development.genetics_results.credible_sets_pre_swap` RENAME TO credible_sets;'
  cat <<'SQL'
CREATE OR REPLACE VIEW `phewas-development.genetics_results.credible_sets_v` AS
SELECT
  *,
  CONCAT(chr, ':', pos, ':', ref, ':', alt) AS variant,
  LEAST(aaf, 1 - aaf) AS maf,
  <the generated CASE> AS resource
FROM `phewas-development.genetics_results.credible_sets`;
SQL
} > /tmp/rollback.sql
cat /tmp/rollback.sql   # read every line; confirm the CASE is complete, not truncated
bq query --project_id=phewas-development --use_legacy_sql=false --nouse_cache < /tmp/rollback.sql
```

The `<the generated CASE>` placeholder is the only thing you must fill in, from
`python scripts/generate_resource_sql.py generate credible_sets_v` at the pre-swap commit.
**Build this file and verify it before you start §4.6**, not during the incident — that is
the whole point of having it ready.

Two things to check before running it:

- §4.5 first. If a loader ran during the §4.2→§4.6 window, `credible_sets_pre_swap` is not
  a valid rollback target and this script would serve a partial-data table to every reader.
- Same residual risk as §4.6: DDL is not transactional. If the script stops after the
  second `ALTER TABLE`, the view is broken for everyone; re-run just the
  `CREATE OR REPLACE VIEW` statement (the renames are already done) to close the window.

Afterwards, revert the four repo files in git as described above, and once you are settled,
drop or keep `credible_sets_new` (the failed new-layout table) deliberately.

**After `credible_sets_pre_swap` is dropped**, there is no rollback short of a full reload
from GCS via `scripts/load_credsets_coloc.sh` — *unless* you are still inside BigQuery's
time-travel window.

`FOR SYSTEM_TIME AS OF` does **not** work here. It requires the table to *currently exist*;
against a dropped table it fails with "Not found: Table ... was not found". For a deleted
table the only form that works is the **snapshot decorator with `bq cp`**, restoring under
a new name:

```bash
# @<epoch-millis> must be a timestamp when the table still existed, i.e. BEFORE the DROP.
bq cp 'phewas-development:genetics_results.credible_sets_pre_swap@1754000000000' \
      phewas-development:genetics_results.credible_sets_restored
```

Compute the millisecond timestamp from the time of the drop, not from "an hour ago":

```bash
# e.g. 10 minutes before a drop you know happened at 2026-08-07T09:30:00Z
date -u -d '2026-08-07T09:20:00Z' +%s000
```

`CREATE TABLE ... CLONE \`...credible_sets_pre_swap\`` with the same `@<millis>` decorator
also works. Verify the restored table's row count against the §3 precondition-4 count
before treating it as the rollback target, then rename it into place.

**Hard limits — read before relying on this:**

- The decorator timestamp must fall inside the table's lifetime. Pick a point comfortably
  before the `DROP`, and before any loader `DELETE` (see §4.5) if one may have run.
- BigQuery's time-travel window is **7 days by default** (dataset-configurable, 2–7 days;
  check `bq show --format=prettyjson genetics_results` for `maxTimeTravelHours`). Past
  that, **there is no recovery path at all** — reload from GCS is the only option.
- §4.6 says to drop `credible_sets_pre_swap` "only after a week". That is *exactly* the
  time-travel window, so by the time you drop it the recovery margin is already gone.
  Treat the drop as irreversible, and do not drop it while any doubt remains.

---

## 8. Follow-ups outside this repo

`configs/datasets.yaml` is canonical in `genetics-results-suite` (the copy in this repo is
a gitignored, generated copy — never edit it here). Two entries in it become **wrong** the
moment this swap lands, and must be corrected in that repo in the same change window:

1. The `credible_sets_v` example description telling agents to "always add the chr filter
   next to variant … without it the same query scans ~20x more data". That number is
   currently correct — measured 20.8× — and becomes wrong: after the swap, adding `chr`
   next to `variant` no longer saves anything and is slightly counterproductive.
2. The accompanying example SQL, which carries a now-pointless `WHERE chr = 12 AND
   variant = ...`.

No such accompanying report exists. Re-derive both numbers instead with
`scripts/bench_credible_sets_layout.py verify`, whose first two rows are exactly this
comparison. Latest run: on the pre-swap layout the `chr` filter is worth **20.7×**
(7,606,541,126 B → 367,715,852 B), and on the new layout adding it *costs* **+13.6%**
(270,624,992 B → 307,509,464 B) — so both claims above hold, and the example SQL should
lose its `chr` predicate.
