# HMS Migration: Cloudera CDH → Huawei Cloud MRS

Toolkit and verified runbook for migrating Hive Metastore (HMS) metadata from a
Cloudera CDH cluster to Huawei Cloud MRS (MapReduce Service).

The tools extract the source catalog from the Cloudera PostgreSQL metastore,
homologate it for the target platform (authority/path rewrites, system DB
exclusion, ACID handling), emit a replayable HiveQL DDL file, and validate the
result with per-table row counts and HDFS file/byte counts.

Verified against a real CDH 7.1.7 cluster (Hive 3.1.3, PostgreSQL 12.9
metastore) migrating to MRS 3.x.

## Why this exists

- **ACID tables don't survive a plain copy.** The target HMS has no
  transaction/write-id history for migrated tables, so copied `delta_`/`base_`
  directories are invisible to the ACID reader (verified on CDH 7.1.7).
  The toolkit flattens ACID data files into a plain directory and recreates
  the table as `EXTERNAL` on the target.
- **Locations must be homologated.** HDFS authorities
  (`cloudera-node.local:8020` → `hacluster`) and warehouse paths
  (`/warehouse/tablespace/...` → `/user/hive/warehouse`) differ between
  platforms; every database, table, and partition location is rewritten.
- **Migrations need evidence.** Structural comparison (databases, tables,
  columns, partition keys) plus row counts and HDFS file/byte counts, with a
  CI-friendly exit code.

## Pipeline

```
Cloudera CDH (source)                          Huawei Cloud MRS (target)
──────────────────────                         ─────────────────────────
hms_extract.py ──► hms_catalog_source.json
                         │
                         ▼
                 hms_homologate.py ──► out/ddl_replay.sql ──────► beeline (replay DDL)
                                    ├► out/hms_catalog_target.json
                                    └► out/location_mapping.tsv
                                              │
acid_flatten_stage.sh (ACID data staging)      │
        └──► staging dir + _manifest.tsv ──► hdfs distcp → target locations
                                              │
hms_validate.py --engine impala ◄──────────────┴──► hms_validate.py --engine beeline
        │                                              │
        └──────────────► hms_compare.py ◄──────────────┘
                         (exit 1 on any mismatch)
```

## Tools

| File | Runs on | Purpose |
|---|---|---|
| `hms_extract.py` | Cloudera node | Dump the full HMS catalog (DBs, tables, partitions, columns, serdes, params) from the PostgreSQL metastore into one portable JSON file. Python 3 stdlib only — uses `psql \copy`, no psycopg2. |
| `hms_homologate.py` | Anywhere | Homologate the source catalog for MRS: exclude system DBs, rewrite HDFS authorities, remap warehouse paths, strip stats/transient params, and emit `ddl_replay.sql` plus mapping/exclusion reports. |
| `acid_flatten_stage.sh` | Source cluster | Flatten ACID `base_*`/`delta_*` files into one flat directory per table and write a `_manifest.tsv` for the data copy. |
| `hms_validate.py` | Either cluster | Collect per-table evidence: row counts (`impala-shell` on source, `beeline` on target) and HDFS file/byte counts. |
| `hms_compare.py` | Anywhere | Compare source vs target: structural diff (catalogs) and evidence diff (validations). Exits `1` on any mismatch. |

## Workflow

### 1. Extract the source catalog (on the Cloudera node)

```bash
export HMS_PG_PASSWORD=<hive metastore DB password>   # see runbook §3.3
python3 hms_extract.py --out hms_catalog_source.json
```

Connection defaults: `127.0.0.1:5432/metastore`, user `hive` — override with
`HMS_PG_HOST`, `HMS_PG_PORT`, `HMS_PG_DB`, `HMS_PG_USER`.

### 2. Homologate for MRS (any workstation)

```bash
python3 hms_homologate.py \
  --source hms_catalog_source.json \
  --out-dir out \
  --exclude-db sys --exclude-db information_schema \
  --src-authority cloudera-node.local:8020 \
  --dst-authority hacluster \
  --path-map '/warehouse/tablespace/managed/hive=/user/hive/warehouse' \
  --path-map '/warehouse/tablespace/external/hive=/user/hive/warehouse' \
  --managed-as-external
```

Outputs in `out/`:

| File | Content |
|---|---|
| `ddl_replay.sql` | Replayable HiveQL: `CREATE DATABASE/TABLE/VIEW` + `ALTER TABLE ... ADD PARTITION` with rewritten locations |
| `hms_catalog_target.json` | The homologated target catalog |
| `location_mapping.tsv` | Per-table source → target location map (input to data staging) |
| `exclusions.tsv` | Databases excluded and why |

By default ACID tables are converted to plain `EXTERNAL TABLE`s (to pair with
the flattening step). Use `--acid-keep-transactional` only if you migrate data
via Hive `EXPORT`/`IMPORT`.

### 3. Stage and copy the data

```bash
# on the SOURCE cluster: flatten ACID tables into a staging dir
sudo -u hive ./acid_flatten_stage.sh out/location_mapping.tsv /stag/hms_migration

# copy staged files to their target locations (manifest-driven), e.g. distcp
```

### 4. Replay the DDL on MRS

```bash
beeline -u "$BEELINE_URL" -n hive -f out/ddl_replay.sql
```

### 5. Validate both sides and compare

```bash
# source (Cloudera, via impala-shell)
sudo -u hive python3 hms_validate.py --catalog hms_catalog_source.json \
  --engine impala --out validation_source.json

# target (MRS, via beeline + ZooKeeper service discovery)
python3 hms_validate.py --catalog out/hms_catalog_target.json \
  --engine beeline --jdbc-url "$BEELINE_URL" --user hive \
  --out validation_target.json

# compare structure + evidence (exit 1 on mismatch)
python3 hms_compare.py \
  --source-catalog hms_catalog_source.json \
  --target-catalog out/hms_catalog_target.json \
  --source-validation validation_source.json \
  --target-validation validation_target.json
```

## Requirements

- Python 3 (stdlib only, no pip dependencies)
- `psql` client (extract step, on the Cloudera node)
- `impala-shell` (source validation) / `beeline` (target validation)
- `hdfs` CLI (data staging and validation)

## Runbook

The complete, command-by-command procedure — including pre-checks, HMS backend
credential recovery from the Cloudera credstore, metastore backup, write
freeze, and a Java helper for reading keystore aliases — is in
[`RUNBOOK_HMS_CLOUDERA_TO_MRS.md`](RUNBOOK_HMS_CLOUDERA_TO_MRS.md).

## Repository contents

The `out/` directory contains real outputs from the demo environment
referenced in the runbook, so you can compare your own results against them.
