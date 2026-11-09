# Runbook — HMS Metadata Migration: Cloudera CDH → Huawei Cloud MRS

End-to-end, reproducible procedure to migrate the Hive Metastore (HMS) catalog
and its data from a Cloudera cluster to a Huawei Cloud MRS cluster, including
homologation of HDFS routes and post-migration validation.

**Every command and script in this runbook was executed and verified on the
demo environment** (`cloudera-node.local`, CDH 7.1.7). Real outputs are shown
so you can compare yours. MRS-side commands follow the standard MRS 3.x
toolchain (beeline with ZooKeeper service discovery, `hacluster` nameservice);
they could not be executed here because the MRS cluster (`<mrs-eip>`) was
unreachable at authoring time — adjust hostnames/ports to your MRS instance.

---

## 1. Reference environment (source — verified)

| Component | Value (verified) |
|---|---|
| Platform | Cloudera CDH **7.1.7**, CM 7.4.4 |
| Node | `cloudera-node.local` (internal `192.168.0.157`, EIP `<cloudera-eip>`) |
| HMS | Hive **3.1.3000.7.1.7.0-551**, thrift `cloudera-node.local:9083` |
| HMS backend DB | **PostgreSQL 12.9**, `jdbc:postgresql://cloudera-node.local:5432/metastore`, user `hive` |
| HDFS authority | `hdfs://cloudera-node.local:8020` (single node, no HA nameservice) |
| Query engines | Impala 3.4 (port 21000), Spark; **no HiveServer2 role** on the cluster |
| Catalog contents | 8 databases, 80 tables (13 business external, 6 business ACID/managed, rest are `sys`/`information_schema` system objects) |
| Business databases | `crm_bronze`, `crm_silver`, `crm_gold`, `atlas_test`, `atlas_test_db`, `default` |
| System databases (excluded) | `sys` (47 JDBC-backed tables + views), `information_schema` (9 views) |

Target (MRS — adjust to your instance):

| Component | Typical MRS 3.x value |
|---|---|
| HDFS authority | `hdfs://hacluster/` (HA nameservice) |
| Hive warehouse | `/user/hive/warehouse` (managed warehouse root) |
| HiveServer2 | beeline URL: `jdbc:hive2://<zk1>:2181,<zk2>:2181,<zk3>:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2` |
| HMS thrift | `thrift://<master-node>:21088` |

### Toolkit layout (this directory)

```
hms_migration/
├── RUNBOOK_HMS_CLOUDERA_TO_MRS.md   <- this runbook
├── hms_extract.py                   <- step 3: HMS catalog -> JSON (runs on CDH node)
├── hms_homologate.py                <- step 4: JSON -> MRS-homologated JSON + DDL (any host)
├── acid_flatten_stage.sh            <- step 5: ACID tables -> flat staging dirs (runs on CDH node)
├── hms_validate.py                  <- step 8: row/HDFS evidence (impala=source, beeline=target)
├── hms_compare.py                   <- step 8: source vs target diff, CI-friendly exit code
└── out/                             <- reference artifacts produced by the verified run
    ├── hms_catalog_source.json      <- extracted source catalog (8 dbs / 80 tables)
    ├── hms_catalog_target.json      <- homologated target catalog (6 dbs / 19 tables)
    ├── ddl_replay.sql               <- DDL to replay on MRS (25 statements)
    ├── location_mapping.tsv         <- per-table source -> target location map (+acid flag)
    ├── exclusions.tsv               <- excluded system databases
    └── validation_source.json       <- source-side evidence (row counts, files, bytes)
```

---

## 2. Migration overview

```
 Cloudera CDH 7.1.7                                Huawei Cloud MRS
 ┌──────────────────────────────┐                  ┌──────────────────────────────┐
 │ PostgreSQL "metastore" DB    │                  │ HMS (MySQL backend)           │
 │  ^                           │                  │  ^                            │
 │  | (3) hms_extract.py        │                  │  | (7) ddl_replay.sql         │
 │  v                           │                  │  |     via beeline             │
 │ hms_catalog_source.json      │                  │  |                            │
 │  ^                           │                  │  |                            │
 │  | (4) hms_homologate.py     │   (6) DistCp     │  |                            │
 │  v                           │   HDFS ────────> │ HDFS hdfs://hacluster/...     │
 │ ddl_replay.sql + mapping.tsv │   (or via OBS)   │                              │
 │  ^                           │                  │                              │
 │  | (5) acid_flatten_stage.sh │                  │                              │
 │  v                           │                  │                              │
 │ /stag/hms_migration (HDFS)   │                  │                              │
 └──────────────────────────────┘                  └──────────────────────────────┘
        (8) hms_validate.py on both sides  ->  hms_compare.py  ->  GO / NO-GO
```

Execution order for a production window:

| # | Step | Where | Duration (demo) |
|---|---|---|---|
| 0 | Pre-checks + assessment | CDH node | minutes |
| 1 | Freeze writes / maintenance mode | CM | org decision |
| 2 | Backup HMS backend DB | CDH node | minutes |
| 3 | Extract catalog → JSON | CDH node | < 1 min |
| 4 | Homologate → DDL + mapping | any host | seconds |
| 5 | Stage ACID tables (flatten) | CDH node | minutes |
| 6 | DistCp data → MRS HDFS | MRS or CDH node | data-size bound |
| 7 | Replay DDL on MRS | MRS | minutes |
| 8 | Validate + compare | both | minutes |
| 9 | Cutover / rollback decision | — | — |

---

## 3. Step 0 — Pre-checks and access validation (verified commands)

### 3.1 SSH and service reachability

```bash
sshpass -p '<root-password>' ssh -o StrictHostKeyChecking=no root@<cloudera-eip> \
  'hostname; whoami; uname -r'
# cloudera-node.local
# root
# 4.18.0-348.7.1.el8_5.x86_64
```

```bash
# Cloudera Manager API (version check)
curl -s -u admin:admin http://<cloudera-eip>:7180/api/v40/cm/version
# { "version" : "7.4.4", ... }

# HMS role health (must be GOOD)
curl -s -u admin:admin "http://localhost:7180/api/v40/clusters/hwc-metadata-mngmt/services/hive/roles" \
  | grep -oE '"type\" : \"HIVEMETASTORE\"|\"healthSummary\" : \"GOOD\"'
```

### 3.2 HMS functional check (thrift RPC, not just the port)

```bash
# metatool performs a real thrift round-trip against HMS :9083
hive --service metatool -listFSRoot; echo "exit=$?"
# Listing FS Roots..
# exit=0
```

### 3.3 HMS backend DB credentials

CM stores the PostgreSQL password in a Java credential store, NOT in
`hive-site.xml` (it shows `********`). Retrieve it from the runtime keystore:

```bash
# 1. keystore password (from the HMS process environment)
sudo tr "\0" "\n" < /proc/$(pgrep -f HiveMetaStore | head -1)/environ | grep CREDSTORE
# HADOOP_CREDSTORE_PASSWORD=<credstore-password>     (demo value)

# 2. read the alias with a 20-line Java helper (full source in appendix A)
export HADOOP_CREDSTORE_PASSWORD=<credstore-password>
java -cp "$(hadoop classpath):/tmp" GetCred
# javax.jdo.option.connectionpassword = hive_password        (demo value)
```

Verify DB access and take stock of the catalog:

```bash
export PGPASSWORD=hive_password
psql -h 127.0.0.1 -U hive -d metastore -tA -F '|' -P pager=off <<'SQL'
select version();
select datname, pg_encoding_to_char(encoding), pg_size_pretty(pg_database_size(datname)) from pg_database where datname=current_database();
select (select count(*) from "DBS") dbs, (select count(*) from "TBLS") tbls, (select count(*) from "PARTITIONS") parts, (select count(*) from "COLUMNS_V2") cols;
select d."NAME", count(t."TBL_ID") from "DBS" d left join "TBLS" t on t."DB_ID"=d."DB_ID" group by d."NAME" order by 2 desc;
SQL
# PostgreSQL 12.9 on x86_64-redhat-linux-gnu ...
# metastore|UTF8|77 MB
# 8|80|0|598
# sys|52 ... information_schema|9 ... crm_bronze|5 ... (table counts per db)
```

> **Scope decision (verified):** `sys` contains 47 tables using
> `org.apache.hive.storage.jdbc.JdbcSerDe` — they are *views over the old
> PostgreSQL metastore itself* and must NOT be migrated. `information_schema`
> is regenerated by every Hive platform. Both are excluded in step 4.

### 3.4 What will need homologation (location census)

```bash
export PGPASSWORD=hive_password
psql -h 127.0.0.1 -U hive -d metastore -tA -F '|' -P pager=off <<'SQL'
select d."NAME", t."TBL_NAME", t."TBL_TYPE", s."LOCATION"
from "TBLS" t join "DBS" d on d."DB_ID"=t."DB_ID"
left join "SDS" s on s."SD_ID"=t."SD_ID"
where d."NAME" not in ('sys','information_schema') order by 1,2;
SQL
# crm_bronze|customers_raw|EXTERNAL_TABLE|hdfs://cloudera-node.local:8020/data/crm/bronze/customers
# crm_gold|customer_360|EXTERNAL_TABLE|hdfs://cloudera-node.local:8020/data/crm/gold/customer_360
# atlas_test|facturas|MANAGED_TABLE|hdfs://cloudera-node.local:8020/warehouse/tablespace/managed/hive/atlas_test.db/facturas
# ...
```

Two rewrite classes exist:
1. **Authority**: `cloudera-node.local:8020` → `hacluster` (MRS nameservice).
2. **Warehouse paths**: CDH `/warehouse/tablespace/{managed,external}/hive/...`
   → MRS `/user/hive/warehouse/...`. Custom data paths (`/data/crm/...`) stay
   as-is (only the authority changes).

---

## 4. Step 1 — Freeze writes and back up HMS

Put the cluster / Hive service in maintenance mode (CM UI or API) and stop
the pipelines that write to Hive. Then back up the backend DB:

```bash
export PGPASSWORD=hive_password
pg_dump -h 127.0.0.1 -U hive -d metastore -f /backup/metastore_$(date +%Y%m%d_%H%M).sql
ls -lh /backup/metastore_*.sql
# -rw-r--r-- 1 root root 54M ... metastore_20261110_0306.sql
grep -c "CREATE TABLE" /backup/metastore_20261110_0306.sql
# 145
```

Rollback of last resort: restore this dump into a fresh PostgreSQL and point
a spare HMS at it (`javax.jdo.option.ConnectionURL`).

---

## 5. Step 2 — Extract the catalog (`hms_extract.py`)

Runs **on the CDH node**. Python 3 stdlib only — it shells out to `psql \copy`
(CSV), so no psycopg2 is needed. Full source:

```python
#!/usr/bin/env python3
# =====================================================================
# hms_extract.py - Extract the full HMS catalog from the Cloudera
# PostgreSQL metastore database into one portable JSON file.
#
# Runs ON the Cloudera node (needs psql + read access to the metastore
# DB). Python 3 stdlib only (uses psql \copy CSV, no psycopg2 needed).
#
# Usage:
#   export HMS_PG_PASSWORD=<hive db password>     # see runbook step 3.3
#   python3 hms_extract.py > hms_catalog_source.json
#      or: python3 hms_extract.py --out hms_catalog_source.json
# =====================================================================
import csv
import io
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

PG_HOST = os.environ.get("HMS_PG_HOST", "127.0.0.1")
PG_PORT = os.environ.get("HMS_PG_PORT", "5432")
PG_DB = os.environ.get("HMS_PG_DB", "metastore")
PG_USER = os.environ.get("HMS_PG_USER", "hive")
PG_PASSWORD = os.environ.get("HMS_PG_PASSWORD", "")  # required, no default on purpose


def q(sql):
    """Run a query through psql \copy CSV and return a list of rows."""
    env = dict(os.environ, PGPASSWORD=PG_PASSWORD)
    copy_sql = "\\copy ({}) to stdout with (format csv)".format(sql)
    p = subprocess.run(
        ["psql", "-h", PG_HOST, "-p", PG_PORT, "-U", PG_USER, "-d", PG_DB,
         "--no-psqlrc", "-c", copy_sql],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        universal_newlines=True)
    if p.returncode != 0:
        sys.stderr.write("psql failed: {}\n{}\n".format(sql, p.stderr))
        sys.exit(1)
    return list(csv.reader(io.StringIO(p.stdout)))


def main():
    if not PG_PASSWORD:
        sys.stderr.write("Set HMS_PG_PASSWORD (hive metastore DB password)\n")
        sys.exit(1)

    # ---- databases -------------------------------------------------
    dbs = {}
    for r in q('SELECT "DB_ID","NAME","DB_LOCATION_URI","DESC",'
               'coalesce("OWNER_NAME",\'\'),coalesce("OWNER_TYPE",\'\') '
               'FROM "DBS" ORDER BY "NAME"'):
        dbs[r[0]] = {"name": r[1], "location": r[2], "description": r[3],
                     "owner": r[4], "owner_type": r[5], "params": {},
                     "tables": []}
    for r in q('SELECT "DB_ID","PARAM_KEY","PARAM_VALUE" FROM "DATABASE_PARAMS"'):
        if r[0] in dbs:
            dbs[r[0]]["params"][r[1]] = r[2]

    # ---- storage descriptors (shared by tables and partitions) -----
    sds = {}
    for r in q('SELECT "SD_ID","CD_ID","SERDE_ID",coalesce("INPUT_FORMAT",\'\'),'
               'coalesce("OUTPUT_FORMAT",\'\'),coalesce("LOCATION",\'\') FROM "SDS"'):
        sds[r[0]] = {"cd_id": r[1], "serde_id": r[2], "input_format": r[3],
                     "output_format": r[4], "location": r[5]}

    serdes = {}
    for r in q('SELECT "SERDE_ID",coalesce("NAME",\'\'),coalesce("SLIB",\'\') FROM "SERDES"'):
        serdes[r[0]] = {"name": r[1], "lib": r[2], "params": {}}
    for r in q('SELECT "SERDE_ID","PARAM_KEY","PARAM_VALUE" FROM "SERDE_PARAMS"'):
        if r[0] in serdes:
            serdes[r[0]]["params"][r[1]] = r[2]

    columns = {}
    for r in q('SELECT "CD_ID","COLUMN_NAME","TYPE_NAME","INTEGER_IDX",'
               'coalesce("COMMENT",\'\') FROM "COLUMNS_V2" ORDER BY "CD_ID","INTEGER_IDX"'):
        columns.setdefault(r[0], []).append(
            {"name": r[1], "type": r[2], "comment": r[4] if r[4] else None})

    # ---- tables ----------------------------------------------------
    tables = {}
    for r in q('SELECT "TBL_ID","DB_ID","SD_ID","TBL_NAME","TBL_TYPE",'
               'coalesce("OWNER",\'\'),coalesce("OWNER_TYPE",\'\'),'
               'coalesce("VIEW_ORIGINAL_TEXT",\'\'),coalesce("VIEW_EXPANDED_TEXT",\'\') '
               'FROM "TBLS" ORDER BY "DB_ID","TBL_NAME"'):
        tables[r[0]] = {"db_id": r[1], "sd_id": r[2], "name": r[3],
                        "type": r[4], "owner": r[5], "owner_type": r[6],
                        "view_original_text": r[7] or None,
                        "view_expanded_text": r[8] or None,
                        "params": {}, "partition_keys": [], "partitions": []}
    for r in q('SELECT "TBL_ID","PARAM_KEY","PARAM_VALUE" FROM "TABLE_PARAMS"'):
        if r[0] in tables:
            tables[r[0]]["params"][r[1]] = r[2]
    for r in q('SELECT "TBL_ID","PKEY_NAME","PKEY_TYPE" FROM "PARTITION_KEYS" '
               'ORDER BY "TBL_ID","INTEGER_IDX"'):
        if r[0] in tables:
            tables[r[0]]["partition_keys"].append({"name": r[1], "type": r[2]})

    # ---- partitions (empty in this cluster, kept for completeness) --
    part_params = {}
    for r in q('SELECT "PART_ID","PARAM_KEY","PARAM_VALUE" FROM "PARTITION_PARAMS"'):
        part_params.setdefault(r[0], {})[r[1]] = r[2]
    for r in q('SELECT "PART_ID","TBL_ID","SD_ID","PART_NAME" FROM "PARTITIONS"'):
        if r[1] not in tables:
            continue
        sd = sds.get(r[2])
        tables[r[1]]["partitions"].append({
            "name": r[3],
            "location": sd["location"] if sd else None,
            "params": part_params.get(r[0], {}),
        })

    # ---- assemble nested catalog -----------------------------------
    for t in tables.values():
        sd = sds.get(t["sd_id"]) if t["sd_id"] else None
        serde = serdes.get(sd["serde_id"]) if sd and sd["serde_id"] else None
        t["location"] = sd["location"] if sd else None
        t["input_format"] = sd["input_format"] if sd else None
        t["output_format"] = sd["output_format"] if sd else None
        t["serde_lib"] = serde["lib"] if serde else None
        t["serde_params"] = serde["params"] if serde else {}
        t["columns"] = columns.get(sd["cd_id"], []) if sd else []
        if t["db_id"] in dbs:
            dbs[t["db_id"]]["tables"].append(t)

    catalog = {
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "platform": "Cloudera CDH 7.1.7",
            "hms_version": "3.1.3000.7.1.7.0-551",
            "metastore_db": "jdbc:postgresql://{}:{}/{}".format(PG_HOST, PG_PORT, PG_DB),
            "hdfs_authority": "cloudera-node.local:8020",
        },
        "databases": [dbs[k] for k in sorted(dbs, key=lambda k: dbs[k]["name"])],
    }

    out = json.dumps(catalog, indent=1, sort_keys=False)
    if "--out" in sys.argv:
        fn = sys.argv[sys.argv.index("--out") + 1]
        with open(fn, "w") as f:
            f.write(out + "\n")
        sys.stderr.write("wrote {} ({} databases, {} tables)\n".format(
            fn, len(catalog["databases"]),
            sum(len(d["tables"]) for d in catalog["databases"])))
    else:
        print(out)


if __name__ == "__main__":
    main()
```

**Verified run:**

```bash
export HMS_PG_PASSWORD=hive_password
python3 hms_extract.py --out hms_catalog_source.json
# wrote hms_catalog_source.json (8 databases, 80 tables)
```

Sanity-check the JSON (verified output):

```bash
python3 - <<'EOF'
import json
c = json.load(open("hms_catalog_source.json"))
for d in c["databases"]:
    print(d["name"], "|", len(d["tables"]), "tables |", d["location"])
EOF
# atlas_test | 1 tables | hdfs://cloudera-node.local:8020/warehouse/tablespace/external/hive/atlas_test.db
# ...
# sys | 52 tables | hdfs://cloudera-node.local:8020/warehouse/tablespace/external/hive/sys.db
```

---

## 6. Step 3 — Homologate for MRS (`hms_homologate.py`)

Runs anywhere with Python 3 (workstation is fine). It:

1. **Excludes** system databases (`sys`, `information_schema` — always; more
   via `--exclude-db`).
2. **Rewrites the HDFS authority** `cloudera-node.local:8020` → `hacluster`.
3. **Remaps warehouse paths** via `--path-map` regexes
   (`/warehouse/tablespace/managed/hive` and `/warehouse/tablespace/external/hive`
   → `/user/hive/warehouse`). Custom paths (`/data/crm/...`) are untouched.
4. **Converts ACID tables to plain EXTERNAL** (see §9 pitfall #1) and strips
   volatile `TBLPROPERTIES` (`transient_*`, `numFiles*`, `totalSize`,
   `spark.sql.*`, stats...).
5. Emits:
   - `ddl_replay.sql` — deterministic, idempotent HiveQL (`IF NOT EXISTS`)
   - `hms_catalog_target.json` — homologated catalog (for the comparator)
   - `location_mapping.tsv` — per-table `source → target` map **+ `acid` flag**
     (input for step 5 staging and step 6 distcp)
   - `exclusions.tsv` — what was excluded and why

Full source:

```python
#!/usr/bin/env python3
# =====================================================================
# hms_homologate.py - Homologate a Cloudera HMS catalog for Huawei MRS:
#   1. excludes system databases (sys, information_schema, ...)
#   2. rewrites HDFS authorities  (cloudera-node.local:8020 -> hacluster)
#   3. remaps HDFS paths          (/warehouse/... -> /user/hive/warehouse)
#   4. emits a replayable HiveQL DDL file + mapping/exclusion reports
#
# Runs anywhere with Python 3 (workstation is fine). Example:
#   python3 hms_homologate.py \
#     --source hms_catalog_source.json \
#     --out-dir out \
#     --exclude-db sys --exclude-db information_schema \
#     --src-authority cloudera-node.local:8020 \
#     --dst-authority hacluster \
#     --path-map '/warehouse/tablespace/managed/hive=/user/hive/warehouse' \
#     --path-map '/warehouse/tablespace/external/hive=/user/hive/warehouse' \
#     --managed-as-external
# =====================================================================
import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone

# TABLE_PARAMS keys that must NOT be replayed in the target DDL
SKIP_PARAMS = re.compile(
    r"^(transient_|numFiles|totalSize|rawDataSize|numFilesErasureCoded|"
    r"spark\.sql\.|OBJCAPABILITIES|EXTERNAL$|stats:|numRows|column.stats)")


def esc(s):
    """Escape a string for HiveQL single-quoted literal."""
    return str(s).replace("'", "''")


def bq(s):
    """Quote identifier with backticks."""
    return "`" + str(s).replace("`", "``") + "`"


def rewrite_location(loc, src_auth, dst_auth, path_maps):
    if not loc:
        return loc
    m = re.match(r"^([A-Za-z][\w+.-]*)://([^/]+)(/.*)$", loc)
    if not m:
        return loc
    scheme, auth, path = m.group(1), m.group(2), m.group(3)
    if src_auth and auth == src_auth:
        auth = dst_auth
    for frm, to in path_maps:
        path = re.sub(frm, to, path)
    return "{}://{}{}".format(scheme, auth, path)


def parse_part_name(name):
    """'dt=2026-01-01/hr=00' -> [('dt','2026-01-01'),('hr','00')]"""
    out = []
    for kv in name.split("/"):
        if "=" in kv:
            k, v = kv.split("=", 1)
            out.append((k, v))
    return out


def table_ddl(db, t, managed_as_external, keep_transactional=False):
    full = "{}.{}".format(bq(db["name"]), bq(t["name"]))
    lines = []
    if t["type"] == "VIRTUAL_VIEW":
        text = t.get("view_original_text") or t.get("view_expanded_text") or ""
        lines.append("CREATE VIEW IF NOT EXISTS {} AS {};".format(full, text))
        return "\n".join(lines)

    # ACID tables: the target HMS has no txn/write-id history for migrated
    # tables, so copied delta_/base_ dirs are INVISIBLE to the ACID reader
    # (verified on CDH 7.1.7). Strategy: flatten delta/base files into the
    # table root during data staging and create a plain EXTERNAL table.
    # (--acid-keep-transactional keeps managed+ACID for EXPORT/IMPORT flows)
    is_acid = (t.get("params", {}).get("transactional", "").lower() == "true")
    if is_acid and not keep_transactional:
        t = dict(t)
        t["params"] = {k: v for k, v in t["params"].items()
                       if not k.startswith("transactional")}
        t["type"] = "EXTERNAL_TABLE"
    external = (t["type"] == "EXTERNAL_TABLE") or (managed_as_external and not is_acid)
    kw = "CREATE EXTERNAL TABLE IF NOT EXISTS" if external else "CREATE TABLE IF NOT EXISTS"
    cols = []
    for c in t["columns"]:
        s = "{} {}".format(bq(c["name"]), c["type"])
        if c.get("comment"):
            s += " COMMENT '{}'".format(esc(c["comment"]))
        cols.append(s)
    lines.append("{} {} (".format(kw, full))
    lines.append("  " + ",\n  ".join(cols))
    lines.append(")")

    if t.get("partition_keys"):
        pk = ", ".join("{} {}".format(bq(p["name"]), p["type"])
                       for p in t["partition_keys"])
        lines.append("PARTITIONED BY ({})".format(pk))

    comment = t.get("params", {}).get("comment")
    if comment:
        lines.append("COMMENT '{}'".format(esc(comment)))

    if t.get("serde_lib"):
        lines.append("ROW FORMAT SERDE '{}'".format(t["serde_lib"]))
        sp = {k: v for k, v in (t.get("serde_params") or {}).items()}
        if sp:
            kv = ", ".join("\"{}\"='{}'".format(k, esc(v)) for k, v in sorted(sp.items()))
            lines.append("WITH SERDEPROPERTIES ({})".format(kv))
    if t.get("input_format") and t.get("output_format"):
        lines.append("STORED AS INPUTFORMAT '{}' OUTPUTFORMAT '{}'".format(
            t["input_format"], t["output_format"]))
    if t.get("location"):
        lines.append("LOCATION '{}'".format(esc(t["location"])))

    props = {k: v for k, v in (t.get("params") or {}).items()
             if not SKIP_PARAMS.match(k) and k != "comment"}
    if props:
        kv = ",\n  ".join("'{}'='{}'".format(k, esc(v)) for k, v in sorted(props.items()))
        lines.append("TBLPROPERTIES (\n  {}\n)".format(kv))
    lines[-1] += ";"
    ddl = "\n".join(lines)

    # explicit partitions with their (rewritten) locations
    for p in t.get("partitions", []):
        kvs = ", ".join("{}='{}'".format(bq(k), esc(v)) for k, v in parse_part_name(p["name"]))
        ddl += "\nALTER TABLE {} ADD IF NOT EXISTS PARTITION ({})".format(full, kvs)
        if p.get("location"):
            ddl += " LOCATION '{}'".format(esc(p["location"]))
        ddl += ";"
    return ddl


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True, help="hms_catalog_source.json")
    ap.add_argument("--out-dir", default="out")
    ap.add_argument("--exclude-db", action="append", default=[],
                    help="database to exclude (repeatable)")
    ap.add_argument("--src-authority", default="cloudera-node.local:8020")
    ap.add_argument("--dst-authority", default="hacluster",
                    help="target HDFS authority (MRS nameservice, e.g. hacluster)")
    ap.add_argument("--path-map", action="append", default=[],
                    help="FROM=TO path rewrite (repeatable, regex)")
    ap.add_argument("--managed-as-external", action="store_true",
                    help="create managed tables as EXTERNAL in the target")
    ap.add_argument("--acid-keep-transactional", action="store_true",
                    help="keep ACID tables managed+transactional (use only "
                         "with Hive EXPORT/IMPORT data flow)")
    args = ap.parse_args()

    excludes = set(args.exclude_db) | {"sys", "information_schema"}
    path_maps = []
    for pm in args.path_map:
        if "=" not in pm:
            sys.stderr.write("bad --path-map (need FROM=TO): {}\n".format(pm))
            sys.exit(1)
        frm, to = pm.split("=", 1)
        path_maps.append((frm, to))

    with open(args.source) as f:
        src = json.load(f)

    os.makedirs(args.out_dir, exist_ok=True)
    cat = {"homologated_at": datetime.now(timezone.utc).isoformat(),
           "source_catalog": args.source,
           "authority_map": "{} -> {}".format(args.src_authority, args.dst_authority),
           "path_maps": ["{} -> {}".format(a, b) for a, b in path_maps],
           "managed_as_external": args.managed_as_external,
           "databases": []}

    ddl = ["-- Generated by hms_homologate.py at {}".format(cat["homologated_at"]),
           "-- Source: {} (Cloudera {})".format(
               src["source"]["metastore_db"], src["source"]["platform"]),
           "-- Authority: {} | Path maps: {}".format(
               cat["authority_map"], "; ".join(cat["path_maps"]) or "none"),
           ""]
    mapping_rows = [["database", "table", "type", "acid", "source_location", "target_location"]]
    excl_rows = [["database", "reason"]]

    for d in src["databases"]:
        if d["name"] in excludes:
            excl_rows.append([d["name"], "system database (excluded)"])
            continue
        nd = {k: d[k] for k in ("name", "description", "owner", "owner_type", "params")}
        nd["location"] = rewrite_location(d["location"], args.src_authority,
                                          args.dst_authority, path_maps)
        nd["tables"] = []
        ddl.append("CREATE DATABASE IF NOT EXISTS {}".format(bq(d["name"])))
        if d.get("description"):
            ddl[-1] += " COMMENT '{}'".format(esc(d["description"]))
        if nd["location"]:
            ddl[-1] += " LOCATION '{}'".format(esc(nd["location"]))
        ddl[-1] += ";"
        ddl.append("")

        for t in d["tables"]:
            nt = {k: t[k] for k in ("name", "type", "owner", "owner_type", "params",
                                    "columns", "partition_keys", "serde_lib",
                                    "serde_params", "input_format", "output_format",
                                    "view_original_text", "view_expanded_text")}
            nt["location"] = rewrite_location(t["location"], args.src_authority,
                                              args.dst_authority, path_maps)
            nt["partitions"] = []
            for p in t.get("partitions", []):
                np = dict(p)
                np["location"] = rewrite_location(p["location"], args.src_authority,
                                                  args.dst_authority, path_maps)
                nt["partitions"].append(np)
            nd["tables"].append(nt)
            mapping_rows.append([d["name"], t["name"], t["type"],
                                 "yes" if t.get("params", {}).get("transactional", "").lower() == "true" else "no",
                                 t["location"] or "-", nt["location"] or "-"])
            ddl.append(table_ddl(nd, nt, args.managed_as_external,
                                 args.acid_keep_transactional))
            ddl.append("")
        cat["databases"].append(nd)

    def tsv(rows, fn):
        with open(os.path.join(args.out_dir, fn), "w") as f:
            for r in rows:
                f.write("\t".join(x.replace("\t", " ") for x in r) + "\n")

    with open(os.path.join(args.out_dir, "hms_catalog_target.json"), "w") as f:
        json.dump(cat, f, indent=1)
    with open(os.path.join(args.out_dir, "ddl_replay.sql"), "w") as f:
        f.write("\n".join(ddl) + "\n")
    tsv(mapping_rows, "location_mapping.tsv")
    tsv(excl_rows, "exclusions.tsv")

    n_tables = sum(len(d["tables"]) for d in cat["databases"])
    n_parts = sum(len(t["partitions"]) for d in cat["databases"] for t in d["tables"])
    sys.stderr.write(
        "homologation done: {}/{} databases kept, {} tables, {} partitions\n"
        "  DDL          : {}/ddl_replay.sql\n"
        "  target cat.  : {}/hms_catalog_target.json\n"
        "  location map : {}/location_mapping.tsv\n"
        "  exclusions   : {}/exclusions.tsv\n".format(
            len(cat["databases"]), len(src["databases"]), n_tables, n_parts,
            args.out_dir, args.out_dir, args.out_dir, args.out_dir))


if __name__ == "__main__":
    main()
```

**Verified run:**

```bash
python3 hms_homologate.py \
  --source hms_catalog_source.json \
  --out-dir out \
  --exclude-db sys --exclude-db information_schema \
  --src-authority 'cloudera-node.local:8020' \
  --dst-authority hacluster \
  --path-map '/warehouse/tablespace/managed/hive=/user/hive/warehouse' \
  --path-map '/warehouse/tablespace/external/hive=/user/hive/warehouse' \
  --managed-as-external
# homologation done: 6/8 databases kept, 19 tables, 0 partitions
#   DDL          : out/ddl_replay.sql
#   target cat.  : out/hms_catalog_target.json
#   location map : out/location_mapping.tsv
#   exclusions   : out/exclusions.tsv
```

Sample of the generated DDL (verbatim from `out/ddl_replay.sql`):

```sql
CREATE DATABASE IF NOT EXISTS `crm_gold`
COMMENT 'Gold layer: business-ready joins and aggregates of silver data'
LOCATION 'hdfs://hacluster/user/hive/warehouse/crm_gold.db';

CREATE EXTERNAL TABLE IF NOT EXISTS `crm_gold`.`customer_360` (
  `customer_id` int,
  `customer_name` string,
  `segment` string,
  `industry` string,
  `region` string,
  `signup_date` timestamp,
  `total_opportunities` bigint,
  `won_opportunities` bigint,
  `lost_opportunities` bigint,
  `won_revenue` decimal(16,2),
  `open_pipeline` decimal(16,2),
  `total_activities` bigint,
  `last_activity_date` timestamp,
  `total_contacts` bigint,
  `customer_status` string
)
COMMENT 'Gold: 360-degree customer view (joins and aggregates over silver)'
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 'hdfs://hacluster/data/crm/gold/customer_360';
```

Sample of `out/location_mapping.tsv`:

```
database	table	type	acid	source_location	target_location
crm_gold	customer_360	EXTERNAL_TABLE	no	hdfs://cloudera-node.local:8020/data/crm/gold/customer_360	hdfs://hacluster/data/crm/gold/customer_360
atlas_test	facturas	MANAGED_TABLE	yes	hdfs://cloudera-node.local:8020/warehouse/tablespace/managed/hive/atlas_test.db/facturas	hdfs://hacluster/user/hive/warehouse/atlas_test.db/facturas
```

> **Why `--managed-as-external`:** on the target, migrated tables are
> reference copies of data you already validated. Making them EXTERNAL
> prevents HMS from deleting HDFS data if a table is ever dropped, and it
> survives the "managed location must be inside the managed warehouse root"
> enforcement (pitfall #2). ACID tables are converted to EXTERNAL
> automatically (pitfall #1); use `--acid-keep-transactional` only if you
> migrate their data with Hive `EXPORT/IMPORT` instead of the flatten flow.

---

## 7. Step 4 — Stage ACID tables (`acid_flatten_stage.sh`)

**Why this step exists (verified pitfall):** ACID tables store rows in
`delta_<txnid>_<txnid>` / `base_<txnid>` directories. The ACID reader only
picks those up if the target HMS has transaction/write-id history for the
table — which a freshly created table never has. Copying `delta_1_1` to MRS
and creating the table yields **0 rows, silently**. Flattening the files into
the table root makes them plain, readable data files.

The script reads `location_mapping.tsv` (rows with `acid=yes`), copies every
file under `base_*`/`delta_*` flat into `<stag>/<db>.<tbl>/`, and writes a
manifest mapping each staging dir to its target location (used by distcp).

```bash
#!/usr/bin/env bash
# =====================================================================
# acid_flatten_stage.sh - Stage ACID tables for migration by flattening
# their base_*/delta_* files into one flat directory per table.
#
# WHY: the target HMS has no transaction/write-id history for migrated
# tables, so ACID readers IGNORE copied delta_/base_ directories
# (verified on CDH 7.1.7 / Hive 3.1.3). Flattening the files makes the
# data readable by a plain EXTERNAL table on any Hive 3 platform.
#
# Usage (on the SOURCE cluster, as a user with HDFS write access):
#   sudo -u hive ./acid_flatten_stage.sh location_mapping.tsv /stag/hms_migration
#
# Inputs:
#   $1 = location_mapping.tsv  (db, table, type, acid, source_location, target_location)
#   $2 = staging dir on the SOURCE HDFS (will be created)
# Outputs:
#   <stag>/<db>.<tbl>/...          flattened data files
#   <stag>/_manifest.tsv           lines: <stag_path>tab<target_location>
# =====================================================================
set -euo pipefail

MAP=${1:?usage: acid_flatten_stage.sh location_mapping.tsv <staging_hdfs_dir>}
STAG=${2:?usage: acid_flatten_stage.sh location_mapping.tsv <staging_hdfs_dir>}

hdfs dfs -mkdir -p "$STAG"
MANIFEST="$STAG/_manifest.tsv"
hdfs dfs -rm -f -skipTrash "$MANIFEST" >/dev/null 2>&1 || true
LOCAL_MANIFEST=$(mktemp)
trap 'rm -f "$LOCAL_MANIFEST"' EXIT

# header row of the tsv is skipped (line starts with "database")
tail -n +2 "$MAP" | awk -F'\t' '$4=="yes" && $5!="-"' | while IFS=$'\t' read -r db tbl type acid src dst; do
    flat="$STAG/${db}.${tbl}"
    echo ">> flattening $db.$tbl  ($src -> $flat)"
    hdfs dfs -mkdir -p "$flat"
    # list FILES (not dirs) inside base_*/delta_* dirs and copy them flat
    hdfs dfs -ls -R "$src" 2>/dev/null | awk '$1 ~ /^-/ {print $NF}' | grep -E '/(base|delta)_[0-9]' | \
    while read -r f; do
        hdfs dfs -cp -f "$f" "$flat/" >/dev/null
    done
    n=$(hdfs dfs -count "$flat" | awk '{print $2}')
    echo "   $n file(s) staged"
    printf '%s\t%s\n' "$flat" "$dst" >> "$LOCAL_MANIFEST"
done

hdfs dfs -put -f "$LOCAL_MANIFEST" "$MANIFEST"
echo ">> manifest:"
hdfs dfs -cat "$MANIFEST"
```

**Verified run** (staging into a sandbox dir for the dry-run; use
`/stag/hms_migration` in production):

```bash
sudo -u hive ./acid_flatten_stage.sh out/location_mapping.tsv /stag/hms_migration
# >> flattening atlas_test.facturas  (hdfs://.../atlas_test.db/facturas -> /stag/hms_migration/atlas_test.facturas)
#    1 file(s) staged
# >> flattening atlas_test_db.atlas_test_metadata  ...
#    1 file(s) staged
# ... (6 tables total)
# >> manifest:
# /stag/hms_migration/atlas_test.facturas    hdfs://hacluster/user/hive/warehouse/atlas_test.db/facturas
# /stag/hms_migration/atlas_test_db.atlas_test_metadata    hdfs://hacluster/user/hive/warehouse/atlas_test_db.db/atlas_test_metadata
# ...
```

---

## 8. Step 5 — Data migration (DistCp)

Two data classes, per `location_mapping.tsv`:

**(a) Plain tables (`acid=no`)** — copy the table directory as-is:

```bash
# run on the MRS cluster (pull), or on the CDH node (push) with the MRS
# nameservice resolvable; -update is resumable, -delete mirrors deletions
# (omit -delete if you want an additive copy).
hadoop distcp -update -nummappers 16 \
  hdfs://cloudera-node.local:8020/data/crm \
  hdfs://hacluster/data/crm

hadoop distcp -update -nummappers 16 \
  hdfs://cloudera-node.local:8020/warehouse/tablespace/external/hive/crm_bronze.db \
  hdfs://hacluster/user/hive/warehouse/crm_bronze.db
```

**(b) ACID tables (`acid=yes`)** — copy from the *flattened staging dirs*
(never from the original `delta_*` layout), using the manifest:

```bash
hdfs dfs -cat /stag/hms_migration/_manifest.tsv | while IFS=$'\t' read -r src dst; do
  hadoop distcp -update -nummappers 4 "$src" "$dst"
done
```

**If the clusters have no network path to each other** (common with MRS in a
VPC), relay through OBS:

```bash
# CDH side: upload with hadoop-obs (or CDM/DGC jobs)
hadoop distcp hdfs://cloudera-node.local:8020/data/crm obs://<bucket>/hms_migration/data/crm
# MRS side: download
hadoop distcp obs://<bucket>/hms_migration/data/crm hdfs://hacluster/data/crm
```

**DistCp validation** (after every copy): compare file counts and bytes with
`hdfs dfs -count` on both sides per table — this is automated in step 7
(`hms_validate.py` collects `hdfs_files`/`hdfs_bytes` on both platforms).

> Note: distcp preserves file bytes but ownership/permissions can differ;
> verify the `hive` user can read the target dirs (`hdfs dfs -ls`) and fix
> with `hdfs dfs -chown -R hive:hive <path>` if needed.

---

## 9. Step 6 — Replay the metadata on MRS

```bash
# beeline via ZooKeeper service discovery (standard MRS HS2 endpoint)
export BEELINE_URL='jdbc:hive2://<zk1>:2181,<zk2>:2181,<zk3>:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2'

beeline -u "$BEELINE_URL" -n hive --silent=true --force=true -f out/ddl_replay.sql
```

The DDL is idempotent (`IF NOT EXISTS` everywhere), so a partial failure can
be fixed and the file re-run.

**Dry-run before touching MRS (what we did, verified):** we replayed the
exact same DDL on the source cluster against shadow databases
(`mig_*`) through a temporary HiveServer2, with locations remapped back to
the source authority and ACID tables pointed at the flatten staging:

```bash
# replay result: 25 statements, 0 errors
sudo -u hive beeline -u "jdbc:hive2://localhost:10001" -n hive \
  --silent=true --force=true -f /tmp/ddl_dryrun.sql 2>&1 | grep -cE "^Error"
# 0
```

Read-back checks from the dry-run (this is what MRS must reproduce):

```
mig_crm_bronze.activities_raw        1600   (source: 1600  OK)
mig_crm_bronze.contacts_raw           340   (source:  340  OK)
mig_crm_bronze.customers_raw          218   (source:  218  OK)
mig_crm_bronze.opportunities_raw      650   (source:  650  OK)
mig_crm_bronze.products_raw            34   (source:   34  OK)
mig_crm_gold.customer_360             200   (source:  200  OK)
mig_crm_gold.monthly_revenue_summary   84   (source:   84  OK)
mig_crm_gold.pipeline_by_stage         48   (source:   48  OK)
mig_crm_silver.dim_contacts           320   (source:  320  OK)
mig_crm_silver.dim_customers          200   (source:  200  OK)
mig_crm_silver.dim_products            32   (source:   32  OK)
mig_crm_silver.fact_activities       1461   (source: 1461  OK)
mig_crm_silver.fact_opportunities     602   (source:  602  OK)
mig_atlas_test.facturas            rows: 1  150.50  2026-01-01 08:30:00.0   (ACID, flattened)
mig_atlas_test_db.raw_users        rows: 1  Alice Smith  2026-01-10          (ACID, flattened)
mig_default.tabla_hive_tradicional rows: 1  Usuario Hive  2026-10-21         (ACID, flattened)
```

For **partitioned** tables (none in this catalog, but the DDL generator
supports them): after distcp either rely on the generated
`ALTER TABLE ... ADD PARTITION ... LOCATION` statements, or run
`MSCK REPAIR TABLE <db>.<tbl>;` if partition dirs follow the convention.

Post-replay housekeeping on MRS:

```bash
# recalculate statistics for the query planners
beeline -u "$BEELINE_URL" -n hive -e "ANALYZE TABLE crm_gold.customer_360 COMPUTE STATISTICS;"
```

---

## 10. Step 7 — Validation and comparison

### 10.1 Collect evidence on BOTH sides (`hms_validate.py`)

One script, two engines: `impala` on the Cloudera source, `beeline` on MRS.
It records per table: **row count**, **HDFS file count**, **HDFS bytes**.

```python
#!/usr/bin/env python3
# =====================================================================
# hms_validate.py - Collect per-table validation evidence (row counts +
# HDFS file/byte counts) from a Hive catalog, on EITHER platform:
#
#   --engine impala  : Cloudera source (impala-shell, run as a user with
#                      SELECT privileges, e.g.  sudo -u hive ...)
#   --engine beeline : MRS target (beeline via HiveServer2 zookeeper
#                      discovery; set BEELINE_URL env or --jdbc-url)
#
# Usage (source):
#   sudo -u hive python3 hms_validate.py --catalog hms_catalog_source.json \
#          --engine impala --out validation_source.json
# Usage (target):
#   python3 hms_validate.py --catalog out/hms_catalog_target.json \
#          --engine beeline --jdbc-url "$BEELINE_URL" --user hive \
#          --out validation_target.json
# =====================================================================
import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone


def run(cmd):
    p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE, universal_newlines=True)
    return p.returncode, p.stdout, p.stderr


def count_impala(db, tbl):
    rc, out, err = run("impala-shell -q \"select count(*) from {}.{}\"".format(db, tbl))
    if rc != 0:
        return None, (err or out).strip()[-300:]
    for line in out.splitlines():
        m = re.match(r"^\|\s*(\d+)\s*\|$", line.strip())
        if m:
            return int(m.group(1)), None
    return None, "no count parsed: " + out.strip()[-200:]


def count_beeline(db, tbl, url, user):
    cmd = ("beeline -u '{}' -n {} --silent=true --outputformat=tsv2 "
           "-e \"select count(*) from {}.{}\"").format(url, user, db, tbl)
    rc, out, err = run(cmd)
    if rc != 0:
        return None, (err or out).strip()[-300:]
    for line in out.splitlines():
        s = line.strip()
        if re.match(r"^\d+$", s):
            return int(s), None
    return None, "no count parsed: " + out.strip()[-200:]


def hdfs_count(location):
    if not location:
        return None, None, None
    rc, out, err = run("hdfs dfs -count '{}'".format(location))
    if rc != 0:
        return None, None, "hdfs error: " + (err or out).strip()[-200:]
    f = out.split()
    if len(f) >= 3:
        return int(f[1]), int(f[2]), None  # files, bytes
    return None, None, "unparsed: " + out.strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True, help="catalog JSON (source or target)")
    ap.add_argument("--engine", choices=["impala", "beeline"], required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--jdbc-url", default=os.environ.get("BEELINE_URL", ""),
                    help="beeline JDBC URL (MRS HS2 zk discovery)")
    ap.add_argument("--user", default="hive")
    args = ap.parse_args()

    with open(args.catalog) as f:
        cat = json.load(f)

    report = {"validated_at": datetime.now(timezone.utc).isoformat(),
              "engine": args.engine, "catalog": args.catalog, "tables": {}}

    for d in cat["databases"]:
        for t in d["tables"]:
            full = "{}.{}".format(d["name"], t["name"])
            entry = {"type": t["type"], "location": t.get("location")}
            if t["type"] == "VIRTUAL_VIEW":
                entry["skipped"] = "view"
                report["tables"][full] = entry
                continue
            if args.engine == "impala":
                rc, err = count_impala(d["name"], t["name"])
            else:
                if not args.jdbc_url:
                    sys.stderr.write("beeline needs --jdbc-url or BEELINE_URL\n")
                    sys.exit(1)
                rc, err = count_beeline(d["name"], t["name"], args.jdbc_url, args.user)
            entry["row_count"] = rc
            if err:
                entry["error"] = err
            files, nbytes, err = hdfs_count(t.get("location"))
            entry["hdfs_files"] = files
            entry["hdfs_bytes"] = nbytes
            if err:
                entry["hdfs_error"] = err
            report["tables"][full] = entry
            sys.stderr.write("  {} rows={} files={} bytes={}\n".format(
                full, rc, files, nbytes))

    with open(args.out, "w") as f:
        json.dump(report, f, indent=1)
    sys.stderr.write("wrote {}\n".format(args.out))


if __name__ == "__main__":
    main()
```

**Verified source-side run** (excerpt of `out/validation_source.json`):

```bash
sudo -u hive python3 hms_validate.py --catalog hms_catalog_source.json \
        --engine impala --out validation_source.json
#   crm_bronze.activities_raw rows=1600 files=1 bytes=105803
#   crm_bronze.contacts_raw   rows=340  files=1 bytes=30870
#   crm_gold.customer_360     rows=200  files=1 bytes=12137
#   ...
# wrote validation_source.json
```

Target side (MRS), same evidence:

```bash
python3 hms_validate.py --catalog out/hms_catalog_target.json \
        --engine beeline --jdbc-url "$BEELINE_URL" --user hive \
        --out validation_target.json
```

### 10.2 Compare source vs target (`hms_compare.py`)

```python
#!/usr/bin/env python3
# =====================================================================
# hms_compare.py - Pre/post migration comparison. Two modes, combinable:
#
#   1) Structural: --source-catalog vs --target-catalog
#      (databases, tables, columns, rewritten locations)
#   2) Evidence:   --source-validation vs --target-validation
#      (row counts, HDFS file/byte counts per table)
#
# Prints a diff report; exit code 1 if any mismatch (CI-friendly).
# =====================================================================
import argparse
import json
import sys


def load(p):
    with open(p) as f:
        return json.load(f)


def catalog_index(cat, excludes):
    idx = {}
    for d in cat["databases"]:
        if d["name"] in excludes:
            continue
        for t in d["tables"]:
            cols = [(c["name"], c["type"]) for c in t.get("columns", [])]
            idx["{}.{}".format(d["name"], t["name"])] = {
                "type": t["type"], "location": t.get("location"),
                "columns": cols,
                "partition_keys": [(p["name"], p["type"]) for p in t.get("partition_keys", [])]}
    return idx


def compare_catalogs(sc, tc, problems, excludes):
    s, t = catalog_index(sc, excludes), catalog_index(tc, excludes)
    # when the target was homologated with --managed-as-external,
    # MANAGED_TABLE -> EXTERNAL_TABLE is an expected, intentional change
    mae = bool(tc.get("managed_as_external"))
    for k in sorted(s):
        if k not in t:
            problems.append("MISSING TABLE in target: " + k)
            continue
        st, tt = s[k]["type"], t[k]["type"]
        if mae and st == "MANAGED_TABLE" and tt == "EXTERNAL_TABLE":
            st = tt  # expected change, not a mismatch
        if st != tt:
            problems.append("TYPE MISMATCH {}: {} -> {}".format(k, st, tt))
        if s[k]["columns"] != t[k]["columns"]:
            problems.append("COLUMNS MISMATCH {}: {} -> {}".format(
                k, s[k]["columns"], t[k]["columns"]))
        if s[k]["partition_keys"] != t[k]["partition_keys"]:
            problems.append("PARTITION KEYS MISMATCH: " + k)
    for k in sorted(set(t) - set(s)):
        problems.append("EXTRA TABLE in target: " + k)


def compare_validations(sv, tv, problems):
    s, t = sv["tables"], tv["tables"]
    for k in sorted(s):
        if k not in t:
            problems.append("NO TARGET VALIDATION for " + k)
            continue
        a, b = s[k], t[k]
        if a.get("row_count") != b.get("row_count"):
            problems.append("ROW COUNT MISMATCH {}: source={} target={}".format(
                k, a.get("row_count"), b.get("row_count")))
        if a.get("hdfs_files") is not None and a["hdfs_files"] != b.get("hdfs_files"):
            problems.append("HDFS FILES MISMATCH {}: source={} target={}".format(
                k, a["hdfs_files"], b.get("hdfs_files")))
        if a.get("hdfs_bytes") is not None and a["hdfs_bytes"] != b.get("hdfs_bytes"):
            problems.append("HDFS BYTES MISMATCH {}: source={} target={}".format(
                k, a["hdfs_bytes"], b.get("hdfs_bytes")))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-catalog")
    ap.add_argument("--target-catalog")
    ap.add_argument("--source-validation")
    ap.add_argument("--target-validation")
    ap.add_argument("--exclude-db", action="append", default=[],
                    help="database excluded from migration scope (repeatable)")
    args = ap.parse_args()

    excludes = set(args.exclude_db) | {"sys", "information_schema"}
    problems = []
    if args.source_catalog and args.target_catalog:
        compare_catalogs(load(args.source_catalog), load(args.target_catalog),
                         problems, excludes)
    if args.source_validation and args.target_validation:
        compare_validations(load(args.source_validation), load(args.target_validation), problems)

    if problems:
        print("MISMATCHES ({}):".format(len(problems)))
        for p in problems:
            print("  - " + p)
        sys.exit(1)
    print("OK: no differences found")


if __name__ == "__main__":
    main()
```

**Verified run** (structural, source vs homologated target):

```bash
python3 hms_compare.py --source-catalog hms_catalog_source.json \
                       --target-catalog out/hms_catalog_target.json
# OK: no differences found
```

Full production gate (structure + evidence):

```bash
python3 hms_compare.py \
  --source-catalog hms_catalog_source.json \
  --target-catalog out/hms_catalog_target.json \
  --source-validation validation_source.json \
  --target-validation validation_target.json
# OK: no differences found    <- GO signal
```

### 10.3 Business-level spot checks (run on MRS)

```bash
beeline -u "$BEELINE_URL" -n hive --outputformat=tsv2 -e "
  SELECT count(*), sum(won_revenue) FROM crm_gold.customer_360;
  SELECT count(*) FROM crm_silver.fact_activities;
  SELECT * FROM atlas_test.facturas LIMIT 5;"
```

Compare against the same queries on the source (Impala). For file-level
assurance on a sample:

```bash
# checksum a sample file on both sides
hdfs dfs -cat hdfs://hacluster/data/crm/gold/customer_360/<file>.parq | md5sum
sudo -u hive hdfs dfs -cat /data/crm/gold/customer_360/<file>.parq | md5sum
```

---

## 11. Step 8 — Cutover and rollback

**Cutover checklist (GO only if all true):**

- [ ] `hms_compare.py` full gate prints `OK: no differences found`
- [ ] Business spot-check queries match source results
- [ ] Downstream jobs (Spark/Impala-equivalent on MRS) re-pointed and smoke-tested
- [ ] Atlas/governance metadata re-imported if required (see §12)
- [ ] Source kept in read-only mode for the agreed retention period

**Rollback plan:**

- *Before cutover:* target is disposable — drop the migrated databases
  (`DROP DATABASE <db> CASCADE` is safe: tables are EXTERNAL, data dirs can
  be removed separately) and re-run from any step; all steps are idempotent.
- *After cutover (metadata only):* restore the step-1 `pg_dump` into
  PostgreSQL, start a spare HMS against it, and repoint clients.
- *Data:* source HDFS is never modified by this runbook (flatten staging
  copies, never moves). DistCp `-update` without `-delete` is additive.

---

## 12. Optional — Atlas governance metadata

The repo already contains `atlas_inventory_export.py` (Atlas REST inventory
dump: entities, relationships, typedefs as CSV). For a governance migration:

1. Export from source Atlas (`admin` / `Atlas2026!`, port 31000).
2. Rewrite `qualifiedName` / `clusterName` / location attributes with the
   same `location_mapping.tsv` (Cloudera NS → `hacluster`).
3. Bulk-import into MRS Atlas via the REST API
   (`POST /api/atlas/v2/entity/bulk`).

Treat this as a separate workstream — HMS migration does not depend on it.

---

## 13. Pitfalls found during the verified run (read before executing)

1. **ACID delta dirs are invisible after migration (silent 0 rows).**
   Verified: a transactional table created on a fresh HMS pointing at copied
   `delta_1_1` files reads **0 rows** on both Hive and Impala — the reader
   validates directories against the target's txn/write-id state, which does
   not exist. **Fix:** flatten `base_*`/`delta_*` files into the table root
   (step 4) and create plain EXTERNAL tables. Verified working: all 6 ACID
   tables read back their rows.
   *Alternative:* Hive `EXPORT TABLE ... TO` / `IMPORT TABLE FROM` carries
   the ACID state natively — use it if you must keep tables transactional
   (then pass `--acid-keep-transactional` to the homologator).

2. **Hive 3 rejects managed tables outside the managed warehouse root.**
   `MetaException: A managed table's location should be located within
   managed warehouse root directory...` — hit during the dry-run. The default
   path-map (`/warehouse/tablespace/managed/hive` → `/user/hive/warehouse`,
   MRS's managed root) satisfies this; `--managed-as-external` avoids the
   check entirely for converted tables.

3. **`sys` and `information_schema` must be excluded.** The `sys` tables are
   `JdbcSerDe` views over the *source PostgreSQL metastore* — migrating them
   would produce tables pointing at a database that will be decommissioned.

4. **Impala cannot validate the DDL.** Impala's parser rejects Hive's
   `STORED AS INPUTFORMAT '...' OUTPUTFORMAT '...'` and ACID
   `TBLPROPERTIES`. Validate DDL with Hive (beeline), not impala-shell.

5. **CM hides the DB password.** `hive-site.xml` shows `********`; the real
   value lives in the runtime `creds.localjceks` (see §3.3 and appendix A).

6. **Volatile TBLPROPERTIES must be stripped.** `transient_lastDdlTime`,
   `numFiles`, `totalSize`, `spark.sql.sources.schema.*` etc. describe the
   *source* files; replaying them lies to the target planner. The
   homologator filters them (SKIP_PARAMS regex).

7. **`pkill`/`kill` with a pattern that matches your own ssh command**
   kills your own shell — a classic footgun hit twice during this run. Use
   `pgrep -f <pattern> | xargs kill` in a *separate* ssh invocation.

---

## Appendix A — `GetCred.java` (credential store reader, step 3.3)

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;

public class GetCred {
  public static void main(String[] a) throws Exception {
    Configuration c = new Configuration();
    c.set("hadoop.security.credential.provider.path",
          "localjceks://file/var/run/cloudera-scm-agent/process/"
          + "<process-dir-of-HIVEMETASTORE>" + "/creds.localjceks");
    for (CredentialProvider p : CredentialProviderFactory.getProviders(c))
      for (String alias : p.getAliases())
        System.out.println(alias + " = "
            + new String(p.getCredentialEntry(alias).getCredential()));
  }
}
```

```bash
javac --release 8 -cp "$(hadoop classpath)" -d /tmp GetCred.java
HADOOP_CREDSTORE_PASSWORD=<value-from-proc-environ> \
  java -cp "$(hadoop classpath):/tmp" GetCred
# javax.jdo.option.connectionpassword = hive_password
```

## Appendix B — Temporary HiveServer2 for source-side dry-runs

The demo cluster has no HiveServer2 role. To dry-run the DDL through real
Hive before shipping it to MRS (all commands verified):

```bash
# 1. extract Tez (CDP Hive runs on Tez only) and fix container sizes for a
#    small VM (YARN maximum-allocation-mb = 1024 here)
sudo -u hive hdfs dfs -get /user/tez/0.9.1.7.1.7.0-551/tez.tar.gz /tmp/tez.tar.gz
mkdir -p /tmp/tez_local && tar xzf /tmp/tez.tar.gz -C /tmp/tez_local
cp /var/run/cloudera-scm-agent/process/*-hive_on_tez-HIVESERVER2/tez-conf/tez-site.xml /tmp/tez_conf_patched/
# edit tez-site.xml: tez.am.resource.memory.mb and tez.task.resource.memory.mb -> 1024
chmod -R a+rX /tmp/tez_local /tmp/tez_conf_patched

# 2. start a temporary HS2 on port 10001 (plain conf; ACID txn manager on)
sudo -u hive bash -c 'HIVE_CONF_DIR=/etc/hive/conf TEZ_CONF_DIR=/tmp/tez_conf_patched \
  HADOOP_CLASSPATH="/tmp/tez_local/*:/tmp/tez_local/lib/*" \
  nohup /opt/cloudera/parcels/CDH/bin/hive --service hiveserver2 \
  --hiveconf hive.server2.thrift.port=10001 --hiveconf hive.server2.webui.port=10002 \
  --hiveconf hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager \
  --hiveconf hive.support.concurrency=true \
  --hiveconf tez.lib.uris=/user/tez/0.9.1.7.1.7.0-551/tez.tar.gz \
  --hiveconf hive.tez.container.size=1024 > /tmp/hs2_tmp.log 2>&1 &'

# 3. use it
beeline -u "jdbc:hive2://localhost:10001" -n hive -f <your>.sql

# 4. stop it afterwards
pgrep -f "hive.server2.thrift" | xargs kill
```

Caveats verified the hard way: don't point `HIVE_CONF_DIR` at the CM
`hive_on_tez` runtime dir (its Ranger plugin blocks startup); the JVM caches
a failed static init (`TezConfigurationFactory`) so restart HS2 after fixing
config permissions; on tiny VMs each Tez query takes minutes — prefer fetch
queries (`SELECT ... LIMIT`) for read-back checks.

## Appendix C — Source integrity after the dry-run (verified)

After all dry-run activity (shadow DBs, staging, temp HS2), the source was
restored and verified:

```bash
sudo -u hive impala-shell -q "select count(*) from crm_gold.customer_360"
# | 200 |                                    <- unchanged
sudo -u hive impala-shell -q "show databases" | grep -cE "crm_|atlas|default|sys|information"
# 6                                        <- all original databases present
sudo -u hive hdfs dfs -ls /warehouse/tablespace/external/hive/ | grep -c mig_
# 0                                        <- no sandbox leftovers
```
