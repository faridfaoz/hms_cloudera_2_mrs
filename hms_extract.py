#!/usr/bin/env python3
# r""" placeholder - Extract the full HMS catalog from the Cloudera
# PostgreSQL metastore database into one portable JSON file.
#
# Runs ON the Cloudera node (needs psql + read access to the metastore
# DB). Python 3 stdlib only (uses psql \\copy CSV, no psycopg2 needed).
#
# Usage:
#   export HMS_PG_PASSWORD=<hive db password>     # see runbook step 2
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
    """Run a query through psql \\copy CSV and return a list of rows."""
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
