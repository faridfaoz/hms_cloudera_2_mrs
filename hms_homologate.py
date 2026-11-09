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
