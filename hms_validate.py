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
