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
#
# Usage:
#   python3 hms_compare.py --source-catalog hms_catalog_source.json \
#          --target-catalog out/hms_catalog_target.json \
#          --source-validation validation_source.json \
#          --target-validation validation_target.json
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
