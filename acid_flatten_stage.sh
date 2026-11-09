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
