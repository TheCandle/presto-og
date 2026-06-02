#!/bin/bash

set -euo pipefail

# Add EXPLAIN ANALYZE to all TPCH SQLs.
# Special case for q15:
#   create view ...;
#   select ...;
#   drop view ...;
# We only add EXPLAIN ANALYZE before the SELECT statement.
for file in ./*.sql; do
  [ -f "$file" ] || continue

  if [ "$(basename "$file")" = "q15.sql" ] && rg -qi "^\s*create\s+view\b" "$file"; then
    tmp_file="${file}.tmp"
    awk '
      BEGIN {
        IGNORECASE = 1
        in_create = 0
        create_done = 0
        added = 0
      }
      {
        if (!create_done && $0 ~ /^[[:space:]]*create[[:space:]]+view\b/) {
          in_create = 1
        }
        if (in_create && $0 ~ /;[[:space:]]*$/) {
          in_create = 0
          create_done = 1
        }
        if (create_done && !added && $0 ~ /^[[:space:]]*select\b/) {
          print "EXPLAIN ANALYZE"
          added = 1
        }
        print
      }
      END {
        if (!added) {
          # fallback: keep file unchanged if pattern not found
        }
      }
    ' "$file" > "$tmp_file"
    mv "$tmp_file" "$file"
    continue
  fi

  if rg -qi "^\s*EXPLAIN\s+ANALYZE\b" "$file"; then
    continue
  fi

  tmp_file="${file}.tmp"
  {
    printf 'EXPLAIN ANALYZE\n'
    cat "$file"
  } > "$tmp_file"
  mv "$tmp_file" "$file"
done
