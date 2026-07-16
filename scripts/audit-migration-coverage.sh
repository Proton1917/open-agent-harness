#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
manifest="$root/docs/MIGRATION_COVERAGE.tsv"
strict=false
if [[ "${1:-}" == "--strict" ]]; then
  strict=true
elif [[ $# -ne 0 ]]; then
  echo 'usage: scripts/audit-migration-coverage.sh [--strict]' >&2
  exit 2
fi

for required in awk comm find sort tar; do
  if ! command -v "$required" >/dev/null 2>&1; then
    printf 'missing_migration_audit_dependency=%s\n' "$required" >&2
    exit 1
  fi
done
[[ -f "$manifest" ]] || {
  echo 'migration_coverage_manifest_present=false' >&2
  exit 1
}

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

awk -F '\t' '
  /^#/ || NF == 0 { next }
  NF != 6 { printf "invalid_manifest_line=%d fields=%d\n", NR, NF > "/dev/stderr"; bad=1 }
  $1 !~ /^(tool|command|native)$/ { printf "invalid_manifest_kind=%s line=%d\n", $1, NR > "/dev/stderr"; bad=1 }
  $3 !~ /^(implemented|equivalent|excluded|pending)$/ { printf "invalid_manifest_status=%s line=%d\n", $3, NR > "/dev/stderr"; bad=1 }
  seen[$1 SUBSEP $2]++ > 0 { printf "duplicate_manifest_source=%s:%s\n", $1, $2 > "/dev/stderr"; bad=1 }
  END { exit bad }
' "$manifest"

awk -F '\t' '$1 == "tool" { print $2 }' "$manifest" | sort -u > "$tmp/expected-tools"
snapshot="$root/reference/source-snapshot/src/tools"
if [[ -d "$snapshot" ]]; then
  find "$snapshot" -mindepth 1 -maxdepth 1 -type d \
    ! -name shared ! -name testing -exec basename {} \; \
    | sort -u > "$tmp/actual-tools"
  if ! comm -3 "$tmp/expected-tools" "$tmp/actual-tools" > "$tmp/tool-diff" \
    || [[ -s "$tmp/tool-diff" ]]; then
    sed 's/^/tool_manifest_mismatch=/' "$tmp/tool-diff" >&2
    exit 1
  fi
  echo 'source_tool_inventory_complete=true'
else
  echo 'source_tool_inventory=reference_not_present'
fi

awk -F '\t' '$1 == "command" { print $2 }' "$manifest" | sort -u > "$tmp/expected-commands"
commands="$root/reference/source-snapshot/src/commands"
if [[ -d "$commands" ]]; then
  find "$commands" -mindepth 1 -maxdepth 1 -type d -exec basename {} \; \
    | sort -u > "$tmp/actual-commands"
  if ! comm -3 "$tmp/expected-commands" "$tmp/actual-commands" > "$tmp/command-diff" \
    || [[ -s "$tmp/command-diff" ]]; then
    sed 's/^/command_manifest_mismatch=/' "$tmp/command-diff" >&2
    exit 1
  fi
  echo 'source_command_inventory_complete=true'
else
  echo 'source_command_inventory=reference_not_present'
fi

awk -F '\t' '$1 == "native" { print $2 }' "$manifest" | sort -u > "$tmp/expected-native"
archive="$(find "$root/reference" -maxdepth 1 -type f -name '*-code-2.1.207-reverse-engineering-20260712.tar.zst' -print -quit 2>/dev/null || true)"
if [[ -n "$archive" ]]; then
  tar --zstd -tf "$archive" \
    | sed -n 's#^decompiled/embedded/\$bunfs/root/\([^/]*\.node\)$#\1#p' \
    | sort -u > "$tmp/actual-native"
  if ! comm -3 "$tmp/expected-native" "$tmp/actual-native" > "$tmp/native-diff" \
    || [[ -s "$tmp/native-diff" ]]; then
    sed 's/^/native_manifest_mismatch=/' "$tmp/native-diff" >&2
    exit 1
  fi
  echo 'archive_native_inventory_complete=true'
else
  echo 'archive_native_inventory=reference_not_present'
fi

while IFS=$'\t' read -r kind source status implementation tests note; do
  [[ -n "$kind" && "${kind:0:1}" != '#' ]] || continue
  if [[ "$status" == implemented || "$status" == equivalent ]]; then
    for field in "$implementation" "$tests"; do
      IFS=';' read -r -a paths <<< "$field"
      for path in "${paths[@]}"; do
        [[ -n "$path" && "$path" != '-' && -e "$root/$path" ]] || {
          printf 'migration_evidence_missing=%s:%s:%s\n' "$kind" "$source" "$path" >&2
          exit 1
        }
      done
    done
  fi
  [[ -n "$note" ]] || {
    printf 'migration_note_missing=%s:%s\n' "$kind" "$source" >&2
    exit 1
  }
done < "$manifest"

for status in implemented equivalent excluded pending; do
  count="$(awk -F '\t' -v status="$status" '$3 == status { count++ } END { print count + 0 }' "$manifest")"
  printf 'migration_%s=%s\n' "$status" "$count"
done

pending="$(awk -F '\t' '$3 == "pending" { count++ } END { print count + 0 }' "$manifest")"
if $strict && (( pending != 0 )); then
  echo 'migration_tool_command_native_strict_complete=false' >&2
  exit 1
fi
echo 'migration_manifest_evidence_present=true'
if $strict; then
  echo 'migration_tool_command_native_strict_complete=true'
fi
