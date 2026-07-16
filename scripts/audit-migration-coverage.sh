#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
manifest="$root/docs/MIGRATION_COVERAGE.tsv"
family_inventory="$root/docs/MIGRATION_FAMILY_INVENTORY.tsv"
family_categories="$root/docs/MIGRATION_FAMILY_CATEGORIES.tsv"
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
for required_manifest in "$manifest" "$family_inventory" "$family_categories"; do
  [[ -f "$required_manifest" ]] || {
    printf 'migration_coverage_manifest_missing=%s\n' "${required_manifest#$root/}" >&2
    exit 1
  }
done

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

awk -F '\t' '
  /^#/ || NF == 0 { next }
  NF != 6 { printf "invalid_manifest_line=%d fields=%d\n", NR, NF > "/dev/stderr"; bad=1 }
  $1 !~ /^(tool|command|service|native)$/ { printf "invalid_manifest_kind=%s line=%d\n", $1, NR > "/dev/stderr"; bad=1 }
  $3 !~ /^(implemented|equivalent|excluded|pending)$/ { printf "invalid_manifest_status=%s line=%d\n", $3, NR > "/dev/stderr"; bad=1 }
  seen[$1 SUBSEP $2]++ > 0 { printf "duplicate_manifest_source=%s:%s\n", $1, $2 > "/dev/stderr"; bad=1 }
  END { exit bad }
' "$manifest"

awk -F '\t' '
  /^#/ || NF == 0 { next }
  NF != 3 { printf "invalid_family_inventory_line=%d fields=%d\n", NR, NF > "/dev/stderr"; bad=1 }
  $1 !~ /^(utility|hook|component)$/ { printf "invalid_family_inventory_kind=%s line=%d\n", $1, NR > "/dev/stderr"; bad=1 }
  $2 == "" { printf "empty_family_inventory_source line=%d\n", NR > "/dev/stderr"; bad=1 }
  $3 !~ /^[a-z0-9][a-z0-9-]*$/ { printf "invalid_family_inventory_category=%s line=%d\n", $3, NR > "/dev/stderr"; bad=1 }
  seen[$1 SUBSEP $2]++ > 0 { printf "duplicate_family_inventory_source=%s:%s\n", $1, $2 > "/dev/stderr"; bad=1 }
  END { exit bad }
' "$family_inventory"

awk -F '\t' '
  /^#/ || NF == 0 { next }
  NF != 5 { printf "invalid_family_category_line=%d fields=%d\n", NR, NF > "/dev/stderr"; bad=1 }
  $1 !~ /^[a-z0-9][a-z0-9-]*$/ { printf "invalid_family_category=%s line=%d\n", $1, NR > "/dev/stderr"; bad=1 }
  $2 !~ /^(implemented|equivalent|excluded|pending)$/ { printf "invalid_family_category_status=%s line=%d\n", $2, NR > "/dev/stderr"; bad=1 }
  seen[$1]++ > 0 { printf "duplicate_family_category=%s\n", $1 > "/dev/stderr"; bad=1 }
  END { exit bad }
' "$family_categories"

awk -F '\t' '
  NR == FNR {
    if ($0 !~ /^#/ && NF != 0) categories[$1]=1
    next
  }
  $0 !~ /^#/ && NF != 0 && !($3 in categories) {
    printf "unknown_family_category=%s:%s:%s\n", $1, $2, $3 > "/dev/stderr"
    bad=1
  }
  END { exit bad }
' "$family_categories" "$family_inventory"

awk -F '\t' '$0 !~ /^#/ && NF != 0 { print $1 }' "$family_categories" | sort -u > "$tmp/declared-family-categories"
awk -F '\t' '$0 !~ /^#/ && NF != 0 { print $3 }' "$family_inventory" | sort -u > "$tmp/used-family-categories"
if ! comm -3 "$tmp/declared-family-categories" "$tmp/used-family-categories" > "$tmp/family-category-diff" \
  || [[ -s "$tmp/family-category-diff" ]]; then
  sed 's/^/family_category_usage_mismatch=/' "$tmp/family-category-diff" >&2
  exit 1
fi

audit_source_family() {
  local family="$1"
  local directory="$2"
  local label="$3"
  awk -F '\t' -v family="$family" '$1 == family { print $2 }' "$family_inventory" \
    | sort -u > "$tmp/expected-$family"
  if [[ -d "$directory" ]]; then
    find "$directory" -mindepth 1 -maxdepth 1 \( -type d -o -type f \) -exec basename {} \; \
      | sed -E 's/\.(ts|tsx|js|jsx)$//' \
      | sort -u > "$tmp/actual-$family"
    if ! comm -3 "$tmp/expected-$family" "$tmp/actual-$family" > "$tmp/$family-diff" \
      || [[ -s "$tmp/$family-diff" ]]; then
      sed "s/^/${family}_inventory_mismatch=/" "$tmp/$family-diff" >&2
      exit 1
    fi
    printf 'source_%s_inventory_complete=true\n' "$label"
  else
    printf 'source_%s_inventory=reference_not_present\n' "$label"
  fi
}

audit_source_family utility "$root/reference/source-snapshot/src/utils" utility
audit_source_family hook "$root/reference/source-snapshot/src/hooks" hook
audit_source_family component "$root/reference/source-snapshot/src/components" component

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

awk -F '\t' '$1 == "service" { print $2 }' "$manifest" | sort -u > "$tmp/expected-services"
services="$root/reference/source-snapshot/src/services"
if [[ -d "$services" ]]; then
  find "$services" -mindepth 1 -maxdepth 1 \( -type d -o -type f \) -exec basename {} \; \
    | sed 's/\.tsx$//; s/\.ts$//' \
    | sort -u > "$tmp/actual-services"
  if ! comm -3 "$tmp/expected-services" "$tmp/actual-services" > "$tmp/service-diff" \
    || [[ -s "$tmp/service-diff" ]]; then
    sed 's/^/service_manifest_mismatch=/' "$tmp/service-diff" >&2
    exit 1
  fi
  echo 'source_service_inventory_complete=true'
else
  echo 'source_service_inventory=reference_not_present'
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

while IFS=$'\t' read -r category status implementation tests note; do
  [[ -n "$category" && "${category:0:1}" != '#' ]] || continue
  if [[ "$status" == implemented || "$status" == equivalent ]]; then
    for field in "$implementation" "$tests"; do
      IFS=';' read -r -a paths <<< "$field"
      for path in "${paths[@]}"; do
        [[ -n "$path" && "$path" != '-' && -e "$root/$path" ]] || {
          printf 'migration_family_evidence_missing=%s:%s\n' "$category" "$path" >&2
          exit 1
        }
      done
    done
  fi
  [[ -n "$note" ]] || {
    printf 'migration_family_note_missing=%s\n' "$category" >&2
    exit 1
  }
done < "$family_categories"

for status in implemented equivalent excluded pending; do
  count="$(awk -F '\t' -v status="$status" '$3 == status { count++ } END { print count + 0 }' "$manifest")"
  printf 'migration_%s=%s\n' "$status" "$count"
  family_count="$(awk -F '\t' -v wanted="$status" '
    NR == FNR {
      if ($0 !~ /^#/ && NF != 0) category_status[$1]=$2
      next
    }
    $0 !~ /^#/ && NF != 0 && category_status[$3] == wanted { count++ }
    END { print count + 0 }
  ' "$family_categories" "$family_inventory")"
  printf 'migration_family_%s=%s\n' "$status" "$family_count"
done

pending="$(awk -F '\t' '$3 == "pending" { count++ } END { print count + 0 }' "$manifest")"
family_pending="$(awk -F '\t' '
  NR == FNR {
    if ($0 !~ /^#/ && NF != 0) category_status[$1]=$2
    next
  }
  $0 !~ /^#/ && NF != 0 && category_status[$3] == "pending" { count++ }
  END { print count + 0 }
' "$family_categories" "$family_inventory")"
if $strict && (( pending != 0 || family_pending != 0 )); then
  echo 'migration_tool_command_service_native_strict_complete=false' >&2
  echo 'migration_source_families_strict_complete=false' >&2
  exit 1
fi
echo 'migration_manifest_evidence_present=true'
if $strict; then
  echo 'migration_tool_command_service_native_strict_complete=true'
  echo 'migration_source_families_strict_complete=true'
fi
