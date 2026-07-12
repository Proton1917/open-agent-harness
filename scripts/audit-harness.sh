#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"

if [[ "$(basename "$root")" != "open-agent-harness" ]]; then
  echo 'project_directory_name_valid=false' >&2
  exit 1
fi
echo 'project_directory_name_valid=true'

rust_files="$(find "$root/src" "$root/tests" -type f -name '*.rs' | wc -l | tr -d ' ')"
rust_lines="$(find "$root/src" "$root/tests" -type f -name '*.rs' -print0 | xargs -0 wc -l | tail -1 | awk '{print $1}')"

printf 'rust_files=%s\n' "$rust_files"
printf 'rust_lines=%s\n' "$rust_lines"

scan_paths=(
  "$root/Cargo.toml"
  "$root/Cargo.lock"
  "$root/src"
  "$root/tests"
  "$root/scripts"
  "$root/.cargo"
  "$root/.github"
  "$root/CHANGELOG.md"
  "$root/MIGRATION.md"
)

non_rust_runtime="$(find "$root/src" -type f ! -name '*.rs' -print -quit)"
if [[ -n "$non_rust_runtime" ]]; then
  printf 'non_rust_runtime_file=%s\n' "$non_rust_runtime" >&2
  exit 1
fi
echo 'runtime_is_rust=true'

if ! rg -q 'rustflags = \["-D", "warnings"\]' "$root/.cargo/config.toml" \
  || ! rg -q 'RUSTFLAGS: -D warnings' "$root/.github/workflows/ci.yml"; then
  echo 'warnings_are_errors=false' >&2
  exit 1
fi
echo 'warnings_are_errors=true'

if rg -n \
  -e '#!?\[allow\([^]]*(warnings|dead_code|unused(_[a-z_]+)?|clippy::(all|correctness|suspicious))[^]]*\)\]' \
  -e 'todo!\(' \
  -e 'unimplemented!\(' \
  -e 'dbg!\(' \
  -e '\b(TODO|FIXME|XXX)\b' \
  "$root/src"; then
  echo 'source_quality_shortcut_free=false' >&2
  exit 1
fi
if rg -n \
  -e '-A[[:space:]]*warnings' \
  -e '--cap-lints[=[:space:]]*allow' \
  "$root/.cargo" "$root/.github" "$root/scripts"; then
  echo 'warning_bypass_free=false' >&2
  exit 1
fi
echo 'source_quality_shortcut_free=true'
echo 'warning_bypass_free=true'

removed_terms=('cl''aude' 'anth''ropic')
release_binary="$root/target/release/open-agent-harness"
if [[ ! -x "$release_binary" ]]; then
  echo 'release_binary_present=false' >&2
  exit 1
fi
if find "$root/Cargo.toml" "$root/Cargo.lock" "$root/src" -newer "$release_binary" -print -quit \
  | rg -q .; then
  echo 'release_binary_current=false' >&2
  exit 1
fi
echo 'release_binary_present=true'
echo 'release_binary_current=true'

for term in "${removed_terms[@]}"; do
  if rg -n -i "$term" "${scan_paths[@]}"; then
    echo 'brand_free=false' >&2
    exit 1
  fi
  if strings "$release_binary" | rg -q -i "$term"; then
    echo 'binary_brand_free=false' >&2
    exit 1
  fi
done

echo 'brand_free=true'
echo 'binary_brand_free=true'

if [[ -n "$(git -C "$root" ls-files reference)" ]]; then
  echo 'reference_untracked=false' >&2
  exit 1
fi
echo 'reference_untracked=true'

if [[ -n "$(git -C "$root" ls-files 'AGENTS.md' '**/AGENTS.md')" ]] \
  || ! git -C "$root" check-ignore --no-index -q AGENTS.md; then
  echo 'agents_instructions_ignored=false' >&2
  exit 1
fi
echo 'agents_instructions_ignored=true'

reference_git=""
if [[ -d "$root/reference" ]]; then
  reference_git="$(find "$root/reference" -mindepth 2 -maxdepth 2 -type d -name .git -print -quit)"
fi
if [[ -n "$reference_git" ]]; then
  reference_root="$(dirname "$reference_git")"
  if [[ -n "$(git -C "$reference_root" status --short)" ]]; then
    echo 'reference_clean=false' >&2
    exit 1
  fi
fi
echo 'reference_clean=true'

if rg -n 'https?://' "$root/src" \
  | rg -v -e 'http://127\.0\.0\.1:8080' -e 'https?://[^[:space:]\"]*\.invalid'; then
  echo 'hardcoded_remote_endpoint_free=false' >&2
  exit 1
fi
echo 'hardcoded_remote_endpoint_free=true'

sensitive_runtime_terms=(
  'event_logging'
  'growthbook'
  'datadog'
  'telemetry'
  'device_id'
  'machine_id'
  'account_uuid'
  'organization_uuid'
)
for term in "${sensitive_runtime_terms[@]}"; do
  if rg -n -i "$term" "$root/src"; then
    echo 'hidden_metadata_free=false' >&2
    exit 1
  fi
done
echo 'hidden_metadata_free=true'

while IFS= read -r tracked; do
  [[ -f "$root/$tracked" ]] || continue
  size="$(wc -c < "$root/$tracked" | tr -d ' ')"
  if (( size > 1048576 )); then
    printf 'tracked_file_too_large=%s:%s\n' "$tracked" "$size" >&2
    exit 1
  fi
done < <(git -C "$root" ls-files)
echo 'tracked_files_open_source_sized=true'
