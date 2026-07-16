#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"

for required in cargo file git jq rg strings; do
  if ! command -v "$required" >/dev/null 2>&1; then
    printf 'missing_audit_dependency=%s\n' "$required" >&2
    exit 1
  fi
done
echo 'audit_dependencies_present=true'

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

non_rust_runtime="$(find "$root/src" -type f ! -name '*.rs' ! -name '.DS_Store' -print -quit)"
if [[ -n "$non_rust_runtime" ]]; then
  printf 'non_rust_runtime_file=%s\n' "$non_rust_runtime" >&2
  exit 1
fi
echo 'runtime_is_rust=true'

unexpected_core_sources="$({
  git -C "$root" ls-files \
    | rg '\.(py|pyi|js|jsx|ts|tsx|go|c|cc|cpp|cxx|swift|java|kt|kts|rb|php|lua)$' \
    | rg -v '^(scripts/|tests/fixtures/)' || true
})"
if [[ -n "$unexpected_core_sources" ]]; then
  printf 'unexpected_non_rust_core_sources=%s\n' "$unexpected_core_sources" >&2
  exit 1
fi

non_rust_code_lines=0
while IFS= read -r helper; do
  [[ -n "$helper" && -f "$root/$helper" ]] || continue
  lines="$(wc -l < "$root/$helper" | tr -d ' ')"
  non_rust_code_lines=$((non_rust_code_lines + lines))
done < <(
  git -C "$root" ls-files \
    | rg '\.(sh|bash|zsh|py|pyi|js|jsx|ts|tsx|go|c|cc|cpp|cxx|swift|java|kt|kts|rb|php|lua)$' || true
)
if (( rust_lines < non_rust_code_lines * 4 )); then
  printf 'rust_primary=false rust_lines=%s helper_lines=%s\n' "$rust_lines" "$non_rust_code_lines" >&2
  exit 1
fi
printf 'helper_code_lines=%s\n' "$non_rust_code_lines"
echo 'rust_primary=true'

if ! rg -q 'rustflags = \["-D", "warnings"\]' "$root/.cargo/config.toml" \
  || ! rg -q 'RUSTFLAGS: -D warnings' "$root/.github/workflows/ci.yml" \
  || ! rg -q 'rust-version = "1\.85"' "$root/Cargo.toml" \
  || ! rg -q 'cargo \+1\.85\.0 check --locked --all-targets' "$root/.github/workflows/ci.yml"; then
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
  "$root/src" "$root/tests"; then
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
if {
  find "$root/Cargo.toml" "$root/Cargo.lock" -newer "$release_binary" -print
  find "$root/src" -type f -name '*.rs' -newer "$release_binary" -print
} | rg -q .; then
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
  tracked_brand_hits="$(
    git -C "$root" grep -n -i "$term" -- . \
      ':(exclude)README.md' \
      ':(exclude)CONTRIBUTING.md' \
      ':(exclude)docs/MIGRATION_COVERAGE.tsv' \
      ':(exclude)docs/MIGRATION_FAMILY_INVENTORY.tsv' \
      ':(exclude)docs/MIGRATION_PROTOCOL_INVENTORY.tsv' \
      ':(exclude)docs/MIGRATION_SURFACE_INVENTORY.tsv' || true
  )"
  if [[ -n "$tracked_brand_hits" ]]; then
    printf 'brand_text_outside_documented_exceptions=%s\n' "$tracked_brand_hits" >&2
    exit 1
  fi
  if strings "$release_binary" | rg -q -i "$term"; then
    echo 'binary_brand_free=false' >&2
    exit 1
  fi
done

echo 'brand_free=true'
echo 'brand_exceptions_limited_to_readme_contributing_and_source_inventories=true'
echo 'binary_brand_free=true'

if [[ -n "$(git -C "$root" ls-files reference)" ]]; then
  echo 'reference_untracked=false' >&2
  exit 1
fi
echo 'reference_untracked=true'
if git -C "$root" rev-list --objects --all \
  | sed -n 's/^[0-9a-f][0-9a-f]* //p' \
  | rg -q '^reference/'; then
  echo 'reference_absent_from_history=false' >&2
  exit 1
fi
echo 'reference_absent_from_history=true'

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
    echo 'reference_nested_git_clean=false' >&2
    exit 1
  fi
  echo 'reference_nested_git_clean=true'
else
  echo 'reference_nested_git=not_present'
fi

reference_archive_name="${removed_terms[0]}-code-2.1.207-reverse-engineering-20260712.tar.zst"
reference_archive="$root/reference/$reference_archive_name"
reference_checksum="$reference_archive.sha256"
if [[ -e "$reference_archive" || -e "$reference_checksum" ]]; then
  if [[ ! -f "$reference_archive" || ! -f "$reference_checksum" ]]; then
    echo 'reference_archive_checksum=incomplete_pair' >&2
    exit 1
  fi
  if command -v shasum >/dev/null 2>&1; then
    if ! (cd "$root/reference" && shasum -a 256 -c "$reference_archive_name.sha256"); then
      echo 'reference_archive_checksum_valid=false' >&2
      exit 1
    fi
  elif command -v sha256sum >/dev/null 2>&1; then
    if ! (cd "$root/reference" && sha256sum -c "$reference_archive_name.sha256"); then
      echo 'reference_archive_checksum_valid=false' >&2
      exit 1
    fi
  else
    echo 'reference_archive_checksum_tool_missing=true' >&2
    exit 1
  fi
  echo 'reference_archive_checksum_valid=true'
else
  echo 'reference_archive_checksum=not_present'
fi

"$root/scripts/audit-migration-coverage.sh" --strict

# Flag only URL literals that begin with a concrete hostname/IP. Bare scheme
# checks (for example `starts_with("https://")`) and runtime-composed hosts are
# configuration logic, not embedded endpoints.
if rg -n -e 'https?://[[:alnum:]]' -e 'https?://\[' "$root/src" \
  | rg -v \
    -e 'http://127\.0\.0\.1(:[0-9]+)?' \
    -e 'https?://[^[:space:]\"]*@127\.0\.0\.1(:[0-9]+)?' \
    -e 'http://\[::1\](:[0-9]+)?' \
    -e 'http://\{address\}' \
    -e 'https?://[^[:space:]\"]*\.invalid'; then
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

if git -C "$root" ls-files -z \
  | xargs -0 rg -n -I \
    -e '-----BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY-----' \
    -e '\bAKIA[0-9A-Z]{16}\b' \
    -e '\bgh[pousr]_[A-Za-z0-9]{30,}\b' \
    -e 'Bearer[[:space:]]+[A-Za-z0-9._=-]{24,}'; then
  echo 'tracked_secret_pattern_free=false' >&2
  exit 1
fi
echo 'tracked_secret_pattern_free=true'

missing_licenses="$(
  cargo metadata --locked --format-version 1 \
    | jq -r '.packages[] | select(.source != null and ((.license // "") == "")) | .name'
)"
if [[ -n "$missing_licenses" ]]; then
  printf 'dependency_license_missing=%s\n' "$missing_licenses" >&2
  exit 1
fi
echo 'dependency_licenses_declared=true'

non_open_licenses="$(
  cargo metadata --locked --format-version 1 \
    | jq -r '.packages[] | select(.source != null) | select((.license // "") | test("BUSL|SSPL|Commons-Clause|Elastic-License|LicenseRef|Proprietary|UNLICENSED"; "i")) | "\(.name):\(.license)"'
)"
if [[ -n "$non_open_licenses" ]]; then
  printf 'dependency_license_rejected=%s\n' "$non_open_licenses" >&2
  exit 1
fi
echo 'dependency_licenses_open=true'

while IFS= read -r tracked; do
  [[ -f "$root/$tracked" ]] || continue
  size="$(wc -c < "$root/$tracked" | tr -d ' ')"
  if (( size > 1048576 )); then
    printf 'tracked_file_too_large=%s:%s\n' "$tracked" "$size" >&2
    exit 1
  fi
  kind="$(file -b "$root/$tracked")"
  if printf '%s\n' "$kind" \
    | rg -q '(^| )(ELF|Mach-O|PE32|shared object|current ar archive|Java class data|WebAssembly|Zip archive|7-zip archive|Zstandard compressed data|gzip compressed data)'; then
    printf 'tracked_opaque_artifact=%s:%s\n' "$tracked" "$kind" >&2
    exit 1
  fi
done < <(git -C "$root" ls-files)
echo 'tracked_files_open_source_sized=true'
echo 'tracked_opaque_artifact_free=true'
