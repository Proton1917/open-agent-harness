#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

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
  "$root/MIGRATION.md"
)

removed_terms=('cl''aude' 'anth''ropic')
for term in "${removed_terms[@]}"; do
  if rg -n -i "$term" "${scan_paths[@]}"; then
    echo 'brand_free=false' >&2
    exit 1
  fi
  if [[ -x "$root/target/release/open-agent-harness" ]] \
    && strings "$root/target/release/open-agent-harness" | rg -q -i "$term"; then
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

reference_git="$(find "$root/reference" -mindepth 2 -maxdepth 2 -type d -name .git -print -quit)"
if [[ -n "$reference_git" ]]; then
  reference_root="$(dirname "$reference_git")"
  if [[ -n "$(git -C "$reference_root" status --short)" ]]; then
    echo 'reference_clean=false' >&2
    exit 1
  fi
fi
echo 'reference_clean=true'
