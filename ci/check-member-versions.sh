#!/usr/bin/env bash
# noetl/ai-meta#331 — refuse a release that would publish nothing new.
#
# `.releaserc.json`'s prepareCmd rewrites `^version` in the ROOT Cargo.toml only.
# Workspace MEMBERS carry their own, semantically independent versions, and
# nothing in the pipeline touches them. So a change to `executor/` releases,
# reports green, and publishes a crate that does not contain it — `publish-crate`
# fails on "already exists" *after* the tag is cut, and the only symptom
# downstream is a consumer that mysteriously cannot see the change.
#
# Observed on 2 of the last 8 release-cli runs.
#
# This does NOT auto-bump. The member's version is independent (0.x, its own
# API surface), so the pipeline cannot know whether a change is patch or minor —
# guessing is how a breaking change ships as a patch, which is #330 one repo
# over. It fails loudly and names the file to edit.
#
# Read-only. Exits non-zero on a finding.
set -uo pipefail
cd "$(dirname "$0")/.."

MEMBERS=(executor)

# The last release tag is the baseline. Without one there is nothing to compare
# against and every member reads as unchanged — a false clean.
BASE=$(git describe --tags --abbrev=0 --match 'v*' 2>/dev/null || true)
if [[ -z "$BASE" ]]; then
  echo "  SKIP: no release tag to compare against"
  exit 0
fi
echo "member-version check — baseline $BASE"

fail=0
checked=0
for m in "${MEMBERS[@]}"; do
  manifest="$m/Cargo.toml"
  if [[ ! -f "$manifest" ]]; then
    echo "  MISSING: $manifest — the member list is stale, so a clean result is meaningless"
    fail=1; continue
  fi
  checked=$((checked + 1))

  # Did anything in the member change since the baseline?
  if git diff --quiet "$BASE" -- "$m/"; then
    echo "  $m: unchanged since $BASE — no bump required"
    continue
  fi

  # It changed. Did its version move?
  before=$(git show "$BASE:$manifest" 2>/dev/null | grep -m1 '^version = ' || echo "")
  after=$(grep -m1 '^version = ' "$manifest" || echo "")
  if [[ -z "$after" ]]; then
    echo "  $m: no top-level version in $manifest — cannot verify"
    fail=1; continue
  fi
  if [[ "$before" == "$after" ]]; then
    echo "  $m: CHANGED since $BASE but $after is unmoved."
    echo "     The release would publish nothing new: publish-crate fails on"
    echo "     'already exists' and the published crate will not contain the change."
    echo "     Bump $manifest yourself — the pipeline cannot know patch vs minor,"
    echo "     and guessing is how a breaking change ships as a patch."
    fail=1
  else
    echo "  $m: changed, and $before -> $after"
  fi
done

# Denominator: a pass over zero members reads exactly like a clean one.
echo "  checked=$checked/${#MEMBERS[@]}"
if [[ "$checked" -ne "${#MEMBERS[@]}" ]]; then
  echo "  ABORT: did not check every declared member"
  exit 2
fi
[[ "$fail" -eq 0 ]] && echo "  OK" || echo "  FAILED (noetl/ai-meta#331)"
exit "$fail"
