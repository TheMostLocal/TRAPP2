#!/usr/bin/env bash
# ============================================================
#  >>> DESTINATION: TheMostLocal/TRAPP2-1
#  >>> FILE PATH:   pipeline/fx_crypto_loop.sh
#
#  z147 - FX + CRYPTO, LOOPED (24-hour markets). The old schedule fired a new run every 15 minutes
#  ("*/15") with cancel-in-progress: GitHub drops a lot of runs at :00/:15/:30/:45, and any run that did
#  start could be killed by the next trigger before it pushed - so rates.json / prices.json went hours
#  without an update. Now one job refreshes every 15 minutes (at :02 / :17 / :32 / :47) for up to
#  LOOP_BUDGET_MIN, and an hourly trigger queues the next job behind it, so coverage is continuous even
#  when triggers are dropped. FX feeds the USD conversion of every foreign listing the bot prices.
# ============================================================
set -u
BUDGET=$(( ${LOOP_BUDGET_MIN:-170} * 60 ))
START=$(date +%s)
BRANCH="${GITHUB_REF_NAME:-main}"
FILES="data/fx/rates.json data/crypto/prices.json"
git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

latest() { git fetch -q origin "$BRANCH" && { git merge -q --ff-only FETCH_HEAD 2>/dev/null || git reset -q --hard FETCH_HEAD; }; }

push_retry() {
  for i in 1 2 3 4 5; do
    if git push -q origin "HEAD:$BRANCH"; then echo "  pushed (attempt $i)"; return 0; fi
    git fetch -q origin "$BRANCH"
    git merge -X ours "origin/$BRANCH" --no-edit -q 2>/dev/null || true
    if [ -f .git/MERGE_HEAD ]; then
      for f in $FILES; do git checkout --ours -- "$f" 2>/dev/null || true; git add "$f" 2>/dev/null || true; done
      git commit -q --no-edit 2>/dev/null || true
    fi
    sleep $(( i * 3 ))
  done
  echo "::warning::push failed after 5 attempts"
  return 1
}

one_pass() {
  latest
  python pipeline/fetch_fx.py || echo "::warning::FX fetch failed this pass"
  python pipeline/fetch_crypto.py || echo "::warning::crypto fetch failed this pass"
  for f in $FILES; do [ -e "$f" ] && git add "$f"; done          # z116: one path per git add
  if git diff --staged --quiet; then echo "  no changes"; return 0; fi
  git commit -q -m "FX/crypto refresh $(date -u +%Y-%m-%dT%H:%M:%SZ) [skip ci]"
  push_retry
}

# z161: when this job stops for its TIME BUDGET while its window is still open, it queues the next run of its own
# workflow - GitHub's scheduler went hours without starting anything (2026-10-26: no intraday run by 0900 ET).
# Needs `actions: write` and GH_TOKEN (the job's own token) in the workflow; the concurrency group queues the new
# run behind this one. GitHub's schedule and the Supabase heartbeat (supabase/heartbeat.sql) stay as backstops.
chain_next() {
  [ -n "${GH_TOKEN:-}" ] || { echo "::notice::GH_TOKEN not set - not chaining"; return 0; }
  local wf="${GITHUB_WORKFLOW_REF:-}"
  wf="${wf%@*}"; wf="${wf##*/}"
  { [ -n "$wf" ] && [ -n "${GITHUB_REPOSITORY:-}" ]; } || return 0
  if gh workflow run "$wf" --ref "${GITHUB_REF_NAME:-main}" -R "$GITHUB_REPOSITORY" >/dev/null 2>&1; then
    echo "chained: the next run of $wf is queued"
  else
    echo "::warning::could not queue the next run of $wf (does the workflow grant actions: write?)"
  fi
}

next_wait() {   # seconds to the next :02 / :17 / :32 / :47, at least 60 s away
  local now m s w
  now=$(date +%s); m=$(( (now / 60) % 60 )); s=$(( now % 60 ))
  w=$(( ( (2 - m % 15 + 15) % 15 ) * 60 - s ))
  [ "$w" -lt 60 ] && w=$(( w + 900 ))
  echo "$w"
}

i=0
while :; do
  i=$((i + 1))
  echo "── pass $i · $(date -u +%H:%M:%S)Z · $(TZ=America/New_York date +'%a %H:%M') ET"
  one_pass
  wait=$(next_wait)
  if [ $(( $(date +%s) - START + wait + 180 )) -gt "$BUDGET" ]; then
    echo "budget reached after $i pass(es) - queueing the next run (24/7)"
    chain_next
    break
  fi
  echo "  next pass in $(( wait / 60 )) min"
  sleep "$wait"
done
