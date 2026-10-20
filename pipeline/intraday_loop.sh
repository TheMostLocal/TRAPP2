#!/usr/bin/env bash
# intraday_loop.sh - ONE Actions job writes the whole session's 15-minute tape (z115).
# Shared, identical in TRAPP2 / TRAPP2-1 / TRAPP2-2 / TRAPP2-3.
#
# Why: GitHub's scheduler delays and drops most "every 15 minutes" crons (the
# live tape had 4 points on 2026-10-15: 13:09, 17:36, 20:33, 23:19 UTC). So a
# run now LOOPS: quotes -> corrections -> signals/regime -> tape point -> commit,
# then sleeps to the next 15-minute mark, until the session window ends or the
# job's time budget runs out. GitHub only has to start the job, not keep time.
#
# z117:
#  * START FROM THE LATEST MAIN. A queued run checks out the commit it was
#    TRIGGERED on. 2026-10-16: TRAPP2 run 35 was triggered 19:44 UTC, waited
#    behind the running loop until 21:30, and its first push conflict's "ours
#    wins" merge overwrote the newer tape - 19:45-21:15 UTC lost (the 21:30:16
#    commit still had them). The loop now fast-forwards before its first pass.
#  * SESSION WINDOW. A start outside LOOP_START_UTC..LOOP_STOP_UTC does ONE pass
#    and exits (a 03:20 UTC start looped 5h25m overnight on 10-16).
#
# Env: LOOP_INTERVAL_SEC (900) · LOOP_BUDGET_SEC (20400 = 5h40m; job limit 6h)
#      LOOP_OFFSET_SEC (stagger between repos) · LOOP_START_UTC (HHMM, default 1130)
#      LOOP_STOP_UTC (HHMM, default 2230) · LOOP_MAX_ITERS (1000; 1 = single pass)
#      INTRADAY_FORCE (write a tape point outside hours)
set -u
INTERVAL=${LOOP_INTERVAL_SEC:-900}
BUDGET=${LOOP_BUDGET_SEC:-20400}
OFFSET=${LOOP_OFFSET_SEC:-0}
START=${LOOP_START_UTC:-1130}
STOP=${LOOP_STOP_UTC:-2230}
MAX=${LOOP_MAX_ITERS:-1000}
BRANCH=${GITHUB_REF_NAME:-main}
start=$(date +%s)
hhmm() { echo $((10#$(date -u +%H%M))); }
in_window() { local n; n=$(hhmm); [ "$n" -ge $((10#$START)) ] && [ "$n" -lt $((10#$STOP)) ]; }

# nothing is local yet, so this is a plain fast-forward (reset to the fetched tip as a fallback)
if git fetch -q origin "$BRANCH" 2>/dev/null; then
  git merge -q --ff-only FETCH_HEAD 2>/dev/null || git reset -q --hard FETCH_HEAD
  echo "synced to origin/$BRANCH @ $(git rev-parse --short HEAD)"
else
  echo "::warning::could not fetch origin/$BRANCH - continuing from the checked-out commit"
fi
DAY=$(python pipeline/intraday_session.py 2>/dev/null || echo trading)
SINGLE=""
in_window || SINGLE="started outside the session window (${START}Z-${STOP}Z)"
echo "intraday loop: day=$DAY interval=${INTERVAL}s budget=${BUDGET}s window=${START}Z-${STOP}Z max=$MAX${SINGLE:+ · $SINGLE}"

one_pass() {
  python pipeline/fetch_quotes_fast.py || python pipeline/fetch_data.py || echo "::warning::quote fetch failed this pass"
  python pipeline/human_corrections.py master || true
  python pipeline/compute_signals.py || true
  python pipeline/classify_regime.py || true
  python pipeline/append_intraday_snapshot.py || true
  python pipeline/fetch_intraday_bars.py || true          # z131: 5-min OHLCV incl. pre-market / after hours
  OWN="data/master.csv data/master.json data/intraday data/bars data/signals.json data/regime_current.json data/regime_history.json"
  for f in $OWN; do [ -e "$f" ] && git add -A "$f"; done          # one missing path must not block the rest
  if git diff --staged --quiet; then echo "no quote changes"; return 0; fi
  git commit -q -m "Quotes + intraday tape $(date -u +%Y-%m-%dT%H:%M:%SZ) [skip ci]"
  for k in 1 2 3 4 5; do
    if git push -q origin "HEAD:$BRANCH"; then echo "pushed (attempt $k)"; return 0; fi
    echo "push rejected - reconciling (ours wins for OUR files)"
    git fetch -q origin "$BRANCH"
    git merge -X ours "origin/$BRANCH" --no-edit -q 2>/dev/null || true
    if [ -f .git/MERGE_HEAD ]; then
      for f in $OWN; do [ -e "$f" ] && git checkout --ours -- "$f" 2>/dev/null; done
      for f in $OWN; do [ -e "$f" ] && git add -A "$f"; done
      git commit -q --no-edit 2>/dev/null || true
    fi
    sleep $((k * 2))
  done
  echo "::warning::push failed after 5 attempts this pass"
}

i=0
while :; do
  i=$((i + 1))
  echo "── pass $i · $(date -u +%H:%M:%S)Z"
  one_pass
  # keep the checkout current so the next pass builds on what's on GitHub
  git pull -q --no-rebase -X ours origin "$BRANCH" 2>/dev/null || true
  now=$(date +%s)
  if [ -n "$SINGLE" ]; then echo "$SINGLE - one pass only"; break; fi
  if [ "$DAY" != "trading" ] && [ -z "${INTRADAY_FORCE:-}" ]; then echo "not a trading day ($DAY) - one pass only"; break; fi
  if [ "$i" -ge "$MAX" ]; then echo "max passes reached"; break; fi
  if ! in_window; then echo "past the session window (${STOP}Z) - done"; break; fi
  wait=$(( INTERVAL - (now % INTERVAL) + OFFSET ))
  [ "$wait" -gt "$INTERVAL" ] && wait=$(( wait - INTERVAL ))
  [ "$wait" -lt 30 ] && wait=$(( wait + INTERVAL ))
  if [ $(( now - start + wait + 120 )) -gt "$BUDGET" ]; then echo "time budget reached - the next scheduled start continues"; break; fi
  echo "sleeping ${wait}s"
  sleep "$wait"
done
echo "intraday loop done: $i pass(es)"
