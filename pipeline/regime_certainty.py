#!/usr/bin/env python3
"""
regime_certainty.py — is the regime call DIALED IN? (z93)

Regime shifts are hard to predict, and a regime-based system that trusts a shaky
call fails twice: it takes trades for the wrong reason, and when one of those
trades happens to win, it learns the wrong lesson. So before anything leans on
the regime, this scores how much the current call can be trusted:

  margin      how decisively the classifier picks its regime (top probability
              minus the runner-up). 0.30+ apart = full marks.
  stability   regime flips in the last 20 sessions. A call that flip-flops is
              noise, not a regime. 0 flips = full, 4+ = zero.
  quad        the market nowcast quad vs the latest OFFICIAL GDP/CPI quad:
              same quad = 1, one axis shared (growth or inflation) = 0.5, none = 0.
  cross-check does the market regime agree with the growth axis of the quad?
              (risk-on families with growth-up quads, risk-off with growth-down).
  certainty = 0.30 margin + 0.25 stability + 0.25 quad + 0.20 cross-check

Then the TRACK RECORD: every past daily call is judged by what SPY did over the
next 21 sessions (up families should rise, down families fall, neutral stay
within +/-2.5%). If there are 20+ judged calls and the classifier is not beating
the naive "SPY goes up" base rate, the certainty is capped at 0.50 - a
classifier that doesn't beat a coin weighted to the market's drift isn't a
regime call.

dialedIn = certainty >= 0.65 (and not capped). When it's not, the bot runs
REGIME-AGNOSTIC: no regime tilts, no regime-based sector grading, no
regime-specific pattern trust; it trades on company-level and price evidence.

Writes data/regime_certainty.json. Stdlib only.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
OUT = DATA / "regime_certainty.json"
THRESHOLD = 0.65
HORIZON = 21
MIN_TRACK_N = 20
FAMILY = {"expansion": "up", "risk_on_melt_up": "up", "risk_off": "down", "recession_fear": "down",
          "inflation_shock": "down", "neutral": "flat"}
QUAD_AXES = {1: (1, -1), 2: (1, 1), 3: (-1, 1), 4: (-1, -1)}    # (growth, inflation)


def load(p, default=None):
    try:
        return json.loads(Path(p).read_text())
    except Exception:
        return default


def clip01(x):
    return max(0.0, min(1.0, x))


def main():
    cur = load(DATA / "regime_current.json", {}) or {}
    hist = load(DATA / "regime_history.json", []) or []
    quad = load(DATA / "macro" / "quad.json", {}) or {}
    spy = load(DATA / "history" / "SPY.json", []) or []
    why = []

    # 1. margin
    probs = cur.get("current_probabilities") or {}
    ranked = sorted(probs.values(), reverse=True)
    margin = (ranked[0] - ranked[1]) if len(ranked) >= 2 else 0.0
    s_margin = clip01(margin / 0.30)
    top = max(probs, key=probs.get) if probs else cur.get("regime")
    why.append(f"classifier picks {top} at {ranked[0] * 100:.0f}% vs {ranked[1] * 100:.0f}% runner-up" if len(ranked) >= 2 else "no probabilities")

    # 2. stability
    last20 = [h.get("regime") for h in hist[-20:] if h.get("regime")]
    flips = sum(1 for a, b in zip(last20, last20[1:]) if a != b)
    s_stab = clip01(1 - flips / 4)
    why.append(f"{flips} regime flip(s) in the last {len(last20)} sessions")

    # 3. quad agreement (nowcast vs official)
    qn = (quad.get("current") or {}).get("quad")
    qo = (quad.get("officialLatest") or {}).get("quad")
    if qn in QUAD_AXES and qo in QUAD_AXES:
        shared = sum(1 for a, b in zip(QUAD_AXES[qn], QUAD_AXES[qo]) if a == b)
        s_quad = shared / 2
        why.append(f"quad nowcast Q{qn} vs official Q{qo} ({shared}/2 axes agree)")
    else:
        s_quad = 0.0
        why.append("quad nowcast or official quad missing")

    # 4. cross-check market regime family vs quad growth axis
    fam = FAMILY.get(cur.get("regime") or top, "flat")
    if qn in QUAD_AXES:
        g = QUAD_AXES[qn][0]
        s_cross = 0.5 if fam == "flat" else (1.0 if (fam == "up") == (g > 0) else 0.0)
    else:
        s_cross = 0.5
    why.append(f"market regime family '{fam}' vs quad growth axis → {s_cross:.1f}")

    certainty = 0.30 * s_margin + 0.25 * s_stab + 0.25 * s_quad + 0.20 * s_cross

    # 5. track record vs SPY forward returns
    closes = {str(b.get("date"))[:10]: float(b["close"]) for b in spy if b.get("close")}
    dates = sorted(closes)
    idx = {d: i for i, d in enumerate(dates)}
    calls, correct, n, up_hits = [], 0, 0, 0
    for h in hist:
        d = str(h.get("date"))[:10]
        r = h.get("regime")
        i = idx.get(d)
        row = {"date": d, "regime": r, "family": FAMILY.get(r, "flat")}
        if i is not None and i + HORIZON < len(dates):
            fwd = closes[dates[i + HORIZON]] / closes[d] - 1
            f = row["family"]
            ok = (fwd > 0) if f == "up" else (fwd < 0) if f == "down" else (abs(fwd) < 0.025)
            row.update(fwd21Pct=round(fwd * 100, 2), correct=bool(ok))
            n += 1
            correct += ok
            up_hits += fwd > 0
        calls.append(row)
    acc = correct / n if n else None
    base = up_hits / n if n else None
    capped = False
    if n >= MIN_TRACK_N and acc is not None and base is not None and acc <= base:
        certainty = min(certainty, 0.50)
        capped = True
        why.append(f"track record {acc * 100:.0f}% right over {n} calls does not beat the naive 'SPY rises' rate {base * 100:.0f}% → capped")
    elif n:
        why.append(f"track record {acc * 100:.0f}% right over {n} judged calls (naive base rate {base * 100:.0f}%)"
                   + ("" if n >= MIN_TRACK_N else f" — fewer than {MIN_TRACK_N}, not yet used"))
    else:
        why.append("no judged calls yet (needs 21 sessions after each call)")

    dialed = certainty >= THRESHOLD and not capped
    out = {
        "asOf": cur.get("date"), "generatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "regime": cur.get("regime"), "quadNowcast": qn, "quadOfficial": qo,
        "certainty": round(certainty, 3), "threshold": THRESHOLD, "dialedIn": dialed,
        "mode": "regime-aware" if dialed else "regime-agnostic",
        "components": {"margin": {"value": round(margin, 3), "score": round(s_margin, 3), "weight": 0.30},
                       "stability": {"flips20": flips, "score": round(s_stab, 3), "weight": 0.25},
                       "quadAgreement": {"nowcast": qn, "official": qo, "score": round(s_quad, 3), "weight": 0.25},
                       "crossCheck": {"family": fam, "score": round(s_cross, 3), "weight": 0.20}},
        "trackRecord": {"horizonSessions": HORIZON, "n": n, "correct": correct,
                        "accuracyPct": round(acc * 100, 1) if acc is not None else None,
                        "baseRateUpPct": round(base * 100, 1) if base is not None else None,
                        "minN": MIN_TRACK_N, "capped": capped},
        "why": why,
        "calls": calls[-90:],
    }
    OUT.write_text(json.dumps(out, indent=1))
    print(f"[regime-certainty] {cur.get('regime')} · certainty {certainty:.2f} → {'DIALED IN (regime-aware)' if dialed else 'NOT dialed in (regime-agnostic)'}")
    for w in why:
        print(f"  - {w}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
