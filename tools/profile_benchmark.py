"""
Benchmark profile quality against a golden labeled dataset.

Usage:
    python tools/profile_benchmark.py --predicted profiles_rich.json --golden tmp/golden_profiles.json

Golden record supported fields:
{
  "canonical_artist_id": "ar_...",
  "normalized_name": "artist normalized name",
  "name": "Artist Name",
  "confidence": "HIGH|MEDIUM|LOW",
  "min_profile_score": 75,
  "genre": "Jazz",
  "locale": {"city": "London", "country": "United Kingdom"},
  "platforms": ["spotify", "youtube", "website"],
  "emails": true,
  "must_pass_high": true
}

Notes:
- Record matching priority: canonical_artist_id -> normalized_name -> normalized(name)
- If multiple predicted profiles match one key, best one is picked by profile richness.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import unicodedata
from typing import Any


VALID_CONFIDENCE = {"HIGH", "MEDIUM", "LOW"}


def _norm_text(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower().strip()
    text = re.sub(r"\s+", " ", text)
    return text


def _norm_name(value: Any) -> str:
    return _norm_text(value)


def _to_platform_set(value: Any) -> set[str]:
    if isinstance(value, dict):
        return {str(k).strip().lower() for k, v in value.items() if v}
    if isinstance(value, list):
        return {str(v).strip().lower() for v in value if v}
    return set()


def _load_json_list(path: str) -> list[dict]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        profiles = data.get("profiles")
        if isinstance(profiles, list):
            return [x for x in profiles if isinstance(x, dict)]
    raise ValueError(f"Unsupported JSON structure in {path}")


def _candidate_score(profile: dict) -> tuple[int, int, int]:
    platforms = profile.get("platforms", {}) or {}
    urls = profile.get("urls", []) or []
    return (
        int(profile.get("profile_score") or 0),
        len(platforms),
        len(urls),
    )


def _build_predicted_index(predicted: list[dict]) -> dict[str, list[dict]]:
    idx: dict[str, list[dict]] = {}
    for p in predicted:
        keys: set[str] = set()
        cid = str(p.get("canonical_artist_id") or "").strip()
        if cid:
            keys.add(f"cid:{cid}")
        nn = str(p.get("normalized_name") or "").strip()
        if nn:
            keys.add(f"nn:{_norm_name(nn)}")
        name = str(p.get("name") or "").strip()
        if name:
            keys.add(f"name:{_norm_name(name)}")
        for k in keys:
            idx.setdefault(k, []).append(p)
    return idx


def _resolve_predicted(golden: dict, idx: dict[str, list[dict]]) -> dict | None:
    keys: list[str] = []
    cid = str(golden.get("canonical_artist_id") or "").strip()
    if cid:
        keys.append(f"cid:{cid}")
    nn = str(golden.get("normalized_name") or "").strip()
    if nn:
        keys.append(f"nn:{_norm_name(nn)}")
    name = str(golden.get("name") or "").strip()
    if name:
        keys.append(f"name:{_norm_name(name)}")

    for k in keys:
        candidates = idx.get(k, [])
        if candidates:
            return max(candidates, key=_candidate_score)
    return None


def _init_metric() -> dict[str, Any]:
    return {"passed": 0, "total": 0, "accuracy": None}


def _acc(metrics: dict[str, dict], name: str, ok: bool) -> None:
    m = metrics.setdefault(name, _init_metric())
    m["total"] += 1
    if ok:
        m["passed"] += 1


def _finalize_metrics(metrics: dict[str, dict]) -> None:
    for m in metrics.values():
        total = m["total"]
        m["accuracy"] = round((m["passed"] / total), 4) if total else None


def _platform_prf(tp: int, fp: int, fn: int) -> dict[str, float]:
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return {
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
    }


def evaluate(predicted: list[dict], golden: list[dict]) -> dict[str, Any]:
    idx = _build_predicted_index(predicted)
    metrics: dict[str, dict] = {}
    unmatched: list[str] = []
    matched = 0

    platform_tp = platform_fp = platform_fn = 0
    platform_eval_count = 0

    for g in golden:
        pred = _resolve_predicted(g, idx)
        label = str(g.get("name") or g.get("normalized_name") or g.get("canonical_artist_id") or "<unknown>")
        if not pred:
            unmatched.append(label)
            continue
        matched += 1

        if "canonical_artist_id" in g and g.get("canonical_artist_id"):
            _acc(metrics, "canonical_artist_id", str(pred.get("canonical_artist_id") or "") == str(g["canonical_artist_id"]))

        if "confidence" in g and g.get("confidence"):
            expected_conf = str(g["confidence"]).upper().strip()
            actual_conf = str(pred.get("confidence", "")).upper().strip()
            if expected_conf in VALID_CONFIDENCE:
                _acc(metrics, "confidence", actual_conf == expected_conf)

        if "min_profile_score" in g:
            try:
                min_score = int(g["min_profile_score"])
                actual_score = int(pred.get("profile_score") or 0)
                _acc(metrics, "min_profile_score", actual_score >= min_score)
            except Exception:
                pass

        if "genre" in g and g.get("genre"):
            _acc(metrics, "genre", _norm_text(pred.get("genre")) == _norm_text(g.get("genre")))

        if isinstance(g.get("locale"), dict):
            gl = g["locale"]
            pl = pred.get("locale", {}) or {}
            if "city" in gl and gl.get("city"):
                _acc(metrics, "locale.city", _norm_text(pl.get("city")) == _norm_text(gl.get("city")))
            if "country" in gl and gl.get("country"):
                _acc(metrics, "locale.country", _norm_text(pl.get("country")) == _norm_text(gl.get("country")))
            if "state" in gl and gl.get("state"):
                _acc(metrics, "locale.state", _norm_text(pl.get("state")) == _norm_text(gl.get("state")))

        if "emails" in g:
            expected_emails = g.get("emails")
            actual_emails = [str(x).strip().lower() for x in (pred.get("emails") or []) if x]
            if isinstance(expected_emails, bool):
                _acc(metrics, "emails_present", bool(actual_emails) == expected_emails)
            elif isinstance(expected_emails, list):
                exp_set = {str(x).strip().lower() for x in expected_emails if x}
                _acc(metrics, "emails_overlap", bool(exp_set & set(actual_emails)))

        if "must_pass_high" in g:
            expected = bool(g.get("must_pass_high"))
            flags = pred.get("quality_flags", {}) or {}
            actual = bool(flags.get("must_pass_high"))
            _acc(metrics, "must_pass_high", actual == expected)

        if "platforms" in g:
            expected_set = _to_platform_set(g.get("platforms"))
            actual_set = _to_platform_set(pred.get("platforms"))
            platform_tp += len(expected_set & actual_set)
            platform_fp += len(actual_set - expected_set)
            platform_fn += len(expected_set - actual_set)
            platform_eval_count += 1

    _finalize_metrics(metrics)
    platform_scores = _platform_prf(platform_tp, platform_fp, platform_fn)

    # Weighted aggregate for quick pass/fail:
    # Confidence + must_pass_high + platforms_f1 are weighted more.
    weights = {
        "confidence": 2.0,
        "must_pass_high": 2.0,
        "canonical_artist_id": 1.0,
        "min_profile_score": 1.5,
        "genre": 1.0,
        "locale.city": 1.0,
        "locale.country": 1.0,
        "locale.state": 0.5,
        "emails_present": 1.0,
        "emails_overlap": 1.0,
    }
    weighted_sum = 0.0
    weighted_total = 0.0
    for k, m in metrics.items():
        if m["accuracy"] is None:
            continue
        w = weights.get(k, 1.0)
        weighted_sum += float(m["accuracy"]) * w
        weighted_total += w

    if platform_eval_count > 0:
        weighted_sum += platform_scores["f1"] * 2.0
        weighted_total += 2.0

    overall = round(weighted_sum / weighted_total, 4) if weighted_total else 0.0

    return {
        "totals": {
            "golden_records": len(golden),
            "matched_records": matched,
            "unmatched_records": len(unmatched),
            "coverage": round((matched / len(golden)), 4) if golden else 0.0,
        },
        "field_metrics": metrics,
        "platform_metrics": {
            "evaluated_records": platform_eval_count,
            "tp": platform_tp,
            "fp": platform_fp,
            "fn": platform_fn,
            **platform_scores,
        },
        "overall_score": overall,
        "unmatched_examples": unmatched[:25],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark predicted profiles against a golden dataset.")
    parser.add_argument("--predicted", required=True, help="Path to predicted profiles JSON (e.g. profiles_rich.json)")
    parser.add_argument("--golden", required=True, help="Path to golden labeled JSON")
    parser.add_argument("--report-out", default="", help="Optional path to write JSON report")
    parser.add_argument("--fail-below", type=float, default=None, help="Exit 1 if overall_score is below this threshold")
    args = parser.parse_args()

    predicted = _load_json_list(args.predicted)
    golden = _load_json_list(args.golden)
    report = evaluate(predicted=predicted, golden=golden)

    print(json.dumps(report, indent=2, ensure_ascii=False))

    if args.report_out:
        out_dir = os.path.dirname(args.report_out)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        with open(args.report_out, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

    if args.fail_below is not None and report.get("overall_score", 0.0) < args.fail_below:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
