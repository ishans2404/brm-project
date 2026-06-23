"""
prediction_writer.py
─────────────────────────────────────────────────────────────────────────────
Writes the latest model predictions to Redis so the dashboard API can serve
them in real time.  Call this at the end of every successful inference step
inside v3_inference.py.

Usage (add to the final @step of your Metaflow flow):
─────────────────────────────────────────────────────
    from prediction_writer import write_predictions, write_mill_status

    # After computing predictions:
    write_predictions(
        ys    = float(df_pred["YS"].mean()),
        uts   = float(df_pred["UTS"].mean()),
        el    = float(df_pred["%El"].mean()),
        profile = self.profile,
    )
─────────────────────────────────────────────────────────────────────────────
"""

import os
import json
import redis
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Spec limits used for inline compliance tagging
_SPEC = {
    "YS":  {"min": 596,  "max": 650},
    "UTS": {"min": 698,  "max": 754},
    "EL":  {"min": 14.0, "max": None},
}

_DEFAULT_TTL = 900   # 15 minutes — stale predictions auto-expire


def _redis() -> redis.Redis:
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        db=int(os.getenv("REDIS_DB", 0)),
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
    )


def _compliance(key: str, value: float) -> str:
    spec = _SPEC.get(key, {})
    lo, hi = spec.get("min"), spec.get("max")
    if value is None:
        return "UNKNOWN"
    if lo is not None and value < lo:
        return "BELOW"
    if hi is not None and value > hi:
        return "ABOVE"
    return "OK"


def write_predictions(
    ys:      float,
    uts:     float,
    el:      float,
    profile: str  = None,
    ttl:     int  = _DEFAULT_TTL,
) -> bool:
    """
    Write latest YS / UTS / %El predictions to Redis.

    Args:
        ys      : Predicted Yield Strength (MPa)
        uts     : Predicted Ultimate Tensile Strength (MPa)
        el      : Predicted Percentage Elongation (%)
        profile : Active rolling profile, e.g. '16mm'  (optional)
        ttl     : Key expiry in seconds (default 900 s = 15 min)

    Returns:
        True on success, False if Redis is unavailable.
    """
    try:
        r = _redis()
        ts = datetime.now().isoformat()

        # Build a compact summary stored as JSON for atomic reads
        summary = {
            "YS":        round(float(ys),  2),
            "UTS":       round(float(uts), 2),
            "EL":        round(float(el),  2),
            "timestamp": ts,
            "profile":   profile or os.getenv("PROFILE", "Unknown"),
            "compliance": {
                "YS":  _compliance("YS",  ys),
                "UTS": _compliance("UTS", uts),
                "EL":  _compliance("EL",  el),
            },
        }

        pipe = r.pipeline(transaction=True)
        # Individual keys for backwards-compat with any existing readers
        pipe.set("PRED_YS",        summary["YS"],        ex=ttl)
        pipe.set("PRED_UTS",       summary["UTS"],       ex=ttl)
        pipe.set("PRED_EL",        summary["EL"],        ex=ttl)
        pipe.set("PRED_TIMESTAMP", ts,                   ex=ttl)
        pipe.set("PRED_SUMMARY",   json.dumps(summary),  ex=ttl)

        if profile:
            pipe.set("PROFILE", profile)   # no TTL — persists until changed

        pipe.execute()
        r.close()

        status_str = " | ".join(
            f"{k}={summary['compliance'][k]}" for k in ("YS", "UTS", "EL")
        )
        print(f"[Redis] Predictions written  YS={ys:.1f}  UTS={uts:.1f}  %El={el:.2f}  [{status_str}]")
        return True

    except redis.exceptions.ConnectionError as e:
        print(f"[Redis] Connection failed — cannot write predictions: {e}")
        return False
    except Exception as e:
        print(f"[Redis] Unexpected error writing predictions: {e}")
        return False


def write_mill_status(is_on: bool, ttl: int = 120) -> bool:
    """
    Write mill on/off status to Redis.

    Args:
        is_on : True if the mill is actively rolling, False if idle/stopped.
        ttl   : Key expiry in seconds (default 120 s — refreshed each cycle).
    """
    try:
        r = _redis()
        r.set("MILL_ON", "1" if is_on else "0", ex=ttl)
        r.close()
        return True
    except Exception as e:
        print(f"[Redis] Failed to write mill status: {e}")
        return False


def read_latest() -> dict:
    """Read the most recent prediction summary back from Redis (for testing)."""
    try:
        r = _redis()
        raw = r.get("PRED_SUMMARY")
        r.close()
        return json.loads(raw) if raw else {}
    except Exception:
        return {}


if __name__ == "__main__":
    # Quick smoke-test
    print("Writing test predictions to Redis…")
    ok = write_predictions(ys=625.4, uts=718.2, el=14.8, profile="16mm")
    if ok:
        print("Read back:", read_latest())
