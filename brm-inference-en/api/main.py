"""
api/main.py
─────────────────────────────────────────────────────────────────────────────
FastAPI backend for the BSP BRM Live Inference Dashboard.

Provides:
  REST  → /api/status   /api/history   /api/alerts/recent   /health
  WS    → /ws           (pushes dashboard state every 2 seconds)

State sources:
  Redis    → live predictions (written by prediction_writer.py)
  InfluxDB → historical prediction records
  MongoDB  → persisted alert documents
─────────────────────────────────────────────────────────────────────────────
"""

import asyncio
import json
import os
from datetime import datetime
from typing import List, Optional

import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from influxdb_client import InfluxDBClient
import motor.motor_asyncio

load_dotenv()

# ─── App setup ────────────────────────────────────────────────────────────────
app = FastAPI(
    title="BSP BRM Inference Dashboard API",
    version="2.0",
    description="Real-time YS/UTS prediction dashboard for Bar & Rod Mill, Bhilai Steel Plant",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Configuration ────────────────────────────────────────────────────────────
REDIS_HOST    = os.getenv("REDIS_HOST",   "redis")
REDIS_PORT    = int(os.getenv("REDIS_PORT",  6379))
REDIS_DB      = int(os.getenv("REDIS_DB",    0))

INFLUX_URL    = os.getenv("INFLUX_URL",           "http://host.docker.internal:8086")
INFLUX_TOKEN  = os.getenv("INFLUX_TOKEN_DEPLOY",  "")
INFLUX_ORG    = os.getenv("INFLUX_ORG",           "iit_bh")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET_DEPLOY", "iba_data_12mm")

MONGODB_URI   = os.getenv("MONGODB_URI",        "")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME",      "bsp_alerts")
MONGO_COLL    = os.getenv("MONGO_COLLECTION",   "bsp_alert")

# IS 1786 spec limits for TMT-16mm FE-550D / SeQR-550D
SPEC = {
    "YS":  {"min": 596,  "max": 650,  "unit": "MPa"},
    "UTS": {"min": 698,  "max": 754,  "unit": "MPa"},
    "EL":  {"min": 14.0, "max": None, "unit": "%"},
}

# ─── Connection pools ─────────────────────────────────────────────────────────
redis_pool = aioredis.ConnectionPool.from_url(
    f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
    decode_responses=True,
    max_connections=30,
)

try:
    influx_client = InfluxDBClient(
        url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG
    )
    influx_query = influx_client.query_api()
except Exception:
    influx_query = None

try:
    mongo_client = motor.motor_asyncio.AsyncIOMotorClient(
        MONGODB_URI, serverSelectionTimeoutMS=3000
    )
    mongo_coll = mongo_client[MONGO_DB_NAME][MONGO_COLL]
except Exception:
    mongo_coll = None


# ─── WebSocket manager ────────────────────────────────────────────────────────
class WSManager:
    def __init__(self):
        self._sockets: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._sockets.append(ws)

    def disconnect(self, ws: WebSocket):
        self._sockets.discard(ws) if hasattr(self._sockets, "discard") \
            else (self._sockets.remove(ws) if ws in self._sockets else None)

    async def broadcast(self, payload: dict):
        dead = []
        for ws in list(self._sockets):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

    @property
    def count(self) -> int:
        return len(self._sockets)


manager = WSManager()


# ─── Redis helpers ────────────────────────────────────────────────────────────
async def _redis() -> aioredis.Redis:
    return aioredis.Redis(connection_pool=redis_pool)


async def get_redis_state() -> dict:
    r = await _redis()
    try:
        # Try the atomic summary first (written by prediction_writer.py)
        raw = await r.get("PRED_SUMMARY")
        if raw:
            summary = json.loads(raw)
            predictions = {
                "YS": summary.get("YS"),
                "UTS": summary.get("UTS"),
                "EL": summary.get("EL"),
                "timestamp": summary.get("timestamp"),
            }
            compliance = summary.get("compliance", {})
        else:
            # Fall back to individual keys
            keys = ["PRED_YS", "PRED_UTS", "PRED_EL", "PRED_TIMESTAMP"]
            ys, uts, el, ts = await r.mget(*keys)
            predictions = {
                "YS":        float(ys)  if ys  else None,
                "UTS":       float(uts) if uts else None,
                "EL":        float(el)  if el  else None,
                "timestamp": ts,
            }
            compliance = _compute_compliance(predictions)

        profile  = await r.get("PROFILE")            or "Unknown"
        cal_mode = await r.get("CALIBRATION_MODE")   or "1"
        mill_on  = await r.get("MILL_ON")             or "0"

        return {
            "profile":          profile,
            "calibration_mode": cal_mode == "1",
            "mill_on":          mill_on == "1",
            "predictions":      predictions,
            "compliance":       compliance,
            "data_source":      "redis",
        }
    except Exception as exc:
        return {
            "profile": "Unknown", "calibration_mode": True, "mill_on": False,
            "predictions": {"YS": None, "UTS": None, "EL": None, "timestamp": None},
            "compliance": {"YS": "UNKNOWN", "UTS": "UNKNOWN", "EL": "UNKNOWN"},
            "data_source": "error", "error": str(exc),
        }
    finally:
        await r.aclose()


# ─── InfluxDB helpers ─────────────────────────────────────────────────────────
async def _influx_history(hours: int = 4) -> List[dict]:
    if not influx_query:
        return []
    try:
        q = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -{hours}h)
          |> filter(fn: (r) => r._measurement == "predictions" and
                    (r._field == "YS" or r._field == "UTS" or r._field == "EL"))
          |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")
          |> sort(columns:["_time"])
        """
        tables = influx_query.query(q)
        out = []
        for table in tables:
            for rec in table.records:
                out.append({
                    "time": rec.get_time().isoformat(),
                    "YS":   rec.values.get("YS"),
                    "UTS":  rec.values.get("UTS"),
                    "EL":   rec.values.get("EL"),
                })
        return out
    except Exception:
        return []


async def _influx_latest() -> Optional[dict]:
    """Fetch single most-recent prediction from InfluxDB (fallback for Redis)."""
    if not influx_query:
        return None
    try:
        q = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -15m)
          |> filter(fn: (r) => r._measurement == "predictions" and
                    (r._field == "YS" or r._field == "UTS" or r._field == "EL"))
          |> last()
          |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")
          |> limit(n:1)
        """
        tables = influx_query.query(q)
        for table in tables:
            for rec in table.records:
                return {
                    "YS":       rec.values.get("YS"),
                    "UTS":      rec.values.get("UTS"),
                    "EL":       rec.values.get("EL"),
                    "timestamp": rec.get_time().isoformat(),
                }
    except Exception:
        pass
    return None


# ─── Compliance & alerts ──────────────────────────────────────────────────────
def _compute_compliance(predictions: dict) -> dict:
    result = {}
    for key, limits in SPEC.items():
        val = predictions.get(key)
        if val is None:
            result[key] = "UNKNOWN"
        elif limits["min"] is not None and val < limits["min"]:
            result[key] = "BELOW"
        elif limits["max"] is not None and val > limits["max"]:
            result[key] = "ABOVE"
        else:
            result[key] = "OK"
    return result


def _build_alerts(predictions: dict, compliance: dict) -> List[dict]:
    alerts = []
    ts = datetime.now().isoformat()
    for key, status in compliance.items():
        val = predictions.get(key)
        if val is None or status in ("OK", "UNKNOWN"):
            continue
        lims = SPEC[key]
        if status == "BELOW":
            msg = (f"{key} at {val:.1f} {lims['unit']} — "
                   f"BELOW minimum ({lims['min']} {lims['unit']})")
            sev = "critical"
        else:
            msg = (f"{key} at {val:.1f} {lims['unit']} — "
                   f"ABOVE maximum ({lims['max']} {lims['unit']})")
            sev = "warning"
        alerts.append({
            "type": status, "target": key,
            "value": round(val, 2),
            "message": msg, "severity": sev, "timestamp": ts,
        })
    return alerts


# ─── Payload builder ──────────────────────────────────────────────────────────
async def build_payload() -> dict:
    state = await get_redis_state()
    preds = state["predictions"]

    # If Redis has no predictions, try InfluxDB
    if preds["YS"] is None:
        latest = await _influx_latest()
        if latest:
            preds.update(latest)
            state["compliance"] = _compute_compliance(preds)
            state["data_source"] = "influxdb"

    alerts = _build_alerts(preds, state["compliance"])

    return {
        **state,
        "alerts":             alerts,
        "active_connections": manager.count,
        "server_time":        datetime.now().isoformat(),
    }


# ─── Routes ───────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now().isoformat()}


@app.get("/api/status")
async def api_status():
    return await build_payload()


@app.get("/api/history")
async def api_history(hours: int = 4):
    records = await _influx_history(min(hours, 24))
    return {"history": records, "count": len(records), "hours": hours}


@app.get("/api/alerts/recent")
async def api_alerts(limit: int = 40):
    if mongo_coll is None:
        return {"alerts": []}
    try:
        docs = await mongo_coll.find(
            {}, {"_id": 0}
        ).sort("timestamp", -1).limit(limit).to_list(length=limit)
        return {"alerts": docs}
    except Exception:
        return {"alerts": []}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            payload = await build_payload()
            await ws.send_json(payload)
            await asyncio.sleep(2)
    except (WebSocketDisconnect, Exception):
        manager.disconnect(ws)
