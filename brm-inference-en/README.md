# BSP BRM Live Inference Dashboard — Docker Deployment

Real-time Yield Strength and UTS prediction system for Bar & Rod Mill, Bhilai Steel Plant.

---

## Architecture

```
ibaPDA ──► PostgreSQL ──► inference_trigger.py
                                │
                        prediction_writer.py
                                │
                             Redis ◄────────────────────────┐
                                │                           │
                         FastAPI (api)               mill_status.py
                          /api/*  /ws                get_profile.py
                                │
                     nginx (frontend :3000)
                                │
                         Browser Dashboard
```

**External services** (already running at plant — reached via `host.docker.internal`):
- PostgreSQL → ibaPDA live data stream
- InfluxDB   → time-series archive & prediction history
- MongoDB    → persisted alert documents

**Managed by Docker Compose:**
- Redis   → live state store (predictions, mill status, profile)
- API     → FastAPI REST + WebSocket backend
- Frontend→ nginx serving the React dashboard

---

## File Placement

Copy the following into your `BSP-INFERENCE-V2` project root:

```
BSP-INFERENCE-V2/
├── docker/
│   ├── docker-compose.yml        ← orchestration
│   ├── Dockerfile.inference      ← inference container
│   ├── inference_requirements.txt
│   ├── .env.example
│   └── README.md
├── api/
│   ├── main.py                   ← FastAPI backend
│   ├── Dockerfile
│   └── requirements.txt
├── frontend/
│   ├── index.html                ← React dashboard
│   ├── nginx.conf
│   └── Dockerfile
└── prediction_writer.py          ← NEW: call from v3_inference.py
```

---

## Integration Step (Required)

Add three lines to your **`v3_inference.py`** Metaflow flow,
inside the final `@step` that produces predictions:

```python
# At the top of v3_inference.py
from prediction_writer import write_predictions, write_mill_status

# In your final step, after computing ys_pred, uts_pred, el_pred:
write_predictions(
    ys      = float(ys_pred),
    uts     = float(uts_pred),
    el      = float(el_pred),
    profile = self.profile,     # e.g. '16mm'
)
```

That's it. The dashboard picks up predictions automatically within 2 seconds.

---

## Deployment

### 1. Configure environment

```bash
cd BSP-INFERENCE-V2/docker
cp .env.example .env
nano .env
```

Key changes in `.env`:
- Replace all `localhost` → `host.docker.internal` (for PostgreSQL, InfluxDB, MongoDB)
- Fill in `INFLUX_TOKEN_DEPLOY`, `MONGODB_URI`

### 2. Build and start

```bash
docker-compose up --build -d
```

### 3. Open dashboard

```
http://localhost:3000
```

### 4. Check API directly

```bash
# Health check
curl http://localhost:8000/health

# Latest predictions + mill state
curl http://localhost:8000/api/status | python3 -m json.tool

# 4-hour prediction history
curl http://localhost:8000/api/history?hours=4 | python3 -m json.tool

# Recent alerts
curl http://localhost:8000/api/alerts/recent | python3 -m json.tool
```

---

## Dashboard Features

| Feature | Description |
|---|---|
| Mill ON/OFF status | Derived from MAT_PRESENCE + Ghost Rolling via Redis |
| Rolling profile | Auto-detected by get_profile.py, shown in status bar |
| YS / UTS / %El cards | Live values with IS 1786 spec compliance badge |
| Compliance summary | Per-target BELOW / OK / ABOVE status |
| Active alert banner | Appears immediately when any prediction goes out of spec |
| Prediction chart | Last 60 predictions plotted with spec band lines |
| Alert history log | Scrollable log of all out-of-spec events |
| WebSocket | Pushes updates every 2 seconds — no page refresh needed |
| Auto-reconnect | Dashboard reconnects automatically if connection drops |

---

## Service Management

```bash
# View all logs
docker-compose logs -f

# View only inference logs
docker-compose logs -f inference

# Restart a single service without downtime
docker-compose restart api

# Stop everything
docker-compose down

# Stop and remove volumes (clears Redis data)
docker-compose down -v
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Dashboard shows `—` for predictions | prediction_writer.py not called | Add the 3 lines to v3_inference.py |
| "Reconnecting…" in dashboard | API container not healthy | `docker-compose logs api` |
| Inference fails to connect to PostgreSQL | Still using `localhost` in .env | Change to `host.docker.internal` |
| InfluxDB history not loading | Wrong token or bucket name | Check `INFLUX_TOKEN_DEPLOY`, `INFLUX_BUCKET_DEPLOY` |
| Mill always shows OFF | MILL_ON key not set | Verify mill_status.py is calling `write_mill_status()` |

---

## Spec Limits (IS 1786, FE-550D / SeQR-550D)

| Property | Minimum | Maximum | Unit |
|---|---|---|---|
| Yield Strength (YS) | 596 | 650 | MPa |
| Ultimate Tensile Strength (UTS) | 698 | 754 | MPa |
| Percentage Elongation (% El) | 14.0 | — | % |
