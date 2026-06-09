# Real-Time Security Anomaly Detection Pipeline

Big Data pipeline that detects security anomalies on a real-time event stream
using a **Kappa architecture**: Kafka (ingestion) + Spark Structured Streaming
(processing) + PostgreSQL (live views) + Grafana (dashboard).

> Module: Architecture et Technologies BigData — Master Sécurité IT & BigData

## Architecture

```
[Producer] -> [Kafka] -> [Spark Structured Streaming] -> [PostgreSQL] -> [Grafana]
  Python       buffer      detect anomalies (in-memory)     live views     dashboard
                                     |
                                     v
                              [Parquet history]
```

A synthetic Python producer generates authentication/network events and
deliberately injects anomalies (bursts of failed logins, blacklisted IPs).
Spark consumes the `security-events` Kafka topic in micro-batches, applies
detection rules (e.g. >5 failed logins per IP in a 1-minute window), and writes
alerts + aggregates to PostgreSQL while archiving raw events to Parquet.

## Tech stack (100% open-source)

| Component        | Role                                  |
|------------------|---------------------------------------|
| Docker Compose   | Orchestrates all services on one host |
| Apache Kafka     | Message ingestion (pub/sub buffer)    |
| Apache Spark     | Real-time stream processing (PySpark) |
| PostgreSQL       | Storage of live views / alerts        |
| Parquet          | Raw replayable history                |
| Grafana          | Real-time dashboard                   |

## Project structure

```
security-stream-project/
├── docker-compose.yml              # all services (Kafka, Spark, Postgres, Grafana)
├── producer/
│   ├── producer.py                 # synthetic event generator -> Kafka
│   └── requirements.txt
├── spark/
│   ├── stream_processor.py         # Kafka -> detect -> Postgres + Parquet
│   ├── init_db.sql                 # Postgres schema (auto-loaded)
│   └── requirements.txt
├── dashboard/
│   ├── seed_data.py                # fills Postgres with fake data (dashboard dev)
│   ├── seed_requirements.txt
│   ├── dashboard.py                # optional Streamlit alternative
│   ├── requirements.txt
│   └── grafana/
│       ├── PANEL_QUERIES.md        # SQL for every Grafana panel
│       └── provisioning/           # auto-configures the Postgres datasource
├── data/parquet/                   # raw event archive (runtime)
└── docs/                           # diagrams, report, slides
```

---

## A) Full pipeline (ingestion + processing team)

```bash
# 1. Boot everything
docker compose up -d

# 2. Install host-side Python deps
pip install -r producer/requirements.txt

# 3. Start the synthetic producer (terminal 1)
python producer/producer.py

# 4. Submit the Spark job (terminal 2) — downloads connector JARs on first run
docker compose exec spark spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3 \
  /app/stream_processor.py
```

---

## B) Dashboard work (visualization team) — NO Kafka/Spark needed

The dashboard reads only from PostgreSQL, so it can be built entirely against
seeded fake data while the rest of the pipeline is still in progress. The fake
data uses the **identical schema** to the real Spark output, so nothing needs
to change when the live pipeline is connected later.

```bash
# 1. Start only Postgres + Grafana
docker compose up -d postgres grafana

# 2. Install + run the seed script (fills tables with fake alerts + counts)
pip install -r dashboard/seed_requirements.txt
python dashboard/seed_data.py        # re-run anytime for fresh numbers

# 3. Open Grafana
#    http://localhost:3000   (login: admin / admin)
#    The "SecurityDB" Postgres datasource is already configured.

# 4. Build panels using the SQL in:
#    dashboard/grafana/PANEL_QUERIES.md
#    Set dashboard time range to "Last 1 hour", auto-refresh 5s.
```

When the real pipeline is ready: stop seeding, start the producer + Spark.
The same tables fill with live data and every panel keeps working unchanged.

---

## Service endpoints

| Service   | URL / Port              | Credentials   |
|-----------|-------------------------|---------------|
| Grafana   | http://localhost:3000   | admin / admin |
| Postgres  | localhost:5432          | secu / secret |
| Kafka     | localhost:9092          | —             |

## Team roles

- **Ingestion:** producer + Kafka
- **Processing:** Spark Structured Streaming
- **Visualization/Integration:** PostgreSQL + Grafana + Docker

## Stop / reset

```bash
docker compose down            # stop, keep data
docker compose down -v         # stop and wipe Postgres + Grafana volumes
```
