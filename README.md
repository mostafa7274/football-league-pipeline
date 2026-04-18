# Football League Data Pipeline

![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.1-017CEE?style=flat&logo=apacheairflow)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-E25A1C?style=flat&logo=apachespark)
![MongoDB](https://img.shields.io/badge/MongoDB-7.0-47A248?style=flat&logo=mongodb)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker)
![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=flat&logo=python)

An end-to-end data pipeline that processes football match results, calculates league standings using distributed computing, and stores the output in a NoSQL database — all orchestrated and containerized.

---

## Architecture
match_results.json
│
▼
Apache Spark (PySpark)
├── Reads raw match data
├── Calculates standings (W/D/L/GD/Pts)
└── Writes results to MongoDB
│
▼
MongoDB
└── football_db.standings
│
▼
Apache Airflow (DAG)
├── Task 1: Check MongoDB connection
├── Task 2: Submit PySpark job
└── Task 3: Verify results written

All services run in isolated Docker containers connected via a shared bridge network.

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Apache Airflow 2.9.1 | Pipeline orchestration and scheduling |
| Apache Spark 3.5.1 | Distributed data processing (PySpark) |
| MongoDB 7.0 | NoSQL storage for league standings |
| PostgreSQL 15 | Airflow metadata database |
| Docker + Compose | Containerization and networking |
| Python 3.12 | DAG and PySpark script authoring |

---

## Project Structure
football-league-pipeline/
│
├── dags/
│   └── football_pipeline.py      # Airflow DAG — orchestrates the pipeline
│
├── spark_jobs/
│   └── calculate_standings.py    # PySpark script — processes match data
│
├── data/
│   └── match_results.json        # Input data — one match per line (NDJSON)
│
├── docker/
│   └── docker-compose.yml        # Full stack — Airflow, Spark, MongoDB, Postgres
│
└── README.md

---

## Pipeline Flow

1. **Airflow** triggers the DAG daily (or manually)
2. **Task 1** pings MongoDB to confirm it is reachable
3. **Task 2** submits the PySpark job to the Spark container
4. **PySpark** reads `match_results.json`, computes standings per team, writes to MongoDB
5. **Task 3** queries MongoDB to verify documents were written
6. Results are available in `football_db.standings`

---

## Standings Calculation Logic

For each team, PySpark aggregates all matches (home and away) and calculates:

- **Played** — total matches
- **Wins / Draws / Losses** — match outcomes
- **Goals For / Against** — scoring totals
- **Goal Difference** — goals for minus goals against
- **Points** — 3 per win, 1 per draw, 0 per loss

Results are sorted by points descending, with goal difference as tiebreaker.

---

## Quick Start

### Prerequisites
- Docker Desktop installed and running
- Git

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/football-league-pipeline.git
cd football-league-pipeline
```

### 2. Start all containers
```bash
cd docker
docker-compose up -d
```

Wait about 60 seconds for Airflow to initialize.

### 3. Access the Airflow UI
Open http://localhost:8085 in your browser.
Login with username `admin` and the password from:
```bash
docker exec football_airflow airflow users reset-password --username admin --password admin123
```

### 4. Trigger the pipeline
In the Airflow UI, find `football_league_pipeline` and click the **▶ Run** button.

### 5. Verify results in MongoDB
```bash
docker exec football_mongodb mongosh --eval \
  "db.getSiblingDB('football_db').standings.find().pretty()"
```

---

## Sample Output

### League Standings (MongoDB)
```json
{ "team": "Arsenal",   "played": 4, "wins": 2, "draws": 2, "losses": 0, "goals_for": 7, "goals_against": 4, "goal_difference": 3,  "points": 8 }
{ "team": "Liverpool", "played": 4, "wins": 2, "draws": 1, "losses": 1, "goals_for": 4, "goals_against": 4, "goal_difference": 0,  "points": 7 }
{ "team": "Chelsea",   "played": 4, "wins": 0, "draws": 1, "losses": 3, "goals_for": 4, "goals_against": 7, "goal_difference": -3, "points": 1 }
```

### Airflow DAG — Successful Run
All 3 tasks complete in sequence:
check_mongodb_connection → calculate_standings_spark → verify_mongodb_results

---

## Key Engineering Decisions

**Why PySpark for 6 matches?** Spark is intentionally used here to demonstrate distributed processing skills. In production this pipeline would scale to millions of match records across a full season or multiple leagues with no code changes.

**Why MongoDB?** Match standings are document-shaped data — schema-flexible and easy to query by team. MongoDB is a natural fit and pairs well with Spark via the official connector.

**Why Airflow?** The pipeline is designed to run on a schedule. Airflow provides retry logic, task dependency management, and a UI for monitoring — all essential for production data pipelines.

**Why Docker?** The entire stack runs with a single `docker-compose up` command on any machine, making it fully reproducible.

---

## Author

[LinkedIn]https://github.com/mostafa7274 • [GitHub]https://github.com/mostafa7274
