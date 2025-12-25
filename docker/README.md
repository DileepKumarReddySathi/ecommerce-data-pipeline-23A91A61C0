# Docker Deployment Guide

## Prerequisites

- Docker ≥ 24.0
- Docker Compose ≥ 2.0
- Minimum 4GB RAM
- Minimum 10GB free disk space

---

## Quick Start

### Build & Start Services
```bash
docker-compose up -d
Verify Containers
bash
Copy code
docker ps
Expected services:

docker-postgres

docker-pipeline

Running Pipeline in Docker
bash
Copy code
docker exec -it docker-pipeline python scripts/pipeline_orchestrator.py
Access PostgreSQL
bash
Copy code
psql -h localhost -U postgres -d ecommerce_db
Viewing Logs
bash
Copy code
docker logs docker-pipeline
docker logs docker-postgres
Stop Services
bash
Copy code
docker-compose down
Cleanup (Remove Volumes)
bash
Copy code
docker-compose down -v
Configuration
Environment Variables
DB_HOST=docker-postgres

DB_PORT=5432

DB_NAME=ecommerce_db

DB_USER=postgres

DB_PASSWORD=*****

Volumes
postgres_data → database persistence

./data → pipeline outputs

./logs → execution logs

Troubleshooting
Port Already in Use
bash
Copy code
netstat -ano | findstr 5432
Database Not Ready
Ensure depends_on with healthcheck is present

Permission Issues
bash
Copy code
chmod -R 777 data logs
Container Fails to Start
bash
Copy code
docker logs <container_name>
Data Persistence
PostgreSQL data persists using named volume

Pipeline outputs persist via bind mounts

Restarting containers does NOT delete data