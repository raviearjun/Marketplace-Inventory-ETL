# 🚀 QUICK REFERENCE CARD

## 📍 Project Location
```
C:\dev\datains\techtest\
```

## 🔐 Credentials

**Airflow UI:** http://localhost:8080
- Username: `admin`
- Password: `admin123`

**PostgreSQL:**
- Host: `localhost`
- Port: `5432`
- Database: `marketplace_dw`
- User: `marketplace_user`
- Password: `marketplace_pass_2026`

## ⚡ Essential Commands

### Start Everything
```powershell
cd C:\dev\datains\techtest
docker-compose up -d
```

### Check Status
```powershell
docker ps
# Wait until all show "healthy" (~30s)
```

### Stop Everything
```powershell
docker-compose stop
```

### Restart Everything
```powershell
docker-compose restart
```

### View Logs
```powershell
# Scheduler logs
docker logs marketplace_airflow_scheduler --tail 50

# Webserver logs
docker logs marketplace_airflow_webserver --tail 50

# Database logs
docker logs marketplace_postgres --tail 50
```

### Database Access
```powershell
# Interactive psql
docker exec -it marketplace_postgres psql -U marketplace_user -d marketplace_dw

# Quick query
docker exec marketplace_postgres psql -U marketplace_user -d marketplace_dw -c "SELECT COUNT(*) FROM stores"
```

### Trigger DAG Manually
```powershell
docker exec marketplace_airflow_webserver airflow dags trigger marketplace_etl_pipeline
```

### Check DAG Status
```powershell
docker exec marketplace_airflow_webserver airflow dags list
```

### Backup Database
```powershell
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
docker exec marketplace_postgres pg_dump -U marketplace_user -d marketplace_dw -F c -f "/tmp/backup_$timestamp.dump"
docker cp "marketplace_postgres:/tmp/backup_$timestamp.dump" ".\data\backup_$timestamp.dump"
```

### Restore Database
```powershell
docker cp ".\data\[backup_file].dump" marketplace_postgres:/tmp/
docker exec marketplace_postgres pg_restore -U marketplace_user -d marketplace_dw -c /tmp/[backup_file].dump
```

## 📊 Data Summary

- **Stores:** 890 rows
- **Products:** 10,000 rows
- **ETL Time:** ~16 seconds
- **Backup File:** `data/marketplace_dw_backup_20260121_010444.dump` (4.4 MB)

## 🔧 Troubleshooting

**Containers won't start?**
```powershell
docker-compose down
docker-compose up -d
```

**Port already in use?**
```powershell
# Check what's using port 8080 or 5432
netstat -ano | findstr ":8080"
netstat -ano | findstr ":5432"
```

**DAG not showing in UI?**
```powershell
# Check DAG file syntax
docker exec marketplace_airflow_webserver python /opt/airflow/dags/marketplace_etl_dag.py
```

**Database connection failed?**
```powershell
# Verify postgres is healthy
docker ps --filter "name=marketplace_postgres"
```

## 🎯 Daily Workflow

1. **Morning:** `docker-compose up -d` → Wait 30s → Open http://localhost:8080
2. **Work:** Trigger DAG, monitor execution, query results
3. **Evening:** Backup DB → `docker-compose stop`

## 📁 Important Files

- `docker-compose.yml` - Infrastructure definition
- `.env` - Credentials (DO NOT COMMIT)
- `dags/marketplace_etl_dag.py` - Main ETL orchestration
- `scripts/extract.py` - Data extraction
- `scripts/transform.py` - Data transformation
- `scripts/load.py` - Data loading
- `sql/init.sql` - Database schema
- `SESSION_LOG.md` - Detailed session notes
- `.gitignore` - Git exclusions

## ⚠️ Before Shutdown

1. ✅ Check git status: `git status`
2. ✅ Backup database (if needed)
3. ✅ Stop containers: `docker-compose stop`
4. ✅ Verify stopped: `docker ps -a`

## 🔄 Resume Tomorrow

```powershell
cd C:\dev\datains\techtest
docker-compose up -d
# Wait 30 seconds
docker ps
# All should show "healthy"
```

---

**Last Updated:** 2026-01-21 01:04  
**Status:** ✅ All systems operational
