# ========================================
# Demo Script - Marketplace ETL Pipeline
# ========================================

Write-Host "`n=== 1. Starting Docker Containers ===" -ForegroundColor Cyan
docker-compose up -d

Write-Host "`n=== 2. Waiting for services to be ready ===" -ForegroundColor Cyan
Start-Sleep -Seconds 15

Write-Host "`n=== 3. List Available DAGs ===" -ForegroundColor Cyan
docker exec marketplace_airflow_webserver airflow dags list

Write-Host "`n=== 4. Triggering ETL Pipeline ===" -ForegroundColor Cyan
docker exec marketplace_airflow_webserver airflow dags trigger marketplace_etl_pipeline

Write-Host "`n=== 5. Waiting for pipeline to complete ===" -ForegroundColor Cyan
Start-Sleep -Seconds 20

Write-Host "`n=== 6. Check Pipeline Status ===" -ForegroundColor Cyan
docker exec marketplace_airflow_webserver airflow dags list-runs -d marketplace_etl_pipeline | Select-Object -First 5

Write-Host "`n=== 7. Query: Count Records ===" -ForegroundColor Cyan
docker exec marketplace_postgres psql -U marketplace_user -d marketplace_dw -c "SELECT 'Stores' AS table_name, COUNT(*) AS total FROM stores UNION ALL SELECT 'Products', COUNT(*) FROM products;"

Write-Host "`n=== 8. Query: Sample Data ===" -ForegroundColor Cyan
docker exec marketplace_postgres psql -U marketplace_user -d marketplace_dw -c "SELECT store_name, store_location_city FROM stores LIMIT 3;"

Write-Host "`n=== 9. Query: Join Validation ===" -ForegroundColor Cyan
docker exec marketplace_postgres psql -U marketplace_user -d marketplace_dw -c "SELECT s.store_name, COUNT(p.id) AS products FROM stores s LEFT JOIN products p ON s.store_id = p.store_id GROUP BY s.store_name ORDER BY products DESC LIMIT 3;"

Write-Host "`n=== Demo Complete! ===" -ForegroundColor Green
