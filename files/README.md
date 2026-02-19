# Port KPI Data Pipeline - Complete Setup Guide

## 📋 Overview
This is a complete ETL pipeline that:
- **Extracts** data from CSV file
- **Transforms** using PySpark into Star Schema
- **Loads** into PostgreSQL with proper relationships

## 🏗️ Architecture

### Star Schema Design
```
                    ┌─── dim_vessel
                    │
                    ├─── dim_berth ─── dim_port
                    │
    fact_port_operations
                    ├─── dim_equipment
                    │
                    ├─── dim_service
                    │
                    ├─── dim_call
                    │
                    ├─── dim_ship_owner
                    │
                    └─── dim_date (multiple FK)
```

### Tables
- **dim_date**: All dates with temporal attributes
- **dim_ship_owner**: Ship owner information
- **dim_vessel**: Vessel details (IMO, class, etc.)
- **dim_port**: Port information
- **dim_berth**: Berth/terminal details
- **dim_service**: Service routes
- **dim_call**: Call information
- **dim_equipment**: Equipment types and IDs
- **fact_port_operations**: All metrics and KPIs

## 🐳 Docker Setup

### 1. Install Docker & Docker Compose
```bash
# On Ubuntu/Linux
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Verify installation
docker --version
docker-compose --version
```

### 2. Start PostgreSQL Container
```bash
# Navigate to the project directory
cd /path/to/project

# Start the PostgreSQL container
docker-compose up -d

# Verify it's running
docker ps

# Expected output should show 'port_kpi_db' container running
```

### 3. Verify Database Connection
```bash
# Connect to PostgreSQL
docker exec -it port_kpi_db psql -U port_user -d port_kpi_warehouse

# Inside psql, you can:
\dt              # List all tables
\q               # Quit
```

## 🐍 Python Setup

### 1. Install Python Dependencies
```bash
# Using pip
pip install -r requirements.txt

# Or using conda
conda create -n port_pipeline python=3.10
conda activate port_pipeline
pip install -r requirements.txt
```

### 2. Java Installation (Required for PySpark)
```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install openjdk-11-jdk

# Mac
brew install openjdk@11

# Verify
java -version
```

### 3. Set Environment Variables
```bash
# Export JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64  # Linux
export JAVA_HOME=$(/usr/libexec/java_home)            # Mac

# Add to ~/.bashrc or ~/.zshrc for persistence
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc
source ~/.bashrc
```

## 📊 Data Preparation

### 1. Place CSV File
```bash
# Create data directory
mkdir -p data

# Copy your CSV file here
# The file should be named: Alldataset_port_kpi_.csv
cp /your/path/Alldataset_port_kpi_.csv data/
```

### 2. Verify CSV Structure
```bash
# Check first few lines
head -1 data/Alldataset_port_kpi_.csv  # Check column names
wc -l data/Alldataset_port_kpi_.csv    # Check number of rows
```

## 🚀 Running the Pipeline

### Option 1: Run Locally
```bash
# Install all requirements
pip install -r requirements.txt

# Make sure PostgreSQL is running
docker-compose up -d

# Wait 5-10 seconds for DB to be ready
sleep 10

# Run the pipeline
python etl_pipeline.py
```

### Option 2: Run with Docker (Recommended)
```bash
# Build and run with Docker
docker build -t port-kpi-pipeline .

# Run the container
docker run --network port_pipeline \
  -v $(pwd)/data:/data \
  port-kpi-pipeline python etl_pipeline.py
```

### Option 3: Run with Docker Compose
```bash
# Add spark service to docker-compose.yml and run all together
docker-compose up
```

## 📈 Expected Output

```
============================================================
🚀 PORT KPI ETL PIPELINE STARTED
============================================================
🔧 Initializing Spark Session...
✅ Spark Session created successfully
📡 Connecting to PostgreSQL...
✅ Connected to PostgreSQL successfully

📋 Creating database schema...
✅ Schema created successfully

📖 Reading CSV file: /data/Alldataset_port_kpi_.csv
✅ Loaded 16000 rows
✅ Schema has 95 columns

🔄 Transforming data...
✅ Data types casted successfully

📅 Populating dim_date...
✅ Loaded 1,234 unique dates

⚓ Populating dim_ship_owner...
✅ Loaded 45 ship owners

🚢 Populating dim_vessel...
✅ Loaded vessels

🏭 Populating dim_port...
✅ Loaded 8 ports

🛳️ Populating dim_berth...
✅ Loaded 156 berths

🔧 Populating dim_service...
✅ Loaded 67 services

📞 Populating dim_call...
✅ Loaded 16000 calls

⚙️ Populating dim_equipment...
✅ Loaded 2,345 equipment

📊 Populating fact_port_operations...
✅ Loaded fact records

============================================================
✅ PIPELINE COMPLETED SUCCESSFULLY
============================================================
```

## 🔍 Verification & Queries

### Connect to Database
```bash
# Via psql
psql -h localhost -U port_user -d port_kpi_warehouse

# Docker exec
docker exec -it port_kpi_db psql -U port_user -d port_kpi_warehouse
```

### Useful SQL Queries
```sql
-- Check table row counts
SELECT 'dim_date' as table_name, COUNT(*) as row_count FROM dim_date
UNION ALL SELECT 'dim_vessel', COUNT(*) FROM dim_vessel
UNION ALL SELECT 'dim_berth', COUNT(*) FROM dim_berth
UNION ALL SELECT 'dim_port', COUNT(*) FROM dim_port
UNION ALL SELECT 'dim_service', COUNT(*) FROM dim_service
UNION ALL SELECT 'dim_equipment', COUNT(*) FROM dim_equipment
UNION ALL SELECT 'fact_port_operations', COUNT(*) FROM fact_port_operations;

-- Sample fact data with dimensions
SELECT 
    f.fact_key,
    f.call_key,
    v.vessel_name,
    b.berth_id,
    p.port_name,
    s.service_name,
    f.containers_handled,
    f.operation_time,
    f.status,
    f.delay_weight
FROM fact_port_operations f
LEFT JOIN dim_vessel v ON f.vessel_key = v.vessel_key
LEFT JOIN dim_berth b ON f.berth_key = b.berth_key
LEFT JOIN dim_port p ON f.port_key = p.port_key
LEFT JOIN dim_service s ON f.service_key = s.service_key
LIMIT 10;

-- Get vessel productivity statistics
SELECT 
    v.vessel_name,
    COUNT(f.fact_key) as calls_count,
    AVG(f.vessel_gross_prdvty) as avg_gross_prdvty,
    AVG(f.operation_time) as avg_operation_time,
    SUM(f.containers_handled) as total_containers
FROM fact_port_operations f
JOIN dim_vessel v ON f.vessel_key = v.vessel_key
GROUP BY v.vessel_name
ORDER BY total_containers DESC;

-- Get port utilization metrics
SELECT 
    p.port_name,
    COUNT(DISTINCT f.call_key) as total_calls,
    SUM(f.containers_handled) as total_containers,
    AVG(f.berth_operation_time) as avg_berth_operation_time,
    SUM(CASE WHEN f.status = 'delayed' THEN 1 ELSE 0 END) as delayed_calls
FROM fact_port_operations f
JOIN dim_port p ON f.port_key = p.port_key
GROUP BY p.port_name
ORDER BY total_calls DESC;
```

## 🐛 Troubleshooting

### PostgreSQL Connection Error
```
Error: could not translate host name "localhost" to address
```
**Solution**: Ensure Docker container is running
```bash
docker-compose up -d
docker ps  # Check if port_kpi_db is running
```

### PySpark Not Found
```
Error: Command 'spark-submit' not found
```
**Solution**: Install Spark
```bash
wget https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar -xzf spark-3.5.0-bin-hadoop3.tgz
export SPARK_HOME=/path/to/spark-3.5.0-bin-hadoop3
export PATH=$PATH:$SPARK_HOME/bin
```

### Java Not Found
```
Error: JAVA_HOME is not set
```
**Solution**: Set JAVA_HOME
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
java -version  # Verify
```

### CSV File Not Found
```
Error: No such file or directory: /data/Alldataset_port_kpi_.csv
```
**Solution**: 
1. Place CSV in `./data/` directory
2. Ensure filename matches exactly
3. Check file permissions: `chmod 644 data/Alldataset_port_kpi_.csv`

### Out of Memory Error
```
Error: Java heap space
```
**Solution**: Increase memory in etl_pipeline.py
```python
self.spark = SparkSession.builder \
    .appName("PortKPIPipeline") \
    .config("spark.driver.memory", "4g")  # Increase from 2g
    .config("spark.executor.memory", "4g")
    .getOrCreate()
```

## 📁 Project Structure
```
port-kpi-pipeline/
├── docker-compose.yml          # PostgreSQL container config
├── etl_pipeline.py            # Main ETL script
├── requirements.txt           # Python dependencies
├── data/
│   └── Alldataset_port_kpi_.csv   # Input data
├── README.md                  # This file
└── logs/
    └── pipeline.log          # Pipeline logs
```

## 🔐 Security Notes

### For Production:
1. Change PostgreSQL credentials in `docker-compose.yml`
   ```yaml
   POSTGRES_PASSWORD: your-secure-password-here
   ```

2. Update DB_CONFIG in `etl_pipeline.py`
   ```python
   DB_CONFIG = {
       'host': 'your-prod-host',
       'port': 5432,
       'password': 'your-secure-password',
       ...
   }
   ```

3. Use environment variables
   ```python
   import os
   DB_CONFIG = {
       'password': os.getenv('DB_PASSWORD')
   }
   ```

## 📊 Performance Tips

1. **Increase Partitions** for large datasets:
   ```python
   df = spark.read.option("header", "true").csv(CSV_FILE)
   df.repartition(8)  # Increase parallelism
   ```

2. **Use Caching** for repeated operations:
   ```python
   df.cache()
   df.count()  # Materialize cache
   ```

3. **Monitor Memory**:
   ```bash
   docker stats port_kpi_db
   ```

## 📞 Support

For issues:
1. Check logs: `docker logs port_kpi_db`
2. Check Spark logs in console output
3. Verify CSV file format and encoding
4. Ensure all dependencies are installed: `pip list`

---

**Pipeline Status**: ✅ Ready to Deploy
**Last Updated**: 2025-02-18
**Version**: 1.0
