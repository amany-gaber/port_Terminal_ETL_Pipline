import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, to_timestamp, year, month, dayofmonth, 
    weekofyear, dayofweek, date_format, row_number, substring, lit
)
from pyspark.sql.window import Window
import psycopg2
from psycopg2 import sql
import pandas as pd
from datetime import datetime

# Configuration — use environment variables with defaults for Docker
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgres'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'port_kpi_warehouse'),
    'user': os.getenv('DB_USER', 'port_user'),
    'password': os.getenv('DB_PASSWORD', 'port_password123')
}

EXCEL_FILE = '/data/Alldataset_port_kpi_.xlsx'
DROP_TABLES = os.getenv('ETL_DROP_TABLES', 'false').lower() in ('true', '1', 'yes')

class PortKPIPipeline:
    def __init__(self):
        self.spark = None
        self.conn = None
        self.cursor = None
        self.raw_df = None
        
    def initialize_spark(self):
        """Initialize Spark Session"""
        print("🔧 Initializing Spark Session...")
        self.spark = SparkSession.builder \
            .appName("PortKPIPipeline") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
        print("✅ Spark Session created successfully")
    
    def connect_to_db(self):
        """Connect to PostgreSQL"""
        print("📡 Connecting to PostgreSQL...")
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cursor = self.conn.cursor()
            print("✅ Connected to PostgreSQL successfully")
        except Exception as e:
            print(f"❌ Error connecting to database: {e}")
            raise
    
    def create_schema(self):
        """Create database schema and tables"""
        print("\n📋 Creating database schema...")
        
        if DROP_TABLES:
            print("   ⚠️  ETL_DROP_TABLES=true — dropping existing tables...")
            drop_tables = """
            DROP TABLE IF EXISTS fact_port_operations CASCADE;
            DROP TABLE IF EXISTS dim_equipment CASCADE;
            DROP TABLE IF EXISTS dim_call CASCADE;
            DROP TABLE IF EXISTS dim_service CASCADE;
            DROP TABLE IF EXISTS dim_berth CASCADE;
            DROP TABLE IF EXISTS dim_port CASCADE;
            DROP TABLE IF EXISTS dim_vessel CASCADE;
            DROP TABLE IF EXISTS dim_ship_owner CASCADE;
            DROP TABLE IF EXISTS dim_date CASCADE;
            """
            self.cursor.execute(drop_tables)
            self.conn.commit()
        else:
            print("   ℹ️  ETL_DROP_TABLES=false — keeping existing tables")
        
        # Create Dimension Tables
        dim_date_sql = """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_key SERIAL PRIMARY KEY,
            full_date DATE UNIQUE,
            year INT,
            month INT,
            day INT,
            week INT,
            day_name VARCHAR(20)
        );
        """
        
        dim_ship_owner_sql = """
        CREATE TABLE IF NOT EXISTS dim_ship_owner (
            ship_owner_key SERIAL PRIMARY KEY,
            ship_owner_id VARCHAR(50) UNIQUE,
            ship_owner_name VARCHAR(255),
            ship_owner_group VARCHAR(255)
        );
        """
        
        dim_vessel_sql = """
        CREATE TABLE IF NOT EXISTS dim_vessel (
            vessel_key SERIAL PRIMARY KEY,
            imo VARCHAR(20) UNIQUE,
            vessel_name VARCHAR(255),
            radio_code VARCHAR(50),
            vessel_class VARCHAR(100),
            loa DECIMAL(10,2),
            country_code VARCHAR(5),
            ship_owner_key INT,
            FOREIGN KEY (ship_owner_key) REFERENCES dim_ship_owner(ship_owner_key)
        );
        """
        
        dim_port_sql = """
        CREATE TABLE IF NOT EXISTS dim_port (
            port_key SERIAL PRIMARY KEY,
            port_name VARCHAR(255) UNIQUE,
            country_code VARCHAR(5)
        );
        """
        
        dim_berth_sql = """
        CREATE TABLE IF NOT EXISTS dim_berth (
            berth_key SERIAL PRIMARY KEY,
            berth_id VARCHAR(50) UNIQUE,
            berth_custom_grouping VARCHAR(100),
            port_key INT,
            FOREIGN KEY (port_key) REFERENCES dim_port(port_key)
        );
        """
        
        dim_service_sql = """
        CREATE TABLE IF NOT EXISTS dim_service (
            service_key SERIAL PRIMARY KEY,
            service_id VARCHAR(100) UNIQUE,
            service_name VARCHAR(255)
        );
        """
        
        dim_call_sql = """
        CREATE TABLE IF NOT EXISTS dim_call (
            call_key INT PRIMARY KEY,
            call_id VARCHAR(100) UNIQUE,
            is_specific_call BOOLEAN,
            call_type VARCHAR(100)
        );
        """
        
        dim_equipment_sql = """
        CREATE TABLE IF NOT EXISTS dim_equipment (
            equipment_key SERIAL PRIMARY KEY,
            eqp_id VARCHAR(50) UNIQUE,
            eqp_type VARCHAR(100),
            eqp_type_custom VARCHAR(100),
            equipment_type VARCHAR(100)
        );
        """
        
        fact_operations_sql = """
        CREATE TABLE IF NOT EXISTS fact_port_operations (
            fact_key SERIAL PRIMARY KEY,
            call_key INT NOT NULL,
            equipment_key INT,
            vessel_key INT,
            berth_key INT,
            service_key INT,
            port_key INT,
            arrival_date_key INT,
            departure_date_key INT,
            first_movement_date_key INT,
            last_movement_date_key INT,
            
            -- Time Metrics
            lean_operation_time DECIMAL(10,2),
            net_operation_time DECIMAL(10,2),
            operation_time DECIMAL(10,2),
            berth_net_operation_time DECIMAL(10,2),
            berth_operation_time DECIMAL(10,2),
            duration_in_hours DECIMAL(10,2),
            duration_in_minutes INT,
            idle_time_before_ops DECIMAL(10,2),
            idle_time_after_ops DECIMAL(10,2),
            anchorage_time_in_hours DECIMAL(10,2),
            anchorage_time_in_day DECIMAL(10,2),
            
            -- Container Metrics
            containers_handled INT,
            te_tc20 INT,
            te_tc40 INT,
            te_deba INT,
            te_emba INT,
            te_deba_oog INT,
            te_emba_oog INT,
            
            -- Equipment Metrics
            eqp_operating_time DECIMAL(10,2),
            eqp_networking_time DECIMAL(10,2),
            operating_time_avg DECIMAL(10,2),
            
            -- Productivity
            vessel_gross_prdvty DECIMAL(10,2),
            vessel_net_prdvty DECIMAL(10,2),
            berth_prdvty DECIMAL(10,2),
            
            -- Port Operations
            crane_operating_hours DECIMAL(10,2),
            yard_capacity DECIMAL(10,2),
            current_container_inventory DECIMAL(10,2),
            truck_processing_time DECIMAL(10,2),
            damaged_containers INT,
            gate_moves INT,
            operating_cycles INT,
            
            -- Status & Delay
            status VARCHAR(50),
            delay_weight DECIMAL(10,4),
            
            -- Gear Box Info
            te_gearbox INT,
            te_hatch INT,
            te_sbb INT,
            te_sbtb INT,
            te_twin_lift INT,
            
            FOREIGN KEY (call_key) REFERENCES dim_call(call_key),
            FOREIGN KEY (equipment_key) REFERENCES dim_equipment(equipment_key),
            FOREIGN KEY (vessel_key) REFERENCES dim_vessel(vessel_key),
            FOREIGN KEY (berth_key) REFERENCES dim_berth(berth_key),
            FOREIGN KEY (service_key) REFERENCES dim_service(service_key),
            FOREIGN KEY (port_key) REFERENCES dim_port(port_key),
            FOREIGN KEY (arrival_date_key) REFERENCES dim_date(date_key),
            FOREIGN KEY (departure_date_key) REFERENCES dim_date(date_key),
            FOREIGN KEY (first_movement_date_key) REFERENCES dim_date(date_key),
            FOREIGN KEY (last_movement_date_key) REFERENCES dim_date(date_key)
        );
        """
        
        # Execute all DDL statements
        for sql_stmt in [dim_date_sql, dim_ship_owner_sql, dim_vessel_sql, 
                        dim_port_sql, dim_berth_sql, dim_service_sql, 
                        dim_call_sql, dim_equipment_sql, fact_operations_sql]:
            self.cursor.execute(sql_stmt)
        
        self.conn.commit()
        print("✅ Schema created successfully")
    
    def read_data(self):
        """Read Excel file using pandas then convert to Spark DataFrame"""
        print(f"\n📖 Reading Excel file: {EXCEL_FILE}")
        try:
            # Read Excel with pandas (openpyxl engine)
            self.original_pandas_df = pd.read_excel(EXCEL_FILE, engine='openpyxl')
            print(f"✅ Pandas loaded {len(self.original_pandas_df)} rows, {len(self.original_pandas_df.columns)} columns")
            
            # Convert to Spark DataFrame
            self.raw_df = self.spark.createDataFrame(self.original_pandas_df)
            
            print(f"✅ Loaded {self.raw_df.count()} rows into Spark")
            print(f"✅ Schema has {len(self.raw_df.columns)} columns")
            return self.raw_df
        except Exception as e:
            print(f"❌ Error reading Excel: {e}")
            raise
    
    def transform_data(self):
        """Clean and prepare data for star schema"""
        print("\n🔄 Transforming data...")
        
        df = self.raw_df
        
        # Cast data types — only select columns that exist and are needed
        df = df.select(
            col("Call_Key").cast("int"),
            col("Call_ID").cast("string"),
            col("Arvl_Dt").cast("timestamp"),
            col("Dptr_Dt").cast("timestamp"),
            col("First_Movement_Dt").cast("timestamp"),
            col("Last_Movement_Dt").cast("timestamp"),
            col("Terminal_Site_Key").cast("int"),
            col("Type").cast("string"),
            col("DurationInHours").cast("double"),
            col("DurationInMinutes").cast("int"),
            col("IdleTimeBeforeOps").cast("double"),
            col("IdleTimeAfterOps").cast("double"),
            col("Service_ID").cast("string"),
            col("Service_Nm").cast("string"),
            col("BerthID").cast("string"),
            col("BerthCustomGrouping").cast("string"),
            col("VesselName").cast("string"),
            col("IMO").cast("string"),
            col("RadioCode").cast("string"),
            col("VesselClass").cast("string"),
            col("LOA").cast("double"),
            col("CountryCode").cast("string"),
            col("ShipOwner_Id").cast("string"),
            col("ShipOwnerName").cast("string"),
            col("ShipOwnerGroup").cast("string"),
            col("IsSpecificCall").cast("boolean"),
            col("AnchorageTimeInHours").cast("double"),
            col("AnchorageTimeInDay").cast("double"),
            col("TE_TC20").cast("int"),
            col("TE_TC40").cast("int"),
            col("TE_DEBA").cast("int"),
            col("TE_EMBA").cast("int"),
            col("TE_DEBA_OOG").cast("int"),
            col("TE_EMBA_OOG").cast("int"),
            col("TE_GearBox").cast("int"),
            col("TE_Hatch").cast("int"),
            col("TE_SBB").cast("int"),
            col("TE_SBTB").cast("int"),
            col("TE_Twin_Lift").cast("int"),
            col("Vessel_Gross_Prdvty").cast("double"),
            col("Vessel_Net_Prdvty").cast("double"),
            col("Berth_Prdvty").cast("double"),
            col("Eqp_Key").cast("int"),
            col("Operating_Time").cast("double"),
            col("Networking_Time").cast("double"),
            col("Eqp_ID").cast("string"),
            col("Eqp_Type").cast("string"),
            col("Eqp_Type_Custom").cast("string"),
            col("Operating_Time_Avg").cast("double"),
            col("status").cast("string"),
            col("Delay_Weight").cast("double"),
            col("port_name").cast("string"),
            col("Containers_Handled").cast("int"),
            col("Crane_Operating_Hours").cast("double"),
            col("Yard_Capacity").cast("double"),
            col("Current_Container_Inventory").cast("double"),
            col("Truck_Processing_Time").cast("double"),
            col("Damaged_Containers").cast("int"),
            col("Gate_Moves").cast("int"),
            col("Operating_Cycles").cast("int"),
            col("Equipment_Type").cast("string"),
            col("Lean_Operation_Time").cast("double"),
            col("Net_Operation_Time").cast("double"),
            col("Operation_Time").cast("double"),
            col("Berth_Net_Operation_Time").cast("double"),
            col("Berth_Operation_Time").cast("double"),
        )
        
        print("✅ Data types casted successfully")
        return df
    
    def populate_dim_date(self, df):
        """Populate dimension_date table using original pandas data directly"""
        print("\n📅 Populating dim_date...")
        
        # Use the ORIGINAL pandas DataFrame (not Spark round-tripped) to avoid timestamp loss
        date_columns = ["Arvl_Dt", "Dptr_Dt", "First_Movement_Dt", "Last_Movement_Dt"]
        all_dates = set()
        
        for col_name in date_columns:
            if col_name in self.original_pandas_df.columns:
                series = self.original_pandas_df[col_name]
                for val in series:
                    if val is not None and not (isinstance(val, float) and pd.isna(val)):
                        try:
                            dt = pd.Timestamp(val)
                            if not pd.isna(dt):
                                all_dates.add(dt.date())
                        except:
                            pass
        
        print(f"   Found {len(all_dates)} unique dates")
        
        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        inserted = 0
        for d in sorted(all_dates):
            try:
                insert_sql = """
                INSERT INTO dim_date (full_date, year, month, day, week, day_name)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """
                self.cursor.execute(insert_sql, (
                    d,
                    d.year,
                    d.month,
                    d.day,
                    d.isocalendar()[1],
                    day_names[d.weekday()]
                ))
                inserted += 1
            except Exception as e:
                print(f"⚠️ Date insert error for {d}: {e}")
                self.conn.rollback()
        
        self.conn.commit()
        print(f"✅ Loaded {inserted} unique dates")
    
    def populate_dim_ship_owner(self, df):
        """Populate dim_ship_owner"""
        print("\n⚓ Populating dim_ship_owner...")
        
        ship_owner_df = df.select(
            col("ShipOwner_Id").alias("ship_owner_id"),
            col("ShipOwnerName").alias("ship_owner_name"),
            col("ShipOwnerGroup").alias("ship_owner_group")
        ).distinct().filter(col("ship_owner_id").isNotNull())
        
        self._insert_from_spark_to_postgres(ship_owner_df, "dim_ship_owner")
        print(f"✅ Loaded {ship_owner_df.count()} ship owners")
    
    def populate_dim_vessel(self, df):
        """Populate dim_vessel"""
        print("\n🚢 Populating dim_vessel...")
        
        vessel_df = df.select(
            col("IMO").alias("imo"),
            col("VesselName").alias("vessel_name"),
            col("RadioCode").alias("radio_code"),
            col("VesselClass").alias("vessel_class"),
            col("LOA").alias("loa"),
            col("CountryCode").alias("country_code"),
            col("ShipOwner_Id").alias("ship_owner_id")
        ).distinct().filter(col("imo").isNotNull())
        
        self._insert_from_spark_to_postgres(vessel_df, "dim_vessel", 
                                           foreign_key_join={
                                               "ship_owner_id": ("dim_ship_owner", "ship_owner_id", "ship_owner_key")
                                           })
        print(f"✅ Loaded vessels")
    
    def populate_dim_port(self, df):
        """Populate dim_port"""
        print("\n🏭 Populating dim_port...")
        
        port_df = df.select(
            col("port_name").alias("port_name"),
            col("CountryCode").alias("country_code")
        ).distinct().filter(col("port_name").isNotNull())
        
        self._insert_from_spark_to_postgres(port_df, "dim_port")
        print(f"✅ Loaded {port_df.count()} ports")
    
    def populate_dim_berth(self, df):
        """Populate dim_berth"""
        print("\n🛳️ Populating dim_berth...")
        
        berth_df = df.select(
            col("BerthID").alias("berth_id"),
            col("BerthCustomGrouping").alias("berth_custom_grouping"),
            col("port_name").alias("port_name")
        ).distinct().filter(col("berth_id").isNotNull())
        
        self._insert_from_spark_to_postgres(berth_df, "dim_berth",
                                           foreign_key_join={
                                               "port_name": ("dim_port", "port_name", "port_key")
                                           })
        print(f"✅ Loaded {berth_df.count()} berths")
    
    def populate_dim_service(self, df):
        """Populate dim_service"""
        print("\n🔧 Populating dim_service...")
        
        service_df = df.select(
            col("Service_ID").alias("service_id"),
            col("Service_Nm").alias("service_name")
        ).distinct().filter(col("service_id").isNotNull())
        
        self._insert_from_spark_to_postgres(service_df, "dim_service")
        print(f"✅ Loaded {service_df.count()} services")
    
    def populate_dim_call(self, df):
        """Populate dim_call"""
        print("\n📞 Populating dim_call...")
        
        call_df = df.select(
            col("Call_Key").alias("call_key"),
            col("Call_ID").alias("call_id"),
            col("IsSpecificCall").alias("is_specific_call"),
            col("Type").alias("call_type")
        ).distinct().filter(col("call_key").isNotNull())
        
        self._insert_from_spark_to_postgres(call_df, "dim_call")
        print(f"✅ Loaded {call_df.count()} calls")
    
    def populate_dim_equipment(self, df):
        """Populate dim_equipment"""
        print("\n⚙️ Populating dim_equipment...")
        
        equipment_df = df.select(
            col("Eqp_ID").alias("eqp_id"),
            col("Eqp_Type").alias("eqp_type"),
            col("Eqp_Type_Custom").alias("eqp_type_custom"),
            col("Equipment_Type").alias("equipment_type")
        ).distinct().filter(col("eqp_id").isNotNull())
        
        self._insert_from_spark_to_postgres(equipment_df, "dim_equipment")
        print(f"✅ Loaded {equipment_df.count()} equipment")
    
    def populate_fact_table(self, df):
        """Populate fact_port_operations"""
        print("\n📊 Populating fact_port_operations...")
        
        # Build fact table with source columns
        fact_df = df.select(
            col("Call_Key"),
            col("Eqp_ID"),
            col("IMO"),
            col("BerthID"),
            col("Service_ID"),
            col("port_name"),
            to_date(col("Arvl_Dt")).alias("arrival_date"),
            to_date(col("Dptr_Dt")).alias("departure_date"),
            to_date(col("First_Movement_Dt")).alias("first_movement_date"),
            to_date(col("Last_Movement_Dt")).alias("last_movement_date"),
            col("Lean_Operation_Time"),
            col("Net_Operation_Time"),
            col("Operation_Time"),
            col("Berth_Net_Operation_Time"),
            col("Berth_Operation_Time"),
            col("DurationInHours"),
            col("DurationInMinutes"),
            col("IdleTimeBeforeOps"),
            col("IdleTimeAfterOps"),
            col("AnchorageTimeInHours"),
            col("AnchorageTimeInDay"),
            col("Containers_Handled"),
            col("TE_TC20"),
            col("TE_TC40"),
            col("TE_DEBA"),
            col("TE_EMBA"),
            col("TE_DEBA_OOG"),
            col("TE_EMBA_OOG"),
            col("Operating_Time").alias("eqp_operating_time"),
            col("Networking_Time"),
            col("Operating_Time_Avg"),
            col("Vessel_Gross_Prdvty"),
            col("Vessel_Net_Prdvty"),
            col("Berth_Prdvty"),
            col("Crane_Operating_Hours"),
            col("Yard_Capacity"),
            col("Current_Container_Inventory"),
            col("Truck_Processing_Time"),
            col("Damaged_Containers"),
            col("Gate_Moves"),
            col("Operating_Cycles"),
            col("status"),
            col("Delay_Weight"),
            col("TE_GearBox"),
            col("TE_Hatch"),
            col("TE_SBB"),
            col("TE_SBTB"),
            col("TE_Twin_Lift")
        )
        
        # Convert to Pandas and insert with proper FK resolution
        pandas_df = fact_df.toPandas()
        
        inserted = 0
        errors = 0
        total = len(pandas_df)
        
        for idx, row in pandas_df.iterrows():
            # Get foreign key IDs
            call_key = row.get("Call_Key")
            equipment_key = self._get_fk_id("dim_equipment", "eqp_id", row.get("Eqp_ID"))
            vessel_key = self._get_fk_id("dim_vessel", "imo", row.get("IMO"))
            berth_key = self._get_fk_id("dim_berth", "berth_id", row.get("BerthID"))
            service_key = self._get_fk_id("dim_service", "service_id", row.get("Service_ID"))
            port_key = self._get_fk_id("dim_port", "port_name", row.get("port_name"))
            arrival_date_key = self._get_fk_id("dim_date", "full_date", str(row.get("arrival_date")))
            departure_date_key = self._get_fk_id("dim_date", "full_date", str(row.get("departure_date")))
            first_move_date_key = self._get_fk_id("dim_date", "full_date", str(row.get("first_movement_date")))
            last_move_date_key = self._get_fk_id("dim_date", "full_date", str(row.get("last_movement_date")))
            
            insert_sql = """
            INSERT INTO fact_port_operations (
                call_key, equipment_key, vessel_key, berth_key, service_key, port_key,
                arrival_date_key, departure_date_key, first_movement_date_key, last_movement_date_key,
                lean_operation_time, net_operation_time, operation_time, berth_net_operation_time,
                berth_operation_time, duration_in_hours, duration_in_minutes, idle_time_before_ops,
                idle_time_after_ops, anchorage_time_in_hours, anchorage_time_in_day, containers_handled,
                te_tc20, te_tc40, te_deba, te_emba, te_deba_oog, te_emba_oog, eqp_operating_time,
                eqp_networking_time, operating_time_avg, vessel_gross_prdvty, vessel_net_prdvty,
                berth_prdvty, crane_operating_hours, yard_capacity, current_container_inventory,
                truck_processing_time, damaged_containers, gate_moves, operating_cycles,
                status, delay_weight, te_gearbox, te_hatch, te_sbb, te_sbtb, te_twin_lift
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s
            );
            """
            
            try:
                values = (
                    self._safe_val(call_key),
                    equipment_key,
                    vessel_key,
                    berth_key,
                    service_key,
                    port_key,
                    arrival_date_key,
                    departure_date_key,
                    first_move_date_key,
                    last_move_date_key,
                    self._safe_val(row.get("Lean_Operation_Time")),
                    self._safe_val(row.get("Net_Operation_Time")),
                    self._safe_val(row.get("Operation_Time")),
                    self._safe_val(row.get("Berth_Net_Operation_Time")),
                    self._safe_val(row.get("Berth_Operation_Time")),
                    self._safe_val(row.get("DurationInHours")),
                    self._safe_val(row.get("DurationInMinutes")),
                    self._safe_val(row.get("IdleTimeBeforeOps")),
                    self._safe_val(row.get("IdleTimeAfterOps")),
                    self._safe_val(row.get("AnchorageTimeInHours")),
                    self._safe_val(row.get("AnchorageTimeInDay")),
                    self._safe_val(row.get("Containers_Handled")),
                    self._safe_val(row.get("TE_TC20")),
                    self._safe_val(row.get("TE_TC40")),
                    self._safe_val(row.get("TE_DEBA")),
                    self._safe_val(row.get("TE_EMBA")),
                    self._safe_val(row.get("TE_DEBA_OOG")),
                    self._safe_val(row.get("TE_EMBA_OOG")),
                    self._safe_val(row.get("eqp_operating_time")),
                    self._safe_val(row.get("Networking_Time")),
                    self._safe_val(row.get("Operating_Time_Avg")),
                    self._safe_val(row.get("Vessel_Gross_Prdvty")),
                    self._safe_val(row.get("Vessel_Net_Prdvty")),
                    self._safe_val(row.get("Berth_Prdvty")),
                    self._safe_val(row.get("Crane_Operating_Hours")),
                    self._safe_val(row.get("Yard_Capacity")),
                    self._safe_val(row.get("Current_Container_Inventory")),
                    self._safe_val(row.get("Truck_Processing_Time")),
                    self._safe_val(row.get("Damaged_Containers")),
                    self._safe_val(row.get("Gate_Moves")),
                    self._safe_val(row.get("Operating_Cycles")),
                    self._safe_val(row.get("status")),
                    self._safe_val(row.get("Delay_Weight")),
                    self._safe_val(row.get("TE_GearBox")),
                    self._safe_val(row.get("TE_Hatch")),
                    self._safe_val(row.get("TE_SBB")),
                    self._safe_val(row.get("TE_SBTB")),
                    self._safe_val(row.get("TE_Twin_Lift"))
                )
                self.cursor.execute(insert_sql, values)
                inserted += 1
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"⚠️ Error inserting row {idx}: {e}")
            
            # Progress reporting
            if (idx + 1) % 2000 == 0:
                self.conn.commit()
                print(f"   Progress: {idx + 1}/{total} rows...")
        
        self.conn.commit()
        print(f"✅ Loaded {inserted} fact records ({errors} errors)")
    
    @staticmethod
    def _safe_val(value):
        """Convert pandas NaN/NaT to None for psycopg2"""
        if value is None:
            return None
        if pd.isna(value):
            return None
        return value
    
    def _get_fk_id(self, table_name, column_name, value):
        """Get foreign key ID from a dimension table"""
        if not value or str(value) in ("None", "nan", "NaT"):
            return None
        
        try:
            # Get the primary key column name (first column)
            query = f"SELECT * FROM {table_name} WHERE {column_name} = %s LIMIT 1"
            self.cursor.execute(query, (str(value),))
            result = self.cursor.fetchone()
            if result:
                return result[0]  # Return the primary key (first column)
        except Exception as e:
            # Reset the cursor state on error
            self.conn.rollback()
        return None
    
    def _insert_from_spark_to_postgres(self, spark_df, table_name, foreign_key_join=None):
        """Insert Spark DataFrame to PostgreSQL with foreign key resolution"""
        pandas_df = spark_df.toPandas()
        
        # Handle foreign key joins if needed
        if foreign_key_join:
            for src_col, (fk_table, fk_col, fk_key_col) in foreign_key_join.items():
                fk_values = []
                for value in pandas_df[src_col]:
                    fk_id = self._get_fk_id(fk_table, fk_col, value)
                    fk_values.append(fk_id)
                # Use the explicit FK key column name from the mapping
                pandas_df[fk_key_col] = fk_values
                pandas_df = pandas_df.drop(src_col, axis=1)
        
        # Insert using psycopg2
        columns = list(pandas_df.columns)
        placeholders = ",".join(["%s"] * len(columns))
        insert_query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
        
        inserted = 0
        for _, row in pandas_df.iterrows():
            try:
                # Convert NaN values to None
                clean_row = tuple(None if pd.isna(v) else v for v in row)
                self.cursor.execute(insert_query, clean_row)
                inserted += 1
            except Exception as e:
                print(f"⚠️ Insert error in {table_name}: {e}")
                self.conn.rollback()
        
        self.conn.commit()
        print(f"   Inserted {inserted} rows into {table_name}")
    
    def _get_jdbc_url(self):
        """Get JDBC URL for Spark"""
        return f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    
    def _get_jdbc_properties(self):
        """Get JDBC properties for Spark"""
        return {
            "user": DB_CONFIG['user'],
            "password": DB_CONFIG['password'],
            "driver": "org.postgresql.Driver"
        }
    
    def run(self):
        """Execute the entire pipeline"""
        try:
            print("=" * 60)
            print("🚀 PORT KPI ETL PIPELINE STARTED")
            print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   DB Host: {DB_CONFIG['host']}")
            print(f"   Data File: {EXCEL_FILE}")
            print(f"   Drop Tables: {DROP_TABLES}")
            print("=" * 60)
            
            self.initialize_spark()
            self.connect_to_db()
            self.create_schema()
            
            # Skip loading if data already exists (unless tables were dropped)
            if not DROP_TABLES:
                self.cursor.execute(
                    "SELECT EXISTS ("
                    "  SELECT 1 FROM information_schema.tables WHERE table_name='fact_port_operations'"
                    ")"
                )
                table_exists = self.cursor.fetchone()[0]
                if table_exists:
                    self.cursor.execute("SELECT COUNT(*) FROM fact_port_operations")
                    count = self.cursor.fetchone()[0]
                    if count > 0:
                        print(f"\n✅ Data already loaded ({count} fact records). Skipping ETL.")
                        print("   Set ETL_DROP_TABLES=true to force a full reload.")
                        return
            
            self.read_data()
            transformed_df = self.transform_data()
            
            # Populate dimensions (order matters for FK dependencies)
            self.populate_dim_date(transformed_df)
            self.populate_dim_ship_owner(transformed_df)
            self.populate_dim_vessel(transformed_df)
            self.populate_dim_port(transformed_df)
            self.populate_dim_berth(transformed_df)
            self.populate_dim_service(transformed_df)
            self.populate_dim_call(transformed_df)
            self.populate_dim_equipment(transformed_df)
            
            # Populate fact table
            self.populate_fact_table(transformed_df)
            
            print("\n" + "=" * 60)
            print("✅ PIPELINE COMPLETED SUCCESSFULLY")
            print("=" * 60)
            
        except Exception as e:
            print(f"\n❌ PIPELINE FAILED: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Close connections"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        if self.spark:
            self.spark.stop()

if __name__ == "__main__":
    pipeline = PortKPIPipeline()
    pipeline.run()
