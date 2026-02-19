import json
import sys
import os
from datetime import datetime, date
from kafka import KafkaConsumer
import psycopg2

# ============================================================
# Configuration
# ============================================================

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgres'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'port_kpi_warehouse'),
    'user': os.getenv('DB_USER', 'port_user'),
    'password': os.getenv('DB_PASSWORD', 'port_password123')
}

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092')
TOPIC = os.getenv('KAFKA_TOPIC', 'port_operations')

# ============================================================
# Database Helper Functions
# ============================================================

def get_connection():
    """Create PostgreSQL connection."""
    return psycopg2.connect(**DB_CONFIG)


def get_or_create_date(cursor, conn, date_str):
    """Upsert a date into dim_date and return its key."""
    if not date_str or date_str in ('None', 'nan', 'NaT'):
        return None
    try:
        dt = datetime.fromisoformat(date_str)
        d = dt.date()
    except:
        return None

    cursor.execute("SELECT date_key FROM dim_date WHERE full_date = %s", (d,))
    result = cursor.fetchone()
    if result:
        return result[0]

    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    cursor.execute("""
        INSERT INTO dim_date (full_date, year, month, day, week, day_name)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (full_date) DO NOTHING
        RETURNING date_key
    """, (d, d.year, d.month, d.day, d.isocalendar()[1], day_names[d.weekday()]))
    conn.commit()

    result = cursor.fetchone()
    if result:
        return result[0]

    # If ON CONFLICT hit, fetch the existing key
    cursor.execute("SELECT date_key FROM dim_date WHERE full_date = %s", (d,))
    result = cursor.fetchone()
    return result[0] if result else None


def get_or_create_dim(cursor, conn, table, unique_col, unique_val, extra_cols=None):
    """Generic upsert for dimension tables. Returns the surrogate key."""
    if not unique_val or str(unique_val) in ('None', 'nan'):
        return None

    # Get PK column name (first column)
    cursor.execute(f"SELECT * FROM {table} WHERE {unique_col} = %s LIMIT 1", (str(unique_val),))
    result = cursor.fetchone()
    if result:
        return result[0]

    # Insert new dimension record
    if extra_cols:
        cols = [unique_col] + list(extra_cols.keys())
        vals = [str(unique_val)] + list(extra_cols.values())
    else:
        cols = [unique_col]
        vals = [str(unique_val)]

    placeholders = ", ".join(["%s"] * len(cols))
    col_names = ", ".join(cols)

    try:
        cursor.execute(
            f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
            vals
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"⚠️ Insert into {table} failed: {e}")

    cursor.execute(f"SELECT * FROM {table} WHERE {unique_col} = %s LIMIT 1", (str(unique_val),))
    result = cursor.fetchone()
    return result[0] if result else None


def safe_val(v):
    """Convert None-like values to Python None."""
    if v is None:
        return None
    if isinstance(v, str) and v.lower() in ('none', 'nan', 'nat', ''):
        return None
    return v


# ============================================================
# Process a single Kafka message
# ============================================================

def process_record(data, cursor, conn):
    """Process one port operation record from Kafka into the star schema."""

    # --- Upsert Dimensions ---

    # dim_ship_owner
    get_or_create_dim(cursor, conn, "dim_ship_owner", "ship_owner_id", data.get("ShipOwner_Id"), {
        "ship_owner_name": data.get("ShipOwnerName"),
        "ship_owner_group": data.get("ShipOwnerGroup"),
    })

    # dim_vessel (with FK to ship_owner)
    vessel_key = None
    imo = data.get("IMO")
    if imo:
        cursor.execute("SELECT vessel_key FROM dim_vessel WHERE imo = %s", (str(imo),))
        result = cursor.fetchone()
        if result:
            vessel_key = result[0]
        else:
            # Get ship_owner_key
            so_key = None
            so_id = data.get("ShipOwner_Id")
            if so_id:
                cursor.execute("SELECT ship_owner_key FROM dim_ship_owner WHERE ship_owner_id = %s", (str(so_id),))
                r = cursor.fetchone()
                so_key = r[0] if r else None

            try:
                cursor.execute("""
                    INSERT INTO dim_vessel (imo, vessel_name, radio_code, vessel_class, loa, country_code, ship_owner_key)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (imo) DO NOTHING
                """, (str(imo), data.get("VesselName"), data.get("RadioCode"),
                      data.get("VesselClass"), data.get("LOA"), data.get("CountryCode"), so_key))
                conn.commit()
            except:
                conn.rollback()

            cursor.execute("SELECT vessel_key FROM dim_vessel WHERE imo = %s", (str(imo),))
            result = cursor.fetchone()
            vessel_key = result[0] if result else None

    # dim_port
    port_key = get_or_create_dim(cursor, conn, "dim_port", "port_name", data.get("port_name"), {
        "country_code": data.get("CountryCode")
    })

    # dim_berth (with FK to port)
    berth_key = None
    berth_id = data.get("BerthID")
    if berth_id:
        cursor.execute("SELECT berth_key FROM dim_berth WHERE berth_id = %s", (str(berth_id),))
        result = cursor.fetchone()
        if result:
            berth_key = result[0]
        else:
            try:
                cursor.execute("""
                    INSERT INTO dim_berth (berth_id, berth_custom_grouping, port_key)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (berth_id) DO NOTHING
                """, (str(berth_id), data.get("BerthCustomGrouping"), port_key))
                conn.commit()
            except:
                conn.rollback()

            cursor.execute("SELECT berth_key FROM dim_berth WHERE berth_id = %s", (str(berth_id),))
            result = cursor.fetchone()
            berth_key = result[0] if result else None

    # dim_service
    service_key = get_or_create_dim(cursor, conn, "dim_service", "service_id", data.get("Service_ID"), {
        "service_name": data.get("Service_Nm")
    })

    # dim_call
    call_key = data.get("Call_Key")
    if call_key:
        cursor.execute("SELECT call_key FROM dim_call WHERE call_key = %s", (int(call_key),))
        if not cursor.fetchone():
            try:
                cursor.execute("""
                    INSERT INTO dim_call (call_key, call_id, is_specific_call, call_type)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (int(call_key), data.get("Call_ID"), data.get("IsSpecificCall"), data.get("Type")))
                conn.commit()
            except:
                conn.rollback()

    # dim_equipment
    equipment_key = get_or_create_dim(cursor, conn, "dim_equipment", "eqp_id", data.get("Eqp_ID"), {
        "eqp_type": data.get("Eqp_Type"),
        "eqp_type_custom": data.get("Eqp_Type_Custom"),
        "equipment_type": data.get("Equipment_Type"),
    })

    # dim_date (for each timestamp)
    arrival_date_key = get_or_create_date(cursor, conn, data.get("Arvl_Dt"))
    departure_date_key = get_or_create_date(cursor, conn, data.get("Dptr_Dt"))
    first_move_date_key = get_or_create_date(cursor, conn, data.get("First_Movement_Dt"))
    last_move_date_key = get_or_create_date(cursor, conn, data.get("Last_Movement_Dt"))

    # --- Insert Fact Record ---
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

    values = (
        safe_val(call_key),
        equipment_key,
        vessel_key,
        berth_key,
        service_key,
        port_key,
        arrival_date_key,
        departure_date_key,
        first_move_date_key,
        last_move_date_key,
        safe_val(data.get("Lean_Operation_Time")),
        safe_val(data.get("Net_Operation_Time")),
        safe_val(data.get("Operation_Time")),
        safe_val(data.get("Berth_Net_Operation_Time")),
        safe_val(data.get("Berth_Operation_Time")),
        safe_val(data.get("DurationInHours")),
        safe_val(data.get("DurationInMinutes")),
        safe_val(data.get("IdleTimeBeforeOps")),
        safe_val(data.get("IdleTimeAfterOps")),
        safe_val(data.get("AnchorageTimeInHours")),
        safe_val(data.get("AnchorageTimeInDay")),
        safe_val(data.get("Containers_Handled")),
        safe_val(data.get("TE_TC20")),
        safe_val(data.get("TE_TC40")),
        safe_val(data.get("TE_DEBA")),
        safe_val(data.get("TE_EMBA")),
        safe_val(data.get("TE_DEBA_OOG")),
        safe_val(data.get("TE_EMBA_OOG")),
        safe_val(data.get("Operating_Time")),
        safe_val(data.get("Networking_Time")),
        safe_val(data.get("Operating_Time_Avg")),
        safe_val(data.get("Vessel_Gross_Prdvty")),
        safe_val(data.get("Vessel_Net_Prdvty")),
        safe_val(data.get("Berth_Prdvty")),
        safe_val(data.get("Crane_Operating_Hours")),
        safe_val(data.get("Yard_Capacity")),
        safe_val(data.get("Current_Container_Inventory")),
        safe_val(data.get("Truck_Processing_Time")),
        safe_val(data.get("Damaged_Containers")),
        safe_val(data.get("Gate_Moves")),
        safe_val(data.get("Operating_Cycles")),
        safe_val(data.get("status")),
        safe_val(data.get("Delay_Weight")),
        safe_val(data.get("TE_GearBox")),
        safe_val(data.get("TE_Hatch")),
        safe_val(data.get("TE_SBB")),
        safe_val(data.get("TE_SBTB")),
        safe_val(data.get("TE_Twin_Lift")),
    )

    cursor.execute(insert_sql, values)
    conn.commit()


# ============================================================
# Main Consumer Loop
# ============================================================

print("🔄 Port KPI Consumer starting...")
print(f"   Kafka: {KAFKA_BOOTSTRAP}")
print(f"   Topic: {TOPIC}")
print(f"   DB: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")

# Connect to DB
conn = get_connection()
cursor = conn.cursor()
print("✅ Connected to PostgreSQL")

# Connect to Kafka
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    group_id='port_kpi_consumer'
)
print(f"✅ Subscribed to Kafka topic: {TOPIC}")

event_counter = 0

try:
    for msg in consumer:
        data = msg.value
        event_counter += 1

        try:
            process_record(data, cursor, conn)
            vessel = data.get("VesselName", "?")
            port = data.get("port_name", "?")
            containers = data.get("Containers_Handled", 0)
            print(f"✅ [{event_counter}] {vessel} → {port} | {containers} containers")
            sys.stdout.flush()
        except Exception as e:
            conn.rollback()
            print(f"❌ [{event_counter}] Error: {e}")
            sys.stdout.flush()

except KeyboardInterrupt:
    print(f"\n🛑 Consumer stopped. Processed {event_counter} events.")
except Exception as e:
    print(f"❌ Consumer failed: {e}")
    import traceback
    traceback.print_exc()
finally:
    cursor.close()
    conn.close()
    consumer.close()
