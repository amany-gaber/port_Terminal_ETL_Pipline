import json
import time
import random
import os
from datetime import datetime, timedelta
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'port_operations')

# Kafka connection
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

# ============================================================
# Realistic fake data pools (matching Excel dataset structure)
# ============================================================

VESSELS = [
    {"imo": "9354001", "name": "MSC OSCAR", "class": "Container", "loa": 395.4, "country": "PA", "owner_id": "MSC001", "owner_name": "MSC", "owner_group": "MSC Group"},
    {"imo": "9461867", "name": "MAERSK EDINBURGH", "class": "Container", "loa": 366.0, "country": "DK", "owner_id": "MAE001", "owner_name": "Maersk Line", "owner_group": "AP Moller"},
    {"imo": "9484412", "name": "CMA CGM MARCO POLO", "class": "Container", "loa": 396.0, "country": "FR", "owner_id": "CMA001", "owner_name": "CMA CGM", "owner_group": "CMA CGM Group"},
    {"imo": "9732307", "name": "EVER GOLDEN", "class": "Container", "loa": 400.0, "country": "PA", "owner_id": "EVG001", "owner_name": "Evergreen", "owner_group": "Evergreen Group"},
    {"imo": "9461879", "name": "SAFMARINE NOKWANDA", "class": "Container", "loa": 294.0, "country": "LR", "owner_id": "SAF001", "owner_name": "Safmarine", "owner_group": "Maersk Group"},
    {"imo": "9302152", "name": "COSCO SHIPPING ARIES", "class": "Container", "loa": 349.0, "country": "HK", "owner_id": "COS001", "owner_name": "COSCO", "owner_group": "COSCO Group"},
    {"imo": "9786788", "name": "ONE APUS", "class": "Container", "loa": 364.0, "country": "JP", "owner_id": "ONE001", "owner_name": "ONE", "owner_group": "ONE Network"},
    {"imo": "9525875", "name": "HAMBURG EXPRESS", "class": "Container", "loa": 368.0, "country": "DE", "owner_id": "HAP001", "owner_name": "Hapag-Lloyd", "owner_group": "Hapag-Lloyd AG"},
    {"imo": "9295382", "name": "YANG MING WONDER", "class": "Container", "loa": 335.0, "country": "TW", "owner_id": "YM001", "owner_name": "Yang Ming", "owner_group": "Yang Ming Group"},
    {"imo": "9615295", "name": "PIL CELEBES", "class": "Container", "loa": 261.0, "country": "SG", "owner_id": "PIL001", "owner_name": "PIL", "owner_group": "PIL Group"},
]

PORTS = [
    {"name": "Jeddah Islamic Port", "country": "SA"},
    {"name": "King Abdulaziz Port Dammam", "country": "SA"},
    {"name": "Jubail Commercial Port", "country": "SA"},
    {"name": "Yanbu Commercial Port", "country": "SA"},
]

BERTHS = [
    {"id": "B01", "grouping": "Container Terminal North"},
    {"id": "B02", "grouping": "Container Terminal North"},
    {"id": "B03", "grouping": "Container Terminal South"},
    {"id": "B04", "grouping": "Container Terminal South"},
    {"id": "B05", "grouping": "General Cargo"},
    {"id": "B06", "grouping": "Bulk Terminal"},
]

SERVICES = [
    {"id": "SVC_AAE1", "name": "Asia-Arabia Express 1"},
    {"id": "SVC_ME1", "name": "Middle East Service 1"},
    {"id": "SVC_AE7", "name": "Asia-Europe 7"},
    {"id": "SVC_FE3", "name": "Far East 3"},
    {"id": "SVC_GSS", "name": "Gulf Shuttle Service"},
    {"id": "SVC_IMS", "name": "Indian Med Service"},
    {"id": "SVC_PAX", "name": "Pakistan Express"},
    {"id": "SVC_EAF", "name": "East Africa Feeder"},
]

EQUIPMENT = [
    {"id": "QC01", "type": "QC", "type_custom": "Quay Crane", "equipment_type": "STS"},
    {"id": "QC02", "type": "QC", "type_custom": "Quay Crane", "equipment_type": "STS"},
    {"id": "QC03", "type": "QC", "type_custom": "Quay Crane", "equipment_type": "STS"},
    {"id": "RTG01", "type": "RTG", "type_custom": "Rubber Tyred Gantry", "equipment_type": "Yard"},
    {"id": "RTG02", "type": "RTG", "type_custom": "Rubber Tyred Gantry", "equipment_type": "Yard"},
    {"id": "RS01", "type": "RS", "type_custom": "Reach Stacker", "equipment_type": "Yard"},
]

CALL_TYPES = ["Import", "Export", "Transshipment"]
STATUSES = ["delayed", "non delayed"]
STATUS_WEIGHTS = [30, 70]  # 30% delayed

# Global counter for unique IDs
call_counter = 100000


def generate_port_operation():
    """Generate a single fake port operation record matching Excel schema."""
    global call_counter
    call_counter += 1

    vessel = random.choice(VESSELS)
    port = random.choice(PORTS)
    berth = random.choice(BERTHS)
    service = random.choice(SERVICES)
    equipment = random.choice(EQUIPMENT)

    # Generate realistic timestamps
    now = datetime.utcnow()
    arrival = now - timedelta(hours=random.uniform(12, 72))
    operation_hours = random.uniform(4, 48)
    departure = arrival + timedelta(hours=operation_hours + random.uniform(1, 8))
    first_move = arrival + timedelta(hours=random.uniform(0.5, 4))
    last_move = departure - timedelta(hours=random.uniform(0.5, 2))

    # Container metrics
    tc20 = random.randint(50, 800)
    tc40 = random.randint(20, 400)
    deba = random.randint(0, 200)
    emba = random.randint(0, 200)
    containers = tc20 + tc40 + deba + emba

    # Time metrics
    duration_hours = round((departure - arrival).total_seconds() / 3600, 2)
    idle_before = round(random.uniform(0.5, 6), 2)
    idle_after = round(random.uniform(0.2, 3), 2)
    anchorage_hours = round(random.uniform(0, 24), 2)
    net_op_time = round(operation_hours * random.uniform(0.7, 0.95), 2)
    lean_op_time = round(net_op_time * random.uniform(0.8, 0.95), 2)
    berth_op_time = round(operation_hours * random.uniform(0.85, 1.0), 2)
    berth_net_op_time = round(berth_op_time * random.uniform(0.75, 0.95), 2)

    # Productivity
    gross_prdvty = round(containers / max(operation_hours, 1), 2)
    net_prdvty = round(containers / max(net_op_time, 1), 2)
    berth_prdvty = round(containers / max(berth_op_time, 1), 2)

    # Status
    status = random.choices(STATUSES, weights=STATUS_WEIGHTS)[0]
    delay_weight = round(random.uniform(0.5, 1.0), 4) if status == "delayed" else round(random.uniform(0, 0.3), 4)

    record = {
        "Call_Key": call_counter,
        "Call_ID": f"{vessel['owner_name'][:3].upper()}_{call_counter}",
        "Arvl_Dt": arrival.isoformat(),
        "Dptr_Dt": departure.isoformat(),
        "First_Movement_Dt": first_move.isoformat(),
        "Last_Movement_Dt": last_move.isoformat(),
        "Terminal_Site_Key": random.randint(1, 5),
        "Type": random.choice(CALL_TYPES),
        "DurationInHours": duration_hours,
        "DurationInMinutes": int(duration_hours * 60),
        "IdleTimeBeforeOps": idle_before,
        "IdleTimeAfterOps": idle_after,
        "Service_ID": service["id"],
        "Service_Nm": service["name"],
        "BerthID": berth["id"],
        "BerthCustomGrouping": berth["grouping"],
        "VesselName": vessel["name"],
        "IMO": vessel["imo"],
        "RadioCode": f"RC{vessel['imo'][-4:]}",
        "VesselClass": vessel["class"],
        "LOA": vessel["loa"],
        "CountryCode": vessel["country"],
        "ShipOwner_Id": vessel["owner_id"],
        "ShipOwnerName": vessel["owner_name"],
        "ShipOwnerGroup": vessel["owner_group"],
        "IsSpecificCall": random.choice([True, False]),
        "AnchorageTimeInHours": anchorage_hours,
        "AnchorageTimeInDay": round(anchorage_hours / 24, 2),
        "TE_TC20": tc20,
        "TE_TC40": tc40,
        "TE_DEBA": deba,
        "TE_EMBA": emba,
        "TE_DEBA_OOG": random.randint(0, 10),
        "TE_EMBA_OOG": random.randint(0, 10),
        "TE_GearBox": random.randint(0, 20),
        "TE_Hatch": random.randint(0, 15),
        "TE_SBB": random.randint(0, 10),
        "TE_SBTB": random.randint(0, 10),
        "TE_Twin_Lift": random.randint(0, 30),
        "Vessel_Gross_Prdvty": gross_prdvty,
        "Vessel_Net_Prdvty": net_prdvty,
        "Berth_Prdvty": berth_prdvty,
        "Eqp_ID": equipment["id"],
        "Eqp_Type": equipment["type"],
        "Eqp_Type_Custom": equipment["type_custom"],
        "Equipment_Type": equipment["equipment_type"],
        "Operating_Time": round(random.uniform(2, 20), 2),
        "Networking_Time": round(random.uniform(0.5, 5), 2),
        "Operating_Time_Avg": round(random.uniform(3, 15), 2),
        "Lean_Operation_Time": lean_op_time,
        "Net_Operation_Time": net_op_time,
        "Operation_Time": round(operation_hours, 2),
        "Berth_Net_Operation_Time": berth_net_op_time,
        "Berth_Operation_Time": berth_op_time,
        "port_name": port["name"],
        "Containers_Handled": containers,
        "Crane_Operating_Hours": round(random.uniform(4, 40), 2),
        "Yard_Capacity": round(random.uniform(5000, 20000), 2),
        "Current_Container_Inventory": round(random.uniform(1000, 15000), 2),
        "Truck_Processing_Time": round(random.uniform(10, 60), 2),
        "Damaged_Containers": random.randint(0, 5),
        "Gate_Moves": random.randint(100, 2000),
        "Operating_Cycles": random.randint(50, 500),
        "status": status,
        "Delay_Weight": delay_weight,
    }

    return record


# ============================================================
# Main loop — generate and send events
# ============================================================

INTERVAL = 1.0  # 1 event per second

print("🚀 Port KPI Producer initialized. Generating fake port operations...")
print(f"   Kafka: {KAFKA_BOOTSTRAP}")
print(f"   Topic: {KAFKA_TOPIC}")
print(f"   Interval: {INTERVAL}s")

try:
    while True:
        record = generate_port_operation()
        producer.send(KAFKA_TOPIC, value=record)
        print(f"📦 Event {record['Call_Key']}: {record['VesselName']} → {record['port_name']} | "
              f"{record['Containers_Handled']} containers | {record['status']}")
        time.sleep(INTERVAL)

except KeyboardInterrupt:
    print("\n🛑 Producer stopped by user.")
except Exception as e:
    print(f"❌ Producer failed: {e}")
    import traceback
    traceback.print_exc()
finally:
    producer.close()
