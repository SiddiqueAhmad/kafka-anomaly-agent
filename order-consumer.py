import os
from kafka import KafkaConsumer
import json
from collections import defaultdict
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Connection settings come from the environment (.env), never hardcoded.
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
    "dbname":   os.getenv("DB_NAME", "orders_db"),
}
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# --- connect to your topic ---
consumer = KafkaConsumer(
    "order",
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="anomaly-detector-dev"
)

# 

# cur.execute("""
#     INSERT INTO orders_raw (id, event, customer_id, total, status, raw_payload)
#     VALUES (%s, %s, %s, %s, %s, %s)
# """, (
#     "ord-001",
#     "order_placed",
#     "cust-42",
#     129.99,
#     "pending",
#     json.dumps({"id": "ord-001", "event": "order_placed"})  # full message as JSONB
# ))

# conn.commit()

# print("row written")

# valid order state machine
VALID_TRANSITIONS = {
    "order_placed":    None,           # first event, no prior needed
    "order_confirmed": "order_placed",
    "order_shipped":   "order_confirmed",
    "order_delivered": "order_shipped",
    "order_cancelled": "order_placed",  # can cancel from placed
}

# track per-order last seen event (for sequence checks)
order_state = {}
seen_ids_this_batch = defaultdict(int)
anomalies = []

def check_anomalies(msg):
    findings = []
    event = msg.get("event")
    oid   = msg.get("id")

    # 1. Missing customer_id
    if not msg.get("customer_id"):
        findings.append({
            "type":     "missing_customer_id",
            "order_id": oid,
            "severity": "high"
        })
        cur.execute("""
        INSERT INTO anomalies (order_id, anomaly_type, severity, detail, processed)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        oid,
        "missing_customer_id",
        "high",
        json.dumps(msg),
        False
    ))
        conn.commit()
        print(f"💾 committed anomaly: missing_customer_id for order {oid}")


    # 2. Negative or zero total (only relevant on monetary events)
    if event in ("order_placed", "order_confirmed"):
        total = msg.get("total")
        if total is None or total <= 0:
            findings.append({
                "type":     "invalid_total",
                "order_id": oid,
                "event":    event,
                "value":    total,
                "severity": "high"
            })
            cur.execute("""
            INSERT INTO anomalies (order_id, anomaly_type, severity, detail, processed)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            oid,
            "invalid_total",
            "high",
            json.dumps(msg),
            False
))
            conn.commit()
            print(f"💾 committed anomaly: invalid_total for order {oid}")

    # 3. order_shipped missing tracking_number
    if event == "order_shipped" and not msg.get("tracking_number"):
        findings.append({
            "type":     "missing_tracking_number",
            "order_id": oid,
            "severity": "medium"
        })
        cur.execute("""
            INSERT INTO anomalies (order_id, anomaly_type, severity, detail, processed)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            oid,
            "missing_tracking_number",
            "medium",
            json.dumps(msg),
            False
))
        conn.commit()
        print(f"💾 committed anomaly: missing_tracking_number for order {oid}")

    # 4. order_placed with empty items
    if event == "order_placed":
        items = msg.get("items", [])
        if not items:
            findings.append({
                "type":     "empty_items_on_placement",
                "order_id": oid,
                "severity": "high"
            })
            cur.execute("""
            INSERT INTO anomalies (order_id, anomaly_type, severity, detail, processed)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            oid,
            "empty_items_on_placement",
            "high",
            json.dumps(msg),
            False
))
            conn.commit()
            print(f"💾 committed anomaly: empty_items_on_placement for order {oid}")

    # 5. order_cancelled missing reason
    if event == "order_cancelled" and not msg.get("reason"):
        findings.append({
            "type":     "missing_cancellation_reason",
            "order_id": oid,
            "severity": "low"
        })
        cur.execute("""
            INSERT INTO anomalies (order_id, anomaly_type, severity, detail, processed)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            oid,
            "missing_cancellation_reason",
            "low",
            json.dumps(msg),
            False
))
        conn.commit()
        print(f"💾 committed anomaly: missing_cancellation_reason for order {oid}")
        


    # 6. Event sequence violation
    expected_prior = VALID_TRANSITIONS.get(event)
    if expected_prior and oid:
        last_seen = order_state.get(oid)
        if last_seen != expected_prior:
            findings.append({
                "type":         "sequence_violation",
                "order_id":     oid,
                "event":        event,
                "expected_prior": expected_prior,
                "actual_prior": last_seen,
                "severity":     "high"
            })
            cur.execute("""
                INSERT INTO anomalies (order_id, anomaly_type, severity, detail, processed)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                oid,
                "sequence_violation",
                "high",
                json.dumps({"message": msg, "expected_prior": expected_prior, "actual_prior": last_seen}),
                False
            ))
            conn.commit()
            print(f"💾 committed anomaly: sequence_violation for order {oid}")
    # update state machine
    if oid:
        order_state[oid] = event

    return findings


print("Listening for orders...")

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
for message in consumer:
    msg = message.value

    event = msg.get("event")
    oid   = msg.get("id")
    customer_id   = msg.get("customer_id")
    total   = msg.get("total")
    status   = msg.get("status")

    

    cur.execute("""
    INSERT INTO orders_raw (id, event, customer_id, total, status, raw_payload)
    VALUES (%s, %s, %s, %s, %s, %s)
""", (
   oid,
    event,
    customer_id,
    total,
    status,
    json.dumps(msg)  # full message as JSONB
))
    consumer.commit()
    print(f"💾 committed Kafka offset for order {oid}")

    print(f"Received: {msg}")

    findings = check_anomalies(msg)
    if findings:
        for f in findings:
            print(f"🚨 ANOMALY: {f}")
        conn.commit()  # persist anomaly rows before committing the Kafka offset
        print(f"💾 committed {len(findings)} anomaly row(s) for order {oid}")
    else:
        print(f"  ✓ clean")
