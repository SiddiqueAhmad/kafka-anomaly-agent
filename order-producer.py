from confluent_kafka import Producer
import json
import time

producer = Producer({"bootstrap.servers": "localhost:9092"})

TOPIC = "order"


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


# messages = [
#     {"id": "ord-011", "event": "order_placed", "customer_id": "cust-42", "items": ["item-101", "item-202"], "total": -1.99, "status": "pending"},
#     {"id": "ord-012", "event": "order_shipped", "customer_id": "cust-17", "items": ["item-305"], "total": 10.99, "status": "confirmed"},
#     {"id": "ord-013", "event": "order_placed", "customer_id": "cust-42", "order_id": "ord-001", "tracking_number": "TRK-998877", "status": "shipped"},
#     {"id": "ord-014", "event": "order_placed", "customer_id": "cust-17", "order_id": "ord-002", "status": "delivered"},
#     {"id": "ord-015", "event": "order_placed", "customer_id": "cust-88", "items": ["item-410"], "total": 75.00, "reason": "customer_request", "status": "cancelled"},
# ]

messages = [
    # ============================================================
    # ORIGINAL ANOMALIES
    # ============================================================
    {"id": "ord-011", "event": "order_placed", "customer_id": "cust-42", "items": ["item-101", "item-202"], "total": -1.99, "status": "pending"},
    {"id": "ord-012", "event": "order_shipped", "customer_id": "cust-17", "items": ["item-305"], "total": 10.99, "status": "confirmed"},
    {"id": "ord-013", "event": "order_placed", "customer_id": "cust-42", "order_id": "ord-001", "tracking_number": "TRK-998877", "status": "shipped"},
    {"id": "ord-014", "event": "order_placed", "customer_id": "cust-17", "order_id": "ord-002", "status": "delivered"},
    {"id": "ord-015", "event": "order_placed", "customer_id": "cust-88", "items": ["item-410"], "total": 75.00, "reason": "customer_request", "status": "cancelled"},

    # ============================================================
    # CANCELLATIONS — various reasons
    # ============================================================
    {"id": "ord-016", "event": "order_cancelled", "customer_id": "cust-21", "items": ["item-501"], "total": 45.00, "reason": "changed_mind", "status": "cancelled"},
    {"id": "ord-017", "event": "order_cancelled", "customer_id": "cust-33", "items": ["item-602", "item-603"], "total": 120.50, "reason": "found_cheaper_elsewhere", "status": "cancelled"},
    {"id": "ord-018", "event": "order_cancelled", "customer_id": "cust-44", "items": ["item-701"], "total": 89.99, "reason": "delivery_too_slow", "status": "cancelled"},
    {"id": "ord-019", "event": "order_cancelled", "customer_id": "cust-55", "items": ["item-802"], "total": 30.00, "reason": "payment_failed", "status": "cancelled"},
    {"id": "ord-020", "event": "order_cancelled", "customer_id": "cust-66", "items": ["item-901"], "total": 250.00, "reason": "out_of_stock", "status": "cancelled"},
    
    # ANOMALY: cancelled but status still shows shipped (inconsistent state)
    {"id": "ord-021", "event": "order_cancelled", "customer_id": "cust-77", "items": ["item-1001"], "total": 60.00, "reason": "changed_mind", "status": "shipped"},
    
    # ANOMALY: cancelled with no reason given
    {"id": "ord-022", "event": "order_cancelled", "customer_id": "cust-88", "items": ["item-1102"], "total": 15.99, "status": "cancelled"},
    
    # ANOMALY: cancelled after delivery (impossible state)
    {"id": "ord-023", "event": "order_cancelled", "customer_id": "cust-99", "items": ["item-1203"], "total": 200.00, "reason": "changed_mind", "status": "delivered"},

    # ============================================================
    # RETURNS — various reasons
    # ============================================================
    {"id": "ord-024", "event": "order_returned", "customer_id": "cust-21", "order_id": "ord-005", "items": ["item-501"], "total": 45.00, "reason": "product_damaged", "status": "returned"},
    {"id": "ord-025", "event": "order_returned", "customer_id": "cust-33", "order_id": "ord-006", "items": ["item-602"], "total": 75.00, "reason": "wrong_item_shipped", "status": "returned"},
    {"id": "ord-026", "event": "order_returned", "customer_id": "cust-44", "order_id": "ord-007", "items": ["item-703"], "total": 110.00, "reason": "not_as_described", "status": "returned"},
    {"id": "ord-027", "event": "order_returned", "customer_id": "cust-55", "order_id": "ord-008", "items": ["item-804"], "total": 55.00, "reason": "size_mismatch", "status": "returned"},
    {"id": "ord-028", "event": "order_returned", "customer_id": "cust-66", "order_id": "ord-009", "items": ["item-905"], "total": 99.99, "reason": "defective_product", "status": "returned"},
    {"id": "ord-029", "event": "order_returned", "customer_id": "cust-77", "order_id": "ord-010", "items": ["item-1006"], "total": 40.00, "reason": "arrived_late", "status": "returned"},
    {"id": "ord-030", "event": "order_returned", "customer_id": "cust-88", "order_id": "ord-011", "items": ["item-1107"], "total": 180.00, "reason": "quality_issue", "status": "returned"},

    # ANOMALY: return without original order reference
    {"id": "ord-031", "event": "order_returned", "customer_id": "cust-99", "items": ["item-1208"], "total": 65.00, "reason": "product_damaged", "status": "returned"},
    
    # ANOMALY: return with no reason
    {"id": "ord-032", "event": "order_returned", "customer_id": "cust-12", "order_id": "ord-012", "items": ["item-1309"], "total": 25.00, "status": "returned"},
    
    # ANOMALY: returned but status shows pending (state mismatch)
    {"id": "ord-033", "event": "order_returned", "customer_id": "cust-23", "order_id": "ord-013", "items": ["item-1410"], "total": 88.00, "reason": "wrong_item_shipped", "status": "pending"},

    # ============================================================
    # REFUNDS — partial and full
    # ============================================================
    {"id": "ord-034", "event": "order_refunded", "customer_id": "cust-21", "order_id": "ord-024", "total": 45.00, "reason": "full_refund_processed", "status": "refunded"},
    {"id": "ord-035", "event": "order_refunded", "customer_id": "cust-33", "order_id": "ord-025", "total": 37.50, "reason": "partial_refund_shipping", "status": "partially_refunded"},
    
    # ANOMALY: refund amount exceeds order total
    {"id": "ord-036", "event": "order_refunded", "customer_id": "cust-44", "order_id": "ord-026", "total": 500.00, "reason": "full_refund_processed", "status": "refunded"},
    
    # ANOMALY: negative refund
    {"id": "ord-037", "event": "order_refunded", "customer_id": "cust-55", "order_id": "ord-027", "total": -25.00, "reason": "partial_refund_shipping", "status": "refunded"},

    # ============================================================
    # DUPLICATE & FRAUD-LIKE EVENTS
    # ============================================================
    # ANOMALY: same order placed twice within seconds (possible duplicate)
    {"id": "ord-038", "event": "order_placed", "customer_id": "cust-12", "items": ["item-1500"], "total": 99.00, "status": "pending"},
    {"id": "ord-039", "event": "order_placed", "customer_id": "cust-12", "items": ["item-1500"], "total": 99.00, "status": "pending"},
    
    # ANOMALY: extremely high order value (potential fraud)
    {"id": "ord-040", "event": "order_placed", "customer_id": "cust-new-01", "items": ["item-1600"], "total": 15000.00, "status": "pending"},
    
    # ANOMALY: zero total order
    {"id": "ord-041", "event": "order_placed", "customer_id": "cust-23", "items": ["item-1700"], "total": 0.00, "status": "pending"},

    # ============================================================
    # SHIPPING ANOMALIES
    # ============================================================
    # ANOMALY: shipped without tracking
    {"id": "ord-042", "event": "order_shipped", "customer_id": "cust-34", "order_id": "ord-014", "status": "shipped"},
    
    # ANOMALY: delivered without prior shipped event (event sequence skip)
    {"id": "ord-043", "event": "order_delivered", "customer_id": "cust-45", "order_id": "ord-015", "total": 67.00, "status": "delivered"},
    
    # ANOMALY: tracking number malformed
    {"id": "ord-044", "event": "order_shipped", "customer_id": "cust-56", "order_id": "ord-016", "tracking_number": "INVALID", "status": "shipped"},
]
for msg in messages:
    producer.produce(
        TOPIC,
        key=msg["id"],
        value=json.dumps(msg),
        callback=delivery_report,
    )
    producer.poll(0)
    time.sleep(0.5)

producer.flush()
print("Done.")
