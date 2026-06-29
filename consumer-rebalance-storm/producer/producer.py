import os, json, time, random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC       = "order-event"
RATE        = float(os.getenv("LAB_RATE_PER_SEC", "15"))
DURATION    = int(os.getenv("LAB_DURATION_SEC", "240"))
POISON_RATE = float(os.getenv("LAB_POISON_RATE", "0.03"))


def make_producer():
    for attempt in range(10):
        try:
            p = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode(),
                key_serializer=lambda k: k.encode(),
                acks="all",
                linger_ms=5,
            )
            print(f"[Producer] Kết nối Kafka thành công: {BOOTSTRAP}")
            return p
        except NoBrokersAvailable:
            print(f"[Producer] Chưa kết nối được, thử lại {attempt+1}/10...")
            time.sleep(3)
    raise RuntimeError("Không kết nối được Kafka sau 10 lần thử")


def main():
    producer  = make_producer()
    interval  = 1.0 / RATE
    total     = int(RATE * DURATION)

    print("=" * 60)
    print("  Consumer Rebalance Storm Lab — order-event producer")
    print("=" * 60)
    print(f"  Rate         : {RATE} msg/s")
    print(f"  Duration     : {DURATION}s (~{total} messages)")
    print(f"  Poison rate  : {POISON_RATE*100:.1f}% messages")
    print(f"  Mở http://localhost:8080 để xem consumer group & lag")
    print(f"  Mở http://localhost:8081/lab/metrics để xem rebalance count")
    print("=" * 60)

    sent      = 0
    start     = time.time()
    next_send = start

    while sent < total:
        now = time.time()
        if now < next_send:
            time.sleep(next_send - now)

        is_poison = random.random() < POISON_RATE
        # customerId random cardinality cao — tránh hot-key, tập trung quan sát rebalance
        event = {
            "orderId"   : f"ORD-{sent:08d}",
            "customerId": f"CUS-{random.randint(1, 1_000_000)}",
            "producedAt": int(time.time() * 1000),
            "poison"    : is_poison,
        }

        producer.send(TOPIC, key=event["customerId"], value=event)
        sent += 1
        next_send = start + (sent / RATE)

        if is_poison:
            print(f"  ☣  Đã bắn POISON message #{sent} (orderId={event['orderId']})")

        if sent % 200 == 0:
            elapsed = time.time() - start
            print(f"  Đã gửi {sent}/{total} | {sent/elapsed:.1f} msg/s thực tế")

    producer.flush()
    elapsed = time.time() - start
    print(f"\nXONG — {sent} message trong {elapsed:.1f}s")
    print("Chạy: docker compose run --rm monitor   để xem chẩn đoán rebalance")
    producer.close()


if __name__ == "__main__":
    main()
