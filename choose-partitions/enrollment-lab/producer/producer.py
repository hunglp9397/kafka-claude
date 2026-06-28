import os, json, time, random, math
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC     = "enrollment-event"
CAMPAIGN  = "ct-he-2026"
PROVINCES = ["hanoi", "hcm", "danang", "haiphong", "cantho",
             "nghean", "thanhhoa", "binhduong", "dongnai", "cantho"]

def make_producer():
    for attempt in range(10):
        try:
            p = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode(),
                key_serializer=lambda k: k.encode(),
                acks="all",
                linger_ms=5,           # batch nhỏ để tăng throughput
                batch_size=16384,
            )
            print(f"[Producer] Kết nối Kafka thành công: {BOOTSTRAP}")
            return p
        except NoBrokersAvailable:
            print(f"[Producer] Chưa kết nối được, thử lại {attempt+1}/10...")
            time.sleep(3)
    raise RuntimeError("Không kết nối được Kafka sau 10 lần thử")


def send_batch(producer, phase: str, total: int, rate_per_sec: float):
    """
    Gửi `total` message với tốc độ `rate_per_sec` msg/giây.
    Dùng token-bucket để giữ rate ổn định.
    """
    interval    = 1.0 / rate_per_sec
    sent        = 0
    start       = time.time()
    next_send   = start

    while sent < total:
        now = time.time()
        if now < next_send:
            time.sleep(next_send - now)

        student_id = f"STU-{random.randint(100_000, 999_999)}"
        province   = random.choice(PROVINCES)

        event = {
            "studentId" : student_id,
            "provinceId": province,
            "campaignId": CAMPAIGN,
            "producedAt": int(time.time() * 1000),
            "phase"     : phase,
        }

        producer.send(TOPIC, key=province, value=event)
        sent     += 1
        next_send = start + (sent / rate_per_sec)

        if sent % 1000 == 0:
            elapsed = time.time() - start
            actual_rate = sent / elapsed if elapsed > 0 else 0
            print(f"  [{phase}] Đã gửi {sent}/{total} | "
                  f"Rate thực tế: {actual_rate:.0f} msg/s")

    producer.flush()
    elapsed = time.time() - start
    print(f"[{phase}] XONG — {sent} message trong {elapsed:.1f}s "
          f"({sent/elapsed:.0f} msg/s thực tế)\n")


def main():
    producer = make_producer()

    print("=" * 55)
    print("  Bắt đầu mô phỏng CT Hè 2026 — Uniclass Lab")
    print("=" * 55)
    print()
    print("Mở http://localhost:8080 để xem consumer lag real-time")
    print("Mở http://localhost:8081/lab/metrics để xem latency")
    print()

    # ── Giai đoạn 1: Warm-up ──────────────────────────────────────
    # Lưu lượng thấp trước khi mở đăng ký
    print("[Giai đoạn 1] WARM-UP: 10 msg/s × 30s = 300 messages")
    send_batch(producer, phase="warmup", total=300, rate_per_sec=10)
    print("Nghỉ 5s trước khi spike...")
    time.sleep(5)

    # ── Giai đoạn 2: Spike ────────────────────────────────────────
    # 80% học sinh đăng ký trong 10 phút đầu
    # 40.000 × 80% / 600s ≈ 53 msg/s, nhân 3x buffer = 160 msg/s
    print("[Giai đoạn 2] SPIKE: 160 msg/s × 60s = 9.600 messages")
    print("  → Đây là lúc hệ thống chịu tải nặng nhất!")
    send_batch(producer, phase="spike", total=9_600, rate_per_sec=160)
    print("Nghỉ 5s...")
    time.sleep(5)

    # ── Giai đoạn 3: Taper down ───────────────────────────────────
    # Lưu lượng giảm dần sau spike
    print("[Giai đoạn 3] TAPER DOWN: 30 msg/s × 60s = 1.800 messages")
    send_batch(producer, phase="taper", total=1_800, rate_per_sec=30)

    print("=" * 55)
    print("  Producer xong! Chờ consumer xử lý hết lag...")
    print("  Sau đó chạy: docker compose run --rm analyzer")
    print("=" * 55)

    producer.close()


if __name__ == "__main__":
    main()
