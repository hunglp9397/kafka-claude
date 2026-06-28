import os, math, time, json
import urllib.request, urllib.error

CONSUMER_URL  = os.getenv("CONSUMER_URL", "http://localhost:8081")
METRICS_URL   = f"{CONSUMER_URL}/lab/metrics"

# ── Thông số bài toán CT Hè 2026 ──────────────────────────────────────
TOTAL_STUDENTS     = 50_000
SPIKE_PERCENT      = 0.80      # 80% đăng ký trong cửa sổ spike
SPIKE_WINDOW_SEC   = 600       # 10 phút
SAFETY_FACTOR      = 3         # buffer × 3 để chịu spike thực tế
NUM_BROKERS        = 3         # số Kafka broker trong prod


def fetch_metrics() -> dict:
    for attempt in range(10):
        try:
            with urllib.request.urlopen(METRICS_URL, timeout=5) as r:
                return json.loads(r.read())
        except Exception as e:
            print(f"  Chưa lấy được metrics ({attempt+1}/10): {e}")
            time.sleep(3)
    raise RuntimeError(f"Không kết nối được consumer tại {METRICS_URL}")


def percentile_label(p: int) -> str:
    return f"P{p}"


def calculate(metrics: dict):
    p95_ms = metrics.get("processingP95", 0)
    p50_ms = metrics.get("processingP50", 0)
    p99_ms = metrics.get("processingP99", 0)
    tps    = metrics.get("throughputPerSecond", 0)
    total  = metrics.get("totalProcessed", 0)
    e2e95  = metrics.get("e2eP95", 0)

    if p95_ms == 0:
        print("[!] Chưa có đủ dữ liệu. Hãy chạy producer trước!")
        return

    # ── Bước 1: Throughput đo được ────────────────────────────────
    throughput_per_thread = 1000 / p95_ms  # msg/s mỗi thread

    # ── Bước 2: Throughput yêu cầu ────────────────────────────────
    required_peak_tps = (TOTAL_STUDENTS * SPIKE_PERCENT) / SPIKE_WINDOW_SEC
    required_tps_buffered = required_peak_tps * SAFETY_FACTOR

    # ── Bước 3: Số consumer cần thiết ─────────────────────────────
    num_consumers_needed = math.ceil(required_tps_buffered / throughput_per_thread)

    # ── Bước 4: Số partition (bội số của số broker) ────────────────
    num_partitions = math.ceil(num_consumers_needed / NUM_BROKERS) * NUM_BROKERS

    # ── In kết quả ────────────────────────────────────────────────
    sep = "=" * 58
    print()
    print(sep)
    print("  KẾT QUẢ ĐO LAB — UNICLASS CT HÈ 2026")
    print(sep)

    print("\n[1] Số liệu đo được từ consumer:")
    print(f"    Tổng message đã xử lý : {total:,}")
    print(f"    Throughput thực tế     : {tps} msg/s")
    print(f"    Processing P50         : {p50_ms}ms")
    print(f"    Processing P95         : {p95_ms}ms  ← dùng để tính")
    print(f"    Processing P99         : {p99_ms}ms")
    print(f"    End-to-end P95         : {e2e95}ms")

    print("\n[2] Tính throughput per thread:")
    print(f"    1000ms / {p95_ms}ms (P95) = {throughput_per_thread:.1f} msg/s mỗi thread")

    print("\n[3] Throughput yêu cầu:")
    print(f"    Peak = {TOTAL_STUDENTS:,} × {int(SPIKE_PERCENT*100)}% / {SPIKE_WINDOW_SEC}s")
    print(f"         = {required_peak_tps:.0f} msg/s")
    print(f"    + Safety ×{SAFETY_FACTOR} = {required_tps_buffered:.0f} msg/s")

    print("\n[4] Số consumer cần thiết:")
    print(f"    {required_tps_buffered:.0f} / {throughput_per_thread:.1f} = {num_consumers_needed} consumers")

    print("\n[5] Số partition đề xuất:")
    print(f"    Làm tròn lên bội số của {NUM_BROKERS} brokers")
    print(f"    → {num_partitions} PARTITIONS")

    print(f"\n{'─'*58}")
    print("  GỢI Ý CẤU HÌNH pods × concurrency:")
    print(f"{'─'*58}")

    options = []
    for pods in [2, 3, 6, 9]:
        if pods > num_partitions:
            continue
        concurrency = math.ceil(num_partitions / pods)
        total_c = pods * concurrency
        fit = "✓ FIT" if total_c == num_partitions else f"  ({total_c} consumers)"
        options.append((pods, concurrency, total_c))
        print(f"    {pods} pods × concurrency={concurrency:2d} → {total_c:2d} consumers  {fit}")

    print(f"\n{'─'*58}")
    print("  LỆNH TẠO TOPIC TRÊN KAFKA:")
    print(f"{'─'*58}")
    print(f"""
    docker exec lab-kafka kafka-topics \\
      --alter \\
      --topic enrollment-event \\
      --partitions {num_partitions} \\
      --bootstrap-server localhost:9092
""")
    print(sep)
    print()


if __name__ == "__main__":
    print(f"Đang lấy metrics từ {METRICS_URL}...")
    metrics = fetch_metrics()
    calculate(metrics)
