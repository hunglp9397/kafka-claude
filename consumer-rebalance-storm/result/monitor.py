import os, time, json, urllib.request

CONSUMER_URL    = os.getenv("CONSUMER_URL", "http://localhost:8081")
METRICS_URL     = f"{CONSUMER_URL}/lab/metrics"
DURATION        = int(os.getenv("LAB_MONITOR_SECONDS", "180"))
INTERVAL        = int(os.getenv("LAB_MONITOR_INTERVAL_SEC", "5"))
STORM_THRESHOLD = int(os.getenv("LAB_STORM_THRESHOLD", "3"))  # >= N rebalance trong cửa sổ theo dõi = storm


def fetch_metrics() -> dict:
    for attempt in range(10):
        try:
            with urllib.request.urlopen(METRICS_URL, timeout=5) as r:
                return json.loads(r.read())
        except Exception as e:
            print(f"  Chưa lấy được metrics ({attempt+1}/10): {e}")
            time.sleep(3)
    raise RuntimeError(f"Không kết nối được consumer tại {METRICS_URL}")


def main():
    print(f"Theo dõi {METRICS_URL} trong {DURATION}s, lấy mẫu mỗi {INTERVAL}s...\n")
    header = f"{'time':>6} {'processed':>10} {'tps':>6} {'rebalances':>11} {'poison':>7} {'partitions':>10} {'lastRebalance':>14}"
    print(header)
    print("-" * len(header))

    start_rebalances = None
    final = {}
    elapsed = 0

    while elapsed <= DURATION:
        m = fetch_metrics()
        rebalances = m.get("rebalanceCount", 0)
        if start_rebalances is None:
            start_rebalances = rebalances

        final = m
        print(f"{elapsed:5d}s {m.get('totalProcessed', 0):10d} {m.get('throughputPerSecond', 0):6.1f} "
              f"{rebalances:11d} {m.get('poisonMessagesProcessed', 0):7d} "
              f"{m.get('assignedPartitions', 0):10d} {m.get('secondsSinceLastRebalance', -1):11d}s")

        time.sleep(INTERVAL)
        elapsed += INTERVAL

    rebalances_during = final.get("rebalanceCount", 0) - start_rebalances

    sep = "=" * 60
    print(f"\n{sep}")
    print("  CHẨN ĐOÁN")
    print(sep)
    print(f"  Rebalance xảy ra trong lúc theo dõi : {rebalances_during}")
    print(f"  Poison message đã xử lý             : {final.get('poisonMessagesProcessed', 0)}")
    print(f"  Partition đang được consumer giữ    : {final.get('assignedPartitions', 0)}")

    if rebalances_during >= STORM_THRESHOLD:
        print()
        print("  🔥 REBALANCE STORM PHÁT HIỆN!")
        print("  Nguyên nhân: poison message xử lý lâu hơn max.poll.interval.ms")
        print("  → coordinator coi consumer là 'chết', đuổi khỏi group")
        print("  → group rebalance — với eager assignor (RangeAssignor mặc định),")
        print("    TẤT CẢ consumer trong group đều bị revoke partition, không chỉ")
        print("    consumer đang kẹt — lag tăng vọt trên toàn bộ topic")
        print()
        print("  GỢI Ý FIX (đổi từng biến rồi chạy lại để so sánh số rebalance):")
        print("   1. Tăng max.poll.interval.ms lớn hơn thời gian xử lý chậm nhất có thể")
        print("      LAB_MAX_POLL_INTERVAL_MS=60000")
        print("   2. Giảm max.poll.records để mỗi vòng poll ôm ít message hơn")
        print("      LAB_MAX_POLL_RECORDS=10")
        print("   3. Không block thread chính — đẩy downstream call chậm/khả nghi")
        print("      vào executor riêng có timeout, hoặc route sang DLQ")
        print("   4. Đổi sang CooperativeStickyAssignor để rebalance chỉ ảnh hưởng")
        print("      partition cần đổi chủ, không phải toàn bộ group:")
        print("      LAB_PARTITION_ASSIGNOR=org.apache.kafka.clients.consumer.CooperativeStickyAssignor")
    else:
        print()
        print("  ✓ Không phát hiện rebalance storm trong cửa sổ theo dõi.")
        print("    Nếu vừa đổi config để fix, đây là kết quả mong đợi.")

    print(sep)


if __name__ == "__main__":
    main()
