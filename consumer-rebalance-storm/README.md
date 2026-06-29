# Consumer Rebalance Storm Lab

Lab mô phỏng một trong những lỗi production phổ biến nhất của Kafka consumer:
**một message "độc" (poison pill) làm consumer xử lý quá lâu, vượt quá
`max.poll.interval.ms` → coordinator coi consumer là chết → đuổi khỏi group →
toàn bộ group bị rebalance → lag tăng vọt trên TẤT CẢ partition, không chỉ
partition đang gặp vấn đề.**

Nếu poison message xuất hiện lặp lại đều đặn (downstream API hay timeout,
DB hay deadlock, GC pause dài...) thì hiện tượng này lặp đi lặp lại liên tục
→ **rebalance storm**: consumer group không bao giờ ổn định, throughput thực
tế gần như bằng 0 dù consumer "trông vẫn đang chạy".

---

## Yêu cầu

- Docker Desktop (hoặc Docker Engine + Compose plugin)
- 4GB RAM trống
- Port trống: 8080, 8081, 9092

---

## Cấu trúc

```
consumer-rebalance-storm/
├── docker-compose.yml
├── consumer/                   # Spring Boot Kafka consumer
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/com/uniclass/lab/
│       ├── RebalanceLabApplication.java
│       ├── OrderEvent.java
│       ├── OrderConsumer.java      # nơi poison message làm block thread
│       ├── KafkaConsumerConfig.java # đăng ký rebalance listener để đo
│       ├── MetricsService.java
│       └── MetricsController.java
├── producer/                   # Python — bắn order-event + poison message
│   ├── Dockerfile
│   └── producer.py
└── result/                     # Python — theo dõi & chẩn đoán rebalance
    ├── Dockerfile
    └── monitor.py
```

---

## Chạy lab từ A đến Z

### Bước 1 — Khởi động infrastructure

```bash
cd consumer-rebalance-storm
docker compose up -d zookeeper kafka kafka-init kafka-ui
```

Kiểm tra topic đã tạo:

```bash
docker compose logs kafka-init
# Phải thấy: "Topic đã tạo xong!"
```

### Bước 2 — Build và chạy consumer (kịch bản STORM, mặc định)

```bash
docker compose up -d --build consumer
docker logs -f storm-consumer
# Phải thấy: "Started RebalanceLabApplication"
```

### Bước 3 — Mở Kafka UI

Truy cập **http://localhost:8080** → Topics → `order-event` → Consumers
→ quan sát consumer group `order-processor`. Khi storm xảy ra bạn sẽ thấy
trạng thái group nhảy qua `PreparingRebalance` / `CompletingRebalance` liên tục.

### Bước 4 — Chạy producer và monitor cùng lúc

Mở 2 terminal:

```bash
# Terminal 1
docker compose run --rm producer

# Terminal 2 (chạy gần như đồng thời)
docker compose run --rm monitor
```

`monitor` in ra timeline mỗi 5s: số message đã xử lý, throughput, **số lần
rebalance**, số poison message, số partition đang được giữ. Cuối cùng in
chẩn đoán + gợi ý fix.

### Kết quả mẫu (kịch bản STORM mặc định)

```
  time  processed    tps  rebalances  poison  partitions lastRebalance
------------------------------------------------------------------------
    0s         42   14.0           0       0           6          -1s
    5s         98   13.5           1       1           4           2s
   10s        103    8.2           2       2           0           1s
   15s        145    9.6           3       2           6           4s
   ...
============================================================
  CHẨN ĐOÁN
============================================================
  Rebalance xảy ra trong lúc theo dõi : 7
  Poison message đã xử lý             : 7
  Partition đang được consumer giữ    : 6

  🔥 REBALANCE STORM PHÁT HIỆN!
  ...
```

Lưu ý cột `partitions` nhảy về 0 rồi về lại 6 — đó là dấu hiệu eager
rebalance (RangeAssignor) revoke **toàn bộ** partition của mọi consumer
trong group, dù chỉ 1 thread bị kẹt vì poison message.

---

## Kịch bản thử nghiệm — sửa biến môi trường trong `docker-compose.yml`

| Kịch bản | LAB_MAX_POLL_INTERVAL_MS | LAB_MAX_POLL_RECORDS | LAB_PARTITION_ASSIGNOR | Quan sát |
|---|---|---|---|---|
| **STORM (mặc định)** | 10000 | 50 | RangeAssignor (eager) | Rebalance liên tục, lag toàn topic tăng vọt |
| Tăng timeout | 60000 | 50 | RangeAssignor | Poison message không còn làm consumer bị đuổi — không rebalance |
| Giảm batch | 10000 | 10 | RangeAssignor | Ít message kẹt cùng lúc hơn, nhưng vẫn rebalance nếu poison đủ lâu |
| Cooperative | 10000 | 50 | `org.apache.kafka.clients.consumer.CooperativeStickyAssignor` | Vẫn rebalance, nhưng chỉ consumer kẹt mất partition — các thread khác không bị ảnh hưởng |

Sau khi đổi, build lại và restart:

```bash
docker compose up -d --build --force-recreate consumer
```

---

## Bài học rút ra

1. **`max.poll.interval.ms` là hợp đồng giữa consumer và coordinator**: nếu
   business logic trong listener có thể chạy lâu hơn giá trị này (gọi API
   chậm, lock DB, batch xử lý lớn...), consumer sẽ bị coi là chết dù vẫn
   "sống" — không phải do crash mà do *chậm*.
2. **Eager rebalance (assignor mặc định) có blast radius toàn group**: một
   consumer kẹt sẽ kéo lag tăng trên cả những partition đang xử lý bình
   thường. `CooperativeStickyAssignor` thu nhỏ blast radius này.
3. **Manual ack + rebalance = nguy cơ duplicate**: message đang xử lý lúc
   bị revoke (chưa kịp `ack.acknowledge()`) sẽ được redeliver sau khi
   partition gán lại — consumer logic cần idempotent.
4. **Fix đúng không phải là "tăng timeout vô hạn"**: tăng
   `max.poll.interval.ms` chỉ che giấu vấn đề downstream chậm. Cách bền
   vững hơn là cách ly downstream call chậm/khả nghi (timeout riêng, circuit
   breaker, hoặc route message lỗi sang DLQ) để thread poll loop không bao
   giờ bị block lâu.

---

## Dừng lab

```bash
docker compose down -v
```
