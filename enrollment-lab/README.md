# Enrollment Lab — Uniclass CT Hè 2026

Lab đo throughput thực tế và tính số Kafka partition cần thiết.  
Toàn bộ chạy trên Docker, không cần cài Java hay Python local.

---

## Yêu cầu

- Docker Desktop (hoặc Docker Engine + Compose plugin)
- 4GB RAM trống (Kafka + Spring Boot + Python)
- Port trống: 8080, 8081, 9092

---

## Cấu trúc

```
enrollment-lab/
├── docker-compose.yml         # orchestrate toàn bộ
├── consumer/                  # Spring Boot Kafka consumer
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/
├── producer/                  # Python — giả lập spike CT Hè
│   ├── Dockerfile
│   └── producer.py
└── result/                    # Python — đọc metrics, tính partition
    ├── Dockerfile
    └── analyze.py
```

---

## Chạy lab từ A đến Z

### Bước 1 — Khởi động infrastructure

```bash
cd enrollment-lab
docker compose up -d zookeeper kafka kafka-init kafka-ui
```

Chờ khoảng 30 giây, kiểm tra Kafka đã sẵn sàng:

```bash
docker compose logs kafka-init
# Phải thấy: "Topic đã tạo xong!"
```

---

### Bước 2 — Build và chạy consumer

```bash
docker compose up -d consumer
```

Lần đầu build mất ~3-5 phút (Maven tải dependencies).  
Kiểm tra consumer đã chạy:

```bash
docker logs -f lab-consumer
# Phải thấy: "Started EnrollmentLabApplication"
```

---

### Bước 3 — Mở Kafka UI để quan sát

Truy cập: **http://localhost:8080**

- Vào **Topics → enrollment-event → Consumers**
- Quan sát consumer group `enrollment-processor`
- Cột **Lag** sẽ tăng khi producer chạy, giảm khi consumer xử lý kịp

---

### Bước 4 — Chạy producer (giả lập spike)

```bash
docker compose run --rm producer
```

Producer sẽ chạy 3 giai đoạn (~3 phút):

```
[Giai đoạn 1] WARM-UP   : 10 msg/s × 30s  =  300 messages
[Giai đoạn 2] SPIKE      : 160 msg/s × 60s = 9.600 messages  ← tải nặng nhất
[Giai đoạn 3] TAPER DOWN : 30 msg/s × 60s  = 1.800 messages
```

Trong lúc producer chạy, xem metrics consumer real-time:

```bash
# Terminal khác
watch -n 2 'curl -s http://localhost:8081/lab/metrics | python3 -m json.tool'
```

---

### Bước 5 — Đọc kết quả và tính số partition

Chờ consumer xử lý hết lag (Lag = 0 trên Kafka UI), rồi chạy:

```bash
docker compose run --rm analyzer
```

Kết quả mẫu:

```
==========================================================
  KẾT QUẢ ĐO LAB — UNICLASS CT HÈ 2026
==========================================================

[1] Số liệu đo được từ consumer:
    Tổng message đã xử lý : 11,700
    Throughput thực tế     : 37.5 msg/s
    Processing P50         : 76ms
    Processing P95         : 92ms  ← dùng để tính
    Processing P99         : 105ms
    End-to-end P95         : 1840ms

[2] Tính throughput per thread:
    1000ms / 92ms (P95) = 10.9 msg/s mỗi thread

[3] Throughput yêu cầu:
    Peak = 50,000 × 80% / 600s = 67 msg/s
    + Safety ×3 = 200 msg/s

[4] Số consumer cần thiết:
    200 / 10.9 = 19 consumers

[5] Số partition đề xuất:
    Làm tròn lên bội số của 3 brokers
    → 21 PARTITIONS

──────────────────────────────────────────────────────────
  GỢI Ý CẤU HÌNH pods × concurrency:
──────────────────────────────────────────────────────────
    3 pods × concurrency= 7 → 21 consumers  ✓ FIT
    6 pods × concurrency= 4 → 24 consumers  (24 consumers)
    9 pods × concurrency= 3 → 27 consumers  (27 consumers)
```

---

## Kịch bản thử nghiệm

Thay đổi 2 biến môi trường trong `docker-compose.yml` để so sánh:

| Kịch bản | LAB_CONCURRENCY | LAB_SIMULATED_PROCESSING_MS | Quan sát |
|----------|----------------|------------------------------|----------|
| Baseline | 1 | 80 | Lag tăng mạnh lúc spike |
| Tốt hơn  | 3 | 80 | Lag giảm, có đủ không? |
| Fit      | 6 | 80 | Lag gần 0 suốt spike |
| Thực tế  | 3 | *logic thật* | Con số tin cậy cho prod |

Sau khi thay, restart consumer:

```bash
docker compose up -d --force-recreate consumer
```

---

## Thay logic giả lập bằng logic thật

Mở file `consumer/src/main/java/com/uniclass/lab/EnrollmentConsumer.java`,  
tìm dòng `simulateProcessing()` và thay bằng:

```java
// Thay thế dòng này:
simulateProcessing();

// Bằng logic thật của Uniclass:
enrollmentRepository.save(toEntity(event));   // ghi DB
crmQueue.push(event);                          // push CRM
```

Rebuild:

```bash
docker compose up -d --build consumer
```

---

## Dừng lab

```bash
docker compose down
```

Xóa toàn bộ data Kafka:

```bash
docker compose down -v
```

---

## Troubleshooting

**Consumer không kết nối được Kafka:**
```bash
docker compose logs kafka | grep "started"
# Phải thấy: KafkaServer id=1 started
```

**Producer lỗi NoBrokersAvailable:**
```bash
# Chờ kafka-init chạy xong
docker compose logs kafka-init
```

**Build consumer quá chậm:**
```bash
# Maven cache được giữ trong Docker layer
# Lần 2 build sẽ nhanh hơn nhiều vì dependencies đã cache
docker compose up -d --build consumer
```
