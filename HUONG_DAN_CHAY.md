# Hướng dẫn vận hành

Tài liệu mô tả cách dựng lại toàn bộ hệ thống: batch layer trên Google Colab, speed layer và serving layer trên Docker. Kiến trúc và phương pháp xem tại [README.md](README.md).

## Yêu cầu

| Thành phần | Yêu cầu |
|---|---|
| Google Colab | Batch layer. Cấu hình mặc định dành cho Colab Pro High-RAM (8 vCPU, 50 GB RAM) |
| Docker Desktop | Speed layer và serving layer. Cấp tối thiểu 8 GB RAM |
| Dữ liệu | 10 file CSV của CSE-CIC-IDS2018 (6,9 GB) trong `data/`, tải theo [data/README.md](data/README.md) |
| Phiên bản (cố định trong image) | Spark 4.0.1 · Java 17 · Python 3.10 · Kafka 3.9.1 (KRaft) · MongoDB 8.0 |

Repo kèm sẵn mô hình (`models/`) và kết quả (`results/`) của lần chạy batch trên Colab Pro. Vì vậy speed layer và serving layer chạy được ngay, không cần chạy batch trước.

## Khởi động nhanh

```bash
docker compose build
docker compose up -d
```

| Địa chỉ | Dịch vụ |
|---|---|
| http://localhost:8000 | Web demo (FastAPI) |
| http://localhost:8080 | Spark master: 2 worker, ứng dụng `ids-stream-scorer` |
| http://localhost:4040 | Spark UI của job streaming (tab Structured Streaming) |
| `localhost:29092` | Kafka, topic `flows` (4 partition) |
| `mongodb://localhost:27017` | MongoDB, database `ids` |

Job streaming sẵn sàng khi `docker compose logs scorer` in ra dòng `[scorer] running`.

## Batch layer (Google Colab)

Toàn bộ batch layer chạy bằng notebook [`notebooks/colab_batch.ipynb`](notebooks/colab_batch.ipynb). Notebook tự clone repo vào Drive, cài `pyspark==4.0.1` (để mô hình nạp được trong container Spark 4.0.1), tải dữ liệu khi thiếu, và ghi kết quả vào `colab_out/{results,models}`.

| # | Giai đoạn | Lệnh trong notebook | Đầu ra | Thời gian* |
|---|---|---|---|---|
| 1 | ETL: làm sạch, khử trùng lặp, Parquet | `ids.etl` | `/content/work/parquet/{clean,dedup}`, `quality_report.json` | 15 phút |
| 2 | Kịch bản 1: chia ngẫu nhiên, có / không trùng lặp | `ids.train scenario1` | `scenario1.json`, `models/binary`, `models/deploy.json` | 2 giờ |
| 3 | Random Forest đa lớp | `ids.train multiclass` | `multiclass.json`, `models/multiclass_rf` | 18 phút |
| 4 | Kịch bản 2: leave-one-day-out | `ids.train scenario2 --resume` | `scenario2.json` | 2 giờ 34 phút |
| 5 | Benchmark khả năng mở rộng | `ids.bench prepare`, `ids.bench all --resume` | `scalability.json` | 5 giờ 7 phút |
| 6 | Biểu đồ và bảng tổng hợp | `ids.figures` | `figures/`, `summary.md` | < 1 phút |

<sub>* Đo trên Colab Pro High-RAM (8 vCPU, 50 GB RAM) với cấu hình mặc định: 100% tập train cho bước 2–3, 50% cho bước 4.</sub>

**Khi bị ngắt phiên**: chạy lại ô thiết lập, rồi chạy tiếp từ bước đang dở. Parquet được khôi phục từ `MyDrive/ie212_parquet` nên không phải chạy lại bước 1; bước 4 và 5 bỏ qua phần đã xong nhờ `--resume`.

**Cập nhật kết quả trong repo**: tải `MyDrive/ie212-ids2018/colab_out/` về, chép đè `results/` và `models/`.

**Tham số chính của `ids.train`** (các biến `S1_TRAIN_FRAC`, `S2_TRAIN_FRAC`, `S2_PRESET` trong ô thiết lập)

| Tham số | Mặc định | Ý nghĩa |
|---|---|---|
| `--models` | `lr,dt,rf,gbt` | Các mô hình cần huấn luyện |
| `--preset` | `full` | `full` hoặc `fast`: số cây, độ sâu, số vòng lặp |
| `--train-frac` | `1.0` | Tỷ lệ lấy mẫu phân tầng **chỉ trên tập train**. Nhãn hiếm giữ nguyên; validation và test luôn đầy đủ |
| `--days` | tất cả | Kịch bản 2: danh sách ngày làm tập test |
| `--resume` | tắt | Kịch bản 2: bỏ qua các fold đã có kết quả |
| `--seed` | `42` | Seed của phép chia theo hash |

Trên Colab miễn phí (2 vCPU, khoảng 12 GB RAM), đặt `S1_TRAIN_FRAC = 0.3` và `S2_TRAIN_FRAC = 0.2`.

## Speed layer và serving layer

Job streaming nạp `models/deploy.json` khi khởi động. Sau khi chép mô hình mới từ Colab đè lên `models/`, chạy `docker compose restart scorer` để áp dụng.

**Nguồn dữ liệu đầu vào**
- *Phát lại*: chọn trên trang Tổng quan, hoặc chạy lệnh
  `docker compose exec api python -m app.producer --file /data/<file>.csv --rate 1000 --start-pct 25`
- *Tải file*: trang Phân tích file nhận CSV định dạng CICFlowMeter. File mẫu: `samples/sample_upload_bot.csv`.

**Cấu hình runtime**: ngưỡng, c_FP và c_FN được lưu trong collection `config` của MongoDB. Job streaming đọc cấu hình này ở mỗi micro-batch, nên thay đổi trên trang Cấu hình ngưỡng có hiệu lực ngay mà không cần khởi động lại.

**Biến môi trường của dịch vụ `scorer`** (thêm hoặc sửa trong mục `environment` của dịch vụ `scorer` trong `docker-compose.yml`)

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `TRIGGER` | `2 seconds` | Chu kỳ micro-batch |
| `MAX_OFFSETS_PER_TRIGGER` | `20000` | Số bản ghi tối đa mỗi micro-batch |
| `METRIC_WINDOW` | `10 seconds` | Độ rộng cửa sổ thống kê |
| `STARTING_OFFSETS` | `latest` | Vị trí bắt đầu đọc Kafka |
| `MODELS_DIR`, `CHECKPOINT_DIR` | `/opt/ids/models`, `/opt/ids/work/checkpoints/scorer` | Thư mục mô hình và checkpoint |

**Dữ liệu trong MongoDB** (database `ids`): `alerts`, `metrics` (cửa sổ 10 s), `upload_jobs`, `upload_results`, `config`, `scorer_status`. Có thể xem bằng MongoDB Compass, hoặc bằng lệnh `docker compose exec mongo mongosh ids`.

## Kiểm tra nhất quán batch / streaming

Script dưới đây chấm điểm cùng một ngày dữ liệu theo hai đường: Parquet của batch, và CSV dạng chuỗi giống dữ liệu đi qua Kafka. Hai tỷ lệ cảnh báo trùng nhau chứng tỏ không có train/serve skew. Script cần Parquet trong `work/parquet`: tải bản sao lưu `MyDrive/ie212_parquet` của notebook về thư mục này.

```bash
docker compose --profile batch run --rm batch python3 scripts/check_skew.py --day 2018-02-14
```

## Xử lý sự cố

| Hiện tượng | Nguyên nhân và cách xử lý |
|---|---|
| Container thoát với mã 137, hoặc `OutOfMemoryError` | Thiếu bộ nhớ. Tăng RAM cho Docker hoặc giảm `SPARK_DRIVER_MEMORY` |
| `port is already allocated` | Cổng đang bị tiến trình khác dùng, ví dụ một MongoDB cài sẵn dưới dạng service. Dừng tiến trình đó hoặc đổi cổng trong `docker-compose.yml` |
| `scorer` khởi động lại liên tục | Thiếu `models/deploy.json` hoặc `models/binary/`. Khôi phục bằng `git checkout models`, hoặc chép lại `colab_out/models/` từ Drive |
| Web báo scorer đang chờ dữ liệu | Đây là hành vi bình thường: Structured Streaming chỉ chạy micro-batch khi Kafka có bản ghi mới |
| Muốn reset speed layer | `docker compose down -v`, xoá `work/checkpoints/`, rồi `docker compose up -d` |
| Đường dẫn bị đổi khi dùng Git Bash | Đặt `export MSYS_NO_PATHCONV=1` |
