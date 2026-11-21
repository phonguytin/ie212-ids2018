# Hướng dẫn vận hành

Tài liệu mô tả cách dựng lại toàn bộ hệ thống, gồm batch layer, speed layer và serving layer. Kiến trúc và phương pháp xem tại [README.md](README.md).

## Yêu cầu

| Thành phần | Yêu cầu |
|---|---|
| Docker Desktop | Cấp tối thiểu 8 GB RAM; 12–16 GB nếu chạy batch trên toàn bộ dữ liệu |
| Dữ liệu | 10 file CSV của CSE-CIC-IDS2018 (6,9 GB) trong `data/`, tải theo [data/README.md](data/README.md) |
| Phiên bản (cố định trong image) | Spark 4.0.1 · Java 17 · Python 3.10 · Kafka 3.9.1 (KRaft) · MongoDB 8.0 |

Repo kèm sẵn mô hình đã huấn luyện (`models/`) và kết quả tham chiếu (`results/`). Vì vậy speed layer và serving layer chạy được ngay, không cần chạy batch trước.

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

## Batch layer

Mọi job batch chạy trong dịch vụ `batch` (profile `batch`), dùng chung image với cụm Spark. Nên dừng speed layer bằng `docker compose stop` trước khi chạy để giải phóng bộ nhớ.

| # | Giai đoạn | Lệnh | Đầu ra | Thời gian* |
|---|---|---|---|---|
| 1 | ETL: làm sạch, khử trùng lặp, Parquet | `ids.etl` | `work/parquet/{clean,dedup}`, `results/quality_report.json` | 16 phút |
| 2 | Kịch bản 1: chia ngẫu nhiên, có / không trùng lặp | `ids.train scenario1` | `results/scenario1.json`, `models/binary`, `models/deploy.json` | 30 phút |
| 3 | Random Forest đa lớp | `ids.train multiclass` | `results/multiclass.json`, `models/multiclass_rf` | 4 phút |
| 4 | Kịch bản 2: leave-one-day-out | `ids.train scenario2` | `results/scenario2.json` | 50 phút |
| 5 | Benchmark khả năng mở rộng | `ids.bench prepare`, `ids.bench all` | `results/scalability.json` | 1–2 giờ |
| 6 | Biểu đồ và bảng tổng hợp | `ids.figures` | `results/figures/`, `results/summary.md` | < 1 phút |

<sub>* Đo trên máy 12 core, 32 GB RAM, Docker 16 GB.</sub>

```bash
B="docker compose --profile batch run --rm batch python3 -m"
$B ids.etl --input data --output work/parquet --report results/quality_report.json
$B ids.train scenario1  --preset full --train-frac 0.3
$B ids.train multiclass --preset full --train-frac 0.3
$B ids.train scenario2  --preset fast --train-frac 0.2 --resume
$B ids.bench prepare
$B ids.bench all --cores 1,2,4,8 --fracs 0.25,0.5,1.0 --resume
$B ids.figures
```

Trên PowerShell, thay dòng gán `B=...` bằng cách gõ đầy đủ tiền tố `docker compose --profile batch run --rm batch python3 -m` trước mỗi lệnh.

**Tham số chính của `ids.train`**

| Tham số | Mặc định | Ý nghĩa |
|---|---|---|
| `--models` | `lr,dt,rf,gbt` | Các mô hình cần huấn luyện |
| `--preset` | `full` | `full` hoặc `fast`: số cây, độ sâu, số vòng lặp |
| `--train-frac` | `1.0` | Tỷ lệ lấy mẫu phân tầng **chỉ trên tập train**. Nhãn hiếm giữ nguyên; validation và test luôn đầy đủ |
| `--days` | tất cả | Kịch bản 2: danh sách ngày làm tập test |
| `--resume` | tắt | Kịch bản 2: bỏ qua các fold đã có kết quả |
| `--parquet`, `--results`, `--models-out`, `--work` | `work/parquet`, `results`, `models`, `work` | Thư mục vào và ra |
| `--seed` | `42` | Seed của phép chia theo hash |

Mỗi job ghi kết quả từng phần sau mỗi mô hình. Bộ nhớ driver chỉnh bằng biến `SPARK_DRIVER_MEMORY` (mặc định `10g`), ví dụ `run --rm -e SPARK_DRIVER_MEMORY=6g batch ...`.

**Chạy trên Google Colab**: dùng `notebooks/colab_batch.ipynb`. Notebook cài `pyspark==4.0.1` để mô hình sinh ra trên Colab tương thích với container.

**Kiểm thử nhanh trên tập con**: đặt 1 file CSV vào `data_mini/` (đã nằm trong `.gitignore`) rồi chuyển toàn bộ đầu ra sang `work/mini/`. Cả pipeline chạy khoảng 6 phút và không ghi đè `models/` hay `results/`.

```bash
$B ids.etl --input data_mini --output work/mini/parquet --report work/mini/results/quality_report.json
$B ids.train scenario1  --parquet work/mini/parquet --work work/mini --results work/mini/results --models-out work/mini/models --models lr,rf --preset fast
$B ids.train multiclass --parquet work/mini/parquet --work work/mini --results work/mini/results --models-out work/mini/models --preset fast
$B ids.figures --results work/mini/results
```

## Speed layer và serving layer

Job streaming nạp `models/deploy.json` khi khởi động. Sau khi huấn luyện lại, chạy `docker compose restart scorer` để áp dụng mô hình mới.

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

Script dưới đây chấm điểm cùng một ngày dữ liệu theo hai đường: Parquet của batch, và CSV dạng chuỗi giống dữ liệu đi qua Kafka. Hai tỷ lệ cảnh báo trùng nhau chứng tỏ không có train/serve skew.

```bash
docker compose --profile batch run --rm batch python3 scripts/check_skew.py --day 2018-02-14
```

## Xử lý sự cố

| Hiện tượng | Nguyên nhân và cách xử lý |
|---|---|
| Container thoát với mã 137, hoặc `OutOfMemoryError` | Thiếu bộ nhớ. Tăng RAM cho Docker hoặc giảm `SPARK_DRIVER_MEMORY` |
| `port is already allocated` | Cổng đang bị tiến trình khác dùng, ví dụ một MongoDB cài sẵn dưới dạng service. Dừng tiến trình đó hoặc đổi cổng trong `docker-compose.yml` |
| `scorer` khởi động lại liên tục | Thiếu `models/deploy.json` hoặc `models/binary/`. Chạy lại batch, hoặc khôi phục bằng `git checkout models` |
| Web báo scorer đang chờ dữ liệu | Đây là hành vi bình thường: Structured Streaming chỉ chạy micro-batch khi Kafka có bản ghi mới |
| Muốn reset speed layer | `docker compose down -v`, xoá `work/checkpoints/`, rồi `docker compose up -d` |
| Đường dẫn bị đổi khi dùng Git Bash | Đặt `export MSYS_NO_PATHCONV=1` |
