# Phát hiện xâm nhập mạng nhạy chi phí trên CSE-CIC-IDS2018 bằng Apache Spark và Kafka

Đồ án IE212. Hệ thống học máy phân tán trên Apache Spark phát hiện luồng mạng độc hại trong khoảng 16 triệu luồng của CSE-CIC-IDS2018. Ngưỡng cảnh báo được chọn theo **chi phí** (`Cost = c_FP·FP + c_FN·FN`) thay vì accuracy. Phần thời gian thực đi qua Kafka → Spark Structured Streaming → MongoDB, có trang web cho người vận hành.

```
                 ┌────────────────────── Batch layer (Google Colab hoặc Docker "batch") ──────────────────────┐
 10 CSV (6,9 GB) │ ids.etl: làm sạch, thống nhất schema, khử trùng lặp → Parquet theo ngày + quality_report  │
 ───────────────►│ ids.train: LR / DT / RF / GBT (nhị phân) + RF đa lớp, kịch bản 1 & 2, quét ngưỡng chi phí │──► models/ + results/
                 │ ids.bench: Spark vs pandas/scikit-learn trên 25/50/100% dữ liệu thật                        │
                 └─────────────────────────────────────────────────────────────────────────────────────────────┘
                 ┌──────────── Speed layer (Docker) ─────────────┐        ┌──────── Serving layer (Docker) ────────┐
 phát lại 1 ngày │ Kafka topic "flows" → Spark Structured        │        │ MongoDB: alerts, metrics (cửa sổ 10 s), │
 / file tải lên ►│ Streaming (1 master + 2 worker): nạp mô hình, │──────► │ upload_results, config (ngưỡng)          │
                 │ đọc ngưỡng từ MongoDB mỗi micro-batch         │        │ FastAPI + web: tổng quan, cảnh báo,     │
                 └───────────────────────────────────────────────┘        │ tải file, thực nghiệm, cấu hình ngưỡng  │
                                                                          └─────────────────────────────────────────┘
```

## Cấu trúc thư mục

| Đường dẫn | Nội dung |
|---|---|
| `src/ids/schema.py` | Tên cột chuẩn hoá, 15 nhãn gốc → 7 họ tấn công, danh sách cột định danh bị loại |
| `src/ids/etl.py` | CSV → Parquet (`clean/`, `dedup/` phân vùng theo `day`) + `results/quality_report.json` |
| `src/ids/features.py` | Các bước biến đổi nằm **bên trong** PipelineModel (điền giá trị lỗi, log có dấu, chuẩn hoá) |
| `src/ids/train.py` | Kịch bản 1, kịch bản 2, RF đa lớp, chọn và lưu mô hình triển khai |
| `src/ids/evaluate.py` | Histogram điểm 1000 bin → mọi chỉ số tại mọi ngưỡng, PR-AUC, chi phí, ngưỡng tối ưu |
| `src/ids/bench.py` | Benchmark khả năng mở rộng Spark vs pandas/scikit-learn |
| `src/ids/figures.py` | Biểu đồ PNG + `results/summary.md` cho báo cáo |
| `src/ids/stream_scorer.py` | Job Spark Structured Streaming: Kafka → chấm điểm → MongoDB |
| `web/app/` | FastAPI (`main.py`), producer Kafka (`producer.py`), giao diện (`static/`) |
| `notebooks/colab_batch.ipynb` | Chạy toàn bộ batch layer trên Google Colab |
| `docker-compose.yml`, `docker/spark/Dockerfile`, `web/Dockerfile` | Hạ tầng tái lập bằng Docker |
| `data/` | 10 file CSV (không đưa vào git, tải theo `data/README.md`) |
| `models/` | `binary/`, `multiclass_rf/`, `deploy.json`: mô hình RF đã huấn luyện sẵn, sinh ra từ batch layer, đủ để chạy demo ngay |
| `results/` | Kết quả tham chiếu: JSON, `figures/`, `summary.md` |
| `samples/sample_upload_bot.csv` | File mẫu để thử trang Phân tích file (Benign + Bot, có nhãn) |
| `scripts/check_skew.py` | Kiểm tra batch và streaming có cho cùng kết quả không (train/serve skew) |
| `HUONG_DAN_CHAY.md` | **Hướng dẫn chạy lại từng bước**, nên đọc trước |

> Mới bắt đầu? Đọc [HUONG_DAN_CHAY.md](HUONG_DAN_CHAY.md).

## Yêu cầu

* Docker Desktop (cấp ít nhất 8 GB RAM cho Docker; chạy batch cục bộ trên toàn bộ dữ liệu nên cấp 12–16 GB).
* Hoặc Google Colab cho batch layer (không cần cài gì trên máy).
* Phiên bản cố định: Spark 4.0.1 (Java 17, Python 3.10 trong container), Kafka 3.9.1, MongoDB 8.0. Colab cài `pyspark==4.0.1` để mô hình lưu ở Colab nạp được trong container.

## 1. Tải dữ liệu

Xem [data/README.md](data/README.md): tải 10 file CSV (6,9 GB) bằng AWS CLI, không cần tài khoản, hoặc từ thư mục Google Drive.

## 2. Batch layer

### Cách A: Google Colab

Mở [`notebooks/colab_batch.ipynb`](https://colab.research.google.com/github/phonguytin/ie212-ids2018/blob/main/notebooks/colab_batch.ipynb) trên Colab, rồi chạy lần lượt các ô.

- Notebook tự clone repo vào `MyDrive/ie212-ids2018`. Dữ liệu lấy từ `MyDrive/ie212_data` nếu đã có đủ 10 file, nếu không thì tự tải từ AWS.
- Kết quả và mô hình được ghi vào `colab_out/{results,models}`, tách riêng khỏi kết quả của bản Docker.
- Parquet được sao lưu ở `MyDrive/ie212_parquet` (khoảng 3,4 GB), nên khi bị ngắt phiên có thể chạy tiếp mà không phải làm lại ETL.
- Ô thiết lập tự tính bộ nhớ driver, số phân vùng shuffle và số core benchmark theo máy.

Notebook mặc định cấu hình cho Colab Pro High-RAM (8 vCPU, 50 GB RAM, khoảng 10 giờ): 100% tập train cho kịch bản 1 và đa lớp, 50% cho kịch bản 2. Trên Colab miễn phí (2 vCPU, khoảng 12 GB), đặt `S1_TRAIN_FRAC = 0.3` và `S2_TRAIN_FRAC = 0.2`. `--train-frac` lấy mẫu phân tầng **chỉ trên tập train**, giữ nguyên nhãn hiếm; validation và test luôn đầy đủ.

### Cách B: Docker trên máy (dịch vụ `batch`)

```powershell
docker compose build
```

```powershell
docker compose --profile batch run --rm batch python3 -m ids.etl --input data --output work/parquet --report results/quality_report.json
```

```powershell
docker compose --profile batch run --rm batch python3 -m ids.train scenario1 --preset full --train-frac 0.3
```

```powershell
docker compose --profile batch run --rm batch python3 -m ids.train multiclass --preset full --train-frac 0.3
```

```powershell
docker compose --profile batch run --rm batch python3 -m ids.train scenario2 --preset fast --train-frac 0.2 --resume
```

```powershell
docker compose --profile batch run --rm batch python3 -m ids.bench prepare
```

```powershell
docker compose --profile batch run --rm batch python3 -m ids.bench all --cores 1,2,4,8 --fracs 0.25,0.5,1.0 --resume
```

```powershell
docker compose --profile batch run --rm batch python3 -m ids.figures
```

Tham số chính của `ids.train`: `--models lr,dt,rf,gbt`, `--preset full|fast`, `--train-frac`, `--days 2018-02-14,...` (kịch bản 2), `--seed`. Mỗi kịch bản ghi kết quả từng phần sau mỗi mô hình, nên dừng giữa chừng vẫn giữ được phần đã xong (`--resume` cho kịch bản 2).

## 3. Speed layer + serving layer

Cần có `models/deploy.json`, `models/binary/` (và tuỳ chọn `models/multiclass_rf/`) từ bước 2.

```powershell
docker compose up -d --build
```

| Địa chỉ | Thành phần |
|---|---|
| http://localhost:8000 | Trang web demo |
| http://localhost:8080 | Spark master (thấy 2 worker và ứng dụng `ids-stream-scorer`) |
| http://localhost:4040 | Spark UI của job streaming (tab Structured Streaming) |
| localhost:29092 | Kafka (từ máy host), topic `flows` |
| localhost:27017 | MongoDB, database `ids` |

Xem log của job streaming:

```powershell
docker compose logs -f scorer
```

### Các trang web

* **Tổng quan**: chọn một ngày dữ liệu và tốc độ phát lại (luồng/giây) rồi bấm *Bắt đầu phát lại*. Trang hiển thị lưu lượng, cảnh báo, TP/FP/FN, precision/recall, chi phí cộng dồn, độ trễ, phân bố loại tấn công và cảnh báo mới nhất, tự làm mới mỗi 2 giây. Gợi ý: `Wednesday-14-02-2018` (FTP/SSH brute force ở khoảng 0–35% đầu file, phần sau là Benign) và `Friday-02-03-2018` (Botnet ở khoảng 0–35% đầu file). Bắt đầu từ 25–30% để thấy cả tấn công lẫn luồng bình thường.
* **Cảnh báo**: lọc theo loại, nguồn, TP/FP, xác suất; bấm một dòng để xem toàn bộ đặc trưng của luồng.
* **Phân tích file**: tải lên CSV định dạng CICFlowMeter; các dòng đi qua **chính** Kafka → Spark, trang theo dõi tiến độ và thống kê kết quả (có thêm TP/FP/FN nếu file có cột `Label`).
* **Thực nghiệm**: chất lượng dữ liệu, bảng kịch bản 1 và 2, recall từng loại tấn công, ma trận nhầm lẫn đa lớp, biểu đồ mở rộng. Thanh trượt tỷ lệ chi phí tính lại ngưỡng tối ưu cho mọi mô hình.
* **Cấu hình ngưỡng**: đổi c_FP, c_FN, ngưỡng; nút *Áp dụng ngưỡng tối ưu chi phí* tính ngưỡng từ histogram validation của mô hình đang triển khai. Job streaming đọc cấu hình ở mỗi micro-batch nên thay đổi có hiệu lực ngay.

Phát lại bằng dòng lệnh (không qua web):

```powershell
docker compose exec api python -m app.producer --file /data/Wednesday-14-02-2018_TrafficForML_CICFlowMeter.csv --rate 1000 --start-pct 25
```

## Phương pháp

**Làm sạch** (`ids.etl`)
* Đọc mọi cột dạng chuỗi, đổi tên về dạng chuẩn (`Flow Byts/s` → `flow_byts_s`), `unionByName` để gộp file ngày 20/02 (84 cột) với 9 file còn lại (80 cột).
* Đánh dấu và loại các dòng tiêu đề bị lặp giữa file (`Label == "Label"`).
* Ép kiểu double; dòng chứa NaN, ±Infinity hoặc chuỗi không phải số bị loại (thống kê theo cột trong báo cáo).
* Loại các cột định danh `flow_id, src_ip, src_port, dst_ip, dst_port, timestamp` để mô hình không học thuộc IP/cổng/khung giờ tấn công của mạng thí nghiệm. Loại thêm các cột hằng số.
* Khử trùng lặp trên toàn bộ đặc trưng + nhãn (`groupBy` có `dup_count`), đồng thời đếm các vector đặc trưng giống hệt nhau nhưng mang nhãn khác nhau (nhiễu nhãn, xem Liu et al. 2022).

**Mô hình** (`ids.train`): Logistic Regression, Decision Tree, Random Forest, GBT cho bài toán nhị phân; Random Forest cho 7 họ tấn công. Mỗi mô hình là một `PipelineModel` gồm `SQLTransformer` (điền 0 cho giá trị lỗi, `sign(x)·log(1+|x|)`), `VectorAssembler`, `StandardScaler` (chỉ cho LR, chỉ fit trên train) và bộ phân lớp. Vì phần tiền xử lý nằm trong mô hình nên batch và streaming dùng cùng một phép biến đổi. Trọng số lớp `N / (K·N_c)` tính trên tập train.

**Đánh giá**
* Kịch bản 1: chia train/val/test 70/15/15 theo hash của id dòng (cùng điểm cắt cho mọi nhãn nên phân tầng theo nhãn), chạy trên dữ liệu có trùng lặp và đã khử trùng lặp. Báo cáo thêm tỷ lệ dòng test có bản sao y hệt trong train.
* Kịch bản 2: leave-one-day-out trên dữ liệu đã khử trùng lặp. Mỗi ngày test được đánh dấu các họ tấn công chưa xuất hiện trong 9 ngày huấn luyện.
* Chỉ số: precision, recall, F1, PR-AUC, ROC-AUC, chi phí và recall/chi phí bỏ sót của từng loại tấn công.

**Tối ưu chi phí**: mỗi tập dự đoán được thu gọn trên Spark thành histogram P(tấn công) 1000 bin theo nhãn, từ đó tính FP/FN cho mọi ngưỡng `k/1000` cùng lúc. Ngưỡng tối ưu được chọn trên **validation** cho các tỷ lệ c_FP:c_FN = 1:10, 1:50, 1:100 rồi áp lên test và so với ngưỡng 0,5. Mô hình triển khai là mô hình có chi phí validation nhỏ nhất ở tỷ lệ 1:50 (kịch bản 1, dữ liệu khử trùng lặp).

**Khả năng mở rộng**: dữ liệu thật lấy mẫu lồng nhau 25% ⊂ 50% ⊂ 100% (không nhân bản dữ liệu giả). Mỗi lần đo chạy trong một tiến trình riêng, ghi thời gian và RAM đỉnh của cả cây tiến trình (gồm JVM). Lần chạy bị hệ điều hành kill được ghi `oom`, lần chạy quá giờ ghi `timeout`.

## Đối chiếu mục tiêu đề cương

| Mục tiêu | Thực hiện |
|---|---|
| Pipeline PySpark trên toàn bộ ~16 triệu luồng, làm sạch, khử trùng lặp, Parquet | `ids.etl` → `work/parquet/{clean,dedup}/day=*`, `results/quality_report.json` |
| So sánh 4 thuật toán MLlib theo chi phí | `ids.train scenario1/scenario2`, `results/scenario*.json`, trang Thực nghiệm |
| Tác động của chất lượng dữ liệu và cách chia | Kịch bản 1 (có / không trùng lặp), kịch bản 2 (theo ngày), báo cáo chất lượng |
| Khả năng mở rộng trên dữ liệu thật | `ids.bench`, `results/scalability.json`, `figures/scalability.png` |
| Kafka + Structured Streaming + MongoDB | `ids.stream_scorer`, `docker-compose.yml` |
| Web demo cho người vận hành | `web/app` (5 trang) |
| Tái lập bằng Docker | `docker compose up`, dịch vụ `batch` |

## Hạn chế và hướng phát triển

* Liu et al. (2022) chỉ ra lỗi gán nhãn và lỗi sinh đặc trưng trong CSE-CIC-IDS2018. Nhóm Infiltration có nhiều vector đặc trưng trùng với Benign (được đếm trong `conflicting_label_rows`), nên kết quả trên lớp này bị giới hạn bởi chất lượng nhãn, không phải bởi mô hình.
* Áp dụng cùng pipeline trên phiên bản đã sửa lỗi BCCC-CSE-CIC-IDS2018 (khoảng 46 triệu bản ghi, Shafi et al. 2025) để so sánh ảnh hưởng của chất lượng nhãn; thử nghiệm học sâu.

## Tài liệu tham khảo

1. Leevy, J. L., & Khoshgoftaar, T. M. (2020). A survey and analysis of intrusion detection models based on CSE-CIC-IDS2018 Big Data. *Journal of Big Data*, 7, 104. https://doi.org/10.1186/s40537-020-00382-x
2. Sharafaldin, I., Habibi Lashkari, A., & Ghorbani, A. A. (2018). Toward Generating a New Intrusion Detection Dataset and Intrusion Traffic Characterization. *ICISSP 2018*, 108–116. https://doi.org/10.5220/0006639801080116
3. Liu, L., Engelen, G., Lynar, T., Essam, D., & Joosen, W. (2022). Error Prevalence in NIDS datasets: A Case Study on CIC-IDS-2017 and CSE-CIC-IDS-2018. *IEEE CNS 2022*, 254–262. https://doi.org/10.1109/CNS56114.2022.9947235
4. Shafi, M., Habibi Lashkari, A., & Haghighian Roudsari, A. (2025). Toward Generating a Large Scale Intrusion Detection Dataset and Intruders Behavioral Profiling Using NTLFlowLyzer. *Journal of Network and Systems Management*, 33, 44. https://doi.org/10.1007/s10922-025-09917-0
