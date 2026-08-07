# Hướng dẫn Tháng 1–2: Data Pipeline "NYC Taxi" với Docker Compose + MinIO + Spark

Tài liệu này đi cùng project `nyc-taxi-pipeline/`. Mục tiêu không phải là "chạy được" mà là **hiểu vì sao từng bước cần thiết** — vì sau 2 tháng, khi bạn học streaming, dbt, Airflow, tất cả đều xây trên các khái niệm nền tảng ở đây.

Nhịp học: 1–2h/ngày, ~5 buổi/tuần. Guide chia theo 8 tuần (= 2 tháng). Đừng vội nhảy tuần — nếu tuần nào chưa "cảm" được khái niệm, cứ ở lại đó thêm vài buổi, tốc độ không quan trọng bằng việc hiểu đúng bản chất.

---

## 0. Kiến trúc chúng ta đang xây

```
[NYC TLC website]  --(download)-->  [MinIO: raw/]  --(Spark transform)-->  [MinIO: silver/]
     (nguồn thật)                    (bronze zone)                          (dữ liệu sạch)
```

Đây là **medallion architecture** thu nhỏ — mô hình phổ biến nhất trong data lakehouse hiện nay (Databricks, Microsoft Fabric, hầu hết công ty modern data stack đều dùng biến thể của nó):

- **Raw/Bronze** — dữ liệu gốc, y nguyên như khi nhận, không sửa. Đây là "bằng chứng gốc", nếu transform sau này sai, bạn luôn quay lại đây chạy lại được.
- **Silver** — đã làm sạch, đúng kiểu dữ liệu, loại bỏ rác. Đây là tầng mà hầu hết công việc phân tích thực sự dùng.
- **Gold** (tháng sau) — đã tổng hợp theo nghiệp vụ (ví dụ: doanh thu theo ngày/khu vực), tầng này dashboard sẽ đọc trực tiếp.

Tại sao không transform luôn 1 bước từ nguồn ra "sạch"? Vì tách tầng cho phép: (1) debug dễ hơn — biết lỗi ở bước ingest hay bước transform, (2) đổi logic transform mà không cần tải lại data nguồn, (3) nhiều consumer (dashboard, ML, báo cáo) có thể dùng lại cùng 1 tầng silver mà không phải viết lại logic clean data.

---

## 1. Chuẩn bị môi trường (buổi 1, ~30-45 phút)

Cần có: Docker Desktop (Windows/Mac) hoặc Docker Engine + Docker Compose plugin (Linux). Kiểm tra:

```bash
docker --version
docker compose version
```

Nếu chưa có, cài Docker Desktop theo hướng dẫn chính thức của Docker cho hệ điều hành của bạn.

**Vì sao dùng Docker Compose ngay từ đầu, thay vì cài MinIO/Spark trực tiếp lên máy?**
Ba lý do một senior sẽ luôn nói: (1) *reproducibility* — máy bạn 6 tháng nữa cài thêm bao nhiêu thứ, laptop khác cũng chạy được y hệt, không có kiểu "chạy được trên máy tui mà"; (2) *isolation* — bạn có thể xoá sạch (`docker compose down -v`) và làm lại từ đầu trong 2 phút nếu làm hỏng gì, không sợ "phá máy"; (3) *giống thật* — production thật sự cũng chạy dạng container (Kubernetes, ECS...), quen Docker Compose là bước đệm tự nhiên.

Clone/copy project vào máy, rồi vào thư mục:

```bash
cd nyc-taxi-pipeline
cat .env   # xem thử các biến môi trường đã cấu hình
```

---

## 2. Cấu trúc project — hiểu từng file trước khi chạy

```
nyc-taxi-pipeline/
├── docker-compose.yml     # khai báo các service: minio, minio-init, jobs
├── Dockerfile             # image dùng chung để chạy code Python/PySpark
├── requirements.txt       # thư viện Python cần cho image "jobs"
├── .env                   # biến môi trường (user/password MinIO, tên bucket)
├── ingestion/
│   └── download_to_raw.py # Tháng 1: tải data + đẩy vào raw zone
├── transform/
│   └── bronze_to_silver.py# Tháng 2: đọc raw, làm sạch bằng Spark, ghi silver
└── data/                  # (gitignore) nơi container mount ra để lưu file tạm
```

Mở từng file `docker-compose.yml`, `Dockerfile`, `ingestion/download_to_raw.py` và đọc phần comment trong đó **trước khi chạy** — comment được viết chi tiết chính là một phần của guide này, không phải phụ chú.

---

## Tuần 1: Dựng MinIO, hiểu Object Storage

### Buổi 1–2: Khởi động MinIO

```bash
docker compose up -d minio minio-init
docker compose ps
```

`minio-init` sẽ tự tạo 3 bucket (`raw`, `silver`, `gold`) rồi tự tắt (exit code 0) — đây là ví dụ đầu tiên về **infrastructure as code**: thay vì bấm chuột tạo bucket trên UI (không lặp lại được, không review được qua git), bạn khai báo bằng lệnh và chạy lại được ở bất kỳ máy nào.

Kiểm tra log của `minio-init` để chắc chắn 3 bucket được tạo:

```bash
docker compose logs minio-init
```

### Buổi 3: Khám phá MinIO Console

Mở browser vào `http://localhost:9001`, đăng nhập bằng `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` trong file `.env` (mặc định `minioadmin` / `minioadmin123`). Bạn sẽ thấy 3 bucket rỗng. Đây chính là giao diện tương đương AWS S3 Console — MinIO implement cùng API với S3 nên mọi thứ bạn học ở đây gần như convert 1:1 sang AWS/GCP/Azure thật sau này.

**Khái niệm cần nắm:** object storage khác filesystem thông thường ở chỗ nó không có "thư mục" thật — `year=2024/month=01/file.parquet` chỉ là một **key** (chuỗi string) có dấu `/`, MinIO/S3 hiển thị giống cây thư mục cho dễ nhìn nhưng bản chất là flat key-value store. Điều này giải thích tại sao object storage scale ngang tốt hơn filesystem truyền thống rất nhiều.

**Checkpoint tuần 1:** bạn giải thích được (không cần nhìn tài liệu) — object storage là gì, tại sao ta dùng nó làm raw/bronze zone thay vì Postgres, và bucket khác gì "ổ đĩa mạng".

---

## Tuần 2: Ingestion — đưa dữ liệu thật vào raw zone

### Buổi 1: Build image "jobs"

```bash
docker compose build jobs
```

Đọc `Dockerfile` trước khi build. Chú ý dòng cài `default-jdk-headless` — **PySpark không tự chạy được nếu không có Java**, vì Spark engine viết bằng Scala/Java, chạy trên JVM; PySpark chỉ là lớp Python gọi qua JVM đó bằng thư viện `py4j`. Đây là lỗi số 1 người mới học Spark gặp phải ("JAVA_HOME is not set") — bạn sẽ không gặp vì Dockerfile đã cài sẵn.

### Buổi 2: Đọc kỹ `ingestion/download_to_raw.py`

Đừng chạy ngay — đọc code trước, đặc biệt 3 khái niệm được ghi trong docstring đầu file:

1. **Immutable raw zone** — raw không bao giờ bị sửa/xoá.
2. **Idempotency** — script kiểm tra `head_object` trước khi tải lại, nên chạy lại nhiều lần không gây trùng dữ liệu.
3. **Hive-style partitioning** — path dạng `year=2024/month=01/` để sau này Spark/Presto đọc nhanh hơn (partition pruning).

### Buổi 3: Chạy ingestion lần đầu

```bash
docker compose run --rm jobs python ingestion/download_to_raw.py --year-months 2024-01
```

Theo dõi log: script sẽ tải file parquet ~40-50MB từ NYC TLC rồi upload lên MinIO. Sau khi xong, vào lại MinIO Console (`localhost:9001`), mở bucket `raw` → bạn sẽ thấy đường dẫn `yellow_tripdata/year=2024/month=01/yellow_tripdata_2024-01.parquet`.

> Lưu ý mạng: nếu công ty/mạng bạn chặn domain `d37ci6vzurychx.cloudfront.net`, thử mạng khác hoặc dùng VPN. Đây là domain CDN chính thức của NYC TLC, không phải link lạ.

### Buổi 4: Kiểm chứng tính idempotent

Chạy lại **đúng lệnh y như trên** lần thứ 2:

```bash
docker compose run --rm jobs python ingestion/download_to_raw.py --year-months 2024-01
```

Bạn sẽ thấy log in ra `SKIP (đã tồn tại...)` thay vì tải lại. Đây chính là hành vi bạn muốn khi 1 pipeline được chạy lại do retry/lỗi mạng/schedule chạy trùng — không tạo ra dữ liệu trùng lặp, không tốn băng thông tải lại vô ích. Thử thêm `--force` để hiểu flag ghi đè hoạt động thế nào.

### Buổi 5: Tải thêm nhiều tháng, làm quen thao tác thật

```bash
docker compose run --rm jobs python ingestion/download_to_raw.py --year-months 2024-02 2024-03 2024-04
```

**Checkpoint tuần 2:** bạn giải thích được idempotency là gì và tại sao nó quan trọng trong pipeline thật (nghĩ tới trường hợp Airflow retry task giữa đêm mà không ai giám sát).

---

## Tuần 3: Làm quen Spark — trước khi viết transform thật

Đừng vội nhảy vào file `bronze_to_silver.py` — trước tiên làm quen Spark qua shell tương tác để cảm nhận **lazy evaluation**, khái niệm quan trọng nhất của Spark.

### Buổi 1–2: Mở PySpark shell trong container

```bash
docker compose run --rm jobs pyspark
```

Trong shell, thử:

```python
df = spark.range(1, 1000000)          # KHÔNG có gì chạy thật ở đây
df2 = df.filter(df.id % 2 == 0)       # vẫn chưa chạy - chỉ là "kế hoạch"
df2.count()                            # ACTION - Spark MỚI THỰC SỰ chạy ở đây
```

Đây là **lazy evaluation**: các lệnh `.filter()`, `.select()`, `.withColumn()` gọi là **transformation** — Spark chỉ ghi nhận vào "execution plan" (giống một công thức toán), không chạy ngay. Chỉ khi gặp **action** (`.count()`, `.show()`, `.write()`, `.collect()`) Spark mới thực sự thực thi toàn bộ plan đã tích lũy. Lý do thiết kế này: Spark có thể **tối ưu hoá toàn bộ chuỗi lệnh** trước khi chạy (ví dụ gộp nhiều filter lại, đẩy filter xuống sớm hơn để đọc ít dữ liệu hơn — gọi là "predicate pushdown"), thay vì chạy máy móc từng lệnh một cách rời rạc.

Thử xem "execution plan" bằng:

```python
df2.explain()
```

Gõ `exit()` để rời shell.

### Buổi 3: Spark UI

Chạy lại với port mở ra ngoài để xem Spark UI:

```bash
docker compose run --rm --service-ports jobs pyspark
```

Trong lúc shell đang mở, vào browser `http://localhost:4040` — đây là giao diện giám sát Spark, hiển thị Jobs/Stages/Tasks. Khi bạn gọi 1 action (`count()`), thử refresh trang này, bạn sẽ thấy job vừa chạy xuất hiện. Đây chính là công cụ bạn sẽ dùng để debug performance khi Spark chạy chậm trong công việc thật sau này.

**Checkpoint tuần 3:** phân biệt được transformation vs action, giải thích tại sao `.filter()` "chạy" instant còn `.count()` mất vài giây.

---

## Tuần 4: Đọc hiểu file `transform/bronze_to_silver.py`

Dành cả tuần này chỉ để **đọc và hiểu**, chưa cần chạy vội. Đây là file quan trọng nhất trong 2 tháng, nên đọc kỹ docstring đầu file rồi đọc từng hàm.

Các điểm cần tự hỏi và trả lời được (nếu bí, đọc lại comment trong file):

- Tại sao script tải file từ MinIO xuống local trước khi cho Spark đọc, thay vì để Spark đọc trực tiếp `s3a://`? (gợi ý: đơn giản hoá để tránh vấn đề version JAR `hadoop-aws` — một cái hố kinh điển khi mới học Spark. Đây là quyết định đánh đổi có chủ đích, không phải "cách đúng duy nhất".)
- Vì sao filter `trip_distance > 0`, `fare_amount > 0`, `passenger_count > 0` được coi là "data quality" chứ không phải business logic?
- `.repartition("pickup_year", "pickup_month")` trước khi `.write partitionBy(...)` để làm gì? (gợi ý: nếu không repartition, Spark có thể tạo ra rất nhiều file nhỏ trong mỗi partition — vấn đề gọi là "small file problem", rất tốn khi query sau này).

**Checkpoint tuần 4:** vẽ lại (trên giấy hoặc note) toàn bộ luồng dữ liệu của file này từ đầu đến cuối, không nhìn code.

---

## Tuần 5: Chạy transform thật, quan sát kết quả

### Buổi 1: Chạy job

```bash
docker compose run --rm jobs python transform/bronze_to_silver.py
```

Đọc kỹ log theo 4 bước script in ra. Quan sát số dòng "trước" vs "sau" khi lọc — đây là con số bạn nên luôn log trong mọi transform job thật, vì nó là tín hiệu đầu tiên phát hiện data quality có vấn đề (ví dụ nếu tự nhiên tuần này bị loại 80% dữ liệu, phải có gì bất thường ở nguồn).

### Buổi 2: Kiểm tra kết quả trên MinIO Console

Vào bucket `silver`, bạn sẽ thấy cấu trúc:

```
yellow_tripdata_clean/pickup_year=2024/pickup_month=1/part-00000-....parquet
yellow_tripdata_clean/pickup_year=2024/pickup_month=2/part-00000-....parquet
...
```

Đây chính là Hive-style partitioning được Spark tự tạo ra từ `.partitionBy()`.

### Buổi 3: Đọc lại dữ liệu silver để xác nhận

```bash
docker compose run --rm jobs pyspark
```

```python
df = spark.read.parquet("/app/data/silver_local")
df.printSchema()
df.groupBy("pickup_year", "pickup_month").count().orderBy("pickup_year", "pickup_month").show()
```

So sánh số dòng mỗi tháng với log ở Buổi 1 để chắc chắn không có gì mất mát bất thường.

### Buổi 4–5: Chạy lại pipeline với nhiều tháng hơn

Quay lại `ingestion`, tải thêm vài tháng data (ví dụ 2023-11, 2023-12), rồi chạy lại transform. Quan sát: script transform hiện tại đọc **toàn bộ** raw zone mỗi lần chạy (không phải chỉ file mới) — đây là **full refresh** pattern, đơn giản nhưng không hiệu quả khi data lớn. Ghi chú lại câu hỏi này, vì tháng 3 khi học Airflow/incremental load, bạn sẽ quay lại cải tiến chính điểm này thành **incremental processing**.

**Checkpoint tuần 5:** tự tay đọc số liệu output và phát hiện được nếu có bất thường (ví dụ 1 tháng bị lọc mất quá nhiều dòng).

---

## Tuần 6: Data quality tự tay mở rộng (bài tập chủ động)

Đây là tuần bạn tự sửa code — cách học hiệu quả nhất là tự đụng tay, không chỉ chạy theo hướng dẫn.

Gợi ý bài tập (làm ít nhất 2 trong số này):

1. Thêm điều kiện lọc `trip_duration_minutes` quá dài (>180 phút) hoặc quá ngắn (<1 phút) — đây là loại "outlier" thường gặp trong dữ liệu GPS/taxi thật.
2. Thêm cột `is_weekend` (boolean) tính từ `pickup_date` bằng `F.dayofweek()`.
3. Thêm 1 bước ghi log ra file riêng (không chỉ print) liệt kê rõ **bao nhiêu dòng bị loại vì lý do gì** (tách riêng từng điều kiện filter, đếm delta), chứ không chỉ tổng số trước/sau. Đây chính là kỹ năng "observability" trong data quality mà pipeline production luôn cần.
4. Thử đổi `spark.sql.shuffle.partitions` (mặc định 200, quá nhiều cho dataset nhỏ của bạn) xuống ví dụ 8, rồi so sánh thời gian chạy qua Spark UI.

**Checkpoint tuần 6:** đã tự sửa code Spark ít nhất 1 lần và tự chạy lại thành công mà không cần copy nguyên từ hướng dẫn.

---

## Tuần 7: Bài tập nâng cao (tuỳ chọn nhưng rất đáng làm) — kết nối Spark trực tiếp với MinIO qua `s3a://`

Ở tháng 1-2 ta đã đơn giản hoá bằng cách tải file xuống local trước khi cho Spark đọc. Đây là lúc thử làm "đúng chuẩn production": để Spark đọc/viết trực tiếp `s3a://raw/...`.

Việc cần thêm vào `SparkSession.builder`:

```python
spark = (
    SparkSession.builder
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
```

Sau đó thay vì đọc từ `/app/data/bronze_local`, đọc trực tiếp `spark.read.parquet("s3a://raw/yellow_tripdata/")`.

**Cảnh báo trước:** đây là bài tập để bạn *trải nghiệm* sự phức tạp version-matching thật (giữa version Spark, Hadoop, hadoop-aws, aws-java-sdk-bundle phải khớp nhau — sai 1 version là lỗi khó hiểu). Nếu bạn loay hoay quá 1-2 buổi không ra, **quay lại pattern hiện tại** (tải local rồi xử lý) — đó vẫn là cách nhiều pipeline thật sự dùng khi làm việc với dataset không quá lớn, không có gì "sai" khi chọn đơn giản hoá.

**Checkpoint tuần 7:** hiểu được vì sao S3 connector cho Spark "khó" hơn tưởng — dù không bắt buộc phải làm được.

---

## Tuần 8: Tổng kết Tháng 1-2, dọn dẹp, viết lại bằng lời của bạn

### Buổi 1-2: Viết README cho project của chính bạn

Viết lại (không copy nguyên guide này) phần README ngắn giải thích: pipeline làm gì, chạy thế nào, kiến trúc ra sao. Đây là kỹ năng viết documentation — nhà tuyển dụng nhìn README của bạn trước khi nhìn code.

### Buổi 3: Đẩy code lên GitHub

```bash
git init
git add .
git commit -m "Month 1-2: ingestion + Spark transform pipeline for NYC taxi data"
git remote add origin <your-repo-url>
git push -u origin main
```

(`.gitignore` đã loại `.env` và `data/` sẵn — không commit credentials, dù chỉ là credentials local giả.)

### Buổi 4-5: Tự kiểm tra lại toàn bộ

Xoá hết và làm lại từ đầu để chắc chắn hướng dẫn (của bạn viết) đủ rõ để tự bạn 1 tháng nữa đọc lại vẫn hiểu:

```bash
docker compose down -v   # xoá container + volume, coi như máy sạch
docker compose up -d minio minio-init
docker compose build jobs
docker compose run --rm jobs python ingestion/download_to_raw.py --year-months 2024-01
docker compose run --rm jobs python transform/bronze_to_silver.py
```

Nếu chạy trôi chảy từ đầu đến cuối — chúc mừng, bạn đã hoàn thành nền tảng của 1 data pipeline thật.

---

## Xem trước Tháng 3

Tháng 3 chúng ta sẽ: (1) thêm Postgres làm data warehouse, thiết kế star schema (fact_trips + dimension theo ngày/khu vực), (2) học dbt để viết transformation-as-code, tầng gold sẽ được build bằng dbt models thay vì script Python thuần. Bạn không cần chuẩn bị gì trước — chỉ cần đảm bảo pipeline tháng 1-2 chạy vững.

---

## Troubleshooting thường gặp

**`docker compose up` báo lỗi port đã dùng (`port is already allocated`)** — máy bạn đang có service khác chiếm port 9000/9001. Đổi port map trong `docker-compose.yml`, ví dụ `"9010:9000"`.

**Lỗi `Cannot connect to the Docker daemon`** — Docker Desktop chưa mở. Mở app Docker Desktop rồi thử lại.

**Ingestion báo lỗi HTTP 403/404 khi tải file** — kiểm tra lại tháng bạn nhập có tồn tại chưa (NYC TLC có độ trễ công bố dữ liệu vài tháng, tháng quá gần hiện tại có thể chưa có). Thử tháng cũ hơn, ví dụ `2023-06`.

**PySpark báo lỗi liên quan Java/`JAVA_HOME`** — chỉ xảy ra nếu bạn chạy PySpark ngoài Docker (trực tiếp trên máy). Trong Docker, Java đã được cài sẵn trong `Dockerfile`. Nếu vẫn gặp, chạy `docker compose build jobs --no-cache` để build lại image.

**Spark chạy rất chậm dù dataset nhỏ** — bình thường ở lần chạy đầu (JVM khởi động, Spark có nhiều overhead cố định). Với dataset vài chục MB tới vài GB, pandas/DuckDB thực ra nhanh hơn Spark — điều đó không có nghĩa Spark "tệ", chỉ là Spark được thiết kế để scale tới dataset không thể chạy trên 1 máy, overhead đó là chi phí đổi lấy khả năng scale ngang.

---

## Một lưu ý về nguồn: đường dẫn download NYC TLC

Script dùng domain chính thức `https://d37ci6vzurychx.cloudfront.net/trip-data/`. Nếu domain này đổi trong tương lai (các cơ quan chính phủ đôi khi đổi hạ tầng), trang tham chiếu chính thức để lấy lại link mới là: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
