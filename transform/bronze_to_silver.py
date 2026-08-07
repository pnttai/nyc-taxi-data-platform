"""
Transform job - Buoc 2 cua pipeline (Thang 2): doc du lieu raw/bronze,
lam sach + chuan hoa, roi ghi ra silver zone. Day la buoc dung Spark thuc su.

Vi sao chia lam 2 buoc rieng (download roi moi Spark, khong doc S3 truc tiep)?
--------------------------------------------------------------------------
Ve nguyen tac production, Spark co the doc/viet truc tiep vao S3/MinIO qua
connector "s3a://" (thu vien hadoop-aws). Nhung connector nay doi hoi ghep
dung phien ban giua Spark - Hadoop - hadoop-aws - aws-java-sdk-bundle, va day
la mot trong nhung "hell" kinh dien nhat khi moi hoc Spark (JAR version
mismatch). De ban tap trung hoc BAN CHAT cua Spark (DataFrame, transform,
partition, lazy evaluation) truoc, o giai doan nay ta don gian hoa: dung
boto3 tai file tu MinIO xuong dia cuc bo, cho Spark xu ly nhu file local, roi
upload ket qua nguoc lai MinIO. Bai tap nang cao o cuoi GUIDE se huong dan
ban ket noi s3a:// truc tiep khi ban da vung concept.

Cac khai niem Spark quan trong duoc minh hoa trong file nay:

- SparkSession: "cong vao" duy nhat de lam viec voi Spark. Moi thu (doc file,
  chay SQL, tao DataFrame) deu bat dau tu day.

- Lazy evaluation: cac lenh nhu .filter(), .withColumn(), .select() KHONG
  chay ngay - Spark chi xay dung "execution plan" (giong 1 cong thuc). No chi
  THUC SU chay khi gap 1 "action" nhu .write(), .count(), .show(). Day la ly
  do ban se thay code chay "instant" cho toi dong .write() thi moi thay may
  chay.

- partitionBy() khi write: giong nhu partition Hive-style o buoc ingestion,
  giup query sau nay (vi du "lay du lieu ngay 2024-01-15") khong can quet
  toan bo dataset.
"""

import glob
import os
import shutil

import boto3
from botocore.client import Config
from pyspark.sql import SparkSession, functions as F

BRONZE_LOCAL_DIR = "/app/data/bronze_local"
SILVER_LOCAL_DIR = "/app/data/silver_local"
RAW_PREFIX = "yellow_tripdata/"


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def download_raw_from_minio(s3, bucket: str):
    """Tai toan bo object trong raw/yellow_tripdata/ xuong dia cuc bo de Spark doc."""
    os.makedirs(BRONZE_LOCAL_DIR, exist_ok=True)
    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=RAW_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            local_path = os.path.join(BRONZE_LOCAL_DIR, os.path.basename(key))
            if os.path.exists(local_path):
                continue
            print(f"  downloading s3://{bucket}/{key} -> {local_path}")
            s3.download_file(bucket, key, local_path)
            count += 1
    print(f"Da tai {count} file moi tu raw zone.")


def upload_silver_to_minio(s3, bucket: str):
    """Upload toan bo cay thu muc partition (year=.../month=.../*.parquet) len MinIO."""
    uploaded = 0
    for root, _, files in os.walk(SILVER_LOCAL_DIR):
        for f in files:
            if f.startswith("_") or f.startswith("."):
                continue  # bo qua file _SUCCESS, .crc cua Spark
            local_path = os.path.join(root, f)
            rel_path = os.path.relpath(local_path, SILVER_LOCAL_DIR)
            key = f"yellow_tripdata_clean/{rel_path}".replace(os.sep, "/")
            s3.upload_file(local_path, bucket, key)
            uploaded += 1
    print(f"Da upload {uploaded} file len s3://{bucket}/yellow_tripdata_clean/")


def main():
    raw_bucket = os.environ.get("MINIO_BUCKET_RAW", "raw")
    silver_bucket = os.environ.get("MINIO_BUCKET_SILVER", "silver")

    s3 = get_s3_client()

    print("=" * 60)
    print("BUOC 1/4: Tai raw data tu MinIO ve local de Spark doc")
    print("=" * 60)
    download_raw_from_minio(s3, raw_bucket)

    print("=" * 60)
    print("BUOC 2/4: Khoi tao SparkSession (local mode)")
    print("=" * 60)
    spark = (
        SparkSession.builder.appName("bronze_to_silver_yellow_taxi")
        .master("local[*]")  # dung toan bo CPU core cua may lam "worker" gia lap
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")  # bot log rac, chi giu warning/error

    print("=" * 60)
    print("BUOC 3/4: Doc + lam sach du lieu (transform)")
    print("=" * 60)
    files = glob.glob(os.path.join(BRONZE_LOCAL_DIR, "*.parquet"))
    if not files:
        print("Khong co file nao trong bronze zone. Hay chay ingestion script truoc.")
        spark.stop()
        return

    # .read.parquet() O DAY MOI CHI KHAI BAO, CHUA DOC THAT (lazy).
    df = spark.read.parquet(*files)
    print(f"Schema goc ({len(files)} file duoc gop lai thanh 1 DataFrame):")
    df.printSchema()

    clean = (
        df
        # loc bo ban ghi ro rang la du lieu ban/loi: trip_distance <= 0,
        # fare_amount am, passenger_count <= 0. Day la buoc "data quality"
        # co ban nhat ma moi pipeline production deu phai co.
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("passenger_count") > 0)
        .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
        # them cot tinh toan (derived column) - gia tri nay khong co trong
        # raw data, ta tu tinh o tang silver de tang gold/dashboard dung lai
        # ma khong phai tinh lai nhieu lan.
        .withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp("tpep_dropoff_datetime")
                - F.unix_timestamp("tpep_pickup_datetime")
            )
            / 60.0,
        )
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_year", F.year("tpep_pickup_datetime"))
        .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
    )

    before = df.count()   # ACTION dau tien -> Spark moi thuc su doc toan bo file
    after = clean.count()  # ACTION thu hai -> Spark thuc su chay lai filter
    print(f"So dong truoc khi lam sach: {before:,}")
    print(f"So dong sau khi lam sach:   {after:,}  (loai {before - after:,} dong ban/loi)")

    print("=" * 60)
    print("BUOC 4/4: Ghi ra silver zone, PARTITION BY nam/thang")
    print("=" * 60)
    if os.path.exists(SILVER_LOCAL_DIR):
        shutil.rmtree(SILVER_LOCAL_DIR)  # demo don gian: ghi lai tu dau moi lan chay

    (
        clean.repartition("pickup_year", "pickup_month")
        .write.mode("overwrite")
        .partitionBy("pickup_year", "pickup_month")
        .parquet(SILVER_LOCAL_DIR)
    )

    print("Upload ket qua silver zone len MinIO...")
    upload_silver_to_minio(s3, silver_bucket)

    spark.stop()
    print("Hoan tat transform bronze -> silver.")


if __name__ == "__main__":
    main()
